//! Analyzer rule that makes oid / oid-alias columns compare against strings
//! the way Postgres does.
//!
//! # Background
//! Postgres catalog columns such as `pg_class.relnamespace` or
//! `pg_attribute.atttypid` are object identifiers stored as `integer`
//! (Arrow `Int32`). Clients routinely compare them against *string* literals,
//! e.g. `WHERE relnamespace = 'public'` or `WHERE atttypid = '23'`, and
//! Postgres resolves the string to an oid (name lookup for names, direct for
//! numerics).
//!
//! DataFusion has no notion of "oid". Its type coercion turns
//! `Int32_col = Utf8('public')` into `CAST(col AS Utf8) = Utf8('public')`,
//! which silently matches nothing and returns an empty result.
//!
//! # Fix
//! Columns marked with the [`OID_ALIAS_KEY`](crate::pg_catalog::oid_field)
//! Field metadata are recognized by this rule. When such a column is compared
//! against a string literal, the string is resolved to an oid using Postgres
//! semantics:
//!
//! * a **numeric** string (`'16417'`) becomes a literal `Int32` oid;
//! * a **name** string (`'public'`) becomes a scalar subquery that looks the
//!   name up in the backing catalog table (`pg_namespace`, `pg_class`,
//!   `pg_proc`).
//!
//! The rule is order-independent: it handles both the pre-coercion form
//! `col = 'x'` and the post-coercion form `CAST(col AS Utf8) = 'x'`.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::common::Column;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{DFSchemaRef, ExprSchema, Result, ScalarValue, Spans};
use datafusion::datasource::{TableProvider, provider_as_source};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    BinaryExpr, Cast, Expr, LogicalPlan, LogicalPlanBuilder, Operator, Subquery, TryCast,
};
use datafusion::optimizer::analyzer::AnalyzerRule;

use crate::pg_catalog::oid_field::{self, OID_ALIAS_KEY};

/// Synchronous accessor for the pg_catalog tables backing name->oid lookups.
///
/// Implemented by `PgCatalogSchemaProvider`, which can build a table provider
/// without async I/O (dynamic tables defer data fetch to execution time via a
/// streaming source).
pub trait OidLookupProvider: Send + Sync + Debug {
    /// Build the named pg_catalog table as a [`TableProvider`], or `None` if it
    /// does not exist.
    fn build_table_provider(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;
}

/// Rewrite oid / oid-alias column comparisons against string literals to use
/// Postgres name/numeric oid resolution.
#[derive(Debug)]
pub struct OidStringCoercion {
    provider: Arc<dyn OidLookupProvider>,
}

impl OidStringCoercion {
    pub fn new(provider: Arc<dyn OidLookupProvider>) -> Self {
        Self { provider }
    }
}

impl AnalyzerRule for OidStringCoercion {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut rewriter = PlanRewriter {
            provider: &self.provider,
        };
        Ok(plan.rewrite(&mut rewriter)?.data)
    }

    fn name(&self) -> &str {
        "oid_string_coercion"
    }
}

/// Plan-level rewriter: visits every node top-down and rewrites `Filter`
/// predicates.
struct PlanRewriter<'a> {
    provider: &'a Arc<dyn OidLookupProvider>,
}

impl<'a> TreeNodeRewriter for PlanRewriter<'a> {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        // Rewrite every expression in every plan node (filters, join ON
        // filters, projections, ...) using that node's schema. The
        // schema-free cast arm (which carries its own kind via the
        // PgOidTypePlanner metadata) is what makes projections and
        // oid-column casts like `classoid = 'pg_namespace'::regclass` work;
        // the schema is only consulted by the bare `col = 'str'` path.
        let schema = plan.schema().clone();
        plan.map_expressions(|expr| {
            let mut rewriter = ExprOidRewriter {
                schema: schema.clone(),
                provider: self.provider,
            };
            expr.rewrite(&mut rewriter)
        })
    }
}

/// Expression-level rewriter: rewrites oid-alias comparisons inside one
/// predicate.
struct ExprOidRewriter<'a> {
    schema: DFSchemaRef,
    provider: &'a Arc<dyn OidLookupProvider>,
}

impl<'a> TreeNodeRewriter for ExprOidRewriter<'a> {
    type Node = Expr;

    fn f_down(&mut self, e: Expr) -> Result<Transformed<Expr>> {
        match rewrite_expr_shallow(&e, &self.schema, self.provider)? {
            Some(new_e) => Ok(Transformed::yes(new_e)),
            None => Ok(Transformed::no(e)),
        }
    }
}

/// Examine a single predicate expression and, if it is an oid-alias comparison
/// against a string, return the rewritten expression.
fn rewrite_expr_shallow(
    e: &Expr,
    schema: &DFSchemaRef,
    provider: &Arc<dyn OidLookupProvider>,
) -> Result<Option<Expr>> {
    match e {
        Expr::BinaryExpr(b) if matches!(b.op, Operator::Eq | Operator::NotEq) => {
            // Try both operand orders: `col = 'x'` and `'x' = col`.
            for (col_side, lit_side) in [(&b.left, &b.right), (&b.right, &b.left)] {
                let Some(col) = unwrap_column(col_side) else {
                    continue;
                };
                let Some(kind) = oid_kind(&col, schema) else {
                    continue;
                };
                let Some(s) = as_str_literal(lit_side) else {
                    continue;
                };
                if let Some(resolved) = resolve_operand(&kind, s, provider)? {
                    return Ok(Some(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(Expr::Column(col)),
                        op: b.op,
                        right: Box::new(resolved),
                    })));
                }
            }
            Ok(None)
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let Some(col) = unwrap_column(expr) else {
                return Ok(None);
            };
            let Some(kind) = oid_kind(&col, schema) else {
                return Ok(None);
            };

            // Resolve every string-literal entry; if any is a name we cannot
            // resolve (no backing table), leave the whole IN alone rather than
            // producing a half-coerced list.
            let mut new_list = Vec::with_capacity(list.len());
            let mut any_changed = false;
            for item in list {
                let Some(s) = as_str_literal(item) else {
                    new_list.push(item.clone());
                    continue;
                };
                match resolve_operand(&kind, s, provider)? {
                    Some(resolved) => {
                        any_changed = true;
                        new_list.push(resolved);
                    }
                    None => return Ok(None),
                }
            }
            if any_changed {
                Ok(Some(Expr::InList(InList {
                    expr: Box::new(Expr::Column(col)),
                    list: new_list,
                    negated: *negated,
                })))
            } else {
                Ok(None)
            }
        }
        // An explicit oid-alias cast whose kind is stamped on its target Field
        // by `PgOidTypePlanner` -- e.g. `'public'::regnamespace`,
        // `typinput = 'array_in'::regproc`, or `classoid = 'pg_namespace'::regclass`
        // (where `classoid` is a plain `oid`, so only the CAST declares the kind).
        // Resolve the cast operand to an oid (literal for numeric strings,
        // name-lookup subquery otherwise), in ANY plan position (this arm is
        // reached via `map_expressions`, so projections / filters / joins all
        // benefit). Returning `None` here for a non-string operand lets the
        // rewriter descend into nested casts (e.g. `'x'::regclass::oid`).
        Expr::Cast(Cast {
            expr: operand,
            field,
        })
        | Expr::TryCast(TryCast {
            expr: operand,
            field,
        }) if kind_from_field(field).is_some() => {
            let kind = kind_from_field(field).unwrap();
            if let Some(s) = immediate_str_literal(operand) {
                resolve_operand(&kind, s, provider)
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

/// If `e` is a `Column`, or a single-level `CAST`/`TRY_CAST` around a `Column`
/// (the post-coercion shape produced by DataFusion's type coercion), return
/// the inner [`Column`].
fn unwrap_column(e: &Expr) -> Option<Column> {
    match e {
        Expr::Column(c) => Some(c.clone()),
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => {
            match expr.as_ref() {
                Expr::Column(c) => Some(c.clone()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The oid-alias kind of `col` if it is annotated in the schema, else `None`.
fn oid_kind(col: &Column, schema: &DFSchemaRef) -> Option<String> {
    schema
        .field_from_column(col)
        .ok()?
        .metadata()
        .get(OID_ALIAS_KEY)
        .cloned()
}

/// The oid-alias kind stamped on a cast's target [`Field`] by the
/// `PgOidTypePlanner`, if any (e.g. `'x'::regclass` -> `"regclass"`).
fn kind_from_field(field: &datafusion::arrow::datatypes::Field) -> Option<String> {
    field.metadata().get(OID_ALIAS_KEY).cloned()
}

/// The string value of `e` if it is an *immediate* string literal (NOT peeking
/// through a cast), else `None`. Used to recognize the operand of an oid-alias
/// cast (`'name'::regclass`) without conflating it with a nested inner cast.
fn immediate_str_literal(e: &Expr) -> Option<&str> {
    if let Expr::Literal(s, _) = e {
        match s {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
            ScalarValue::Utf8View(Some(s)) => Some(s.as_str()),
            _ => None,
        }
    } else {
        None
    }
}

/// If `e` is a string literal -- possibly wrapped in a `CAST`/`TRY_CAST` to an
/// integer type that DataFusion's type-coercion analyzer produces when it sees
/// an `Int32` oid column compared against a string -- return its value;
/// otherwise `None`.
///
/// Peeking through the cast keeps this rule order-independent: whether our
/// rule runs before type coercion (bare `'str'`) or after it
/// (`CAST('str' AS Int32)`), the same string is recovered.
fn as_str_literal(e: &Expr) -> Option<&str> {
    let inner = match e {
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => expr.as_ref(),
        other => other,
    };
    if let Expr::Literal(s, _) = inner {
        match s {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
            ScalarValue::Utf8View(Some(s)) => Some(s.as_str()),
            _ => None,
        }
    } else {
        None
    }
}

/// The `(table, name_column)` used to resolve a name to an oid for a given
/// oid-alias kind. Returns `None` for kinds without a backing catalog table
/// (e.g. bare `oid`, or `regtype` where `pg_type` is unavailable) -- those only
/// support numeric resolution.
fn lookup_target(kind: &str) -> Option<(&'static str, &'static str)> {
    match kind {
        oid_field::kind::REGNAMESPACE => Some(("pg_namespace", "nspname")),
        oid_field::kind::REGCLASS => Some(("pg_class", "relname")),
        oid_field::kind::REGTYPE => Some(("pg_type", "typname")),
        oid_field::kind::REGPROC => Some(("pg_proc", "proname")),
        _ => None,
    }
}

/// Resolve a string operand to an oid-valued expression.
///
/// * numeric strings -> literal `Int32` (Postgres behavior for every
///   oid-alias type);
/// * names -> a scalar subquery `SELECT oid FROM <table> WHERE <namecol> = $s`
///   for kinds that have a backing table.
fn resolve_operand(
    kind: &str,
    s: &str,
    provider: &Arc<dyn OidLookupProvider>,
) -> Result<Option<Expr>> {
    // Numeric string -> literal oid (valid for every oid-alias kind).
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        return Ok(s
            .parse::<i32>()
            .ok()
            .map(|n| Expr::Literal(ScalarValue::Int32(Some(n)), None)));
    }

    // Name -> oid lookup subquery (only for kinds with a backing table).
    let Some((table, name_col)) = lookup_target(kind) else {
        return Ok(None);
    };
    let Some(provider) = provider.build_table_provider(table)? else {
        return Ok(None);
    };

    let source = provider_as_source(provider);
    let plan = LogicalPlanBuilder::scan(table.to_string(), source, None)?
        .filter(eq_str(name_col, s))?
        .project(vec![Expr::Column(Column::new_unqualified("oid"))])?
        .build()?;
    Ok(Some(Expr::ScalarSubquery(Subquery {
        subquery: Arc::new(plan),
        outer_ref_columns: vec![],
        spans: Spans::default(),
    })))
}

/// Build `<name_col> = '<s>'` against a freshly-scanned table.
fn eq_str(name_col: &str, s: &str) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::new_unqualified(name_col))),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn lookup_target_only_for_backed_kinds() {
        assert_eq!(
            lookup_target(oid_field::kind::REGNAMESPACE),
            Some(("pg_namespace", "nspname"))
        );
        assert_eq!(
            lookup_target(oid_field::kind::REGCLASS),
            Some(("pg_class", "relname"))
        );
        assert_eq!(
            lookup_target(oid_field::kind::REGPROC),
            Some(("pg_proc", "proname"))
        );
        // bare oid has no backing table -> name resolution unsupported
        assert_eq!(lookup_target(oid_field::kind::OID), None);
    }

    #[test]
    fn unwrap_column_handles_bare_and_cast_forms() {
        let bare = Expr::Column(Column::new_unqualified("relnamespace"));
        assert_eq!(
            unwrap_column(&bare).map(|c| c.name().to_string()),
            Some("relnamespace".to_string())
        );

        let cast = Expr::Cast(Cast {
            expr: Box::new(Expr::Column(Column::new_unqualified("relnamespace"))),
            field: std::sync::Arc::new(datafusion::arrow::datatypes::Field::new(
                "",
                DataType::Utf8,
                true,
            )),
        });
        assert_eq!(
            unwrap_column(&cast).map(|c| c.name().to_string()),
            Some("relnamespace".to_string())
        );

        let trycast = Expr::TryCast(TryCast {
            expr: Box::new(Expr::Column(Column::new_unqualified("oid"))),
            field: std::sync::Arc::new(datafusion::arrow::datatypes::Field::new(
                "",
                DataType::Utf8,
                true,
            )),
        });
        assert_eq!(
            unwrap_column(&trycast).map(|c| c.name().to_string()),
            Some("oid".to_string())
        );

        // a cast around something that isn't a column is not unwrapped
        let nested = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Int32(Some(1)), None)),
            field: std::sync::Arc::new(datafusion::arrow::datatypes::Field::new(
                "",
                DataType::Utf8,
                true,
            )),
        });
        assert_eq!(unwrap_column(&nested), None);
    }

    #[test]
    fn as_str_literal_recognizes_string_variants() {
        let u = Expr::Literal(ScalarValue::Utf8(Some("public".to_string())), None);
        let lu = Expr::Literal(ScalarValue::LargeUtf8(Some("public".to_string())), None);
        let uv = Expr::Literal(ScalarValue::Utf8View(Some("public".to_string())), None);
        let n = Expr::Literal(ScalarValue::Int32(Some(3)), None);
        assert_eq!(as_str_literal(&u), Some("public"));
        assert_eq!(as_str_literal(&lu), Some("public"));
        assert_eq!(as_str_literal(&uv), Some("public"));
        assert_eq!(as_str_literal(&n), None);

        // DataFusion's type-coercion analyzer wraps the literal in a cast to the
        // column's Int32 type (`col = CAST('public' AS Int32)`) when it runs
        // before this rule. The string must still be recoverable so the rule
        // stays order-independent.
        let cast = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("public".to_string())),
                None,
            )),
            field: std::sync::Arc::new(datafusion::arrow::datatypes::Field::new(
                "",
                DataType::Int32,
                true,
            )),
        });
        assert_eq!(as_str_literal(&cast), Some("public"));
    }

    // --- end-to-end: a real pg_catalog with the analyzer rule installed ---

    use crate::pg_catalog::context::EmptyContextProvider;
    use crate::pg_catalog::setup_pg_catalog;
    use datafusion::arrow::array::{StringArray, StringViewArray};
    use datafusion::prelude::SessionContext;

    /// Collect the single-column string values of a query into a sorted Vec.
    async fn utf8_column(ctx: &SessionContext, query: &str) -> Vec<String> {
        let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let col = b.column(0);
            let strs: Vec<Option<&str>> = match col.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect(),
                DataType::LargeUtf8 => col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::GenericStringArray<i64>>()
                    .unwrap()
                    .iter()
                    .collect(),
                DataType::Utf8View => col
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap()
                    .iter()
                    .collect(),
                other => panic!("expected string column, got {other:?}"),
            };
            for v in strs {
                out.push(v.unwrap().to_string());
            }
        }
        out.sort();
        out
    }

    async fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        setup_pg_catalog(&ctx, "datafusion", EmptyContextProvider).unwrap();
        ctx
    }

    /// Find a schema that actually has relations in `pg_class` and return
    /// `(oid_as_string, nspname)`. Data-driven so the test does not depend on
    /// any particular schema being present.
    async fn schema_with_relations(ctx: &SessionContext) -> (String, String) {
        let oids = utf8_column(
            ctx,
            "SELECT DISTINCT relnamespace::text FROM pg_catalog.pg_class ORDER BY 1 LIMIT 1",
        )
        .await;
        assert!(
            !oids.is_empty(),
            "pg_class should contain at least one relation"
        );
        let oid = oids[0].clone();
        let names = utf8_column(
            ctx,
            &format!("SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = {oid}"),
        )
        .await;
        assert_eq!(
            names.len(),
            1,
            "relnamespace oid should map to exactly one schema"
        );
        (oid, names[0].clone())
    }

    #[tokio::test]
    async fn name_string_resolves_like_postgres() {
        let ctx = make_ctx().await;
        let (oid, name) = schema_with_relations(&ctx).await;

        // Baseline: compare relnamespace against the schema oid as an int.
        let baseline = utf8_column(
            &ctx,
            &format!("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = {oid}"),
        )
        .await;
        assert!(
            !baseline.is_empty(),
            "baseline (int oid) should match relations"
        );

        // The headline case: compare against a NAME string. Without the rule
        // DataFusion silently returns 0 rows; with it, Postgres name->oid
        // resolution applies and the same relations match.
        let by_name = utf8_column(
            &ctx,
            &format!("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = '{name}'"),
        )
        .await;
        assert_eq!(
            by_name, baseline,
            "name-string resolution must match int-oid resolution (name={name}, oid={oid})"
        );
    }

    #[tokio::test]
    async fn numeric_string_resolves_to_literal_oid() {
        let ctx = make_ctx().await;
        let (oid, _name) = schema_with_relations(&ctx).await;
        let baseline = utf8_column(
            &ctx,
            &format!("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = {oid}"),
        )
        .await;
        let by_numeric_str = utf8_column(
            &ctx,
            &format!("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = '{oid}'"),
        )
        .await;
        assert_eq!(
            by_numeric_str, baseline,
            "numeric-string resolution must match int-oid resolution"
        );
    }

    #[tokio::test]
    async fn in_list_with_name_resolves() {
        let ctx = make_ctx().await;
        let (oid, name) = schema_with_relations(&ctx).await;
        let baseline = utf8_column(
            &ctx,
            &format!("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = {oid}"),
        )
        .await;
        let via_in = utf8_column(
            &ctx,
            &format!("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace IN ('{name}')"),
        )
        .await;
        assert_eq!(
            via_in, baseline,
            "IN ('{name}') must resolve the name the same as = {oid}"
        );
    }

    /// End-to-end proof that oid metadata embedded in the FEATHER files (not
    /// hand-annotated in the dynamic builder) reaches the analyzer rule.
    /// `pg_type` and `pg_proc` are both static tables loaded from feather;
    /// `pg_type.typinput` is annotated `regproc` purely via the export script.
    #[tokio::test]
    async fn feather_metadata_reaches_rule_for_static_tables() {
        let ctx = make_ctx().await;

        // Find a pg_type row whose typinput maps to a real pg_proc row, so we
        // have a concrete (name, oid) pair to test name resolution.
        let pairs = utf8_column(
            &ctx,
            "SELECT p.proname FROM pg_catalog.pg_proc p \
             JOIN pg_catalog.pg_type t ON t.typinput = p.oid \
             WHERE p.proname IS NOT NULL LIMIT 1",
        )
        .await;
        assert!(!pairs.is_empty(), "should find a pg_type/pg_proc join");
        let proc_name = &pairs[0];

        // Baseline: the int oid of that proc.
        let oid_rows = utf8_column(
            &ctx,
            &format!(
                "SELECT oid::text FROM pg_catalog.pg_proc WHERE proname = '{proc_name}' LIMIT 1"
            ),
        )
        .await;
        let oid = &oid_rows[0];

        // Baseline: pg_type rows whose typinput equals that oid (as int).
        let baseline = utf8_column(
            &ctx,
            &format!("SELECT typname FROM pg_catalog.pg_type WHERE typinput = {oid}"),
        )
        .await;

        // The headline: compare typinput (regproc, from feather metadata)
        // against the proc NAME string. Without the rule this returns 0 rows;
        // with it, the name resolves via pg_proc and matches the baseline.
        let by_name = utf8_column(
            &ctx,
            &format!("SELECT typname FROM pg_catalog.pg_type WHERE typinput = '{proc_name}'"),
        )
        .await;
        assert_eq!(
            by_name, baseline,
            "feather-annotated regproc column must resolve name strings via the rule"
        );
    }
}
