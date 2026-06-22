use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::Array;
use datafusion::sql::sqlparser::ast::ArrayElemTypeDef;
use datafusion::sql::sqlparser::ast::BinaryOperator;
use datafusion::sql::sqlparser::ast::CastKind;
use datafusion::sql::sqlparser::ast::DataType;
use datafusion::sql::sqlparser::ast::Expr;
use datafusion::sql::sqlparser::ast::Function;
use datafusion::sql::sqlparser::ast::FunctionArg;
use datafusion::sql::sqlparser::ast::FunctionArgExpr;
use datafusion::sql::sqlparser::ast::FunctionArgumentList;
use datafusion::sql::sqlparser::ast::FunctionArguments;
use datafusion::sql::sqlparser::ast::Ident;
use datafusion::sql::sqlparser::ast::LimitClause;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::ast::ObjectNamePart;
use datafusion::sql::sqlparser::ast::OrderByKind;
use datafusion::sql::sqlparser::ast::Query;
use datafusion::sql::sqlparser::ast::Select;
use datafusion::sql::sqlparser::ast::SelectItem;
use datafusion::sql::sqlparser::ast::SelectItemQualifiedWildcardKind;
use datafusion::sql::sqlparser::ast::SetExpr;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::ast::TableFunctionArgs;
use datafusion::sql::sqlparser::ast::TableWithJoins;
use datafusion::sql::sqlparser::ast::TypedString;
use datafusion::sql::sqlparser::ast::UnaryOperator;
use datafusion::sql::sqlparser::ast::Value;
use datafusion::sql::sqlparser::ast::ValueWithSpan;
use datafusion::sql::sqlparser::ast::VisitMut;
use datafusion::sql::sqlparser::ast::Visitor;
use datafusion::sql::sqlparser::ast::VisitorMut;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

pub trait SqlStatementRewriteRule: Send + Sync + Debug {
    fn rewrite(&self, s: Statement) -> Statement;
}

/// Rewrite rule for adding alias to duplicated projection
///
/// This rule is to deal with sql like `SELECT n.oid, n.* FROM n`, which is a
/// valid statement in postgres. But datafusion treat it as illegal because of
/// duplicated column oid in projection.
///
/// This rule will add alias to column, when there is a wildcard found in
/// projection.
#[derive(Debug)]
pub struct AliasDuplicatedProjectionRewrite;

impl AliasDuplicatedProjectionRewrite {
    // Rewrites a SELECT statement to alias explicit columns from the same table as a qualified wildcard.
    fn rewrite_select_with_alias(select: &mut Box<Select>) {
        // 1. Collect all table aliases from qualified wildcards.
        let mut wildcard_tables = Vec::new();
        let mut has_simple_wildcard = false;
        for p in &select.projection {
            match p {
                SelectItem::QualifiedWildcard(name, _) => match name {
                    SelectItemQualifiedWildcardKind::ObjectName(objname) => {
                        // for n.oid,
                        let idents = objname
                            .0
                            .iter()
                            .map(|v| v.as_ident().unwrap().value.clone())
                            .collect::<Vec<_>>()
                            .join(".");

                        wildcard_tables.push(idents);
                    }
                    SelectItemQualifiedWildcardKind::Expr(_expr) => {
                        // FIXME:
                    }
                },
                SelectItem::Wildcard(_) => {
                    has_simple_wildcard = true;
                }
                _ => {}
            }
        }

        // If there are no qualified wildcards, there's nothing to do.
        if wildcard_tables.is_empty() && !has_simple_wildcard {
            return;
        }

        // 2. Rewrite the projection, adding aliases to matching columns.
        let mut new_projection = vec![];
        for p in select.projection.drain(..) {
            match p {
                SelectItem::UnnamedExpr(expr) => {
                    let alias_partial = match &expr {
                        // Case for `oid` (unqualified identifier)
                        Expr::Identifier(ident) => Some(ident.clone()),
                        // Case for `n.oid` (compound identifier)
                        Expr::CompoundIdentifier(idents) => {
                            // compare every ident but the last
                            if idents.len() > 1 {
                                let table_name = &idents[..idents.len() - 1]
                                    .iter()
                                    .map(|i| i.value.clone())
                                    .collect::<Vec<_>>()
                                    .join(".");
                                if wildcard_tables.iter().any(|name| name == table_name) {
                                    Some(idents[idents.len() - 1].clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    if let Some(name) = alias_partial {
                        let alias = format!("__alias_{name}");
                        new_projection.push(SelectItem::ExprWithAlias {
                            expr,
                            alias: Ident::new(alias),
                        });
                    } else {
                        new_projection.push(SelectItem::UnnamedExpr(expr));
                    }
                }
                // Preserve existing aliases and wildcards.
                _ => new_projection.push(p),
            }
        }
        select.projection = new_projection;
    }
}

impl SqlStatementRewriteRule for AliasDuplicatedProjectionRewrite {
    fn rewrite(&self, mut statement: Statement) -> Statement {
        if let Statement::Query(query) = &mut statement
            && let SetExpr::Select(select) = query.body.as_mut()
        {
            Self::rewrite_select_with_alias(select);
        }

        statement
    }
}

/// Prepend qualifier for order by or filter when there is qualified wildcard
///
/// Postgres allows unqualified identifier in ORDER BY and FILTER but it's not
/// accepted by datafusion.
#[derive(Debug)]
pub struct ResolveUnqualifiedIdentifer;

impl ResolveUnqualifiedIdentifer {
    fn rewrite_unqualified_identifiers(query: &mut Box<Query>) {
        if let SetExpr::Select(select) = query.body.as_mut() {
            // Step 1: Find all table aliases from FROM and JOIN clauses.
            let table_aliases = Self::get_table_aliases(&select.from);

            // Step 2: Check for a single qualified wildcard in the projection.
            let qualified_wildcard_alias = Self::get_qualified_wildcard_alias(&select.projection);
            if qualified_wildcard_alias.is_none() || table_aliases.is_empty() {
                return; // Conditions not met.
            }

            let wildcard_alias = qualified_wildcard_alias.unwrap();

            // Step 2.5: Collect all projection aliases to avoid rewriting them
            let projection_aliases = Self::get_projection_aliases(&select.projection);

            // Step 3: Rewrite expressions in the WHERE and ORDER BY clauses.
            if let Some(selection) = &mut select.selection {
                Self::rewrite_expr(
                    selection,
                    &wildcard_alias,
                    &table_aliases,
                    &projection_aliases,
                );
            }

            if let Some(OrderByKind::Expressions(order_by_exprs)) =
                query.order_by.as_mut().map(|o| &mut o.kind)
            {
                for order_by_expr in order_by_exprs {
                    Self::rewrite_expr(
                        &mut order_by_expr.expr,
                        &wildcard_alias,
                        &table_aliases,
                        &projection_aliases,
                    );
                }
            }
        }
    }

    fn get_table_aliases(tables: &[TableWithJoins]) -> HashSet<String> {
        let mut aliases = HashSet::new();
        for table_with_joins in tables {
            if let TableFactor::Table {
                alias: Some(alias), ..
            } = &table_with_joins.relation
            {
                aliases.insert(alias.name.value.clone());
            }
            for join in &table_with_joins.joins {
                if let TableFactor::Table {
                    alias: Some(alias), ..
                } = &join.relation
                {
                    aliases.insert(alias.name.value.clone());
                }
            }
        }
        aliases
    }

    fn get_qualified_wildcard_alias(projection: &[SelectItem]) -> Option<String> {
        let mut qualified_wildcards = projection
            .iter()
            .filter_map(|item| {
                if let SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(objname),
                    _,
                ) = item
                {
                    Some(
                        objname
                            .0
                            .iter()
                            .map(|v| v.as_ident().unwrap().value.clone())
                            .collect::<Vec<_>>()
                            .join("."),
                    )
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if qualified_wildcards.len() == 1 {
            Some(qualified_wildcards.remove(0))
        } else {
            None
        }
    }

    fn get_projection_aliases(projection: &[SelectItem]) -> HashSet<String> {
        let mut aliases = HashSet::new();
        for item in projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    aliases.insert(alias.value.clone());
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    aliases.insert(ident.value.clone());
                }
                _ => {}
            }
        }
        aliases
    }

    fn rewrite_expr(
        expr: &mut Expr,
        wildcard_alias: &str,
        table_aliases: &HashSet<String>,
        projection_aliases: &HashSet<String>,
    ) {
        match expr {
            // If the identifier is not a table alias itself and not already aliased in projection, rewrite it.
            Expr::Identifier(ident)
                if !table_aliases.contains(&ident.value)
                    && !projection_aliases.contains(&ident.value) =>
            {
                *expr = Expr::CompoundIdentifier(vec![
                    Ident::new(wildcard_alias.to_string()),
                    ident.clone(),
                ]);
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::rewrite_expr(left, wildcard_alias, table_aliases, projection_aliases);
                Self::rewrite_expr(right, wildcard_alias, table_aliases, projection_aliases);
            }
            // Add more cases for other expression types as needed (e.g., `InList`, `Between`, etc.)
            _ => {}
        }
    }
}

impl SqlStatementRewriteRule for ResolveUnqualifiedIdentifer {
    fn rewrite(&self, mut statement: Statement) -> Statement {
        if let Statement::Query(query) = &mut statement {
            Self::rewrite_unqualified_identifiers(query);
        }

        statement
    }
}

/// Remove datafusion unsupported type annotations
/// it also removes pg_catalog as qualifier
#[derive(Debug)]
pub struct RemoveUnsupportedTypes {
    unsupported_types: HashSet<String>,
}

impl Default for RemoveUnsupportedTypes {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoveUnsupportedTypes {
    pub fn new() -> Self {
        let mut unsupported_types = HashSet::new();

        // NOTE: `regclass`, `regnamespace`, `regproc` and `regtype` are handled
        // for the *forward* (name->oid) direction by `RewriteRegCastToSubquery`,
        // which runs before this rule and rewrites those casts to lookup
        // subqueries. They remain in THIS set so the *reverse* direction --
        // `<oid-column>::regtype` (e.g. `prorettype::regtype::text` for display,
        // or `c.oid::regclass`) -- is stripped down to the bare oid column,
        // which is correct since the column already is an oid.
        for item in [
            "regclass",
            "regproc",
            "regtype",
            "regtype[]",
            "regnamespace",
            "oid",
        ] {
            unsupported_types.insert(item.to_owned());
            unsupported_types.insert(format!("pg_catalog.{item}"));
        }

        Self { unsupported_types }
    }
}

struct RemoveUnsupportedTypesVisitor<'a> {
    unsupported_types: &'a HashSet<String>,
}

impl VisitorMut for RemoveUnsupportedTypesVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            // This is the key part: identify constants with type annotations.
            Expr::TypedString(TypedString {
                data_type,
                value,
                uses_odbc_syntax: _,
            }) if self
                .unsupported_types
                .contains(data_type.to_string().to_lowercase().as_str()) =>
            {
                *expr = Expr::Value(Value::SingleQuotedString(value.to_string()).with_empty_span());
            }
            Expr::Cast {
                data_type,
                expr: value,
                ..
            } if self
                .unsupported_types
                .contains(data_type.to_string().to_lowercase().as_str()) =>
            {
                *expr = *value.clone();
            }

            // Add more match arms for other expression types (e.g., `Function`, `InList`) as needed.
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RemoveUnsupportedTypes {
    fn rewrite(&self, mut statement: Statement) -> Statement {
        let mut visitor = RemoveUnsupportedTypesVisitor {
            unsupported_types: &self.unsupported_types,
        };
        let _ = statement.visit(&mut visitor);
        statement
    }
}

/// Rewrite `::regclass` / `::regnamespace` / `::regtype` / `::regproc` casts
/// to a name->oid lookup subquery.
///
/// In postgres these are all `oid` (int4) alias types: casting a *name* to one
/// of them yields the matching catalog oid. DataFusion has no such types, so a
/// bare `'trigger'::regtype` would otherwise be stripped to the string
/// `'trigger'` and later crash an oid comparison with
/// "Cannot cast string to Int32". This rule instead rewrites the cast into a
/// `(SELECT oid FROM pg_catalog.<table> WHERE name = ...) ` subquery.
///
/// Two shapes are rewritten (and their `pg_catalog.`-qualified variants):
///   * `<name>::regTYPE::oid`
///   * `<name>::regTYPE` (bare cast)
///
/// # Direction guard
///
/// Only the **forward** direction -- name-to-oid -- is rewritten. We require
/// the cast operand to be a string literal (`'foo'`) or a placeholder (`$1`),
/// which is how clients spell "resolve this name to an oid". The **reverse**
/// direction, `<oid-column>::regtype` (e.g. `prorettype::regtype::text` for
/// display, or the left side of `prorettype::regtype != 'trigger'::regtype`),
/// has a column operand and is intentionally left untouched here; it is
/// stripped to the bare oid column by `RemoveUnsupportedTypes`, which is the
/// correct behavior since the column already *is* an oid.
#[derive(Debug)]
pub struct RewriteRegCastToSubquery(HashMap<String, Box<Query>>);

/// One oid-alias type plus the name->oid lookup query that resolves it. The
/// query must contain exactly one `$1` placeholder, into which the cast operand
/// (as text) is substituted.
struct RegCastSpec {
    /// Bare lowercased type name, e.g. `"regclass"`. Both `regclass` and
    /// `pg_catalog.regclass` are matched against this.
    type_name: &'static str,
    query: &'static str,
}

const REG_CAST_SPECS: &[RegCastSpec] = &[
    // regclass: schema-qualified relation name -> pg_class.oid
    RegCastSpec {
        type_name: "regclass",
        query: "SELECT c.oid
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p
WHERE n.nspname = COALESCE(
    CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END,
    current_schema()
)
AND c.relname = p.parts[-1]",
    },
    // regnamespace: schema name -> pg_namespace.oid (single identifier)
    RegCastSpec {
        type_name: "regnamespace",
        query: "SELECT n.oid
FROM pg_catalog.pg_namespace n
CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p
WHERE n.nspname = p.parts[-1]",
    },
    // regtype: schema-qualified type name -> pg_type.oid
    RegCastSpec {
        type_name: "regtype",
        query: "SELECT t.oid
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p
WHERE n.nspname = COALESCE(
    CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END,
    current_schema()
)
AND t.typname = p.parts[-1]",
    },
    // regproc: schema-qualified function name -> pg_proc.oid
    RegCastSpec {
        type_name: "regproc",
        query: "SELECT pr.oid
FROM pg_catalog.pg_proc pr
JOIN pg_catalog.pg_namespace n ON n.oid = pr.pronamespace
CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p
WHERE n.nspname = COALESCE(
    CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END,
    current_schema()
)
AND pr.proname = p.parts[-1]",
    },
];

impl Default for RewriteRegCastToSubquery {
    fn default() -> Self {
        Self::new()
    }
}

impl RewriteRegCastToSubquery {
    pub fn new() -> Self {
        let dialect = PostgreSqlDialect {};
        let mut queries = HashMap::new();
        for spec in REG_CAST_SPECS {
            let query = Parser::parse_sql(&dialect, spec.query)
                .map(|mut stmts| {
                    let stmt = stmts.remove(0);
                    if let Statement::Query(query) = stmt {
                        query
                    } else {
                        unreachable!()
                    }
                })
                .expect("Failed to parse prepared query");
            queries.insert(spec.type_name.to_owned(), query);
        }
        Self(queries)
    }
}

struct RewriteRegCastToSubqueryVisitor<'a>(&'a HashMap<String, Box<Query>>);

impl RewriteRegCastToSubqueryVisitor<'_> {
    /// Normalize a cast's `DataType` to the bare lowercased type name used as a
    /// key in [`REG_CAST_SPECS`], e.g. `pg_catalog.RegClass` -> `regclass`.
    fn normalize_type_name(data_type: &DataType) -> String {
        let lower = data_type.to_string().to_lowercase();
        lower
            .strip_prefix("pg_catalog.")
            .unwrap_or(&lower)
            .to_owned()
    }

    /// True when `expr` is the forward (name->oid) cast operand we rewrite:
    /// a string literal or a prepared-statement placeholder. Column operands
    /// (the reverse oid->regtype direction) are deliberately excluded.
    fn is_name_operand(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(_),
                ..
            }) | Expr::Value(ValueWithSpan {
                value: Value::Placeholder(_),
                ..
            })
        )
    }

    /// If `expr` is a single-quoted string whose contents are a base-10
    /// integer, return that integer as a SQL number literal.
    ///
    /// In Postgres, `'16417'::regclass` (a *numeric* string) resolves directly
    /// to the relation with that oid -- it does NOT do a name lookup. Emitting
    /// the literal oid here is both more correct *and* avoids embedding a
    /// `(SELECT ...)` subquery in contexts DataFusion can't decorrelate (e.g.
    /// `VALUES ('16417'::regclass)` inside an `IN (...)`).
    fn numeric_string_to_literal(expr: &Expr) -> Option<Expr> {
        if let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) = expr
            && s.bytes().all(|b| b.is_ascii_digit())
            && !s.is_empty()
        {
            // negative numbers can't be oids; the ascii_digit check above also
            // rejects signs, so this is always non-negative.
            return Some(Expr::Value(
                Value::Number(s.clone(), false).with_empty_span(),
            ));
        }
        None
    }

    /// Build `(SELECT oid FROM pg_catalog.<table> WHERE name = <expr-as-text>)`
    /// by cloning the type's query template and substituting `$1` with `expr`.
    fn create_subquery(template: &Query, expr: &Expr) -> Expr {
        struct PlaceholderReplacer(Expr);

        impl VisitorMut for PlaceholderReplacer {
            type Break = ();

            fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<Self::Break> {
                if let Expr::Value(ValueWithSpan {
                    value: Value::Placeholder(_placeholder),
                    ..
                }) = e
                {
                    *e = self.0.clone();
                }
                ControlFlow::Continue(())
            }
        }

        let mut query = template.clone();
        let mut replacer = PlaceholderReplacer(expr.clone());
        let _ = query.visit(&mut replacer);
        Expr::Subquery(Box::new(query))
    }
}

impl VisitorMut for RewriteRegCastToSubqueryVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Cast {
            kind,
            data_type,
            expr: outer_operand,
            ..
        } = expr
        else {
            return ControlFlow::Continue(());
        };
        if *kind != CastKind::DoubleColon {
            return ControlFlow::Continue(());
        }

        let outer_type = Self::normalize_type_name(data_type);

        // Pattern 1: `<name>::regTYPE::oid` -- the outer cast is to `oid` and
        // its operand is itself a `::regTYPE` cast of a name operand.
        if outer_type == "oid" {
            if let Expr::Cast {
                kind: inner_kind,
                data_type: inner_dt,
                expr: inner_operand,
                ..
            } = outer_operand.as_ref()
                && *inner_kind == CastKind::DoubleColon
                && self.0.contains_key(&Self::normalize_type_name(inner_dt))
                && Self::is_name_operand(inner_operand)
            {
                *expr = Self::rewrite_operand(
                    self.0
                        .get(&Self::normalize_type_name(inner_dt))
                        .map(|b| b.as_ref()),
                    inner_operand,
                );
            }
            return ControlFlow::Continue(());
        }

        // Pattern 2: `<name>::regTYPE` (bare cast).
        if self.0.contains_key(&outer_type) && Self::is_name_operand(outer_operand) {
            *expr =
                Self::rewrite_operand(self.0.get(&outer_type).map(|b| b.as_ref()), outer_operand);
        }

        ControlFlow::Continue(())
    }
}

impl RewriteRegCastToSubqueryVisitor<'_> {
    /// Rewrite a forward cast operand to either a literal oid (when it's a
    /// numeric string, matching Postgres) or the name->oid lookup subquery.
    fn rewrite_operand(template: Option<&Query>, operand: &Expr) -> Expr {
        if let Some(lit) = Self::numeric_string_to_literal(operand) {
            return lit;
        }
        Self::create_subquery(
            template.expect("template present for non-numeric operand"),
            operand,
        )
    }
}

impl SqlStatementRewriteRule for RewriteRegCastToSubquery {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RewriteRegCastToSubqueryVisitor(&self.0);
        let _ = s.visit(&mut visitor);
        s
    }
}

/// Cast `array_upper`/`array_lower` results to `bigint` when used as
/// `generate_series` arguments.
///
/// DataFusion's `generate_series` requires Int64 bounds, but our
/// `array_upper`/`array_lower` UDFs return Int32 (correct Postgres semantics --
/// Postgres `array_upper` returns `int4`). Postgres implicitly coerces int4 to
/// int8 for `generate_series`; DataFusion does not, so without this rule
/// `generate_series(1, array_upper(current_schemas(false), 1))` fails with
/// "Argument #2 must be an INTEGER". This rule performs the coercion the way
/// Postgres would, removing the need to blacklist such client queries.
#[derive(Debug)]
pub struct CastArrayBoundsForGenerateSeries;

struct CastArrayBoundsForGenerateSeriesVisitor;

impl CastArrayBoundsForGenerateSeriesVisitor {
    /// True when `expr` is a call to `array_upper`/`array_lower` (optionally
    /// `pg_catalog.`-qualified) -- the Int32-returning UDFs that collide with
    /// `generate_series`'s Int64 requirement.
    fn is_array_bounds_call(expr: &Expr) -> bool {
        if let Expr::Function(f) = expr
            && let Some(ObjectNamePart::Identifier(ident)) = f.name.0.last()
        {
            let v = ident.value.to_lowercase();
            return v == "array_upper" || v == "array_lower";
        }
        false
    }

    /// Wrap `expr` in `::bigint` if it's an array_bounds call.
    fn maybe_cast(expr: &mut Expr) {
        if Self::is_array_bounds_call(expr) {
            let inner = std::mem::replace(expr, Expr::Value(Value::Null.with_empty_span()));
            *expr = Expr::Cast {
                expr: Box::new(inner),
                kind: CastKind::DoubleColon,
                data_type: DataType::BigInt(None),
                array: false,
                format: None,
            };
        }
    }
}

impl VisitorMut for CastArrayBoundsForGenerateSeriesVisitor {
    type Break = ();

    fn pre_visit_table_factor(&mut self, tf: &mut TableFactor) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, args, .. } = tf
            && let Some(ObjectNamePart::Identifier(ident)) = name.0.last()
            && ident.value.to_lowercase() == "generate_series"
            && let Some(TableFunctionArgs {
                args: func_args, ..
            }) = args
        {
            for fa in func_args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = fa {
                    Self::maybe_cast(e);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for CastArrayBoundsForGenerateSeries {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = CastArrayBoundsForGenerateSeriesVisitor;
        let _ = s.visit(&mut visitor);
        s
    }
}

/// Rewrite Postgres's ANY operator to array_contains
#[derive(Debug)]
pub struct RewriteArrayAnyAllOperation;

struct RewriteArrayAnyAllOperationVisitor;

impl RewriteArrayAnyAllOperationVisitor {
    fn any_to_array_cofntains(&self, left: &Expr, right: &Expr) -> Expr {
        let array = if let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(array_literal),
            ..
        }) = right
        {
            let array_literal = array_literal.trim();
            if array_literal.starts_with('{') && array_literal.ends_with('}') {
                let items = array_literal.trim_matches(|c| c == '{' || c == '}' || c == ' ');
                let items = items.split(',').map(|s| s.trim()).filter(|s| !s.is_empty());

                // For now, we assume the data type is string
                let elems = items
                    .map(|s| {
                        Expr::Value(Value::SingleQuotedString(s.to_string()).with_empty_span())
                    })
                    .collect();
                Expr::Array(Array {
                    elem: elems,
                    named: true,
                })
            } else {
                right.clone()
            }
        } else {
            right.clone()
        };

        Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("array_contains")]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(array)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(left.clone())),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })
    }
}

impl VisitorMut for RewriteArrayAnyAllOperationVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => match compare_op {
                BinaryOperator::Eq => {
                    *expr = self.any_to_array_cofntains(left.as_ref(), right.as_ref());
                }
                BinaryOperator::NotEq => {
                    // TODO:left not equals to any element in array
                }
                _ => {}
            },
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => match compare_op {
                BinaryOperator::Eq => {
                    // TODO: left equals to every element in array
                }
                BinaryOperator::NotEq => {
                    *expr = Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: Box::new(self.any_to_array_cofntains(left.as_ref(), right.as_ref())),
                    }
                }
                _ => {}
            },
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RewriteArrayAnyAllOperation {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RewriteArrayAnyAllOperationVisitor;

        let _ = s.visit(&mut visitor);

        s
    }
}

/// Prepend qualifier to table_name
///
/// Postgres has pg_catalog in search_path by default so it allow access to
/// `pg_namespace` without `pg_catalog.` qualifier
#[derive(Debug)]
pub struct PrependUnqualifiedPgTableName;

struct PrependUnqualifiedPgTableNameVisitor;

impl VisitorMut for PrependUnqualifiedPgTableNameVisitor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, args, .. } = table_factor {
            // not a table function
            if args.is_none()
                && name.0.len() == 1
                && let ObjectNamePart::Identifier(ident) = &name.0[0]
                && ident.value.starts_with("pg_")
            {
                *name = ObjectName(vec![
                    ObjectNamePart::Identifier(Ident::new("pg_catalog")),
                    name.0[0].clone(),
                ]);
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for PrependUnqualifiedPgTableName {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = PrependUnqualifiedPgTableNameVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

#[derive(Debug)]
pub struct FixArrayLiteral;

struct FixArrayLiteralVisitor;

impl FixArrayLiteralVisitor {
    fn is_string_type(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Text | DataType::Varchar(_) | DataType::Char(_) | DataType::String(_)
        )
    }
}

impl VisitorMut for FixArrayLiteralVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Cast {
            kind,
            expr,
            data_type,
            ..
        } = expr
            && kind == &CastKind::DoubleColon
            && let DataType::Array(arr) = data_type
        {
            // cast some to
            if let Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(array_literal),
                ..
            }) = expr.as_ref()
            {
                let items = array_literal.trim_matches(|c| c == '{' || c == '}' || c == ' ');
                let items = items.split(',').map(|s| s.trim()).filter(|s| !s.is_empty());

                let is_text = match arr {
                    ArrayElemTypeDef::AngleBracket(dt) => Self::is_string_type(dt.as_ref()),
                    ArrayElemTypeDef::SquareBracket(dt, _) => Self::is_string_type(dt.as_ref()),
                    ArrayElemTypeDef::Parenthesis(dt) => Self::is_string_type(dt.as_ref()),
                    _ => false,
                };

                let elems = items
                    .map(|s| {
                        if is_text {
                            Expr::Value(Value::SingleQuotedString(s.to_string()).with_empty_span())
                        } else {
                            Expr::Value(Value::Number(s.to_string(), false).with_empty_span())
                        }
                    })
                    .collect();
                **expr = Expr::Array(Array {
                    elem: elems,
                    named: true,
                });
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for FixArrayLiteral {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = FixArrayLiteralVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Remove qualifier from unsupported items
///
/// This rewriter removes qualifier from following items:
/// 1. type cast: for example: `pg_catalog.text`
/// 2. function name: for example: `pg_catalog.array_to_string`,
/// 3. table function name
#[derive(Debug)]
pub struct RemoveQualifier;

struct RemoveQualifierVisitor;

impl VisitorMut for RemoveQualifierVisitor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        // remove table function qualifier
        if let TableFactor::Table { name, args, .. } = table_factor
            && args.is_some()
        {
            //  multiple idents in name, which means it's a qualified table name
            if name.0.len() > 1
                && let Some(last_ident) = name.0.pop()
            {
                *name = ObjectName(vec![last_ident]);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Cast { data_type, .. } => {
                // rewrite custom pg_catalog. qualified types
                let data_type_str = data_type.to_string();
                match data_type_str.as_str() {
                    "pg_catalog.text" => {
                        *data_type = DataType::Text;
                    }
                    "pg_catalog.int2[]" => {
                        *data_type = DataType::Array(ArrayElemTypeDef::SquareBracket(
                            Box::new(DataType::Int16),
                            None,
                        ));
                    }
                    _ => {}
                }
            }
            Expr::Function(function) => {
                // remove qualifier from pg_catalog.function
                let name = &mut function.name;
                if name.0.len() > 1
                    && let Some(last_ident) = name.0.pop()
                {
                    *name = ObjectName(vec![last_ident]);
                }
            }

            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RemoveQualifier {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RemoveQualifierVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Replace `current_user` with `session_user()`
#[derive(Debug)]
pub struct CurrentUserVariableToSessionUserFunctionCall;

struct CurrentUserVariableToSessionUserFunctionCallVisitor;

impl VisitorMut for CurrentUserVariableToSessionUserFunctionCallVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Identifier(ident) = expr
            && ident.quote_style.is_none()
            && ident.value.to_lowercase() == "current_user"
        {
            *expr = Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("session_user")]),
                args: FunctionArguments::None,
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            });
        }

        if let Expr::Function(func) = expr {
            let fname = func
                .name
                .0
                .iter()
                .map(|ident| ident.to_string())
                .collect::<Vec<String>>()
                .join(".");
            if fname.to_lowercase() == "current_user" {
                func.name = ObjectName::from(vec![Ident::new("session_user")])
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for CurrentUserVariableToSessionUserFunctionCall {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = CurrentUserVariableToSessionUserFunctionCallVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Fix collate and regex calls
#[derive(Debug)]
pub struct FixCollate;

struct FixCollateVisitor;

impl VisitorMut for FixCollateVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Collate { expr: inner, .. } => {
                *expr = inner.as_ref().clone();
            }
            Expr::BinaryOp { op, .. } => {
                if let BinaryOperator::PGCustomBinaryOperator(ops) = op
                    && *ops == ["pg_catalog", "~"]
                {
                    *op = BinaryOperator::PGRegexMatch;
                }
            }
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for FixCollate {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = FixCollateVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// A processor to replace unsupported subquery from projection with NULL.
///
/// It will also add `LIMIT 1` to supported subquery to ensure it returns scalar
/// value.
#[derive(Debug)]
pub struct RemoveSubqueryFromProjection;

struct RemoveSubqueryFromProjectionVisitor;

impl RemoveSubqueryFromProjectionVisitor {
    fn has_correlation(&self, query: &Query) -> bool {
        if let SetExpr::Select(select) = &*query.body {
            let table_aliases: HashSet<String> = select
                .from
                .iter()
                .flat_map(|twj| {
                    let mut aliases = HashSet::new();
                    Self::collect_table_aliases_from_table_factor(&twj.relation, &mut aliases);
                    for join in &twj.joins {
                        Self::collect_table_aliases_from_table_factor(&join.relation, &mut aliases);
                    }
                    aliases
                })
                .collect();

            let mut has_correlation = false;
            let mut visitor = CorrelationCheckVisitor(&mut has_correlation, &table_aliases);
            let _ = datafusion::logical_expr::sqlparser::ast::Visit::visit(query, &mut visitor);
            has_correlation
        } else {
            false
        }
    }

    fn has_limit(&self, query: &Query) -> bool {
        query.limit_clause.is_some() || query.fetch.is_some()
    }

    fn collect_table_aliases_from_table_factor(
        table_factor: &TableFactor,
        aliases: &mut HashSet<String>,
    ) {
        if let TableFactor::Table {
            alias: Some(alias), ..
        } = table_factor
        {
            aliases.insert(alias.name.value.clone());
        }
    }
}

struct CorrelationCheckVisitor<'a>(&'a mut bool, &'a HashSet<String>);

impl Visitor for CorrelationCheckVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(_placeholder),
                ..
            }) => {
                *self.0 = true;
            }
            Expr::CompoundIdentifier(idents) if !idents.is_empty() => {
                let table_name = &idents[0].value;
                if !self.1.contains(table_name) {
                    *self.0 = true;
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl VisitorMut for RemoveSubqueryFromProjectionVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            for projection in &mut select.projection {
                match projection {
                    SelectItem::UnnamedExpr(expr) => {
                        if let Expr::Subquery(subquery) = expr {
                            if self.has_correlation(subquery) {
                                *expr = Expr::Value(Value::Null.with_empty_span());
                            } else if !self.has_limit(subquery) {
                                subquery.limit_clause = Some(LimitClause::LimitOffset {
                                    limit: Some(Expr::Value(
                                        Value::Number("1".to_string(), false).with_empty_span(),
                                    )),
                                    offset: None,
                                    limit_by: vec![],
                                });
                            }
                        }
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        if let Expr::Subquery(subquery) = expr {
                            if self.has_correlation(subquery) {
                                *expr = Expr::Value(Value::Null.with_empty_span());
                            } else if !self.has_limit(subquery) {
                                subquery.limit_clause = Some(LimitClause::LimitOffset {
                                    limit: Some(Expr::Value(
                                        Value::Number("1".to_string(), false).with_empty_span(),
                                    )),
                                    offset: None,
                                    limit_by: vec![],
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RemoveSubqueryFromProjection {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RemoveSubqueryFromProjectionVisitor;
        let _ = s.visit(&mut visitor);

        s
    }
}

/// `select version()` should return column named `version` not `version()`
#[derive(Debug)]
pub struct FixVersionColumnName;

struct FixVersionColumnNameVisitor;

impl VisitorMut for FixVersionColumnNameVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            for projection in &mut select.projection {
                if let SelectItem::UnnamedExpr(Expr::Function(f)) = projection
                    && f.name.0.len() == 1
                    && let ObjectNamePart::Identifier(part) = &f.name.0[0]
                    && part.value == "version"
                    && let FunctionArguments::List(args) = &f.args
                    && args.args.is_empty()
                {
                    *projection = SelectItem::ExprWithAlias {
                        expr: Expr::Function(f.clone()),
                        alias: Ident::new("version"),
                    }
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for FixVersionColumnName {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = FixVersionColumnNameVisitor;
        let _ = s.visit(&mut visitor);

        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::sqlparser::parser::ParserError;
    use std::sync::Arc;

    fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let dialect = PostgreSqlDialect {};

        Parser::parse_sql(&dialect, sql)
    }

    fn rewrite(mut s: Statement, rules: &[Arc<dyn SqlStatementRewriteRule>]) -> Statement {
        for rule in rules {
            s = rule.rewrite(s);
        }

        s
    }

    macro_rules! assert_rewrite {
        ($rules:expr, $orig:expr, $rewt:expr) => {
            let sql = $orig;
            let statement = parse(sql).expect("Failed to parse").remove(0);

            let statement = rewrite(statement, $rules);
            assert_eq!(statement.to_string(), $rewt);
        };
    }

    #[test]
    fn test_alias_rewrite() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(AliasDuplicatedProjectionRewrite)];

        assert_rewrite!(
            &rules,
            "SELECT n.oid, n.* FROM pg_catalog.pg_namespace n",
            "SELECT n.oid AS __alias_oid, n.* FROM pg_catalog.pg_namespace n"
        );

        assert_rewrite!(
            &rules,
            "SELECT oid, * FROM pg_catalog.pg_namespace",
            "SELECT oid AS __alias_oid, * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT t1.oid, t2.* FROM tbl1 AS t1 JOIN tbl2 AS t2 ON t1.id = t2.id",
            "SELECT t1.oid, t2.* FROM tbl1 AS t1 JOIN tbl2 AS t2 ON t1.id = t2.id"
        );

        let sql = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace' ORDER BY nspsname";
        let statement = parse(sql).expect("Failed to parse").remove(0);

        let statement = rewrite(statement, &rules);
        assert_eq!(
            statement.to_string(),
            "SELECT n.oid AS __alias_oid, n.*, d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY nspsname"
        );
    }

    #[test]
    fn test_qualifier_prepend() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(ResolveUnqualifiedIdentifer)];

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE nspname = 'pg_catalog' ORDER BY nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname",
            "SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace' ORDER BY nspsname",
            "SELECT n.oid, n.*, d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY n.nspsname"
        );

        assert_rewrite!(
            &rules,
            "SELECT i.*,i.indkey as keys,c.relname,c.relnamespace,c.relam,c.reltablespace,tc.relname as tabrelname,dsc.description FROM pg_catalog.pg_index i INNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid INNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid WHERE i.indrelid=1 ORDER BY tabrelname, c.relname",
            "SELECT i.*, i.indkey AS keys, c.relname, c.relnamespace, c.relam, c.reltablespace, tc.relname AS tabrelname, dsc.description FROM pg_catalog.pg_index i INNER JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid INNER JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid = dsc.objoid WHERE i.indrelid = 1 ORDER BY tabrelname, c.relname"
        );
    }

    #[test]
    fn test_remove_unsupported_types() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![
            Arc::new(RemoveQualifier),
            Arc::new(RemoveUnsupportedTypes::new()),
        ];

        // NOTE: `::regclass` / `::regtype` / `::regproc` / `::regnamespace`
        // (forward, name->oid direction) used to be stripped to a bare string
        // here, but that produced wrong runtime types (they are oid/int4
        // aliases). They are now rewritten to name->oid lookup subqueries by
        // `RewriteRegCastToSubquery` -- see test_rewrite_reg_cast_to_subquery.
        // This rule still strips the *reverse* direction (`col::regtype`).

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
    FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
    LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
    WHERE c.oid = '16386'",
            "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::TEXT END, c.relpersistence, c.relreplident, am.amname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) WHERE c.oid = '16386'"
        );
    }

    #[test]
    fn test_rewrite_reg_cast_to_subquery() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        assert_rewrite!(
            &rules,
            "SELECT $1::regclass::oid",
            "SELECT (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND c.relname = p.parts[-1])"
        );

        assert_rewrite!(
            &rules,
            "SELECT $1::pg_catalog.regclass::oid",
            "SELECT (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND c.relname = p.parts[-1])"
        );

        assert_rewrite!(
            &rules,
            "SELECT $1::pg_catalog.regclass::pg_catalog.oid",
            "SELECT (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND c.relname = p.parts[-1])"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_class WHERE oid = 't'::pg_catalog.regclass::pg_catalog.oid",
            "SELECT * FROM pg_catalog.pg_class WHERE oid = (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace CROSS JOIN (SELECT parse_ident('t'::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND c.relname = p.parts[-1])"
        );

        // Bare `X::regclass` (no outer `::oid`) is rewritten the same way. In
        // postgres `regclass` is an oid (int4) alias type, so stripping it to
        // a bare string would later fail "Cannot cast string to Int32".
        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_class WHERE oid = 'pg_namespace'::regclass",
            "SELECT * FROM pg_catalog.pg_class WHERE oid = (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace CROSS JOIN (SELECT parse_ident('pg_namespace'::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND c.relname = p.parts[-1])"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_class WHERE oid = 't'::pg_catalog.regclass",
            "SELECT * FROM pg_catalog.pg_class WHERE oid = (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace CROSS JOIN (SELECT parse_ident('t'::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND c.relname = p.parts[-1])"
        );
    }

    #[test]
    fn test_rewrite_reg_cast_to_subquery_new_types() {
        // Forward (name->oid) casts for the other oid-alias types. Exact
        // expected strings are generated below from the same templates the
        // rule uses, so this test stays in sync if a template is tweaked.
        let regnamespace_sql = "SELECT n.oid FROM pg_catalog.pg_namespace n CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p WHERE n.nspname = p.parts[-1]";
        let regtype_sql = "SELECT t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND t.typname = p.parts[-1]";
        let regproc_sql = "SELECT pr.oid FROM pg_catalog.pg_proc pr JOIN pg_catalog.pg_namespace n ON n.oid = pr.pronamespace CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND pr.proname = p.parts[-1]";

        let expected = |template: &str, operand: &str| -> String {
            format!(
                "SELECT ({})",
                template.replace("$1::TEXT", &format!("{operand}::TEXT"))
            )
        };

        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        // regnamespace: literal and qualified
        assert_rewrite!(
            &rules,
            "SELECT 'public'::regnamespace",
            expected(regnamespace_sql, "'public'")
        );
        assert_rewrite!(
            &rules,
            "SELECT 'public'::pg_catalog.regnamespace::oid",
            expected(regnamespace_sql, "'public'")
        );

        // regtype: literal (this is the case that crashed pgcli with
        // "Cannot cast string 'trigger' to Int32" before this rule).
        assert_rewrite!(
            &rules,
            "SELECT 'trigger'::regtype",
            expected(regtype_sql, "'trigger'")
        );
        assert_rewrite!(
            &rules,
            "SELECT 'pg_catalog.int4'::regtype::oid",
            expected(regtype_sql, "'pg_catalog.int4'")
        );

        // regproc: literal and parameter operand
        assert_rewrite!(
            &rules,
            "SELECT 'now'::regproc",
            expected(regproc_sql, "'now'")
        );
        assert_rewrite!(
            &rules,
            "SELECT $1::regproc::oid",
            expected(regproc_sql, "$1")
        );
    }

    #[test]
    fn test_reg_cast_leaves_reverse_direction() {
        // The reverse direction (oid-column -> regtype, used for display and
        // for the left side of `col::regtype != 'lit'::regtype`) must NOT be
        // rewritten -- the column is already an oid. It is left untouched here
        // and stripped to the bare column later by RemoveUnsupportedTypes.
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        assert_rewrite!(
            &rules,
            "SELECT prorettype::regtype::text FROM pg_proc",
            "SELECT prorettype::regtype::TEXT FROM pg_proc"
        );
        assert_rewrite!(
            &rules,
            "SELECT atttypid::regtype FROM pg_attribute",
            "SELECT atttypid::regtype FROM pg_attribute"
        );
        assert_rewrite!(
            &rules,
            "SELECT relnamespace::regnamespace FROM pg_class",
            "SELECT relnamespace::regnamespace FROM pg_class"
        );
    }

    #[test]
    fn test_reg_cast_numeric_string_is_literal_oid() {
        // A *numeric* string operand resolves directly to that oid (Postgres
        // behavior), emitting a literal rather than a name-lookup subquery.
        // This also avoids embedding a subquery in contexts DataFusion can't
        // decorrelate (e.g. `VALUES ('16417'::regclass)` inside `IN (...)`).
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteRegCastToSubquery::new())];

        assert_rewrite!(&rules, "SELECT '16417'::regclass", "SELECT 16417");
        assert_rewrite!(
            &rules,
            "SELECT '16417'::pg_catalog.regclass::oid",
            "SELECT 16417"
        );
        assert_rewrite!(&rules, "SELECT '12345'::regtype", "SELECT 12345");
        assert_rewrite!(&rules, "SELECT '42'::regproc", "SELECT 42");
        assert_rewrite!(&rules, "SELECT '99'::regnamespace", "SELECT 99");

        // A *non-numeric* name operand still becomes a lookup subquery.
        assert_rewrite!(
            &rules,
            "SELECT 'pg_namespace'::regclass",
            "SELECT (SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace CROSS JOIN (SELECT parse_ident('pg_namespace'::TEXT) AS parts) p WHERE n.nspname = COALESCE(CASE WHEN array_length(p.parts, 1) > 1 THEN p.parts[1] END, current_schema()) AND c.relname = p.parts[-1])"
        );
    }

    #[test]
    fn test_cast_array_bounds_for_generate_series() {
        // array_upper/array_lower return Int32; generate_series wants Int64.
        // The rule wraps those calls in ::bigint when used as generate_series args.
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(CastArrayBoundsForGenerateSeries)];

        assert_rewrite!(
            &rules,
            "SELECT s.r FROM generate_series(1, array_upper(current_schemas(false), 1)) as s(r)",
            "SELECT s.r FROM generate_series(1, array_upper(current_schemas(false), 1)::BIGINT) AS s (r)"
        );
        assert_rewrite!(
            &rules,
            "SELECT s.r FROM generate_series(array_lower(x, 1), array_upper(x, 1)) as s(r)",
            "SELECT s.r FROM generate_series(array_lower(x, 1)::BIGINT, array_upper(x, 1)::BIGINT) AS s (r)"
        );

        // Non-array-bounds args are left alone (literal ints already work).
        assert_rewrite!(
            &rules,
            "SELECT s.r FROM generate_series(1, 10) as s(r)",
            "SELECT s.r FROM generate_series(1, 10) AS s (r)"
        );
        // array_upper used outside generate_series is NOT touched.
        assert_rewrite!(
            &rules,
            "SELECT array_upper(x, 1)",
            "SELECT array_upper(x, 1)"
        );
    }

    #[test]
    fn test_any_to_array_contains() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteArrayAnyAllOperation)];

        assert_rewrite!(
            &rules,
            "SELECT a = ANY(current_schemas(true))",
            "SELECT array_contains(current_schemas(true), a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a <> ALL(current_schemas(true))",
            "SELECT NOT array_contains(current_schemas(true), a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a = ANY('{r, l, e}')",
            "SELECT array_contains(ARRAY['r', 'l', 'e'], a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a FROM tbl WHERE a = ANY(current_schemas(true))",
            "SELECT a FROM tbl WHERE array_contains(current_schemas(true), a)"
        );
    }

    #[test]
    fn test_prepend_unqualified_table_name() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(PrependUnqualifiedPgTableName)];

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_namespace",
            "SELECT * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_namespace",
            "SELECT * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT typtype, typname, pg_type.oid FROM pg_catalog.pg_type LEFT JOIN pg_namespace as ns ON ns.oid = oid",
            "SELECT typtype, typname, pg_type.oid FROM pg_catalog.pg_type LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = oid"
        );
    }

    #[test]
    fn test_array_literal_fix() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(FixArrayLiteral)];

        assert_rewrite!(
            &rules,
            "SELECT '{a, abc}'::text[]",
            "SELECT ARRAY['a', 'abc']::TEXT[]"
        );

        assert_rewrite!(
            &rules,
            "SELECT '{1, 2}'::int[]",
            "SELECT ARRAY[1, 2]::INT[]"
        );

        assert_rewrite!(
            &rules,
            "SELECT '{t, f}'::bool[]",
            "SELECT ARRAY[t, f]::BOOL[]"
        );
    }

    #[test]
    fn test_remove_qualifier_from_table_function() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(RemoveQualifier)];

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_get_keywords()",
            "SELECT * FROM pg_get_keywords()"
        );
    }

    #[test]
    fn test_current_user() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(CurrentUserVariableToSessionUserFunctionCall)];

        assert_rewrite!(&rules, "SELECT current_user", "SELECT session_user");

        assert_rewrite!(&rules, "SELECT CURRENT_USER", "SELECT session_user");

        assert_rewrite!(
            &rules,
            "SELECT is_null(current_user)",
            "SELECT is_null(session_user)"
        );
    }

    #[test]
    fn test_collate_fix() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(FixCollate)];

        assert_rewrite!(
            &rules,
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(tablename)$' COLLATE pg_catalog.default AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3;",
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname ~ '^(tablename)$' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3"
        );
    }

    #[test]
    fn test_remove_subquery() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RemoveSubqueryFromProjection)];

        assert_rewrite!(
            &rules,
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation LIMIT 1) AS attcollation, a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a WHERE a.attrelid = '16384' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum;",
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), NULL, a.attnotnull, NULL AS attcollation, a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a WHERE a.attrelid = '16384' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        );
    }

    #[test]
    fn test_keep_simple_aggregated_subquery() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RemoveSubqueryFromProjection)];

        assert_rewrite!(
            &rules,
            "SELECT id, (SELECT COUNT(*) FROM pg_catalog.pg_attribute) AS attr_count FROM pg_catalog.pg_class",
            "SELECT id, (SELECT COUNT(*) FROM pg_catalog.pg_attribute LIMIT 1) AS attr_count FROM pg_catalog.pg_class"
        );
    }

    #[test]
    fn test_remove_correlated_subquery() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RemoveSubqueryFromProjection)];

        assert_rewrite!(
            &rules,
            "SELECT a.attname, (SELECT COUNT(*) FROM pg_catalog.pg_attribute WHERE attrelid = a.oid) AS count FROM pg_catalog.pg_attribute a",
            "SELECT a.attname, NULL AS count FROM pg_catalog.pg_attribute a"
        );
    }

    #[test]
    fn test_remove_non_aggregated_subquery() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RemoveSubqueryFromProjection)];

        assert_rewrite!(
            &rules,
            "SELECT id, (SELECT attname FROM pg_catalog.pg_attribute LIMIT 1) AS first_attr FROM pg_catalog.pg_class",
            "SELECT id, (SELECT attname FROM pg_catalog.pg_attribute LIMIT 1) AS first_attr FROM pg_catalog.pg_class"
        );
    }

    #[test]
    fn test_keep_simple_scalar_subquery() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RemoveSubqueryFromProjection)];

        assert_rewrite!(
            &rules,
            "SELECT (SELECT 1) AS constant",
            "SELECT (SELECT 1 LIMIT 1) AS constant"
        );

        assert_rewrite!(
            &rules,
            "SELECT (SELECT 'value') AS str_val",
            "SELECT (SELECT 'value' LIMIT 1) AS str_val"
        );
    }

    #[test]
    fn test_version_rewrite() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(FixVersionColumnName)];

        assert_rewrite!(&rules, "SELECT version()", "SELECT version() AS version");

        // Make sure we don't rewrite things we should leave alone
        assert_rewrite!(&rules, "SELECT version() as foo", "SELECT version() AS foo");
        assert_rewrite!(&rules, "SELECT version(foo)", "SELECT version(foo)");
        assert_rewrite!(&rules, "SELECT foo.version()", "SELECT foo.version()");
    }
}
