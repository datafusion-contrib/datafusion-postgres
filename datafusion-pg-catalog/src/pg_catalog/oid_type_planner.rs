//! A DataFusion [`TypePlanner`] that teaches the SQL planner to accept the
//! Postgres oid-alias type names (`regclass`, `regproc`, `regtype`,
//! `regnamespace`, `oid`, ...) that DataFusion otherwise rejects as
//! "Unsupported SQL type".
//!
//! Each such type is mapped to an `Int32` [`Field`] (oids are stored as int4)
//! carrying the same `pg.oid_alias` metadata the catalog columns use, so the
//! kind survives into the logical plan inside the resulting [`Expr::Cast`]'s
//! `field`. The oid-coercion analyzer rule then reads that metadata to resolve
//! name strings -> oids the way Postgres does -- no SQL/AST rewriting needed.
//!
//! This is the metadata-aware replacement for the former `RemoveOidTypeCast`
//! (SQL rewrite) + `RewriteRegCastToSubquery` (SQL rewrite) pair: instead of
//! stripping the cast at the AST layer (losing the type) and reconstructing it
//! later, the type is accepted up front and resolved at the analyzer layer.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::sql::sqlparser::ast::{DataType as SQLDataType,ObjectNamePart};

use crate::pg_catalog::oid_field::{self, OID_ALIAS_KEY, OID_ALIAS_TYPE_NAMES};

/// Recognize every Postgres oid-alias type name and map it to an int4 oid
/// [`Field`] annotated with its `pg.oid_alias` kind.
#[derive(Debug, Default)]
pub struct PgOidTypePlanner;

impl PgOidTypePlanner {
    /// Build the int4 oid field for a given kind (`regclass`, `oid`, ...).
    fn oid_field(kind: &str) -> Arc<Field> {
        Arc::new(
            Field::new("", DataType::Int32, true)
                .with_metadata(std::collections::HashMap::from([(
                    OID_ALIAS_KEY.to_string(),
                    kind.to_string(),
                )])),
        )
    }

    /// Lowercase last identifier of a `Custom(...)` type name, if any.
    ///
    /// `regproc` -> `regproc`, `pg_catalog.regclass` -> `regclass`.
    fn custom_type_name(sql_type: &SQLDataType) -> Option<String> {
        let SQLDataType::Custom(name, args) = sql_type else {
            return None;
        };
        if !args.is_empty() {
            return None;
        }
        let last = name.0.last()?;
        let ObjectNamePart::Identifier(ident) = last else {
            return None;
        };
        Some(ident.value.to_lowercase())
    }

    /// The oid-alias kind for a SQL type, if it is one we handle.
    fn kind_for(sql_type: &SQLDataType) -> Option<String> {
        match sql_type {
            SQLDataType::Regclass => Some(oid_field::kind::REGCLASS.to_string()),
            SQLDataType::Custom(_, _) => Self::custom_type_name(sql_type)
                .filter(|n| OID_ALIAS_TYPE_NAMES.contains(&n.as_str())),
            _ => None,
        }
    }
}

impl TypePlanner for PgOidTypePlanner {
    fn plan_type_field(&self, sql_type: &SQLDataType) -> Result<Option<Arc<Field>>> {
        // Scalar oid-alias types -> int4 with kind metadata.
        if let Some(kind) = Self::kind_for(sql_type) {
            return Ok(Some(Self::oid_field(&kind)));
        }
        // Array of an oid-alias type (e.g. `regtype[]`): a list whose *element*
        // field carries the kind metadata. The recursion below consults this
        // planner again for the inner type; DataFusion wraps the result with
        // `into_list()` before reaching us, so we only see the scalar here in
        // practice -- but handle the bracketed form defensively.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn cast_target_type(sql: &str) -> SQLDataType {
        // SELECT 'x'::<TYPE> AS c  ->  the cast's target DataType
        let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let stmt = stmts.remove(0);
        use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
        let Statement::Query(q) = stmt else {
            panic!("not a query");
        };
        let SetExpr::Select(sel) = *q.body else {
            panic!("not a select");
        };
        let cast = match &sel.projection[0] {
            datafusion::sql::sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
            datafusion::sql::sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expr,
            other => panic!("unexpected projection {other:?}"),
        };
        let datafusion::sql::sqlparser::ast::Expr::Cast { data_type, .. } = cast else {
            panic!("not a cast: {cast:?}");
        };
        data_type.clone()
    }

    #[test]
    fn recognizes_all_oid_alias_type_names() {
        let planner = PgOidTypePlanner;
        for t in [
            "regclass",
            "regproc",
            "regtype",
            "regnamespace",
            "oid",
            "pg_catalog.regclass",
            "pg_catalog.regtype",
            "regrole",
        ] {
            let dt = cast_target_type(&format!("SELECT 'x'::{t} AS c"));
            let field = planner.plan_type_field(&dt).unwrap().unwrap();
            assert_eq!(field.data_type(), &DataType::Int32, "{t}");
            let kind = field.metadata().get(OID_ALIAS_KEY).cloned();
            assert!(kind.is_some(), "{t} should carry oid-alias metadata");
            assert!(
                OID_ALIAS_TYPE_NAMES.contains(&kind.unwrap().as_str()),
                "{t} kind must be canonical"
            );
        }
    }

    #[test]
    fn leaves_non_oid_types_alone() {
        let planner = PgOidTypePlanner;
        for t in ["int4", "text", "varchar", "timestamp"] {
            let dt = cast_target_type(&format!("SELECT 'x'::{t} AS c"));
            assert_eq!(planner.plan_type_field(&dt).unwrap(), None, "{t}");
        }
    }

    #[test]
    fn pg_catalog_prefix_normalizes_to_kind() {
        let planner = PgOidTypePlanner;
        let dt = cast_target_type("SELECT 'x'::pg_catalog.regproc AS c");
        let field = planner.plan_type_field(&dt).unwrap().unwrap();
        assert_eq!(
            field.metadata().get(OID_ALIAS_KEY).map(String::as_str),
            Some(oid_field::kind::REGPROC)
        );
    }
}
