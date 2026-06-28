//! A DataFusion [`TypePlanner`] that teaches the SQL planner to accept the
//! Postgres type names DataFusion otherwise rejects as "Unsupported SQL type":
//!
//! # oid-alias types
//!
//! `regclass`, `regproc`, `regtype`, `regnamespace`, `oid`, ... Each is mapped
//! to an `Int32` [`Field`] (oids are stored as int4) carrying the same
//! `pg.oid_alias` metadata the catalog columns use, so the kind survives into
//! the logical plan inside the resulting [`Expr::Cast`]'s `field`. The
//! oid-coercion analyzer rule then reads that metadata to resolve name strings
//! -> oids the way Postgres does -- no SQL/AST rewriting needed.
//!
//! This is the metadata-aware replacement for the former `RemoveOidTypeCast`
//! (SQL rewrite) + `RewriteRegCastToSubquery` (SQL rewrite) pair: instead of
//! stripping the cast at the AST layer (losing the type) and reconstructing it
//! later, the type is accepted up front and resolved at the analyzer layer.
//!
//! # `pg_catalog`-qualified builtins
//!
//! `pg_catalog.text`, `pg_catalog.int2`, `pg_catalog.int4`, ... sqlparser can
//! only represent a schema-qualified builtin as the catch-all `Custom` type,
//! which DataFusion rejects. This planner maps the canonical pg name to the
//! same Arrow type its unqualified builtin would produce, so casts like
//! `reloftype::pg_catalog.regtype::pg_catalog.text` plan cleanly. Arrays
//! (`pg_catalog.int2[]`) need no special handling: DataFusion recurses into
//! this planner for the array element type. This replaces the cast branch of
//! the former `RemoveQualifier` SQL rewrite.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::sql::sqlparser::ast::{DataType as SQLDataType, ObjectNamePart};

use crate::pg_catalog::oid_field::{self, OID_ALIAS_KEY, OID_ALIAS_TYPE_NAMES};

/// Recognize Postgres type names DataFusion rejects and map them to Arrow
/// types/metadata at planning time.
#[derive(Debug, Default)]
pub struct PgOidTypePlanner;

impl PgOidTypePlanner {
    /// Build the int4 oid field for a given kind (`regclass`, `oid`, ...).
    fn oid_field(kind: &str) -> Arc<Field> {
        Arc::new(Field::new("", DataType::Int32, true).with_metadata(
            std::collections::HashMap::from([(OID_ALIAS_KEY.to_string(), kind.to_string())]),
        ))
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

    /// Map a `pg_catalog.<name>` custom type to the Arrow [`DataType`] of its
    /// unqualified builtin.
    ///
    /// Returns `None` unless the type is exactly a two-part
    /// `pg_catalog.<builtin>` with no type arguments, or `<name>` is not a
    /// builtin we know about (in which case it is left for DataFusion to error
    /// on, or for another planner/rule to handle). The oid-alias arm in
    /// [`TypePlanner::plan_type_field`] runs first, so oid-alias names
    /// (`pg_catalog.regclass`, ...) never reach here.
    fn pg_catalog_builtin(sql_type: &SQLDataType) -> Option<DataType> {
        let SQLDataType::Custom(name, args) = sql_type else {
            return None;
        };
        if !args.is_empty() || name.0.len() != 2 {
            return None;
        }
        let mut parts = name.0.iter().filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.as_str()),
            _ => None,
        });
        let (schema, type_name) = match (parts.next(), parts.next()) {
            (Some(s), Some(t)) => (s, t),
            _ => return None,
        };
        if !schema.eq_ignore_ascii_case("pg_catalog") {
            return None;
        }
        builtin_arrow_type(type_name)
    }
}

/// Arrow [`DataType`] for a Postgres pg_catalog builtin type name, or `None`
/// if it is not one we map (a user-defined type, or a builtin whose parameters
/// we don't model).
///
/// String types map to `Utf8`, matching DataFusion's default conversion of the
/// unqualified `text`/`varchar`/`char` variants. The
/// `map_string_types_to_utf8view` session option is not consulted because the
/// [`TypePlanner`] trait does not expose session config; if a deployment needs
/// `Utf8View` it can register an additional planner.
fn builtin_arrow_type(name: &str) -> Option<DataType> {
    use DataType::*;
    let dt = match name.to_ascii_lowercase().as_str() {
        // booleans
        "bool" | "boolean" => Boolean,
        // integers
        "int2" | "smallint" => Int16,
        "int4" | "integer" | "int" => Int32,
        "int8" | "bigint" => Int64,
        // floats
        "float4" | "real" => Float32,
        "float8" => Float64,
        // strings -> Utf8 (DataFusion's default for unqualified Text/Varchar)
        "text" | "varchar" | "bpchar" | "char" | "name" => Utf8,
        // bytes
        "bytea" => Binary,
        _ => return None,
    };
    Some(dt)
}

impl TypePlanner for PgOidTypePlanner {
    fn plan_type_field(&self, sql_type: &SQLDataType) -> Result<Option<Arc<Field>>> {
        // 1. Scalar oid-alias types (regclass, oid, ...) -> int4 with kind
        //    metadata. Arrays of these (`regtype[]`) are handled by DataFusion
        //    recursing into this planner for the element type, so no Array arm
        //    is needed here.
        if let Some(kind) = Self::kind_for(sql_type) {
            return Ok(Some(Self::oid_field(&kind)));
        }
        // 2. pg_catalog-qualified builtins (pg_catalog.text, pg_catalog.int2,
        //    ...). sqlparser represents these as `Custom`, which DataFusion
        //    otherwise rejects; map them to their builtin Arrow type. As above,
        //    `pg_catalog.int2[]` works via element-type recursion.
        if let Some(dt) = Self::pg_catalog_builtin(sql_type) {
            return Ok(Some(Arc::new(Field::new("", dt, true))));
        }
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

    #[test]
    fn pg_catalog_builtins_map_to_arrow_types() {
        let planner = PgOidTypePlanner;
        for (sql, expected) in [
            ("pg_catalog.text", DataType::Utf8),
            ("pg_catalog.varchar", DataType::Utf8),
            ("pg_catalog.bpchar", DataType::Utf8),
            ("pg_catalog.name", DataType::Utf8),
            ("pg_catalog.bool", DataType::Boolean),
            ("pg_catalog.boolean", DataType::Boolean),
            ("pg_catalog.int2", DataType::Int16),
            ("pg_catalog.smallint", DataType::Int16),
            ("pg_catalog.int4", DataType::Int32),
            ("pg_catalog.int8", DataType::Int64),
            ("pg_catalog.bigint", DataType::Int64),
            ("pg_catalog.float4", DataType::Float32),
            ("pg_catalog.real", DataType::Float32),
            ("pg_catalog.float8", DataType::Float64),
            ("pg_catalog.bytea", DataType::Binary),
        ] {
            let dt = cast_target_type(&format!("SELECT 'x'::{sql} AS c"));
            let field = planner
                .plan_type_field(&dt)
                .unwrap()
                .unwrap_or_else(|| panic!("{sql} should be handled by the planner"));
            assert_eq!(field.data_type(), &expected, "{sql}");
            // No oid-alias metadata for plain builtins.
            assert!(
                field.metadata().get(OID_ALIAS_KEY).is_none(),
                "{sql} must not carry oid-alias metadata"
            );
        }
    }

    #[test]
    fn pg_catalog_builtin_rejects_non_builtins_and_unknown_schemas() {
        let planner = PgOidTypePlanner;

        // User-defined-looking custom type under pg_catalog is not mapped (left
        // for DataFusion to error on / another rule to handle).
        let dt = cast_target_type("SELECT 'x'::pg_catalog.some_udt AS c");
        assert_eq!(planner.plan_type_field(&dt).unwrap(), None);

        // A builtin name qualified by a different schema is not mapped either.
        let dt = cast_target_type("SELECT 'x'::public.text AS c");
        assert_eq!(planner.plan_type_field(&dt).unwrap(), None);

        // `pg_catalog.regclass` is an oid-alias: handled by the oid arm (Int32
        // + metadata), NOT by the builtin arm.
        let dt = cast_target_type("SELECT 'x'::pg_catalog.regclass AS c");
        let field = planner.plan_type_field(&dt).unwrap().unwrap();
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.metadata().get(OID_ALIAS_KEY).is_some());
    }

    #[test]
    fn pg_catalog_builtin_does_not_need_array_arm() {
        // `pg_catalog.int2[]` parses as Array(Custom(pg_catalog.int2)). The
        // planner only handles the scalar Custom element; DataFusion is the one
        // that recurses into the planner for the element and wraps it into a
        // list. So at the planner level, the Array itself returns None and only
        // its element resolves -- proving no Array arm is required here.
        let planner = PgOidTypePlanner;
        let dt = cast_target_type("SELECT 'x'::pg_catalog.int2[] AS c");
        assert_eq!(planner.plan_type_field(&dt).unwrap(), None);

        // ...but the element type alone resolves to Int16.
        let dt = cast_target_type("SELECT 'x'::pg_catalog.int2 AS c");
        let field = planner.plan_type_field(&dt).unwrap().unwrap();
        assert_eq!(field.data_type(), &DataType::Int16);
    }
}
