use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::sql::sqlparser::ast::Expr;
use datafusion::sql::sqlparser::ast::Ident;
use datafusion::sql::sqlparser::ast::OrderByKind;
use datafusion::sql::sqlparser::ast::Query;
use datafusion::sql::sqlparser::ast::Select;
use datafusion::sql::sqlparser::ast::SelectItem;
use datafusion::sql::sqlparser::ast::SelectItemQualifiedWildcardKind;
use datafusion::sql::sqlparser::ast::SetExpr;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::ast::TableWithJoins;
use datafusion::sql::sqlparser::ast::Value;
use datafusion::sql::sqlparser::ast::VisitMut;
use datafusion::sql::sqlparser::ast::VisitorMut;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::parser::ParserError;

pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};

    Parser::parse_sql(&dialect, sql)
}

pub fn rewrite(mut s: Statement, rules: &[Arc<dyn SqlStatementRewriteRule>]) -> Statement {
    for rule in rules {
        s = rule.rewrite(s);
    }

    s
}

pub trait SqlStatementRewriteRule: Send + Sync {
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
        if let Statement::Query(query) = &mut statement {
            if let SetExpr::Select(select) = query.body.as_mut() {
                Self::rewrite_select_with_alias(select);
            }
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

            // Step 3: Rewrite expressions in the WHERE and ORDER BY clauses.
            if let Some(selection) = &mut select.selection {
                Self::rewrite_expr(selection, &wildcard_alias, &table_aliases);
            }

            if let Some(OrderByKind::Expressions(order_by_exprs)) =
                query.order_by.as_mut().map(|o| &mut o.kind)
            {
                for order_by_expr in order_by_exprs {
                    Self::rewrite_expr(&mut order_by_expr.expr, &wildcard_alias, &table_aliases);
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

    fn rewrite_expr(expr: &mut Expr, wildcard_alias: &str, table_aliases: &HashSet<String>) {
        match expr {
            Expr::Identifier(ident) => {
                // If the identifier is not a table alias itself, rewrite it.
                if !table_aliases.contains(&ident.value) {
                    *expr = Expr::CompoundIdentifier(vec![
                        Ident::new(wildcard_alias.to_string()),
                        ident.clone(),
                    ]);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::rewrite_expr(left, wildcard_alias, table_aliases);
                Self::rewrite_expr(right, wildcard_alias, table_aliases);
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
#[derive(Debug)]
pub struct RemoveUnsupportedTypes {
    unsupported_types: HashSet<String>,
}

impl RemoveUnsupportedTypes {
    pub fn new() -> Self {
        let mut unsupported_types = HashSet::new();
        unsupported_types.insert("regclass".to_owned());

        Self { unsupported_types }
    }
}

struct RemoveUnsupportedTypesVisitor<'a> {
    unsupported_types: &'a HashSet<String>,
}

impl<'a> VisitorMut for RemoveUnsupportedTypesVisitor<'a> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            // This is the key part: identify constants with type annotations.
            Expr::TypedString { value, data_type } => {
                if self
                    .unsupported_types
                    .contains(data_type.to_string().to_lowercase().as_str())
                {
                    *expr =
                        Expr::Value(Value::SingleQuotedString(value.to_string()).with_empty_span());
                }
            }
            Expr::Cast {
                data_type,
                expr: value,
                ..
            } => {
                if self
                    .unsupported_types
                    .contains(data_type.to_string().to_lowercase().as_str())
                {
                    *expr = *value.clone();
                }
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

#[cfg(test)]
mod tests {
    use super::*;

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
            "SELECT n.oid AS __alias_oid, n.* FROM pg_catalog.pg_namespace AS n"
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
    }

    #[test]
    fn test_qualifier_prepend() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(ResolveUnqualifiedIdentifer)];

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE nspname = 'pg_catalog' ORDER BY nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace AS n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname",
            "SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace' ORDER BY nspsname",
            "SELECT n.oid, n.*, d.description FROM pg_catalog.pg_namespace AS n LEFT OUTER JOIN pg_catalog.pg_description AS d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY n.nspsname"
        );
    }

    #[test]
    fn test_remove_unsupported_types() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RemoveUnsupportedTypes::new())];

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pg_catalog'::regclass ORDER BY n.nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace AS n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.oid = 1 AND n.nspname = 'pg_catalog'::regclass ORDER BY n.nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace AS n WHERE n.oid = 1 AND n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname",
            "SELECT n.oid, n.*, d.description FROM pg_catalog.pg_namespace AS n LEFT OUTER JOIN pg_catalog.pg_description AS d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace AS n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );
    }
}
