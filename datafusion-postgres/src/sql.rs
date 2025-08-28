use std::sync::Arc;

use datafusion::sql::sqlparser::ast::Expr;
use datafusion::sql::sqlparser::ast::Ident;
use datafusion::sql::sqlparser::ast::Select;
use datafusion::sql::sqlparser::ast::SelectItem;
use datafusion::sql::sqlparser::ast::SelectItemQualifiedWildcardKind;
use datafusion::sql::sqlparser::ast::SetExpr;
use datafusion::sql::sqlparser::ast::Statement;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alias_rewrite() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(AliasDuplicatedProjectionRewrite)];

        let sql = "SELECT n.oid, n.* FROM pg_catalog.pg_namespace n";
        let statement = parse(sql).expect("Failed to parse").remove(0);

        let statement = rewrite(statement, &rules);
        assert_eq!(
            statement.to_string(),
            "SELECT n.oid AS __alias_oid, n.* FROM pg_catalog.pg_namespace AS n"
        );

        let sql = "SELECT oid, * FROM pg_catalog.pg_namespace";
        let statement = parse(sql).expect("Failed to parse").remove(0);

        let statement = rewrite(statement, &rules);
        assert_eq!(
            statement.to_string(),
            "SELECT oid AS __alias_oid, * FROM pg_catalog.pg_namespace"
        );

        let sql = "SELECT t1.oid, t2.* FROM tbl1 AS t1 JOIN tbl2 AS t2 ON t1.id = t2.id";
        let statement = parse(sql).expect("Failed to parse").remove(0);

        let statement = rewrite(statement, &rules);
        assert_eq!(
            statement.to_string(),
            "SELECT t1.oid, t2.* FROM tbl1 AS t1 JOIN tbl2 AS t2 ON t1.id = t2.id"
        );
    }
}
