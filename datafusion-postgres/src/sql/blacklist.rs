use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::Statement;

use super::parse;
use super::SqlStatementRewriteRule;

const BLACKLIST_SQL_MAPPING: &[(&str, &str)] = &[
    // pgcli startup query
    (
"SELECT s_p.nspname AS parentschema,
                               t_p.relname AS parenttable,
                               unnest((
                                select
                                    array_agg(attname ORDER BY i)
                                from
                                    (select unnest(confkey) as attnum, generate_subscripts(confkey, 1) as i) x
                                    JOIN pg_catalog.pg_attribute c USING(attnum)
                                    WHERE c.attrelid = fk.confrelid
                                )) AS parentcolumn,
                               s_c.nspname AS childschema,
                               t_c.relname AS childtable,
                               unnest((
                                select
                                    array_agg(attname ORDER BY i)
                                from
                                    (select unnest(conkey) as attnum, generate_subscripts(conkey, 1) as i) x
                                    JOIN pg_catalog.pg_attribute c USING(attnum)
                                    WHERE c.attrelid = fk.conrelid
                                )) AS childcolumn
                        FROM pg_catalog.pg_constraint fk
                        JOIN pg_catalog.pg_class      t_p ON t_p.oid = fk.confrelid
                        JOIN pg_catalog.pg_namespace  s_p ON s_p.oid = t_p.relnamespace
                        JOIN pg_catalog.pg_class      t_c ON t_c.oid = fk.conrelid
                        JOIN pg_catalog.pg_namespace  s_c ON s_c.oid = t_c.relnamespace
                        WHERE fk.contype = 'f'",
"SELECT
   NULL::TEXT AS parentschema,
   NULL::TEXT AS parenttable,
   NULL::TEXT AS parentcolumn,
   NULL::TEXT AS childschema,
   NULL::TEXT AS childtable,
   NULL::TEXT AS childcolumn
 WHERE false"),

    // pgcli startup query
    (
"SELECT n.nspname schema_name,
                                       t.typname type_name
                                FROM   pg_catalog.pg_type t
                                       INNER JOIN pg_catalog.pg_namespace n
                                          ON n.oid = t.typnamespace
                                WHERE ( t.typrelid = 0  -- non-composite types
                                        OR (  -- composite type, but not a table
                                              SELECT c.relkind = 'c'
                                              FROM pg_catalog.pg_class c
                                              WHERE c.oid = t.typrelid
                                            )
                                      )
                                      AND NOT EXISTS( -- ignore array types
                                            SELECT  1
                                            FROM    pg_catalog.pg_type el
                                            WHERE   el.oid = t.typelem AND el.typarray = t.oid
                                          )
                                      AND n.nspname <> 'pg_catalog'
                                      AND n.nspname <> 'information_schema'
                                ORDER BY 1, 2;",
"SELECT NULL::TEXT AS schema_name, NULL::TEXT AS type_name WHERE false"
    ),


];

/// A blacklist based sql rewrite, when the input matches, return the output
///
/// This rewriter is for those complex but meaningless queries we won't spend
/// effort to rewrite to datafusion supported version in near future.
#[derive(Debug)]
pub struct BlacklistSqlRewriter(HashMap<Statement, Statement>);

impl SqlStatementRewriteRule for BlacklistSqlRewriter {
    fn rewrite(&self, mut s: Statement) -> Statement {
        if let Some(stmt) = self.0.get(&s) {
            s = stmt.clone();
        }

        s
    }
}

impl BlacklistSqlRewriter {
    pub(crate) fn new() -> BlacklistSqlRewriter {
        let mut mapping = HashMap::new();

        for (sql_from, sql_to) in BLACKLIST_SQL_MAPPING {
            mapping.insert(
                parse(sql_from).unwrap().remove(0),
                parse(sql_to).unwrap().remove(0),
            );
        }

        Self(mapping)
    }
}
