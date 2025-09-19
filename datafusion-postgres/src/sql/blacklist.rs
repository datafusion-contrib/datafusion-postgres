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

// psql \d <table> queries
    (
"SELECT pol.polname, pol.polpermissive,
          CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
          pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
          pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
          CASE pol.polcmd
            WHEN 'r' THEN 'SELECT'
            WHEN 'a' THEN 'INSERT'
            WHEN 'w' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            END AS cmd
        FROM pg_catalog.pg_policy pol
        WHERE pol.polrelid = '$1' ORDER BY 1;",
"SELECT
   NULL::TEXT AS polname,
   NULL::TEXT AS polpermissive,
   NULL::TEXT AS array_to_string,
   NULL::TEXT AS pg_get_expr_1,
   NULL::TEXT AS pg_get_expr_2,
   NULL::TEXT AS cmd
 WHERE false"
    ),

    (
"SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
        pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
          'd' = any(stxkind) AS ndist_enabled,
          'f' = any(stxkind) AS deps_enabled,
          'm' = any(stxkind) AS mcv_enabled,
        stxstattarget
        FROM pg_catalog.pg_statistic_ext
        WHERE stxrelid = '$1'
        ORDER BY nsp, stxname;",
"SELECT
   NULL::INT32 AS oid,
   NULL::TEXT AS stxrelid,
   NULL::TEXT AS nsp,
   NULL::TEXT AS stxname,
   NULL::TEXT AS columns,
   NULL::BOOLEAN AS ndist_enabled,
   NULL::BOOLEAN AS deps_enabled,
   NULL::BOOLEAN AS mcv_enabled,
   NULL::TEXT AS stxstattarget
 WHERE false"
    ),

    (
"SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid
             JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid
        WHERE pc.oid ='$1' and pg_catalog.pg_relation_is_publishable('$1')
        UNION
        SELECT pubname
             , pg_get_expr(pr.prqual, c.oid)
             , (CASE WHEN pr.prattrs IS NOT NULL THEN
                 (SELECT string_agg(attname, ', ')
                   FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,
                        pg_catalog.pg_attribute
                  WHERE attrelid = pr.prrelid AND attnum = prattrs[s])
                ELSE NULL END) FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
             JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
        WHERE pr.prrelid = '$1'
        UNION
        SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
        WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('$1')
        ORDER BY 1;",
"SELECT
   NULL::TEXT AS pubname,
   NULL::TEXT AS _1,
   NULL::TEXT AS _2,
 WHERE false"
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
