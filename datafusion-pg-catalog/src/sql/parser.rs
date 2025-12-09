use std::sync::Arc;

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::Token;
use datafusion::sql::sqlparser::tokenizer::TokenWithSpan;

use super::rules::AliasDuplicatedProjectionRewrite;
use super::rules::CurrentUserVariableToSessionUserFunctionCall;
use super::rules::FixArrayLiteral;
use super::rules::FixCollate;
use super::rules::FixVersionColumnName;
use super::rules::PrependUnqualifiedPgTableName;
use super::rules::RemoveQualifier;
use super::rules::RemoveSubqueryFromProjection;
use super::rules::RemoveUnsupportedTypes;
use super::rules::ResolveUnqualifiedIdentifer;
use super::rules::RewriteArrayAnyAllOperation;
use super::rules::SqlStatementRewriteRule;

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
        WHERE pol.polrelid = $1 ORDER BY 1;",
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
        WHERE stxrelid = $1
        ORDER BY nsp, stxname;",
"SELECT
   NULL::INT AS oid,
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
        WHERE pc.oid = $1 and pg_catalog.pg_relation_is_publishable($1)
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
        WHERE pr.prrelid = $1
        UNION
        SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
        WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable($1)
        ORDER BY 1;",
"SELECT
   NULL::TEXT AS pubname,
   NULL::TEXT AS _1,
   NULL::TEXT AS _2
 WHERE false"
    ),

    // grafana array index magic
    (r#"SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
        FROM
            generate_series(
                array_lower(string_to_array(current_setting('search_path'),','),1),
                array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s"#,
"'public'")
];

/// A parser with Postgres Compatibility for Datafusion
///
/// This parser will try its best to rewrite postgres SQL into a form that
/// datafuiosn supports. It also maintains a blacklist that will transform the
/// statement to a similar version if rewrite doesn't worth the effort for now.
#[derive(Debug)]
pub struct PostgresCompatibilityParser {
    blacklist: Vec<(Vec<Token>, Vec<Token>)>,
    rewrite_rules: Vec<Arc<dyn SqlStatementRewriteRule>>,
}

impl Default for PostgresCompatibilityParser {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresCompatibilityParser {
    pub fn new() -> Self {
        let mut mapping = Vec::with_capacity(BLACKLIST_SQL_MAPPING.len());

        for (sql_from, sql_to) in BLACKLIST_SQL_MAPPING {
            mapping.push((
                Parser::new(&PostgreSqlDialect {})
                    .try_with_sql(sql_from)
                    .unwrap()
                    .into_tokens()
                    .into_iter()
                    .map(|t| t.token)
                    .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
                    .collect(),
                Parser::new(&PostgreSqlDialect {})
                    .try_with_sql(sql_to)
                    .unwrap()
                    .into_tokens()
                    .into_iter()
                    .map(|t| t.token)
                    .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
                    .collect(),
            ));
        }

        Self {
            blacklist: mapping,
            rewrite_rules: vec![
                // make sure blacklist based rewriter it on the top to prevent sql
                // being rewritten from other rewriters
                Arc::new(AliasDuplicatedProjectionRewrite),
                Arc::new(ResolveUnqualifiedIdentifer),
                Arc::new(RewriteArrayAnyAllOperation),
                Arc::new(PrependUnqualifiedPgTableName),
                Arc::new(RemoveQualifier),
                Arc::new(RemoveUnsupportedTypes::new()),
                Arc::new(FixArrayLiteral),
                Arc::new(CurrentUserVariableToSessionUserFunctionCall),
                Arc::new(FixCollate),
                Arc::new(RemoveSubqueryFromProjection),
                Arc::new(FixVersionColumnName),
            ],
        }
    }

    /// return tokens with replacements applied
    fn maybe_replace_tokens(&self, input: &str) -> Result<Vec<TokenWithSpan>, ParserError> {
        let parser = Parser::new(&PostgreSqlDialect {});
        let tokens = parser.try_with_sql(input)?.into_tokens();

        // Filter out whitespace and semicolon tokens for comparison
        let filtered_tokens: Vec<&TokenWithSpan> = tokens
            .iter()
            .filter(|t| !matches!(t.token, Token::Whitespace(_) | Token::SemiColon))
            .collect();

        // Handle empty input
        if filtered_tokens.is_empty() {
            return Ok(tokens);
        }

        // Track which filtered tokens should be replaced and with what
        let mut to_replace = vec![false; filtered_tokens.len()];
        let mut replacements: Vec<Option<(Vec<Token>, usize)>> = vec![None; filtered_tokens.len()];

        // Find all matches of blacklist patterns in the filtered tokens
        for (pattern, replacement) in &self.blacklist {
            if pattern.is_empty() {
                continue;
            }

            // Search for pattern in filtered tokens
            let mut start = 0;
            while start < filtered_tokens.len() {
                if start + pattern.len() > filtered_tokens.len() {
                    break;
                }

                // Check if pattern matches starting at position 'start'
                let mut matches_pattern = true;
                for i in 0..pattern.len() {
                    match &pattern[i] {
                        Token::Placeholder(_) => {
                            // Placeholder matches any token
                        }
                        _ => {
                            if filtered_tokens[start + i].token != pattern[i] {
                                matches_pattern = false;
                                break;
                            }
                        }
                    }
                }

                if matches_pattern {
                    // Mark tokens to be replaced
                    for i in start..start + pattern.len() {
                        to_replace[i] = true;
                    }
                    // Store replacement and pattern length for the first token
                    replacements[start] = Some((replacement.clone(), pattern.len()));
                    // Skip ahead by pattern length to avoid overlapping matches
                    start += pattern.len();
                } else {
                    start += 1;
                }
            }
        }

        // Build the result by replacing matched ranges
        let mut result = Vec::new();
        let mut i = 0;

        while i < tokens.len() {
            // Skip whitespace and semicolons in the original tokens
            if matches!(tokens[i].token, Token::Whitespace(_) | Token::SemiColon) {
                result.push(tokens[i].clone());
                i += 1;
                continue;
            }

            // Find the corresponding index in filtered_tokens
            let filtered_idx = {
                let mut count = 0;
                for (j, token) in tokens.iter().enumerate() {
                    if j == i {
                        break;
                    }
                    if !matches!(token.token, Token::Whitespace(_) | Token::SemiColon) {
                        count += 1;
                    }
                }
                count
            };

            if filtered_idx < to_replace.len() && to_replace[filtered_idx] {
                // This token should be replaced
                if let Some((ref replacement, pattern_len)) = replacements[filtered_idx] {
                    // Add replacement tokens
                    let first_span = tokens[i].span;
                    result.push(TokenWithSpan {
                        token: replacement[0].clone(),
                        span: first_span,
                    });

                    for token in &replacement[1..] {
                        result.push(TokenWithSpan {
                            token: token.clone(),
                            span: first_span,
                        });
                    }
                    // Skip all tokens in this matched pattern
                    // Find how many original tokens correspond to this pattern
                    let mut pattern_len_in_original = 0;
                    let mut filtered_count = 0;
                    let mut j = i;
                    while j < tokens.len() && filtered_count < pattern_len {
                        if !matches!(tokens[j].token, Token::Whitespace(_) | Token::SemiColon) {
                            filtered_count += 1;
                        }
                        pattern_len_in_original += 1;
                        j += 1;
                    }
                    i += pattern_len_in_original;
                } else {
                    // Should not happen, but keep the token just in case
                    result.push(tokens[i].clone());
                    i += 1;
                }
            } else {
                // Keep the original token
                result.push(tokens[i].clone());
                i += 1;
            }
        }

        Ok(result)
    }

    fn parse_tokens(&self, tokens: Vec<TokenWithSpan>) -> Result<Vec<Statement>, ParserError> {
        let parser = Parser::new(&PostgreSqlDialect {});
        parser.with_tokens_with_locations(tokens).parse_statements()
    }

    pub fn parse(&self, input: &str) -> Result<Vec<Statement>, ParserError> {
        let tokens = self.maybe_replace_tokens(input)?;
        let statements = self.parse_tokens(tokens)?;

        let statements: Vec<_> = statements.into_iter().map(|s| self.rewrite(s)).collect();
        //dbg!(&statements[0].to_string());
        Ok(statements)
    }

    pub fn rewrite(&self, mut s: Statement) -> Statement {
        for rule in &self.rewrite_rules {
            s = rule.rewrite(s);
        }

        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_match() {
        let sql = "SELECT pol.polname, pol.polpermissive,
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
            WHERE pol.polrelid = '16384' ORDER BY 1;";

        let parser = PostgresCompatibilityParser::new();
        let actual_tokens = parser
            .maybe_replace_tokens(sql)
            .expect("failed to parse sql")
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        let expected_sql = r#"SELECT
   NULL::TEXT AS polname,
   NULL::TEXT AS polpermissive,
   NULL::TEXT AS array_to_string,
   NULL::TEXT AS pg_get_expr_1,
   NULL::TEXT AS pg_get_expr_2,
   NULL::TEXT AS cmd
 WHERE false"#;

        let expected_tokens = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(expected_sql)
            .unwrap()
            .into_tokens()
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        assert_eq!(actual_tokens, expected_tokens);

        let sql = "SELECT n.nspname schema_name,
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
                                ORDER BY 1, 2";

        let parser = PostgresCompatibilityParser::new();

        let actual_tokens = parser
            .maybe_replace_tokens(sql)
            .expect("failed to parse sql")
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        let expected_sql =
            r#"SELECT NULL::TEXT AS schema_name, NULL::TEXT AS type_name WHERE false"#;

        let expected_tokens = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(expected_sql)
            .unwrap()
            .into_tokens()
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        assert_eq!(actual_tokens, expected_tokens);

        let sql = "SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid
             JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid
        WHERE pc.oid ='16384' and pg_catalog.pg_relation_is_publishable('16384')
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
        WHERE pr.prrelid = '16384'
        UNION
        SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
        WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16384')
        ORDER BY 1;";

        let parser = PostgresCompatibilityParser::new();

        let actual_tokens = parser
            .maybe_replace_tokens(sql)
            .expect("failed to parse sql")
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        let expected_sql = r#"SELECT
   NULL::TEXT AS pubname,
   NULL::TEXT AS _1,
   NULL::TEXT AS _2
 WHERE false"#;

        let expected_tokens = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(expected_sql)
            .unwrap()
            .into_tokens()
            .into_iter()
            .map(|t| t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect::<Vec<_>>();

        assert_eq!(actual_tokens, expected_tokens);
    }

    #[test]
    fn test_empty_query() {
        let parser = PostgresCompatibilityParser::new();
        let result = parser.parse(" ").expect("failed to parse sql");
        assert!(result.is_empty());

        let result = parser.parse("").expect("failed to parse sql");
        assert!(result.is_empty());

        let result = parser.parse(";").expect("failed to parse sql");
        assert!(result.is_empty());
    }

    #[test]
    fn test_partial_match() {
        let parser = PostgresCompatibilityParser::new();

        // Test partial match where the beginning matches a blacklisted query
        // Using a simpler query that doesn't have placeholders for easier testing
        let sql = r#"SELECT
        CASE WHEN
              quote_ident(table_schema) IN (
              SELECT
                CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
              FROM
                generate_series(
                  array_lower(string_to_array(current_setting('search_path'),','),1),
                  array_upper(string_to_array(current_setting('search_path'),','),1)
                ) as i,
                string_to_array(current_setting('search_path'),',') s
              )
          THEN quote_ident(table_name)
          ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
        END AS "table"
        FROM information_schema.tables
        WHERE quote_ident(table_schema) NOT IN ('information_schema',
                                 'pg_catalog',
                                 '_timescaledb_cache',
                                 '_timescaledb_catalog',
                                 '_timescaledb_internal',
                                 '_timescaledb_config',
                                 'timescaledb_information',
                                 'timescaledb_experimental')
        ORDER BY CASE WHEN
              quote_ident(table_schema) IN (
              SELECT
                CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
              FROM
                generate_series(
                  array_lower(string_to_array(current_setting('search_path'),','),1),
                  array_upper(string_to_array(current_setting('search_path'),','),1)
                ) as i,
                string_to_array(current_setting('search_path'),',') s
              ) THEN 0 ELSE 1 END, 1"#;

        let tokens = parser
            .maybe_replace_tokens(sql)
            .expect("failed to parse sql");
        // Should have the beginning replaced with 'SELECT' and the rest preserved
        assert!(tokens.len() > 0);

        let expected_sql = r#"SELECT
        CASE WHEN
              quote_ident(table_schema) IN (
              'public')
          THEN quote_ident(table_name)
          ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
        END AS "table"
        FROM information_schema.tables
        WHERE quote_ident(table_schema) NOT IN ('information_schema',
                                 'pg_catalog',
                                 '_timescaledb_cache',
                                 '_timescaledb_catalog',
                                 '_timescaledb_internal',
                                 '_timescaledb_config',
                                 'timescaledb_information',
                                 'timescaledb_experimental')
        ORDER BY CASE WHEN
              quote_ident(table_schema) IN (
              'public'
              ) THEN 0 ELSE 1 END, 1"#;

        let expected_tokens = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(expected_sql)
            .unwrap()
            .into_tokens();

        // Compare token values (ignoring spans and whitespace)
        let actual_tokens: Vec<_> = tokens
            .iter()
            .map(|t| &t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect();
        let expected_token_values: Vec<_> = expected_tokens
            .iter()
            .map(|t| &t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
            .collect();

        assert_eq!(actual_tokens, expected_token_values);
    }
}
