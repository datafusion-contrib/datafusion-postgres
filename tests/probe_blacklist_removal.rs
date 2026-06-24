//! Probe each BLACKLIST entry's EXACT original sql through the full rewrite +
//! planning + execution pipeline (blacklist bypassed) to find removable entries.
use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser as SqlParser;
use datafusion_pg_catalog::pg_catalog::setup_pg_catalog;
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::auth::AuthManager;

fn rewrite_only(sql: &str) -> String {
    let dialect = PostgreSqlDialect {};
    let parser = PostgresCompatibilityParser::new();
    let mut stmts = SqlParser::parse_sql(&dialect, sql).unwrap();
    parser.rewrite(stmts.remove(0)).to_string()
}

async fn try_query(ctx: &SessionContext, label: &str, sql: &str) {
    let rewritten = rewrite_only(sql);
    match ctx.sql(&rewritten).await {
        Ok(df) => match df.collect().await {
            Ok(_) => println!("ZZZ OK     [{label}]"),
            Err(e) => println!(
                "ZZZ EXEC   [{label}] -> {}",
                e.to_string().lines().next().unwrap_or("")
            ),
        },
        Err(e) => println!(
            "ZZZ PLAN   [{label}] -> {}",
            e.to_string().lines().next().unwrap_or("")
        ),
    }
}

#[tokio::test]
async fn probe_blacklist_removal() {
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    setup_pg_catalog(&ctx, "datafusion", Arc::new(AuthManager::default())).unwrap();

    let labels = [
        "1-pgcli-fk",
        "2-pgcli-types",
        "3-psql-policies",
        "4-statistics",
        "5-publications",
        "6-dbeaver-relsize",
        "7-grafana-arrayidx",
    ];
    for (i, label) in labels.iter().enumerate() {
        let sql = std::fs::read_to_string(format!("/tmp/blist_{}.sql", i + 1)).unwrap();
        // Substitute any $1 placeholders with a literal oid that exists.
        let sql = sql.replace("$1", "1");
        try_query(&ctx, label, &sql).await;
    }
    panic!("probe complete");
}
