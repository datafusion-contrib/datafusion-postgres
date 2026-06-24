//! End-to-end tests for the `array_upper` / `array_lower` UDFs.
//!
//! These queries intentionally do NOT match any blacklist fragment, so they
//! exercise the real parse -> rewrite -> plan -> execute path and prove the
//! UDFs are registered and resolvable. Before these UDFs existed, such queries
//! failed at planning with "Invalid function 'array_upper'".

use pgwire::api::query::SimpleQueryHandler;

use datafusion_postgres::testing::*;

const QUERIES: &[&str] = &[
    // Bare UDFs over an array literal.
    "SELECT array_upper(ARRAY[1, 2, 3], 1), array_lower(ARRAY[1, 2, 3], 1)",
    // pg_catalog-qualified form (qualifier stripped by RemoveQualifier rule).
    "SELECT pg_catalog.array_upper(ARRAY['a', 'b'], 1)",
    "SELECT pg_catalog.array_lower(ARRAY['a', 'b'], 1)",
    // Typical usage pattern: generate_series over array bounds, mirroring the
    // shape grafana/psql clients send (but without matching the blacklisted
    // fragment verbatim).
    "SELECT array_upper(string_to_array('public,utils', ','), 1)",
    "SELECT array_lower(string_to_array('public,utils', ','), 1)",
];

#[tokio::test]
pub async fn test_array_bounds_udfs() {
    env_logger::init();
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .unwrap_or_else(|e| {
                panic!("failed to run sql:\n--------------\n {query}\n--------------\n{e}")
            });
    }
}
