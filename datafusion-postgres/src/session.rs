//! Per-connection DataFusion sessions.
//!
//! The [`SessionContext`] handed to [`crate::serve`] or
//! [`crate::DfSessionService::new`] is a *template* rather than the session
//! queries run on: each pgwire connection gets its own `SessionContext`,
//! created from the template the first time the connection runs a statement.
//!
//! A per-connection session shares the template's catalogs and runtime (memory
//! pool, disk manager, caches), so tables created, inserted into or dropped
//! through one connection are visible to every other -- as tables are in
//! Postgres. What Postgres scopes per session instead -- configuration (`SET`),
//! registered functions and prepared statements (`PREPARE`/`EXECUTE`) -- is
//! split off on first write and stays private to the connection. Each session
//! also gets a session id of its own and starts without prepared statements,
//! even when the template has some.
//!
//! `datafusion.runtime.*` settings are the exception to "private": DataFusion
//! applies them by swapping the session's runtime environment, so a connection
//! changing e.g. `metadata_cache_limit` changes it for every connection
//! sharing the caches, while `memory_limit` gives that connection a private
//! memory pool that no longer counts against the template's.
//!
//! The session is kept in pgwire's [`SessionExtensions`], which needs no
//! bookkeeping of its own: it is created lazily and dropped with the
//! connection.

use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use pgwire::api::ClientInfo;

/// The entry stored in a connection's session extensions.
struct ConnectionSession(Arc<SessionContext>);

/// A new connection session: the template's catalogs and runtime, a session
/// id of its own and no prepared statements.
pub(crate) fn new_connection_session(template: &SessionContext) -> SessionContext {
    // Start from the stored state rather than `template.state()`, which is
    // marked as "execution started" for the query that requested it. Unlike a
    // plain clone of the state, `new_from_existing` does not carry over the
    // template's session id and prepared statements.
    SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(template.state_ref().read().clone()).build(),
    )
}

/// The `SessionContext` of `client`, created from `template` on first use.
pub(crate) fn session_for(
    template: &SessionContext,
    client: &dyn ClientInfo,
) -> Arc<SessionContext> {
    client
        .session_extensions()
        .get_or_insert_with(|| ConnectionSession(Arc::new(new_connection_session(template))))
        .0
        .clone()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::logical_expr::{LogicalPlan, SetVariable, Statement as PlanStatement};
    use std::sync::Arc;

    use super::*;
    use crate::testing::MockClient;

    fn time_zone(ctx: &SessionContext) -> Option<String> {
        ctx.state()
            .config()
            .options()
            .execution
            .time_zone
            .as_ref()
            .map(|tz| tz.as_str().to_string())
    }

    async fn now_type(ctx: &SessionContext) -> DataType {
        ctx.sql("select now()")
            .await
            .unwrap()
            .schema()
            .field(0)
            .data_type()
            .clone()
    }

    async fn run(
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<(), datafusion::error::DataFusionError> {
        ctx.sql(sql).await?.collect().await.map(|_| ())
    }

    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    }

    fn memory_pool(
        ctx: &SessionContext,
    ) -> Arc<dyn datafusion::execution::memory_pool::MemoryPool> {
        ctx.state_ref().read().runtime_env().memory_pool.clone()
    }

    async fn set_time_zone(ctx: &SessionContext, zone: &str) {
        ctx.execute_logical_plan(LogicalPlan::Statement(PlanStatement::SetVariable(
            SetVariable {
                variable: "datafusion.execution.time_zone".to_string(),
                value: zone.to_string(),
            },
        )))
        .await
        .unwrap();
    }

    /// Two clients of one server, and their sessions.
    async fn two_connections(
        template: &SessionContext,
    ) -> (Arc<SessionContext>, Arc<SessionContext>) {
        let alice = MockClient::new();
        let bob = MockClient::new();
        (session_for(template, &alice), session_for(template, &bob))
    }

    #[test]
    fn session_is_stable_per_connection_and_distinct_across_connections() {
        let template = SessionContext::new();
        let alice = MockClient::new();
        let bob = MockClient::new();

        let a1 = session_for(&template, &alice);
        let a2 = session_for(&template, &alice);
        assert!(Arc::ptr_eq(&a1, &a2), "a connection keeps its session");
        let b = session_for(&template, &bob);
        assert!(!Arc::ptr_eq(&a1, &b), "connections do not share a session");
    }

    #[tokio::test]
    async fn session_settings_are_private_while_catalog_and_runtime_are_shared() {
        let template = SessionContext::new();
        let (alice, bob) = two_connections(&template).await;
        assert_eq!(
            now_type(&alice).await,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        set_time_zone(&alice, "America/New_York").await;

        // configuration follows the connection
        assert_eq!(time_zone(&alice).as_deref(), Some("America/New_York"));
        assert_eq!(time_zone(&bob), None);
        assert_eq!(time_zone(&template), None);
        assert_eq!(
            now_type(&alice).await,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into()))
        );
        assert_eq!(
            now_type(&bob).await,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        // DDL and INSERT are visible everywhere
        run(&alice, "create table visits (id bigint)")
            .await
            .unwrap();
        run(&alice, "insert into visits values (1), (2)")
            .await
            .unwrap();
        run(&bob, "insert into visits values (3)").await.unwrap();
        assert_eq!(count(&bob, "select count(*) from visits").await, 3);
        assert_eq!(count(&template, "select count(*) from visits").await, 3);
        run(&bob, "drop table visits").await.unwrap();
        assert!(!alice.table_exist("visits").unwrap());

        // and so is the runtime
        assert!(Arc::ptr_eq(&memory_pool(&alice), &memory_pool(&template)));
        assert!(Arc::ptr_eq(&memory_pool(&bob), &memory_pool(&template)));
    }

    #[tokio::test]
    async fn sql_prepared_statements_are_private_to_the_connection() {
        let template = SessionContext::new();
        let (alice, bob) = two_connections(&template).await;

        run(&alice, "prepare p as select 1").await.unwrap();
        assert_eq!(count(&alice, "execute p").await, 1);
        assert!(
            run(&bob, "execute p").await.is_err(),
            "must not see the other connection's statement"
        );
    }

    #[tokio::test]
    async fn connections_get_their_own_session_id_and_no_prepared_statements() {
        // A template that is not empty: it carries a prepared statement.
        let template = SessionContext::new();
        run(&template, "prepare shared as select 42").await.unwrap();
        assert_eq!(count(&template, "execute shared").await, 42);

        let (alice, bob) = two_connections(&template).await;
        assert_ne!(alice.session_id(), template.session_id());
        assert_ne!(bob.session_id(), template.session_id());
        assert_ne!(alice.session_id(), bob.session_id());
        for connection in [&alice, &bob] {
            assert!(
                run(connection, "execute shared").await.is_err(),
                "a statement prepared on the template must not leak into connections"
            );
        }
        // The template itself is left alone.
        assert_eq!(count(&template, "execute shared").await, 42);
    }
}
