//! Per-connection DataFusion sessions.
//!
//! The [`SessionContext`] handed to [`crate::serve`] or
//! [`crate::DfSessionService::new`] is a *template* rather than the session
//! queries run on: each pgwire connection gets its own `SessionContext`,
//! created from the template the first time the connection runs a statement.
//!
//! DataFusion session states are copy-on-write. A per-connection session shares
//! the template's catalogs and runtime (memory pool, disk manager, caches), so
//! tables created, inserted into or dropped through one connection are visible
//! to every other -- as tables are in Postgres. What Postgres scopes per
//! session instead -- configuration (`SET`), registered functions and prepared
//! statements (`PREPARE`/`EXECUTE`) -- is split off on first write and stays
//! private to the connection.
//!
//! The session is kept in pgwire's [`SessionExtensions`], which needs no
//! bookkeeping of its own: it is created lazily and dropped with the
//! connection.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use pgwire::api::ClientInfo;

/// The entry stored in a connection's session extensions.
struct ConnectionSession(Arc<SessionContext>);

/// A new connection session: a copy-on-write clone of `template`'s state.
pub(crate) fn new_connection_session(template: &SessionContext) -> SessionContext {
    // Clone the stored state rather than `template.state()`: the latter is
    // marked as "execution started" for the query that requested it.
    SessionContext::new_with_state(template.state_ref().read().clone())
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
    use pgwire::api::SessionExtensions;

    use super::*;

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

    async fn scalar(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let value = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        value.to_string()
    }

    struct Client(SessionExtensions);

    impl ClientInfo for Client {
        fn socket_addr(&self) -> std::net::SocketAddr {
            "127.0.0.1:5432".parse().unwrap()
        }
        fn is_secure(&self) -> bool {
            false
        }
        fn protocol_version(&self) -> pgwire::messages::ProtocolVersion {
            pgwire::messages::ProtocolVersion::PROTOCOL3_0
        }
        fn set_protocol_version(&mut self, _version: pgwire::messages::ProtocolVersion) {}
        fn pid_and_secret_key(&self) -> (i32, pgwire::messages::startup::SecretKey) {
            (0, pgwire::messages::startup::SecretKey::I32(0))
        }
        fn set_pid_and_secret_key(
            &mut self,
            _pid: i32,
            _key: pgwire::messages::startup::SecretKey,
        ) {
        }
        fn state(&self) -> pgwire::api::PgWireConnectionState {
            pgwire::api::PgWireConnectionState::ReadyForQuery
        }
        fn set_state(&mut self, _new_state: pgwire::api::PgWireConnectionState) {}
        fn transaction_status(&self) -> pgwire::messages::response::TransactionStatus {
            pgwire::messages::response::TransactionStatus::Idle
        }
        fn set_transaction_status(
            &mut self,
            _new_status: pgwire::messages::response::TransactionStatus,
        ) {
        }
        fn metadata(&self) -> &std::collections::HashMap<String, String> {
            unimplemented!()
        }
        fn metadata_mut(&mut self) -> &mut std::collections::HashMap<String, String> {
            unimplemented!()
        }
        fn session_extensions(&self) -> &SessionExtensions {
            &self.0
        }
        fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
            None
        }
        fn sni_server_name(&self) -> Option<&str> {
            None
        }
    }

    #[test]
    fn session_is_stable_per_connection_and_distinct_across_connections() {
        let template = SessionContext::new();
        let alice = Client(SessionExtensions::new());
        let bob = Client(SessionExtensions::new());

        let a1 = session_for(&template, &alice);
        let a2 = session_for(&template, &alice);
        assert!(Arc::ptr_eq(&a1, &a2), "a connection keeps its session");
        let b = session_for(&template, &bob);
        assert!(!Arc::ptr_eq(&a1, &b), "connections do not share a session");
    }

    #[tokio::test]
    async fn session_settings_are_private_while_catalog_is_shared() {
        let template = SessionContext::new();
        let alice = Client(SessionExtensions::new());
        let bob = Client(SessionExtensions::new());
        let alice = session_for(&template, &alice);
        let bob = session_for(&template, &bob);
        assert_eq!(
            now_type(&alice).await,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        alice
            .execute_logical_plan(datafusion::logical_expr::LogicalPlan::Statement(
                datafusion::logical_expr::Statement::SetVariable(
                    datafusion::logical_expr::SetVariable {
                        variable: "datafusion.execution.time_zone".to_string(),
                        value: "America/New_York".to_string(),
                    },
                ),
            ))
            .await
            .unwrap();

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
        alice
            .sql("create table visits (id bigint)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        alice
            .sql("insert into visits values (1), (2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        bob.sql("insert into visits values (3)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(scalar(&bob, "select count(*) from visits").await, "3");
        assert_eq!(scalar(&template, "select count(*) from visits").await, "3");
        bob.sql("drop table visits")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(!alice.table_exist("visits").unwrap());
    }

    #[tokio::test]
    async fn sql_prepared_statements_are_private_to_the_connection() {
        let template = SessionContext::new();
        let alice = session_for(&template, &Client(SessionExtensions::new()));
        let bob = session_for(&template, &Client(SessionExtensions::new()));

        alice
            .sql("prepare p as select 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(scalar(&alice, "execute p").await, "1");
        assert!(
            bob.sql("execute p").await.is_err(),
            "must not see the other connection's statement"
        );
    }
}
