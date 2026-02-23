use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser;
use log::info;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, Response, Tag};
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::types::format::FormatOptions;

use crate::hooks::prepare_execute::PrepareExecuteHook;
use crate::hooks::set_show::SetShowHook;
use crate::hooks::transactions::TransactionStatementHook;
use crate::hooks::QueryHook;
use crate::{client, planner};
use arrow_pg::datatypes::df;
use arrow_pg::datatypes::{arrow_schema_to_pg_fields, into_pg_type};
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

/// Simple startup handler that does no authentication
pub struct SimpleStartupHandler;

#[async_trait::async_trait]
impl NoopStartupHandler for SimpleStartupHandler {}

pub struct HandlerFactory {
    pub session_service: Arc<DfSessionService>,
}

impl HandlerFactory {
    pub fn new(session_context: Arc<SessionContext>) -> Self {
        let session_service = Arc::new(DfSessionService::new(session_context));
        HandlerFactory { session_service }
    }

    pub fn new_with_hooks(
        session_context: Arc<SessionContext>,
        query_hooks: Vec<Arc<dyn QueryHook>>,
    ) -> Self {
        let session_service = Arc::new(DfSessionService::new_with_hooks(
            session_context,
            query_hooks,
        ));
        HandlerFactory { session_service }
    }
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.session_service.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.session_service.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(SimpleStartupHandler)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LoggingErrorHandler)
    }
}

struct LoggingErrorHandler;

impl ErrorHandler for LoggingErrorHandler {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        info!("Sending error: {error}")
    }
}

/// The pgwire handler backed by a datafusion `SessionContext`
pub struct DfSessionService {
    session_context: Arc<SessionContext>,
    parser: Arc<Parser>,
    query_hooks: Vec<Arc<dyn QueryHook>>,
}

impl DfSessionService {
    pub fn new(session_context: Arc<SessionContext>) -> DfSessionService {
        let hooks: Vec<Arc<dyn QueryHook>> = vec![
            Arc::new(PrepareExecuteHook::new()),
            Arc::new(SetShowHook),
            Arc::new(TransactionStatementHook),
        ];
        Self::new_with_hooks(session_context, hooks)
    }

    pub fn new_with_hooks(
        session_context: Arc<SessionContext>,
        query_hooks: Vec<Arc<dyn QueryHook>>,
    ) -> DfSessionService {
        let parser = Arc::new(Parser {
            session_context: session_context.clone(),
            sql_parser: PostgresCompatibilityParser::new(),
            query_hooks: query_hooks.clone(),
        });
        DfSessionService {
            session_context,
            parser,
            query_hooks,
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        log::debug!("Received query: {query}"); // Log the query for debugging

        let statements = self
            .parser
            .sql_parser
            .parse(query)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // empty query
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let mut results = vec![];
        'stmt: for statement in statements {
            let query = statement.to_string();

            // Call query hooks with the parsed statement
            for hook in &self.query_hooks {
                if let Some(result) = hook
                    .handle_simple_query(&statement, &self.session_context, client)
                    .await
                {
                    results.push(result?);
                    continue 'stmt;
                }
            }

            let df_result = {
                let timeout = client::get_statement_timeout(client);
                if let Some(timeout_duration) = timeout {
                    tokio::time::timeout(timeout_duration, self.session_context.sql(&query))
                        .await
                        .map_err(|_| {
                            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                                "ERROR".to_string(),
                                "57014".to_string(), // query_canceled error code
                                "canceling statement due to statement timeout".to_string(),
                            )))
                        })?
                } else {
                    self.session_context.sql(&query).await
                }
            };

            // Handle query execution errors and transaction state
            let df = match df_result {
                Ok(df) => df,
                Err(e) => {
                    return Err(PgWireError::ApiError(Box::new(e)));
                }
            };

            if matches!(statement, sqlparser::ast::Statement::Insert(_)) {
                let resp = map_rows_affected_for_insert(&df).await?;
                results.push(resp);
            } else {
                // For non-INSERT queries, return a regular Query response
                let format_options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                let resp =
                    df::encode_dataframe(df, &Format::UnifiedText, Some(format_options)).await?;
                results.push(Response::Query(resp));
            }
        }
        Ok(results)
    }
}

#[async_trait]
impl ExtendedQueryHandler for DfSessionService {
    type Statement = (String, Option<(sqlparser::ast::Statement, LogicalPlan)>);
    type QueryParser = Parser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement.0;
        log::debug!("Received execute extended query: {query}"); // Log for debugging

        // Check query hooks first
        if !self.query_hooks.is_empty() {
            if let (_, Some((statement, plan))) = &portal.statement.statement {
                // TODO: in the case where query hooks all return None, we do the param handling again later.
                let param_types = planner::get_inferred_parameter_types(plan)
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                let param_values: ParamValues =
                    df::deserialize_parameters(portal, &ordered_param_types(&param_types))?;

                for hook in &self.query_hooks {
                    if let Some(result) = hook
                        .handle_extended_query(
                            statement,
                            plan,
                            &param_values,
                            &self.session_context,
                            client,
                        )
                        .await
                    {
                        return result;
                    }
                }
            }
        }

        if let (_, Some((statement, plan))) = &portal.statement.statement {
            let param_types = planner::get_inferred_parameter_types(plan)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let param_values =
                df::deserialize_parameters(portal, &ordered_param_types(&param_types))?; // Fixed: Use &param_types

            let plan = plan
                .clone()
                .replace_params_with_values(&param_values)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?; // Fixed: Use
                                                                   // &param_values
            let optimised = self
                .session_context
                .state()
                .optimize(&plan)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let dataframe = {
                let timeout = client::get_statement_timeout(client);
                if let Some(timeout_duration) = timeout {
                    tokio::time::timeout(
                        timeout_duration,
                        self.session_context.execute_logical_plan(optimised),
                    )
                    .await
                    .map_err(|_| {
                        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "57014".to_string(), // query_canceled error code
                            "canceling statement due to statement timeout".to_string(),
                        )))
                    })?
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?
                } else {
                    self.session_context
                        .execute_logical_plan(optimised)
                        .await
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?
                }
            };

            if matches!(statement, sqlparser::ast::Statement::Insert(_)) {
                let resp = map_rows_affected_for_insert(&dataframe).await?;

                Ok(resp)
            } else {
                // For non-INSERT queries, return a regular Query response
                let format_options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                let resp = df::encode_dataframe(
                    dataframe,
                    &portal.result_column_format,
                    Some(format_options),
                )
                .await?;
                Ok(Response::Query(resp))
            }
        } else {
            Ok(Response::EmptyQuery)
        }
    }
}

async fn map_rows_affected_for_insert(df: &DataFrame) -> PgWireResult<Response> {
    // For INSERT queries, we need to execute the query to get the row count
    // and return an Execution response with the proper tag
    let result = df
        .clone()
        .collect()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    // Extract count field from the first batch
    let rows_affected = result
        .first()
        .and_then(|batch| batch.column_by_name("count"))
        .and_then(|col| {
            col.as_any()
                .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        })
        .map_or(0, |array| array.value(0) as usize);

    // Create INSERT tag with the affected row count
    let tag = Tag::new("INSERT").with_oid(0).with_rows(rows_affected);
    Ok(Response::Execution(tag))
}

pub struct Parser {
    session_context: Arc<SessionContext>,
    sql_parser: PostgresCompatibilityParser,
    query_hooks: Vec<Arc<dyn QueryHook>>,
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = (String, Option<(sqlparser::ast::Statement, LogicalPlan)>);

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        log::debug!("Received parse extended query: {sql}"); // Log for debugging

        let mut statements = self
            .sql_parser
            .parse(sql)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if statements.is_empty() {
            return Ok((sql.to_string(), None));
        }

        let statement = statements.remove(0);
        let query = statement.to_string();

        let context = &self.session_context;
        let state = context.state();

        for hook in &self.query_hooks {
            if let Some(logical_plan) = hook
                .handle_extended_parse_query(&statement, context, client)
                .await
            {
                return Ok((query, Some((statement, logical_plan?))));
            }
        }

        let logical_plan = state
            .statement_to_plan(Statement::Statement(Box::new(statement.clone())))
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        Ok((query, Some((statement, logical_plan))))
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        if let (_, Some((_, plan))) = stmt {
            let params = planner::get_inferred_parameter_types(plan)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let mut param_types = Vec::with_capacity(params.len());
            for param_type in ordered_param_types(&params).iter() {
                // Fixed: Use &params
                if let Some(datatype) = param_type {
                    let pgtype = into_pg_type(datatype)?;
                    param_types.push(pgtype);
                } else {
                    param_types.push(Type::UNKNOWN);
                }
            }

            Ok(param_types)
        } else {
            Ok(vec![])
        }
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if let (_, Some((_, plan))) = stmt {
            let schema = plan.schema();
            let fields = arrow_schema_to_pg_fields(
                schema.as_arrow(),
                column_format.unwrap_or(&Format::UnifiedBinary),
                None,
            )?;

            Ok(fields)
        } else {
            Ok(vec![])
        }
    }
}

fn ordered_param_types(types: &HashMap<String, Option<DataType>>) -> Vec<Option<&DataType>> {
    // Datafusion stores the parameters as a map.  In our case, the keys will be
    // `$1`, `$2` etc.  The values will be the parameter types.
    let mut types = types.iter().collect::<Vec<_>>();
    types.sort_by(|a, b| a.0.cmp(b.0));
    types.into_iter().map(|pt| pt.1.as_ref()).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::prelude::SessionContext;
    use futures::Sink;
    use pgwire::api::store::{MemPortalStore, PortalStore};
    use pgwire::api::stmt::StoredStatement;
    use pgwire::api::{
        ClientInfo, ClientPortalStore, PgWireConnectionState, METADATA_USER,
    };
    use pgwire::messages::extendedquery::Bind;
    use pgwire::messages::response::TransactionStatus;
    use pgwire::messages::startup::SecretKey;
    use pgwire::messages::{PgWireBackendMessage, ProtocolVersion};

    use super::*;
    use crate::testing::MockClient;

    // A client that has a real MemPortalStore for extended query protocol tests.
    // This models what a real wire connection client looks like: it holds both
    // ClientInfo metadata AND a portal store for extended protocol state.
    //
    // SQL PREPARE/EXECUTE (via PrepareExecuteHook) stores statements in a
    // separate HashMap inside the hook itself.  Extended protocol Parse/Bind/Execute
    // stores statements and portals in this MemPortalStore.  They are completely
    // independent namespaces.
    struct ExtendedMockClient {
        metadata: HashMap<String, String>,
        portal_store: MemPortalStore<<DfSessionService as ExtendedQueryHandler>::Statement>,
    }

    impl ExtendedMockClient {
        fn new() -> Self {
            let mut metadata = HashMap::new();
            metadata.insert(METADATA_USER.to_string(), "postgres".to_string());
            ExtendedMockClient {
                metadata,
                portal_store: MemPortalStore::new(),
            }
        }
    }

    impl ClientInfo for ExtendedMockClient {
        fn socket_addr(&self) -> std::net::SocketAddr {
            "127.0.0.1:0".parse().unwrap()
        }

        fn is_secure(&self) -> bool {
            false
        }

        fn protocol_version(&self) -> ProtocolVersion {
            ProtocolVersion::PROTOCOL3_0
        }

        fn set_protocol_version(&mut self, _version: ProtocolVersion) {}

        fn pid_and_secret_key(&self) -> (i32, SecretKey) {
            (0, SecretKey::I32(0))
        }

        fn set_pid_and_secret_key(&mut self, _pid: i32, _secret_key: SecretKey) {}

        fn state(&self) -> PgWireConnectionState {
            PgWireConnectionState::ReadyForQuery
        }

        fn set_state(&mut self, _new_state: PgWireConnectionState) {}

        fn transaction_status(&self) -> TransactionStatus {
            TransactionStatus::Idle
        }

        fn set_transaction_status(&mut self, _new_status: TransactionStatus) {}

        fn metadata(&self) -> &HashMap<String, String> {
            &self.metadata
        }

        fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
            &mut self.metadata
        }

        fn client_certificates<'a>(
            &self,
        ) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
            None
        }

        fn sni_server_name(&self) -> Option<&str> {
            None
        }
    }

    impl ClientPortalStore for ExtendedMockClient {
        type PortalStore =
            MemPortalStore<<DfSessionService as ExtendedQueryHandler>::Statement>;

        fn portal_store(&self) -> &Self::PortalStore {
            &self.portal_store
        }
    }

    impl Sink<PgWireBackendMessage> for ExtendedMockClient {
        type Error = std::io::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(
            self: std::pin::Pin<&mut Self>,
            _item: PgWireBackendMessage,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    struct TestHook;

    #[async_trait]
    impl QueryHook for TestHook {
        async fn handle_simple_query(
            &self,
            statement: &sqlparser::ast::Statement,
            _ctx: &SessionContext,
            _client: &mut (dyn ClientInfo + Sync + Send),
        ) -> Option<PgWireResult<Response>> {
            if statement.to_string().contains("magic") {
                Some(Ok(Response::EmptyQuery))
            } else {
                None
            }
        }

        async fn handle_extended_parse_query(
            &self,
            _statement: &sqlparser::ast::Statement,
            _session_context: &SessionContext,
            _client: &(dyn ClientInfo + Send + Sync),
        ) -> Option<PgWireResult<LogicalPlan>> {
            None
        }

        async fn handle_extended_query(
            &self,
            _statement: &sqlparser::ast::Statement,
            _logical_plan: &LogicalPlan,
            _params: &ParamValues,
            _session_context: &SessionContext,
            _client: &mut (dyn ClientInfo + Send + Sync),
        ) -> Option<PgWireResult<Response>> {
            None
        }
    }

    #[tokio::test]
    async fn test_query_hooks() {
        let hook = TestHook;
        let ctx = SessionContext::new();
        let mut client = MockClient::new();

        // Parse a statement that contains "magic"
        let parser = PostgresCompatibilityParser::new();
        let statements = parser.parse("SELECT magic").unwrap();
        let stmt = &statements[0];

        // Hook should intercept
        let result = hook.handle_simple_query(stmt, &ctx, &mut client).await;
        assert!(result.is_some());

        // Parse a normal statement
        let statements = parser.parse("SELECT 1").unwrap();
        let stmt = &statements[0];

        // Hook should not intercept
        let result = hook.handle_simple_query(stmt, &ctx, &mut client).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_multiple_statements_with_hook_continue() {
        // Bug #227: when a hook returned a result, the code used `break 'stmt`
        // which would exit the entire statement loop, preventing subsequent statements
        // from being processed.
        let session_context = Arc::new(SessionContext::new());

        let hooks: Vec<Arc<dyn QueryHook>> = vec![Arc::new(TestHook)];
        let service = DfSessionService::new_with_hooks(session_context, hooks);

        let mut client = MockClient::new();

        // Mix of queries with hooks and those without
        let query = "SELECT magic; SELECT 1; SELECT magic; SELECT 1";

        let results =
            <DfSessionService as SimpleQueryHandler>::do_query(&service, &mut client, query)
                .await
                .unwrap();

        assert_eq!(results.len(), 4, "Expected 4 responses");

        assert!(matches!(results[0], Response::EmptyQuery));
        assert!(matches!(results[1], Response::Query(_)));
        assert!(matches!(results[2], Response::EmptyQuery));
        assert!(matches!(results[3], Response::Query(_)));
    }

    #[tokio::test]
    async fn test_simple_prepare() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE stmt AS SELECT 1",
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("PREPARE")));
    }

    #[tokio::test]
    async fn test_simple_execute_prepared() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // PREPARE a statement first
        let _ = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE stmt AS SELECT 42 AS answer",
        )
        .await
        .unwrap();

        // EXECUTE the prepared statement
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE stmt",
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Response::Query(_)));
    }

    #[tokio::test]
    async fn test_simple_deallocate() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // PREPARE a statement
        let _ = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE stmt AS SELECT 1",
        )
        .await
        .unwrap();

        // DEALLOCATE it
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "DEALLOCATE stmt",
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("DEALLOCATE")));
    }

    #[tokio::test]
    async fn test_execute_nonexistent_statement() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // Try to EXECUTE a statement that was never PREPARED
        let result = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE nonexistent",
        )
        .await;

        assert!(result.is_err(), "EXECUTE of nonexistent statement should fail");
    }

    #[tokio::test]
    async fn test_prepare_execute_deallocate_workflow() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // PREPARE
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE workflow_stmt AS SELECT 100 AS value",
        )
        .await
        .unwrap();
        assert!(matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("PREPARE")));

        // EXECUTE first time
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE workflow_stmt",
        )
        .await
        .unwrap();
        assert!(matches!(results[0], Response::Query(_)));

        // EXECUTE second time (reuse)
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE workflow_stmt",
        )
        .await
        .unwrap();
        assert!(matches!(results[0], Response::Query(_)));

        // DEALLOCATE
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "DEALLOCATE workflow_stmt",
        )
        .await
        .unwrap();
        assert!(matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("DEALLOCATE")));

        // Verify EXECUTE after DEALLOCATE fails
        let err = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE workflow_stmt",
        )
        .await;
        assert!(err.is_err(), "EXECUTE after DEALLOCATE should fail");
    }

    #[tokio::test]
    async fn test_execute_with_integer_parameter() {
        let session_context = Arc::new(SessionContext::new());

        // Create a table with some data
        session_context
            .sql("CREATE TABLE test_params (id BIGINT, name VARCHAR)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session_context
            .sql("INSERT INTO test_params VALUES (1, 'Alice'), (2, 'Bob'), (42, 'Carol')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // PREPARE a parameterized statement
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE param_stmt (BIGINT) AS SELECT id, name FROM test_params WHERE id = $1",
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("PREPARE")));

        // EXECUTE with integer parameter 42
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE param_stmt(42)",
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(
            matches!(results[0], Response::Query(_)),
            "Expected Query response for EXECUTE param_stmt(42)"
        );

        // EXECUTE with a different parameter (id = 1)
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE param_stmt(1)",
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(
            matches!(results[0], Response::Query(_)),
            "Expected Query response for EXECUTE param_stmt(1)"
        );
    }

    #[tokio::test]
    async fn test_deallocate_all() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // PREPARE multiple statements
        for stmt in &["stmt1", "stmt2", "stmt3"] {
            let results = <DfSessionService as SimpleQueryHandler>::do_query(
                &service,
                &mut client,
                &format!("PREPARE {stmt} AS SELECT 1"),
            )
            .await
            .unwrap();
            assert!(
                matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("PREPARE")),
                "Expected PREPARE response for {stmt}"
            );
        }

        // Verify all statements are stored by executing them
        for stmt in &["stmt1", "stmt2", "stmt3"] {
            let results = <DfSessionService as SimpleQueryHandler>::do_query(
                &service,
                &mut client,
                &format!("EXECUTE {stmt}"),
            )
            .await
            .unwrap();
            assert!(
                matches!(results[0], Response::Query(_)),
                "Expected Query response for EXECUTE {stmt} before DEALLOCATE ALL"
            );
        }

        // Run DEALLOCATE ALL
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "DEALLOCATE ALL",
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 1);
        assert!(
            matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("DEALLOCATE")),
            "Expected DEALLOCATE response"
        );

        // Verify all statements are removed - EXECUTE on any should fail
        for stmt in &["stmt1", "stmt2", "stmt3"] {
            let err = <DfSessionService as SimpleQueryHandler>::do_query(
                &service,
                &mut client,
                &format!("EXECUTE {stmt}"),
            )
            .await;
            assert!(
                err.is_err(),
                "EXECUTE {stmt} after DEALLOCATE ALL should fail"
            );
        }
    }

    #[tokio::test]
    async fn test_deallocate_all_prepare_keyword() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // PREPARE a statement
        let _ = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE test_stmt AS SELECT 99",
        )
        .await
        .unwrap();

        // DEALLOCATE PREPARE ALL (PostgreSQL also allows this form)
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "DEALLOCATE PREPARE ALL",
        )
        .await
        .unwrap();
        assert!(
            matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("DEALLOCATE")),
            "Expected DEALLOCATE response for DEALLOCATE PREPARE ALL"
        );

        // Statement should be gone
        let err = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE test_stmt",
        )
        .await;
        assert!(err.is_err(), "EXECUTE after DEALLOCATE PREPARE ALL should fail");
    }

    #[tokio::test]
    async fn test_deallocate_specific_name_still_works() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // PREPARE two statements
        for stmt in &["keep_stmt", "remove_stmt"] {
            let _ = <DfSessionService as SimpleQueryHandler>::do_query(
                &service,
                &mut client,
                &format!("PREPARE {stmt} AS SELECT 1"),
            )
            .await
            .unwrap();
        }

        // DEALLOCATE only one specific statement
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "DEALLOCATE remove_stmt",
        )
        .await
        .unwrap();
        assert!(
            matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("DEALLOCATE")),
            "Expected DEALLOCATE response"
        );

        // keep_stmt should still work
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE keep_stmt",
        )
        .await
        .unwrap();
        assert!(
            matches!(results[0], Response::Query(_)),
            "keep_stmt should still be executable after removing only remove_stmt"
        );

        // remove_stmt should be gone
        let err = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE remove_stmt",
        )
        .await;
        assert!(err.is_err(), "EXECUTE remove_stmt after specific DEALLOCATE should fail");
    }

    #[tokio::test]
    async fn test_execute_with_text_parameter() {
        let session_context = Arc::new(SessionContext::new());

        session_context
            .sql("CREATE TABLE test_text (id BIGINT, name VARCHAR)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session_context
            .sql("INSERT INTO test_text VALUES (1, 'Alice'), (2, 'Bob')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        // PREPARE with a text parameter
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE text_stmt AS SELECT id, name FROM test_text WHERE name = $1",
        )
        .await
        .unwrap();
        assert!(matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("PREPARE")));

        // EXECUTE with text parameter 'Alice'
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE text_stmt('Alice')",
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(
            matches!(results[0], Response::Query(_)),
            "Expected Query response for EXECUTE text_stmt('Alice')"
        );
    }

    // --- Wire protocol interoperability tests ---
    //
    // SQL PREPARE/EXECUTE and extended query protocol Parse/Bind/Execute use
    // SEPARATE storage namespaces:
    //
    //   SQL PREPARE stores in: PrepareExecuteHook.prepared_statements
    //                          (Arc<RwLock<HashMap<String, (Statement, LogicalPlan)>>>)
    //
    //   Wire Parse stores in:  MemPortalStore on the client connection
    //                          (BTreeMap<String, Arc<StoredStatement<...>>>)
    //
    // Statements registered via one mechanism are invisible to the other.

    #[tokio::test]
    async fn test_sql_prepare_extended_execute() {
        // Document namespace separation: SQL PREPARE stores in PrepareExecuteHook,
        // NOT in the wire protocol's MemPortalStore.  A wire-protocol Bind/Execute
        // cannot find a statement registered only via SQL PREPARE.

        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);

        // Step 1: PREPARE via simple query protocol.
        // This stores "wire_sep_stmt" inside PrepareExecuteHook.prepared_statements.
        let mut simple_client = MockClient::new();
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut simple_client,
            "PREPARE wire_sep_stmt AS SELECT 1",
        )
        .await
        .unwrap();
        assert!(
            matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("PREPARE")),
            "SQL PREPARE should succeed"
        );

        // Step 2: Attempt to Bind via the wire/extended protocol.
        // on_bind looks for "wire_sep_stmt" in the client's MemPortalStore,
        // which is entirely separate from PrepareExecuteHook.prepared_statements.
        // The statement was never placed there, so Bind fails.
        let mut ext_client = ExtendedMockClient::new();

        let bind_message = Bind::new(
            None,                      // portal name (unnamed)
            Some("wire_sep_stmt".to_string()), // statement name to look up
            vec![],                    // parameter format codes
            vec![],                    // parameters
            vec![],                    // result format codes
        );

        let bind_result =
            <DfSessionService as ExtendedQueryHandler>::on_bind(
                &service,
                &mut ext_client,
                bind_message,
            )
            .await;

        // Wire Bind cannot find a statement prepared via SQL PREPARE because
        // they live in separate storage namespaces.
        assert!(
            bind_result.is_err(),
            "Wire Bind should NOT find a statement registered only via SQL PREPARE. \
             SQL PREPARE stores in PrepareExecuteHook; wire Parse stores in MemPortalStore. \
             These are separate namespaces."
        );

        let err_msg = format!("{:?}", bind_result.unwrap_err());
        assert!(
            err_msg.contains("wire_sep_stmt") || err_msg.contains("StatementNotFound"),
            "Error should indicate the statement was not found in wire protocol store: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_extended_parse_sql_execute() {
        // Document namespace separation: wire-protocol Parse stores in MemPortalStore,
        // NOT in PrepareExecuteHook.  SQL EXECUTE cannot find a statement registered
        // only via wire Parse.

        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);

        // Step 1: Parse via extended/wire protocol.
        // Parser::parse_sql stores the result in the MemPortalStore on the client.
        // It does NOT populate PrepareExecuteHook.prepared_statements.
        let ext_client = ExtendedMockClient::new();

        let parsed_stmt = service
            .parser
            .parse_sql(&ext_client, "SELECT 1", &[])
            .await
            .unwrap();

        let stored = Arc::new(StoredStatement::new(
            "sql_sep_stmt".to_string(),
            parsed_stmt,
            vec![],
        ));
        ext_client.portal_store().put_statement(stored);

        // Verify the statement IS accessible via the wire protocol store.
        assert!(
            ext_client
                .portal_store()
                .get_statement("sql_sep_stmt")
                .is_some(),
            "Statement should be in MemPortalStore after wire Parse"
        );

        // Step 2: Attempt SQL EXECUTE via simple query protocol.
        // PrepareExecuteHook looks in its own HashMap for "sql_sep_stmt".
        // The statement was never placed there by wire Parse, so EXECUTE fails.
        let mut simple_client = MockClient::new();
        let result = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut simple_client,
            "EXECUTE sql_sep_stmt",
        )
        .await;

        // SQL EXECUTE cannot find a statement registered via wire Parse because
        // they live in separate storage namespaces.
        assert!(
            result.is_err(),
            "SQL EXECUTE should NOT find a statement registered only via wire Parse. \
             Wire Parse stores in MemPortalStore; SQL EXECUTE reads PrepareExecuteHook. \
             These are separate namespaces."
        );

        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("sql_sep_stmt") || err_msg.contains("does not exist"),
            "Error should indicate the statement was not found in PrepareExecuteHook store: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_namespace_isolation() {
        // Document that SQL and wire protocol storage namespaces are fully isolated:
        // the same statement name can exist in both stores simultaneously without
        // collision.  Each store holds its own independent copy.

        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);

        // Register "shared_name" via SQL PREPARE (goes into PrepareExecuteHook).
        let mut simple_client = MockClient::new();
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut simple_client,
            "PREPARE shared_name AS SELECT 'from_sql_prepare' AS source",
        )
        .await
        .unwrap();
        assert!(
            matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("PREPARE")),
            "SQL PREPARE shared_name should succeed"
        );

        // Register "shared_name" via wire Parse (goes into MemPortalStore).
        let ext_client = ExtendedMockClient::new();
        let parsed_stmt = service
            .parser
            .parse_sql(&ext_client, "SELECT 'from_wire_parse' AS source", &[])
            .await
            .unwrap();

        let stored = Arc::new(StoredStatement::new(
            "shared_name".to_string(),
            parsed_stmt,
            vec![],
        ));
        ext_client.portal_store().put_statement(stored);

        // Both stores independently hold "shared_name" without collision.
        // The wire store has its copy.
        assert!(
            ext_client
                .portal_store()
                .get_statement("shared_name")
                .is_some(),
            "Wire MemPortalStore should hold shared_name independently"
        );

        // The SQL store (PrepareExecuteHook) has its copy: SQL EXECUTE succeeds.
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut simple_client,
            "EXECUTE shared_name",
        )
        .await
        .unwrap();
        assert!(
            matches!(results[0], Response::Query(_)),
            "SQL EXECUTE shared_name should succeed from PrepareExecuteHook store"
        );

        // DEALLOCATE via SQL only removes from PrepareExecuteHook, NOT from MemPortalStore.
        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut simple_client,
            "DEALLOCATE shared_name",
        )
        .await
        .unwrap();
        assert!(
            matches!(results[0], Response::Execution(ref tag) if tag == &Tag::new("DEALLOCATE")),
            "SQL DEALLOCATE shared_name should succeed"
        );

        // After SQL DEALLOCATE, the wire store's copy is unaffected.
        assert!(
            ext_client
                .portal_store()
                .get_statement("shared_name")
                .is_some(),
            "Wire MemPortalStore copy of shared_name should survive SQL DEALLOCATE. \
             The two stores are completely independent."
        );

        // After SQL DEALLOCATE, SQL EXECUTE fails because PrepareExecuteHook no longer
        // has it, but the wire protocol store retains its independent copy.
        let result = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut simple_client,
            "EXECUTE shared_name",
        )
        .await;
        assert!(
            result.is_err(),
            "SQL EXECUTE should fail after DEALLOCATE: PrepareExecuteHook no longer holds shared_name"
        );
    }
}
