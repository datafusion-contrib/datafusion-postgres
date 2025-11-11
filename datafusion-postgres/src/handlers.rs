use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ParamValues, ToDFSchema};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser;
use log::info;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeResponse, DescribeStatementResponse, Response, Tag,
};
use pgwire::api::stmt::QueryParser;
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;
use pgwire::types::format::FormatOptions;

use crate::auth::AuthManager;
use crate::client;
use crate::hooks::set_show::SetShowHook;
use crate::hooks::QueryHook;
use arrow_pg::datatypes::df;
use arrow_pg::datatypes::{arrow_schema_to_pg_fields, into_pg_type};
use datafusion_pg_catalog::pg_catalog::context::{Permission, ResourceType};
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

/// Simple startup handler that does no authentication
/// For production, use DfAuthSource with proper pgwire authentication handlers
pub struct SimpleStartupHandler;

#[async_trait::async_trait]
impl NoopStartupHandler for SimpleStartupHandler {}

pub struct HandlerFactory {
    pub session_service: Arc<DfSessionService>,
}

impl HandlerFactory {
    pub fn new(session_context: Arc<SessionContext>, auth_manager: Arc<AuthManager>) -> Self {
        let session_service =
            Arc::new(DfSessionService::new(session_context, auth_manager.clone()));
        HandlerFactory { session_service }
    }

    pub fn new_with_hooks(
        session_context: Arc<SessionContext>,
        auth_manager: Arc<AuthManager>,
        query_hooks: Vec<Arc<dyn QueryHook>>,
    ) -> Self {
        let session_service = Arc::new(DfSessionService::new_with_hooks(
            session_context,
            auth_manager.clone(),
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
    auth_manager: Arc<AuthManager>,
    query_hooks: Vec<Arc<dyn QueryHook>>,
}

impl DfSessionService {
    pub fn new(
        session_context: Arc<SessionContext>,
        auth_manager: Arc<AuthManager>,
    ) -> DfSessionService {
        let hooks: Vec<Arc<dyn QueryHook>> = vec![Arc::new(SetShowHook)];
        Self::new_with_hooks(session_context, auth_manager, hooks)
    }

    pub fn new_with_hooks(
        session_context: Arc<SessionContext>,
        auth_manager: Arc<AuthManager>,
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
            auth_manager,
            query_hooks,
        }
    }

    /// Check if the current user has permission to execute a query
    async fn check_query_permission<C>(&self, client: &C, query: &str) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        // Get the username from client metadata
        let username = client
            .metadata()
            .get("user")
            .map(|s| s.as_str())
            .unwrap_or("anonymous");

        // Parse query to determine required permissions
        let query_lower = query.to_lowercase();
        let query_trimmed = query_lower.trim();

        let (required_permission, resource) = if query_trimmed.starts_with("select") {
            (Permission::Select, self.extract_table_from_query(query))
        } else if query_trimmed.starts_with("insert") {
            (Permission::Insert, self.extract_table_from_query(query))
        } else if query_trimmed.starts_with("update") {
            (Permission::Update, self.extract_table_from_query(query))
        } else if query_trimmed.starts_with("delete") {
            (Permission::Delete, self.extract_table_from_query(query))
        } else if query_trimmed.starts_with("create table")
            || query_trimmed.starts_with("create view")
        {
            (Permission::Create, ResourceType::All)
        } else if query_trimmed.starts_with("drop") {
            (Permission::Drop, self.extract_table_from_query(query))
        } else if query_trimmed.starts_with("alter") {
            (Permission::Alter, self.extract_table_from_query(query))
        } else {
            // For other queries (SHOW, EXPLAIN, etc.), allow all users
            return Ok(());
        };

        // Check permission
        let has_permission = self
            .auth_manager
            .check_permission(username, required_permission, resource)
            .await;

        if !has_permission {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42501".to_string(), // insufficient_privilege
                    format!("permission denied for user \"{username}\""),
                ),
            )));
        }

        Ok(())
    }

    /// Extract table name from query (simplified parsing)
    fn extract_table_from_query(&self, query: &str) -> ResourceType {
        let words: Vec<&str> = query.split_whitespace().collect();

        // Simple heuristic to find table names
        for (i, word) in words.iter().enumerate() {
            let word_lower = word.to_lowercase();
            if (word_lower == "from" || word_lower == "into" || word_lower == "table")
                && i + 1 < words.len()
            {
                let table_name = words[i + 1].trim_matches(|c| c == '(' || c == ')' || c == ';');
                return ResourceType::Table(table_name.to_string());
            }
        }

        // If we can't determine the table, default to All
        ResourceType::All
    }

    async fn try_respond_transaction_statements<C>(
        &self,
        client: &C,
        query_lower: &str,
    ) -> PgWireResult<Option<Response>>
    where
        C: ClientInfo,
    {
        // Transaction handling based on pgwire example:
        // https://github.com/sunng87/pgwire/blob/master/examples/transaction.rs#L57
        match query_lower.trim() {
            "begin" | "begin transaction" | "begin work" | "start transaction" => {
                match client.transaction_status() {
                    TransactionStatus::Idle => {
                        Ok(Some(Response::TransactionStart(Tag::new("BEGIN"))))
                    }
                    TransactionStatus::Transaction => {
                        // PostgreSQL behavior: ignore nested BEGIN, just return SUCCESS
                        // This matches PostgreSQL's handling of nested transaction blocks
                        log::warn!("BEGIN command ignored: already in transaction block");
                        Ok(Some(Response::Execution(Tag::new("BEGIN"))))
                    }
                    TransactionStatus::Error => {
                        // Can't start new transaction from failed state
                        Err(PgWireError::UserError(Box::new(
                            pgwire::error::ErrorInfo::new(
                                "ERROR".to_string(),
                                "25P01".to_string(),
                                "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                            ),
                        )))
                    }
                }
            }
            "commit" | "commit transaction" | "commit work" | "end" | "end transaction" => {
                match client.transaction_status() {
                    TransactionStatus::Idle | TransactionStatus::Transaction => {
                        Ok(Some(Response::TransactionEnd(Tag::new("COMMIT"))))
                    }
                    TransactionStatus::Error => {
                        Ok(Some(Response::TransactionEnd(Tag::new("ROLLBACK"))))
                    }
                }
            }
            "rollback" | "rollback transaction" | "rollback work" | "abort" => {
                Ok(Some(Response::TransactionEnd(Tag::new("ROLLBACK"))))
            }
            _ => Ok(None),
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

        // Check for transaction commands early to avoid SQL parsing issues with ABORT
        let query_lower = query.to_lowercase().trim().to_string();
        if let Some(resp) = self
            .try_respond_transaction_statements(client, &query_lower)
            .await?
        {
            return Ok(vec![resp]);
        }

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
            // TODO: improve statement check by using statement directly
            let query = statement.to_string();
            let query_lower = query.to_lowercase().trim().to_string();

            // Check permissions for the query (skip for SET, transaction, and SHOW statements)
            if !query_lower.starts_with("set")
                && !query_lower.starts_with("begin")
                && !query_lower.starts_with("commit")
                && !query_lower.starts_with("rollback")
                && !query_lower.starts_with("start")
                && !query_lower.starts_with("end")
                && !query_lower.starts_with("abort")
                && !query_lower.starts_with("show")
            {
                self.check_query_permission(client, &query).await?;
            }

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

            // Check if we're in a failed transaction and block non-transaction
            // commands
            if client.transaction_status() == TransactionStatus::Error {
                return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "25P01".to_string(),
                    "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                ),
            )));
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

            if query_lower.starts_with("insert into") {
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

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if let (_, Some((_, plan))) = &target.statement {
            let schema = plan.schema();
            let fields =
                arrow_schema_to_pg_fields(schema.as_arrow(), &Format::UnifiedBinary, None)?;
            let params = plan
                .get_parameter_types()
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let mut param_types = Vec::with_capacity(params.len());
            for param_type in ordered_param_types(&params).iter() {
                // Fixed: Use &params
                if let Some(datatype) = param_type {
                    let pgtype =
                        into_pg_type(&Arc::new(Field::new("item", (*datatype).clone(), true)))?;
                    param_types.push(pgtype);
                } else {
                    param_types.push(Type::UNKNOWN);
                }
            }

            Ok(DescribeStatementResponse::new(param_types, fields))
        } else {
            Ok(DescribeStatementResponse::no_data())
        }
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if let (_, Some((_, plan))) = &target.statement.statement {
            let format = &target.result_column_format;
            let schema = plan.schema();
            let fields = arrow_schema_to_pg_fields(schema.as_arrow(), format, None)?;

            Ok(DescribePortalResponse::new(fields))
        } else {
            Ok(DescribePortalResponse::no_data())
        }
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
        let query = portal
            .statement
            .statement
            .0
            .to_lowercase()
            .trim()
            .to_string();
        log::debug!("Received execute extended query: {query}"); // Log for debugging

        // Check query hooks first
        if !self.query_hooks.is_empty() {
            if let (_, Some((statement, plan))) = &portal.statement.statement {
                // TODO: in the case where query hooks all return None, we do the param handling again later.
                let param_types = plan
                    .get_parameter_types()
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

        // Check permissions for the query (skip for SET and SHOW statements)
        if !query.starts_with("set") && !query.starts_with("show") {
            self.check_query_permission(client, &portal.statement.statement.0)
                .await?;
        }

        if let Some(resp) = self
            .try_respond_transaction_statements(client, &query)
            .await?
        {
            return Ok(resp);
        }

        // Check if we're in a failed transaction and block non-transaction
        // commands
        if client.transaction_status() == TransactionStatus::Error {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "25P01".to_string(),
                    "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                ),
            )));
        }

        if let (_, Some((_, plan))) = &portal.statement.statement {
            let param_types = plan
                .get_parameter_types()
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

            if query.starts_with("insert into") {
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

impl Parser {
    fn try_shortcut_parse_plan(&self, sql: &str) -> Result<Option<LogicalPlan>, DataFusionError> {
        // Check for transaction commands that shouldn't be parsed by DataFusion
        let sql_lower = sql.to_lowercase();
        let sql_trimmed = sql_lower.trim();

        if matches!(
            sql_trimmed,
            "" | "begin"
                | "begin transaction"
                | "begin work"
                | "start transaction"
                | "commit"
                | "commit transaction"
                | "commit work"
                | "end"
                | "end transaction"
                | "rollback"
                | "rollback transaction"
                | "rollback work"
                | "abort"
        ) {
            // Return a dummy plan for transaction commands - they'll be handled by transaction handler
            let dummy_schema = datafusion::common::DFSchema::empty();
            return Ok(Some(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(dummy_schema),
                },
            )));
        }

        // show statement may not be supported by datafusion
        if sql_trimmed.starts_with("show") {
            let show_schema =
                Arc::new(Schema::new(vec![Field::new("show", DataType::Utf8, false)]));
            let df_schema = show_schema.to_dfschema()?;
            return Ok(Some(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: true,
                    schema: Arc::new(df_schema),
                },
            )));
        }

        Ok(None)
    }
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = (String, Option<(sqlparser::ast::Statement, LogicalPlan)>);

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        _types: &[Type],
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

        // Check for transaction commands that shouldn't be parsed by DataFusion
        if let Some(plan) = self
            .try_shortcut_parse_plan(sql)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?
        {
            return Ok((sql.to_string(), Some((statement, plan))));
        }

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
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::testing::MockClient;

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
        let auth_manager = Arc::new(AuthManager::new());

        let hooks: Vec<Arc<dyn QueryHook>> = vec![Arc::new(TestHook)];
        let service = DfSessionService::new_with_hooks(session_context, auth_manager, hooks);

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
}
