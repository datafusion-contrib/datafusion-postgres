use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::{AuthManager, Permission, ResourceType};
use crate::sql::{
    parse, rewrite, AliasDuplicatedProjectionRewrite, BlacklistSqlRewriter,
    CurrentUserVariableToSessionUserFunctionCall, FixArrayLiteral, PrependUnqualifiedPgTableName,
    RemoveTableFunctionQualifier, RemoveUnsupportedTypes, ResolveUnqualifiedIdentifer,
    RewriteArrayAnyAllOperation, SqlStatementRewriteRule,
};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ToDFSchema;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::{Expr, Ident, ObjectName, Statement as SqlStatement};
use log::{info, warn};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::QueryParser;
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;
use tokio::sync::Mutex;

use arrow_pg::datatypes::df;
use arrow_pg::datatypes::{arrow_schema_to_pg_fields, into_pg_type};

// Metadata keys for session-level settings
const METADATA_STATEMENT_TIMEOUT: &str = "statement_timeout_ms";

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
    timezone: Arc<Mutex<String>>,
    auth_manager: Arc<AuthManager>,
    sql_rewrite_rules: Vec<Arc<dyn SqlStatementRewriteRule>>,
}

impl DfSessionService {
    pub fn new(
        session_context: Arc<SessionContext>,
        auth_manager: Arc<AuthManager>,
    ) -> DfSessionService {
        let sql_rewrite_rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![
            // make sure blacklist based rewriter it on the top to prevent sql
            // being rewritten from other rewriters
            Arc::new(BlacklistSqlRewriter::new()),
            Arc::new(AliasDuplicatedProjectionRewrite),
            Arc::new(ResolveUnqualifiedIdentifer),
            Arc::new(RemoveUnsupportedTypes::new()),
            Arc::new(RewriteArrayAnyAllOperation),
            Arc::new(PrependUnqualifiedPgTableName),
            Arc::new(FixArrayLiteral),
            Arc::new(RemoveTableFunctionQualifier),
            Arc::new(CurrentUserVariableToSessionUserFunctionCall),
        ];
        let parser = Arc::new(Parser {
            session_context: session_context.clone(),
            sql_rewrite_rules: sql_rewrite_rules.clone(),
        });
        DfSessionService {
            session_context,
            parser,
            timezone: Arc::new(Mutex::new("UTC".to_string())),
            auth_manager,
            sql_rewrite_rules,
        }
    }

    /// Get statement timeout from client metadata
    fn get_statement_timeout<C>(client: &C) -> Option<std::time::Duration>
    where
        C: ClientInfo,
    {
        client
            .metadata()
            .get(METADATA_STATEMENT_TIMEOUT)
            .and_then(|s| s.parse::<u64>().ok())
            .map(std::time::Duration::from_millis)
    }

    /// Set statement timeout in client metadata
    fn set_statement_timeout<C>(client: &mut C, timeout: Option<std::time::Duration>)
    where
        C: ClientInfo,
    {
        let metadata = client.metadata_mut();
        if let Some(duration) = timeout {
            metadata.insert(
                METADATA_STATEMENT_TIMEOUT.to_string(),
                duration.as_millis().to_string(),
            );
        } else {
            metadata.remove(METADATA_STATEMENT_TIMEOUT);
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

    fn mock_show_response<'a>(name: &str, value: &str) -> PgWireResult<QueryResponse<'a>> {
        let fields = vec![FieldInfo::new(
            name.to_string(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        )];

        let row = {
            let mut encoder = pgwire::api::results::DataRowEncoder::new(Arc::new(fields.clone()));
            encoder.encode_field(&Some(value))?;
            encoder.finish()
        };

        let row_stream = futures::stream::once(async move { row });
        Ok(QueryResponse::new(Arc::new(fields), Box::pin(row_stream)))
    }

    /// Handle structured SET statements from parsed AST (replaces string matching)
    async fn handle_set_statement_structured<'a, C>(
        &self,
        client: &mut C,
        variables: &[ObjectName],
        value: &[Expr],
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo,
    {
        let var_name = variables.first().map(|v| v.to_string()).unwrap_or_default();
        match var_name.to_lowercase().as_str() {
            "time_zone" | "timezone" => {
                if let Some(val) = value.first() {
                    let val_str = val.to_string();
                    let tz = val_str.trim_matches('"').trim_matches('\'');
                    let mut timezone = self.timezone.lock().await;
                    *timezone = tz.to_string();
                    Ok(Response::Execution(Tag::new("SET")))
                } else {
                    Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "42601".to_string(),
                            "Invalid SET TIME ZONE value".to_string(),
                        ),
                    )))
                }
            }
            "statement_timeout" => {
                if let Some(val) = value.first() {
                    let val_str = val.to_string();
                    let timeout_str = val_str.trim_matches('"').trim_matches('\'');

                    let timeout = if timeout_str == "0" || timeout_str.is_empty() {
                        None
                    } else {
                        // Parse timeout value (supports ms, s, min formats)
                        let timeout_ms = if timeout_str.ends_with("ms") {
                            timeout_str.trim_end_matches("ms").parse::<u64>()
                        } else if timeout_str.ends_with("s") {
                            timeout_str
                                .trim_end_matches("s")
                                .parse::<u64>()
                                .map(|s| s * 1000)
                        } else if timeout_str.ends_with("min") {
                            timeout_str
                                .trim_end_matches("min")
                                .parse::<u64>()
                                .map(|m| m * 60 * 1000)
                        } else {
                            // Default to milliseconds
                            timeout_str.parse::<u64>()
                        };

                        match timeout_ms {
                            Ok(ms) if ms > 0 => Some(std::time::Duration::from_millis(ms)),
                            _ => None,
                        }
                    };

                    Self::set_statement_timeout(client, timeout);
                    Ok(Response::Execution(Tag::new("SET")))
                } else {
                    Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "42601".to_string(),
                            "Invalid SET statement_timeout value".to_string(),
                        ),
                    )))
                }
            }
            _ => {
                // Pass unknown SET statements to DataFusion
                let set_sql = format!(
                    "SET {} = {}",
                    var_name,
                    value
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if let Err(e) = self.session_context.sql(&set_sql).await {
                    warn!("SET statement {set_sql} is not supported by datafusion, error {e}, statement ignored");
                }
                Ok(Response::Execution(Tag::new("SET")))
            }
        }
    }

    /// Handle structured SHOW statements from parsed AST (replaces string matching)
    async fn handle_show_statement_structured<'a, C>(
        &self,
        client: &C,
        variable: &[Ident],
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo,
    {
        let var_name = variable
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join("_");
        match var_name.to_lowercase().as_str() {
            "time_zone" | "timezone" => {
                let timezone = self.timezone.lock().await.clone();
                let resp = Self::mock_show_response("TimeZone", &timezone)?;
                Ok(Response::Query(resp))
            }
            "server_version" => {
                let resp = Self::mock_show_response("server_version", "15.0 (DataFusion)")?;
                Ok(Response::Query(resp))
            }
            "transaction_isolation" => {
                let resp = Self::mock_show_response("transaction_isolation", "read uncommitted")?;
                Ok(Response::Query(resp))
            }
            "search_path" => {
                let default_schema = "public";
                let resp = Self::mock_show_response("search_path", default_schema)?;
                Ok(Response::Query(resp))
            }
            "statement_timeout" => {
                let timeout = Self::get_statement_timeout(client);
                let timeout_str = match timeout {
                    Some(duration) => format!("{}ms", duration.as_millis()),
                    None => "0".to_string(),
                };
                let resp = Self::mock_show_response("statement_timeout", &timeout_str)?;
                Ok(Response::Query(resp))
            }
            _ => {
                let catalogs = self.session_context.catalog_names();
                let value = catalogs.join(", ");
                let resp = Self::mock_show_response(&var_name, &value)?;
                Ok(Response::Query(resp))
            }
        }
    }

    /// Handle structured statements using AST instead of fragile string matching
    async fn try_handle_structured_statement<'a, C>(
        &self,
        client: &mut C,
        statement: &SqlStatement,
    ) -> PgWireResult<Option<Response<'a>>>
    where
        C: ClientInfo,
    {
        match statement {
            SqlStatement::SetVariable {
                variables, value, ..
            } => {
                let response = self
                    .handle_set_statement_structured(client, variables, value)
                    .await?;
                Ok(Some(response))
            }
            SqlStatement::ShowVariable { variable } => {
                let response = self
                    .handle_show_statement_structured(client, variable)
                    .await?;
                Ok(Some(response))
            }
            _ => Ok(None),
        }
    }

    /// Legacy string-based SET statement handler (deprecated - use structured AST instead)
    #[deprecated(note = "Use try_handle_structured_statement instead")]
    async fn try_respond_set_statements<'a, C>(
        &self,
        client: &mut C,
        query_lower: &str,
    ) -> PgWireResult<Option<Response<'a>>>
    where
        C: ClientInfo,
    {
        if query_lower.starts_with("set") {
            if query_lower.starts_with("set time zone") {
                let parts: Vec<&str> = query_lower.split_whitespace().collect();
                if parts.len() >= 4 {
                    let tz = parts[3].trim_matches('"');
                    let mut timezone = self.timezone.lock().await;
                    *timezone = tz.to_string();
                    Ok(Some(Response::Execution(Tag::new("SET"))))
                } else {
                    Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "42601".to_string(),
                            "Invalid SET TIME ZONE syntax".to_string(),
                        ),
                    )))
                }
            } else if query_lower.starts_with("set statement_timeout") {
                let parts: Vec<&str> = query_lower.split_whitespace().collect();
                if parts.len() >= 3 {
                    let timeout_str = parts[2].trim_matches('"').trim_matches('\'');

                    let timeout = if timeout_str == "0" || timeout_str.is_empty() {
                        None
                    } else {
                        // Parse timeout value (supports ms, s, min formats)
                        let timeout_ms = if timeout_str.ends_with("ms") {
                            timeout_str.trim_end_matches("ms").parse::<u64>()
                        } else if timeout_str.ends_with("s") {
                            timeout_str
                                .trim_end_matches("s")
                                .parse::<u64>()
                                .map(|s| s * 1000)
                        } else if timeout_str.ends_with("min") {
                            timeout_str
                                .trim_end_matches("min")
                                .parse::<u64>()
                                .map(|m| m * 60 * 1000)
                        } else {
                            // Default to milliseconds
                            timeout_str.parse::<u64>()
                        };

                        match timeout_ms {
                            Ok(ms) if ms > 0 => Some(std::time::Duration::from_millis(ms)),
                            _ => None,
                        }
                    };

                    Self::set_statement_timeout(client, timeout);
                    Ok(Some(Response::Execution(Tag::new("SET"))))
                } else {
                    Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "42601".to_string(),
                            "Invalid SET statement_timeout syntax".to_string(),
                        ),
                    )))
                }
            } else {
                // pass SET query to datafusion
                if let Err(e) = self.session_context.sql(query_lower).await {
                    warn!("SET statement {query_lower} is not supported by datafusion, error {e}, statement ignored");
                }

                // Always return SET success
                Ok(Some(Response::Execution(Tag::new("SET"))))
            }
        } else {
            Ok(None)
        }
    }

    async fn try_respond_transaction_statements<'a, C>(
        &self,
        client: &C,
        query_lower: &str,
    ) -> PgWireResult<Option<Response<'a>>>
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

    /// Legacy string-based SHOW statement handler (deprecated - use structured AST instead) 
    #[deprecated(note = "Use try_handle_structured_statement instead")]
    async fn try_respond_show_statements<'a, C>(
        &self,
        client: &C,
        query_lower: &str,
    ) -> PgWireResult<Option<Response<'a>>>
    where
        C: ClientInfo,
    {
        if query_lower.starts_with("show ") {
            match query_lower.strip_suffix(";").unwrap_or(query_lower) {
                "show time zone" => {
                    let timezone = self.timezone.lock().await.clone();
                    let resp = Self::mock_show_response("TimeZone", &timezone)?;
                    Ok(Some(Response::Query(resp)))
                }
                "show server_version" => {
                    let resp = Self::mock_show_response("server_version", "15.0 (DataFusion)")?;
                    Ok(Some(Response::Query(resp)))
                }
                "show transaction_isolation" => {
                    let resp =
                        Self::mock_show_response("transaction_isolation", "read uncommitted")?;
                    Ok(Some(Response::Query(resp)))
                }
                "show catalogs" => {
                    let catalogs = self.session_context.catalog_names();
                    let value = catalogs.join(", ");
                    let resp = Self::mock_show_response("Catalogs", &value)?;
                    Ok(Some(Response::Query(resp)))
                }
                "show search_path" => {
                    let default_schema = "public";
                    let resp = Self::mock_show_response("search_path", default_schema)?;
                    Ok(Some(Response::Query(resp)))
                }
                "show statement_timeout" => {
                    let timeout = Self::get_statement_timeout(client);
                    let timeout_str = match timeout {
                        Some(duration) => format!("{}ms", duration.as_millis()),
                        None => "0".to_string(),
                    };
                    let resp = Self::mock_show_response("statement_timeout", &timeout_str)?;
                    Ok(Some(Response::Query(resp)))
                }
                "show transaction isolation level" => {
                    let resp = Self::mock_show_response("transaction_isolation", "read_committed")?;
                    Ok(Some(Response::Query(resp)))
                }
                _ => {
                    info!("Unsupported show statement: {query_lower}");
                    let resp = Self::mock_show_response("unsupported_show_statement", "")?;
                    Ok(Some(Response::Query(resp)))
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<'a, C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
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

        let mut statements = parse(query).map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // TODO: deal with multiple statements
        let mut statement = statements.remove(0);

        // Handle SET/SHOW statements using structured AST (replaces fragile string matching)
        if let Some(resp) = self
            .try_handle_structured_statement(client, &statement)
            .await?
        {
            return Ok(vec![resp]);
        }

        // Attempt to rewrite
        statement = rewrite(statement, &self.sql_rewrite_rules);

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

        // SET/SHOW statements now handled by structured AST parsing above

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
            let timeout = Self::get_statement_timeout(client);
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
            Ok(vec![Response::Execution(tag)])
        } else {
            // For non-INSERT queries, return a regular Query response
            let resp = df::encode_dataframe(df, &Format::UnifiedText).await?;
            Ok(vec![Response::Query(resp)])
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for DfSessionService {
    type Statement = (String, LogicalPlan);
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
        let (_, plan) = &target.statement;
        let schema = plan.schema();
        let fields = arrow_schema_to_pg_fields(schema.as_arrow(), &Format::UnifiedBinary)?;
        let params = plan
            .get_parameter_types()
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

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (_, plan) = &target.statement.statement;
        let format = &target.result_column_format;
        let schema = plan.schema();
        let fields = arrow_schema_to_pg_fields(schema.as_arrow(), format)?;

        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let original_sql = &portal.statement.statement.0;
        log::debug!("Received execute extended query: {original_sql}"); // Log for debugging

        // Handle SET/SHOW statements using structured AST (re-parse for AST access)
        if let Ok(parsed_statements) = crate::sql::parse(original_sql) {
            if let Some(statement) = parsed_statements.first() {
                if let Some(resp) = self.try_handle_structured_statement(client, statement).await? {
                    return Ok(resp);
                }
            }
        }

        // Handle transaction statements
        let query_lower = original_sql.to_lowercase().trim().to_string();
        if let Some(resp) = self
            .try_respond_transaction_statements(client, &query_lower)
            .await?
        {
            return Ok(resp);
        }

        // Check permissions for non-SET/SHOW/transaction statements
        if !query_lower.starts_with("set") 
            && !query_lower.starts_with("show") 
            && !query_lower.starts_with("begin")
            && !query_lower.starts_with("commit")
            && !query_lower.starts_with("rollback") {
            self.check_query_permission(client, original_sql).await?;
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

        let (_, plan) = &portal.statement.statement;

        let param_types = plan
            .get_parameter_types()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let param_values = df::deserialize_parameters(portal, &ordered_param_types(&param_types))?; // Fixed: Use &param_types

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
            let timeout = Self::get_statement_timeout(client);
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
        let resp = df::encode_dataframe(dataframe, &portal.result_column_format).await?;
        Ok(Response::Query(resp))
    }
}

pub struct Parser {
    session_context: Arc<SessionContext>,
    sql_rewrite_rules: Vec<Arc<dyn SqlStatementRewriteRule>>,
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

        // Parse and check for SET/SHOW statements using structured AST
        if let Ok(parsed_statements) = crate::sql::parse(sql) {
            if let Some(statement) = parsed_statements.first() {
                if matches!(statement, SqlStatement::SetVariable { .. } | SqlStatement::ShowVariable { .. }) {
                    // Return a dummy plan for SET/SHOW commands - they'll be handled by structured handler
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
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = (String, LogicalPlan);

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Type],
    ) -> PgWireResult<Self::Statement> {
        log::debug!("Received parse extended query: {sql}"); // Log for debugging

        // Check for transaction commands that shouldn't be parsed by DataFusion
        if let Some(plan) = self
            .try_shortcut_parse_plan(sql)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?
        {
            return Ok((sql.to_string(), plan));
        }

        let mut statements = parse(sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let mut statement = statements.remove(0);

        // Attempt to rewrite
        statement = rewrite(statement, &self.sql_rewrite_rules);

        let query = statement.to_string();

        let context = &self.session_context;
        let state = context.state();
        let logical_plan = state
            .statement_to_plan(Statement::Statement(Box::new(statement)))
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        Ok((query, logical_plan))
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
    use super::*;
    use crate::auth::AuthManager;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use std::time::Duration;

    struct MockClient {
        metadata: HashMap<String, String>,
    }

    impl MockClient {
        fn new() -> Self {
            Self {
                metadata: HashMap::new(),
            }
        }
    }

    impl ClientInfo for MockClient {
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
            _secret_key: pgwire::messages::startup::SecretKey,
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

        fn metadata(&self) -> &HashMap<String, String> {
            &self.metadata
        }

        fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
            &mut self.metadata
        }

        fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
            None
        }
    }

    #[tokio::test]
    async fn test_statement_timeout_set_and_show() {
        let session_context = Arc::new(SessionContext::new());
        let auth_manager = Arc::new(AuthManager::new());
        let service = DfSessionService::new(session_context, auth_manager);
        let mut client = MockClient::new();

        // Test setting timeout to 5000ms
        let set_response = service
            .try_respond_set_statements(&mut client, "set statement_timeout '5000ms'")
            .await
            .unwrap();
        assert!(set_response.is_some());

        // Verify the timeout was set in client metadata
        let timeout = DfSessionService::get_statement_timeout(&client);
        assert_eq!(timeout, Some(Duration::from_millis(5000)));

        // Test SHOW statement_timeout
        let show_response = service
            .try_respond_show_statements(&client, "show statement_timeout")
            .await
            .unwrap();
        assert!(show_response.is_some());
    }

    #[tokio::test]
    async fn test_statement_timeout_disable() {
        let session_context = Arc::new(SessionContext::new());
        let auth_manager = Arc::new(AuthManager::new());
        let service = DfSessionService::new(session_context, auth_manager);
        let mut client = MockClient::new();

        // Set timeout first
        service
            .try_respond_set_statements(&mut client, "set statement_timeout '1000ms'")
            .await
            .unwrap();

        // Disable timeout with 0
        service
            .try_respond_set_statements(&mut client, "set statement_timeout '0'")
            .await
            .unwrap();

        let timeout = DfSessionService::get_statement_timeout(&client);
        assert_eq!(timeout, None);
    }

    #[tokio::test]
    async fn test_structured_vs_string_statement_handling() {
        let session_context = Arc::new(SessionContext::new());
        let auth_manager = Arc::new(AuthManager::new());
        let service = DfSessionService::new(session_context, auth_manager);
        let mut client = MockClient::new();

        // Test that structured parsing works for complex SET statements
        use crate::sql::parse;

        // Test with equals sign (structured parsing should handle this better)
        let statements = parse("SET statement_timeout = '5000ms'").unwrap();
        let result = service
            .try_handle_structured_statement(&mut client, &statements[0])
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Verify timeout was set correctly via structured parsing
        let timeout = DfSessionService::get_statement_timeout(&client);
        assert_eq!(timeout, Some(Duration::from_millis(5000)));

        // Test SHOW with structured parsing
        let show_statements = parse("SHOW statement_timeout").unwrap();
        let show_result = service
            .try_handle_structured_statement(&mut client, &show_statements[0])
            .await;
        assert!(show_result.is_ok());
        assert!(show_result.unwrap().is_some());
    }
}
