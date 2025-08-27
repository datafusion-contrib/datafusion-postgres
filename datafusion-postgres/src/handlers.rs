use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::{AuthManager, Permission, ResourceType};
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
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
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;
use tokio::sync::Mutex;

use arrow_pg::datatypes::df;
use arrow_pg::datatypes::{arrow_schema_to_pg_fields, into_pg_type};

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
}

/// The pgwire handler backed by a datafusion `SessionContext`
pub struct DfSessionService {
    session_context: Arc<SessionContext>,
    parser: Arc<Parser>,
    timezone: Arc<Mutex<String>>,
    auth_manager: Arc<AuthManager>,
}

impl DfSessionService {
    pub fn new(
        session_context: Arc<SessionContext>,
        auth_manager: Arc<AuthManager>,
    ) -> DfSessionService {
        let parser = Arc::new(Parser {
            session_context: session_context.clone(),
        });
        DfSessionService {
            session_context,
            parser,
            timezone: Arc::new(Mutex::new("UTC".to_string())),
            auth_manager,
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

    async fn try_respond_set_statements<'a>(
        &self,
        query_lower: &str,
    ) -> PgWireResult<Option<Response<'a>>> {
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
            } else {
                // pass SET query to datafusion
                let df = self
                    .session_context
                    .sql(query_lower)
                    .await
                    .map_err(|err| PgWireError::ApiError(Box::new(err)))?;

                let resp = df::encode_dataframe(df, &Format::UnifiedText).await?;
                Ok(Some(Response::Query(resp)))
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
                    TransactionStatus::Idle | TransactionStatus::Transaction => {
                        Ok(Some(Response::TransactionStart(Tag::new("BEGIN"))))
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

    async fn try_respond_show_statements<'a>(
        &self,
        query_lower: &str,
    ) -> PgWireResult<Option<Response<'a>>> {
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
                    let default_catalog = "datafusion";
                    let resp = Self::mock_show_response("search_path", default_catalog)?;
                    Ok(Some(Response::Query(resp)))
                }
                _ => Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_string(),
                        "42704".to_string(),
                        format!("Unrecognized SHOW command: {query_lower}"),
                    ),
                ))),
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
        let query_lower = query.to_lowercase().trim().to_string();
        log::debug!("Received query: {query}"); // Log the query for debugging

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
            self.check_query_permission(client, query).await?;
        }

        if let Some(resp) = self.try_respond_set_statements(&query_lower).await? {
            return Ok(vec![resp]);
        }

        if let Some(resp) = self
            .try_respond_transaction_statements(client, &query_lower)
            .await?
        {
            return Ok(vec![resp]);
        }

        if let Some(resp) = self.try_respond_show_statements(&query_lower).await? {
            return Ok(vec![resp]);
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

        let df_result = self.session_context.sql(query).await;

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
        let query = portal
            .statement
            .statement
            .0
            .to_lowercase()
            .trim()
            .to_string();
        log::debug!("Received execute extended query: {query}"); // Log for debugging

        // Check permissions for the query (skip for SET and SHOW statements)
        if !query.starts_with("set") && !query.starts_with("show") {
            self.check_query_permission(client, &portal.statement.statement.0)
                .await?;
        }

        if let Some(resp) = self.try_respond_set_statements(&query).await? {
            return Ok(resp);
        }

        if let Some(resp) = self
            .try_respond_transaction_statements(client, &query)
            .await?
        {
            return Ok(resp);
        }

        if let Some(resp) = self.try_respond_show_statements(&query).await? {
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

        let dataframe = match self.session_context.execute_logical_plan(optimised).await {
            Ok(df) => df,
            Err(e) => {
                return Err(PgWireError::ApiError(Box::new(e)));
            }
        };
        let resp = df::encode_dataframe(dataframe, &portal.result_column_format).await?;
        Ok(Response::Query(resp))
    }
}

pub struct Parser {
    session_context: Arc<SessionContext>,
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
        let context = &self.session_context;
        let state = context.state();
        let logical_plan = state
            .create_logical_plan(sql)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        Ok((sql.to_string(), logical_plan))
    }
}

fn ordered_param_types(types: &HashMap<String, Option<DataType>>) -> Vec<Option<&DataType>> {
    // Datafusion stores the parameters as a map.  In our case, the keys will be
    // `$1`, `$2` etc.  The values will be the parameter types.
    let mut types = types.iter().collect::<Vec<_>>();
    types.sort_by(|a, b| a.0.cmp(b.0));
    types.into_iter().map(|pt| pt.1.as_ref()).collect()
}
