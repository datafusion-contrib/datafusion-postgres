use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ParamValues, ToDFSchema};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use log::{info, warn};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use postgres_types::Type;

use crate::client;
use crate::QueryHook;

#[derive(Debug)]
pub struct SetShowHook;

#[async_trait]
impl QueryHook for SetShowHook {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set { .. } => {
                let query = statement.to_string();
                let query_lower = query.to_lowercase();

                try_respond_set_statements(client, &query_lower, session_context).await
            }
            Statement::ShowVariable { .. } | Statement::ShowStatus { .. } => {
                let query = statement.to_string();
                let query_lower = query.to_lowercase();

                try_respond_show_statements(client, &query_lower, session_context).await
            }
            _ => None,
        }
    }

    async fn handle_extended_parse_query(
        &self,
        stmt: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let sql_lower = stmt.to_string().to_lowercase();
        let sql_trimmed = sql_lower.trim();

        if sql_trimmed.starts_with("show") {
            let show_schema =
                Arc::new(Schema::new(vec![Field::new("show", DataType::Utf8, false)]));
            let result = show_schema
                .to_dfschema()
                .map(|df_schema| {
                    LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(df_schema),
                    })
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)));
            Some(result)
        } else if sql_trimmed.starts_with("set") {
            let show_schema = Arc::new(Schema::new(Vec::<Field>::new()));
            let result = show_schema
                .to_dfschema()
                .map(|df_schema| {
                    LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(df_schema),
                    })
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)));
            Some(result)
        } else {
            None
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set { .. } => {
                let query = statement.to_string();
                let query_lower = query.to_lowercase();

                try_respond_set_statements(client, &query_lower, session_context).await
            }
            Statement::ShowVariable { .. } | Statement::ShowStatus { .. } => {
                let query = statement.to_string();
                let query_lower = query.to_lowercase();

                try_respond_show_statements(client, &query_lower, session_context).await
            }
            _ => None,
        }
    }
}

fn mock_show_response(name: &str, value: &str) -> PgWireResult<QueryResponse> {
    let fields = vec![FieldInfo::new(
        name.to_string(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )];

    let row = {
        let mut encoder = DataRowEncoder::new(Arc::new(fields.clone()));
        encoder.encode_field(&Some(value))?;
        encoder.finish()
    };

    let row_stream = futures::stream::once(async move { row });
    Ok(QueryResponse::new(Arc::new(fields), Box::pin(row_stream)))
}

async fn try_respond_set_statements<C>(
    client: &mut C,
    query_lower: &str,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>>
where
    C: ClientInfo + Send + Sync + ?Sized,
{
    if query_lower.starts_with("set") {
        let result = if query_lower.starts_with("set time zone") {
            let parts: Vec<&str> = query_lower.split_whitespace().collect();
            if parts.len() >= 4 {
                let tz = parts[3].trim_matches('"');
                client::set_timezone(client, Some(tz));
                Ok(Response::Execution(Tag::new("SET")))
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

                client::set_statement_timeout(client, timeout);
                Ok(Response::Execution(Tag::new("SET")))
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
            if let Err(e) = session_context.sql(query_lower).await {
                warn!("SET statement {query_lower} is not supported by datafusion, error {e}, statement ignored");
            }

            // Always return SET success
            Ok(Response::Execution(Tag::new("SET")))
        };

        Some(result)
    } else {
        None
    }
}

async fn try_respond_show_statements<C>(
    client: &C,
    query_lower: &str,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>>
where
    C: ClientInfo + ?Sized,
{
    if query_lower.starts_with("show ") {
        let result = match query_lower.strip_suffix(";").unwrap_or(query_lower) {
            "show time zone" => {
                let timezone = client::get_timezone(client).unwrap_or("UTC");
                mock_show_response("TimeZone", &timezone).map(Response::Query)
            }
            "show server_version" => {
                mock_show_response("server_version", "15.0 (DataFusion)").map(Response::Query)
            }
            "show transaction_isolation" => {
                mock_show_response("transaction_isolation", "read uncommitted").map(Response::Query)
            }
            "show catalogs" => {
                let catalogs = session_context.catalog_names();
                let value = catalogs.join(", ");
                mock_show_response("Catalogs", &value).map(Response::Query)
            }
            "show search_path" => {
                let default_schema = "public";
                mock_show_response("search_path", default_schema).map(Response::Query)
            }
            "show statement_timeout" => {
                let timeout = client::get_statement_timeout(client);
                let timeout_str = match timeout {
                    Some(duration) => format!("{}ms", duration.as_millis()),
                    None => "0".to_string(),
                };
                mock_show_response("statement_timeout", &timeout_str).map(Response::Query)
            }
            "show transaction isolation level" => {
                mock_show_response("transaction_isolation", "read_committed").map(Response::Query)
            }
            _ => {
                info!("Unsupported show statement: {query_lower}");
                mock_show_response("unsupported_show_statement", "").map(Response::Query)
            }
        };
        Some(result)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::MockClient;

    #[tokio::test]
    async fn test_statement_timeout_set_and_show() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Test setting timeout to 5000ms
        let set_response = try_respond_set_statements(
            &mut client,
            "set statement_timeout '5000ms'",
            &session_context,
        )
        .await;

        assert!(set_response.is_some());
        assert!(set_response.unwrap().is_ok());

        // Verify the timeout was set in client metadata
        let timeout = client::get_statement_timeout(&client);
        assert_eq!(timeout, Some(Duration::from_millis(5000)));

        // Test SHOW statement_timeout
        let show_response =
            try_respond_show_statements(&client, "show statement_timeout", &session_context).await;

        assert!(show_response.is_some());
        assert!(show_response.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_statement_timeout_disable() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Set timeout first
        let resp = try_respond_set_statements(
            &mut client,
            "set statement_timeout '1000ms'",
            &session_context,
        )
        .await;
        assert!(resp.is_some());
        assert!(resp.unwrap().is_ok());

        // Disable timeout with 0
        let resp =
            try_respond_set_statements(&mut client, "set statement_timeout '0'", &session_context)
                .await;
        assert!(resp.is_some());
        assert!(resp.unwrap().is_ok());

        let timeout = client::get_statement_timeout(&client);
        assert_eq!(timeout, None);
    }
}
