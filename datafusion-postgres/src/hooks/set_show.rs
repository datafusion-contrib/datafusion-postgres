use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ParamValues, ToDFSchema};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    LogicalPlan, ResetVariable, SetVariable, Statement as PlanStatement,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, Reset, ResetStatement, Set, Statement, Value};
use log::{info, warn};
use pgwire::api::ClientInfo;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::startup::ParameterStatus;
use pgwire::types::format::FormatOptions;
use postgres_types::Type;

use crate::QueryHook;
use crate::client;
use crate::hooks::HookClient;

#[derive(Debug)]
pub struct SetShowHook;

#[async_trait]
impl QueryHook for SetShowHook {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set { .. } => {
                try_respond_set_statements(client, statement, session_context).await
            }
            Statement::Reset { .. } => {
                try_respond_reset_statements(client, statement, session_context).await
            }
            Statement::ShowVariable { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowCatalogs { .. } => {
                try_respond_show_statements(client, statement, session_context).await
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
        match stmt {
            Statement::Set { .. } | Statement::Reset { .. } => {
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
            }
            Statement::ShowVariable { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowCatalogs { .. } => {
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
            }
            _ => None,
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set { .. } => {
                try_respond_set_statements(client, statement, session_context).await
            }
            Statement::Reset { .. } => {
                try_respond_reset_statements(client, statement, session_context).await
            }
            Statement::ShowVariable { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowCatalogs { .. } => {
                try_respond_show_statements(client, statement, session_context).await
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
        Ok(encoder.take_row())
    };

    let row_stream = futures::stream::once(async move { row });
    Ok(QueryResponse::new(Arc::new(fields), Box::pin(row_stream)))
}

async fn try_respond_set_statements(
    client: &mut dyn HookClient,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>> {
    let Statement::Set(set_statement) = statement else {
        return None;
    };

    match &set_statement {
        Set::SingleAssignment {
            scope: None,
            hivevar: false,
            variable,
            values,
        } => {
            let var = variable.to_string().to_lowercase();
            if var == "statement_timeout" {
                let value = values[0].to_string();
                let timeout_str = value.trim_matches('"').trim_matches('\'');

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
                return Some(Ok(Response::Execution(Tag::new("SET"))));
            } else if matches!(
                var.as_str(),
                "datestyle"
                    | "bytea_output"
                    | "intervalstyle"
                    | "application_name"
                    | "extra_float_digits"
                    | "search_path"
            ) && !values.is_empty()
            {
                // postgres configuration variables
                let value = values[0].clone();
                if let Expr::Value(value) = value {
                    let val_str = value.into_string().unwrap_or_else(|| "".to_string());
                    client.metadata_mut().insert(var.clone(), val_str);
                    if let Some((name, value)) = parameter_status_for_var(&var, &*client)
                        && let Err(e) = client
                            .send_message(PgWireBackendMessage::ParameterStatus(
                                ParameterStatus::new(name, value),
                            ))
                            .await
                    {
                        return Some(Err(e));
                    }
                    return Some(Ok(Response::Execution(Tag::new("SET"))));
                }
            } else if is_time_zone_parameter(&var) {
                // `SET timezone TO <zone>` and friends: the session owns the
                // time zone. Values this hook cannot read fall through to the
                // generic path below.
                if let Some(result) =
                    apply_time_zone(client, values.as_slice(), session_context).await
                {
                    return Some(result);
                }
            }
        }
        Set::SetTimeZone {
            local: false,
            value,
        } => {
            if let Some(result) =
                apply_time_zone(client, std::slice::from_ref(value), session_context).await
            {
                return Some(result);
            }
        }
        _ => {}
    }

    // fallback to datafusion and ignore all errors
    if let Err(e) = execute_set_statement(session_context, statement.clone()).await {
        warn!(
            "SET statement {statement} is not supported by datafusion, error {e}, statement ignored",
        );
    }

    // Always return SET success
    Some(Ok(Response::Execution(Tag::new("SET"))))
}

/// Handle `RESET`. As with `SET`, DataFusion owns the settings and what it
/// does not know is ignored rather than reported.
async fn try_respond_reset_statements(
    client: &mut dyn HookClient,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>> {
    let Statement::Reset(ResetStatement { reset }) = statement else {
        return None;
    };

    match reset {
        Reset::ConfigurationParameter(name) => {
            // Mirrors DataFusion's own `RESET` planning.
            let name = name.to_string().to_lowercase();
            let variable = if is_time_zone_parameter(&name) {
                "datafusion.execution.time_zone"
            } else {
                &name
            };
            if let Err(e) = session_context
                .execute_logical_plan(LogicalPlan::Statement(PlanStatement::ResetVariable(
                    ResetVariable {
                        variable: variable.to_string(),
                    },
                )))
                .await
            {
                warn!(
                    "RESET statement {statement} is not supported by datafusion, error {e}, statement ignored",
                );
            }
        }
        other => {
            warn!("RESET statement {statement} is not supported ({other:?}), statement ignored");
        }
    }

    // The client's view of the time zone follows the session's setting.
    if let Err(e) = sync_client_timezone(client, session_context).await {
        return Some(Err(e));
    }

    Some(Ok(Response::Execution(Tag::new("RESET"))))
}

/// The session's effective time zone as the user wrote it, or `None` while
/// the session uses the default.
fn session_time_zone(session_context: &SessionContext) -> Option<String> {
    session_context
        .state()
        .config()
        .options()
        .execution
        .time_zone
        .as_ref()
        .map(|tz| tz.as_str().to_string())
}

/// Whether a `SET`/`RESET` target names `datafusion.execution.time_zone`,
/// under any of the spellings DataFusion maps onto it.
fn is_time_zone_parameter(parameter: &str) -> bool {
    matches!(
        parameter,
        "timezone" | "time.zone" | "datafusion.execution.time_zone"
    )
}

/// Set the time zone of a connection's session.
///
/// The value goes through DataFusion's own `SET` handling rather than being
/// written to the config directly: an unknown zone is rejected by the config
/// (leaving the current setting untouched), and the functions that depend on
/// the time zone (such as `now()`) are re-initialized with the new value.
async fn set_session_timezone(
    session_context: &SessionContext,
    zone: &str,
) -> Result<(), DataFusionError> {
    session_context
        .execute_logical_plan(LogicalPlan::Statement(PlanStatement::SetVariable(
            SetVariable {
                variable: "datafusion.execution.time_zone".to_string(),
                value: zone.to_string(),
            },
        )))
        .await
        .map(|_| ())
}

/// Reset the time zone of a connection's session to its default.
async fn reset_session_timezone(session_context: &SessionContext) -> Result<(), DataFusionError> {
    session_context
        .execute_logical_plan(LogicalPlan::Statement(PlanStatement::ResetVariable(
            ResetVariable {
                variable: "datafusion.execution.time_zone".to_string(),
            },
        )))
        .await
        .map(|_| ())
}

/// The value of a time zone assignment, as written in the statement.
enum TimeZoneValue<'a> {
    Zone(&'a str),
    /// `DEFAULT`, and `LOCAL` (there is no server time zone to fall back to):
    /// reset to the default.
    Default,
    /// Anything else: leave the statement to the generic `SET` path.
    Other,
}

fn time_zone_value(expr: &Expr) -> TimeZoneValue<'_> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(zone) | Value::DoubleQuotedString(zone) => {
                TimeZoneValue::Zone(zone)
            }
            _ => TimeZoneValue::Other,
        },
        Expr::Identifier(ident) => match ident.value.to_ascii_lowercase().as_str() {
            "default" | "local" => TimeZoneValue::Default,
            _ => TimeZoneValue::Zone(&ident.value),
        },
        _ => TimeZoneValue::Other,
    }
}

/// Handle `SET TIME ZONE <value>` / `SET timezone TO <value>` on a
/// connection's session, then report the effective time zone to the client.
///
/// Returns `None` for a value this hook does not interpret, leaving the
/// statement to the generic `SET` path.
async fn apply_time_zone(
    client: &mut dyn HookClient,
    values: &[Expr],
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>> {
    let [value] = values else {
        return None;
    };
    let applied = match time_zone_value(value) {
        TimeZoneValue::Zone(zone) => set_session_timezone(session_context, zone)
            .await
            .map_err(|e| invalid_time_zone(zone, &e)),
        TimeZoneValue::Default => reset_session_timezone(session_context)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e))),
        TimeZoneValue::Other => return None,
    };
    let response = match applied {
        Ok(()) => sync_client_timezone(client, session_context)
            .await
            .map(|_| Response::Execution(Tag::new("SET"))),
        Err(e) => Err(e),
    };
    Some(response)
}

/// Postgres' error for an unknown time zone (`22023`, invalid_parameter_value).
fn invalid_time_zone(zone: &str, err: &DataFusionError) -> PgWireError {
    let mut info = ErrorInfo::new(
        "ERROR".to_string(),
        "22023".to_string(),
        format!("invalid value for parameter \"TimeZone\": \"{zone}\""),
    );
    info.detail = Some(err.to_string());
    PgWireError::UserError(Box::new(info))
}

/// Make the client's view of the time zone match the session's effective
/// `datafusion.execution.time_zone`, reporting a change like Postgres does.
async fn sync_client_timezone(
    client: &mut dyn HookClient,
    session_context: &SessionContext,
) -> PgWireResult<()> {
    // A session that never set a zone reports `UTC`.
    let effective = session_time_zone(session_context);
    if effective.as_deref() == client::get_timezone(client) {
        return Ok(());
    }
    client::set_timezone(client, effective.as_deref());
    client
        .send_message(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
            "TimeZone".to_string(),
            effective.unwrap_or_else(|| "UTC".to_string()),
        )))
        .await
}

fn parameter_status_for_var(
    var: &str,
    client: &(impl ClientInfo + ?Sized),
) -> Option<(String, String)> {
    let display_name = match var {
        "datestyle" => "DateStyle",
        "intervalstyle" => "IntervalStyle",
        "bytea_output" => "bytea_output",
        "application_name" => "application_name",
        "extra_float_digits" => "extra_float_digits",
        "search_path" => "search_path",
        _ => return None,
    };
    let value = client.metadata().get(var)?.clone();
    Some((display_name.to_string(), value))
}

async fn execute_set_statement(
    session_context: &SessionContext,
    statement: Statement,
) -> Result<(), DataFusionError> {
    let state = session_context.state();
    let logical_plan = state
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
            statement,
        )))
        .await
        .and_then(|logical_plan| state.optimize(&logical_plan))?;

    session_context
        .execute_logical_plan(logical_plan)
        .await
        .map(|_| ())
}

async fn try_respond_show_statements(
    client: &dyn HookClient,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>> {
    // Handle SHOW CATALOGS separately since it's its own variant in sqlparser 0.62+
    if let Statement::ShowCatalogs { .. } = statement {
        let catalogs = session_context.catalog_names();
        let value = catalogs.join(", ");
        return Some(mock_show_response("Catalogs", &value).map(Response::Query));
    }

    let Statement::ShowVariable { variable } = statement else {
        return None;
    };

    let variables = variable
        .iter()
        .map(|v| v.value.to_lowercase())
        .collect::<Vec<_>>();
    let variables_ref = variables.iter().map(|s| s.as_str()).collect::<Vec<_>>();

    match variables_ref.as_slice() {
        ["time", "zone"] | ["timezone"] => {
            // The session owns the time zone; the client metadata mirrors it.
            let timezone = session_time_zone(session_context).unwrap_or_else(|| "UTC".to_string());
            Some(mock_show_response("TimeZone", &timezone).map(Response::Query))
        }
        ["server_version"] => {
            let version = format!(
                "datafusion {} on {} {}",
                session_context.state().version(),
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION")
            );
            Some(mock_show_response("server_version", &version).map(Response::Query))
        }
        ["transaction_isolation"] => Some(
            mock_show_response("transaction_isolation", "read uncommitted").map(Response::Query),
        ),
        ["catalogs"] => {
            let catalogs = session_context.catalog_names();
            let value = catalogs.join(", ");
            Some(mock_show_response("Catalogs", &value).map(Response::Query))
        }
        ["statement_timeout"] => {
            let timeout = client::get_statement_timeout(client);
            let timeout_str = match timeout {
                Some(duration) => format!("{}ms", duration.as_millis()),
                None => "0".to_string(),
            };
            Some(mock_show_response("statement_timeout", &timeout_str).map(Response::Query))
        }
        ["transaction", "isolation", "level"] => {
            Some(mock_show_response("transaction_isolation", "read_committed").map(Response::Query))
        }
        _ => {
            let val = client
                .metadata()
                .get(&variables[0])
                .map(|v| v.to_string())
                .or_else(|| match variables[0].as_str() {
                    "bytea_output" => Some(FormatOptions::default().bytea_output),
                    "datestyle" => Some(FormatOptions::default().date_style),
                    "intervalstyle" => Some(FormatOptions::default().interval_style),
                    "extra_float_digits" => {
                        Some(FormatOptions::default().extra_float_digits.to_string())
                    }
                    "application_name" => Some(
                        DefaultServerParameterProvider::default()
                            .application_name
                            .unwrap_or("".to_owned()),
                    ),
                    "search_path" => Some(DefaultServerParameterProvider::default().search_path),
                    _ => None,
                });
            if let Some(val) = val {
                Some(mock_show_response(&variables[0], &val).map(Response::Query))
            } else {
                info!("Unsupported show statement: {statement}");
                Some(mock_show_response("unsupported_show_statement", "").map(Response::Query))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::arrow::datatypes::TimeUnit;
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use super::*;
    use crate::testing::MockClient;

    fn parse(sql: &str) -> Statement {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statement()
            .unwrap()
    }

    async fn set(
        session_context: &SessionContext,
        client: &mut MockClient,
        sql: &str,
    ) -> PgWireResult<Response> {
        try_respond_set_statements(client, &parse(sql), session_context)
            .await
            .expect("SET is handled by the hook")
    }

    async fn reset(
        session_context: &SessionContext,
        client: &mut MockClient,
        sql: &str,
    ) -> PgWireResult<Response> {
        try_respond_reset_statements(client, &parse(sql), session_context)
            .await
            .expect("RESET is handled by the hook")
    }

    /// What `SHOW TIME ZONE` reports.
    async fn shown_time_zone(client: &MockClient, session_context: &SessionContext) -> String {
        let response =
            try_respond_show_statements(client, &parse("show time zone"), session_context)
                .await
                .expect("SHOW TIME ZONE is handled by the hook")
                .unwrap();
        crate::testing::scalar_text(response).await
    }

    /// The `ParameterStatus` messages sent so far.
    fn parameter_statuses(client: &MockClient) -> Vec<(String, String)> {
        client
            .sent_messages()
            .iter()
            .filter_map(|message| match message {
                PgWireBackendMessage::ParameterStatus(status) => {
                    Some((status.name.clone(), status.value.clone()))
                }
                _ => None,
            })
            .collect()
    }

    fn command_tag(response: Response) -> String {
        let Response::Execution(tag) = response else {
            panic!("Expected Execution response, got: {response:?}");
        };
        pgwire::messages::response::CommandComplete::from(tag).tag
    }

    /// The type `now()` is planned with on `session_context`.
    async fn now_type(session_context: &SessionContext) -> DataType {
        session_context
            .sql("select now()")
            .await
            .unwrap()
            .schema()
            .field(0)
            .data_type()
            .clone()
    }

    fn timestamp_in(zone: Option<&str>) -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, zone.map(Arc::from))
    }

    /// A connection session as the service derives it from a template, with
    /// its client.
    fn connection() -> (SessionContext, SessionContext, MockClient) {
        let template = SessionContext::new();
        let session_context = crate::session::new_connection_session(&template);
        (template, session_context, MockClient::new())
    }
    #[tokio::test]
    async fn test_statement_timeout_set_and_show() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Test setting timeout to 5000ms
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '5000ms'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let set_response =
            try_respond_set_statements(&mut client, &statement, &session_context).await;

        assert!(set_response.is_some());
        assert!(set_response.unwrap().is_ok());

        // Verify the timeout was set in client metadata
        let timeout = client::get_statement_timeout(&client);
        assert_eq!(timeout, Some(Duration::from_millis(5000)));

        // Test SHOW statement_timeout
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("show statement_timeout")
            .unwrap()
            .parse_statement()
            .unwrap();
        let show_response =
            try_respond_show_statements(&client, &statement, &session_context).await;

        assert!(show_response.is_some());
        assert!(show_response.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_bytea_output_set_and_show() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Test setting bytea_output to hex
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set bytea_output = 'hex'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let set_response =
            try_respond_set_statements(&mut client, &statement, &session_context).await;

        assert!(set_response.is_some());
        assert!(set_response.unwrap().is_ok());

        // Verify the value was set in client metadata
        let bytea_output = client.metadata().get("bytea_output").unwrap();
        assert_eq!(bytea_output, "hex");

        // Test SHOW bytea_output
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("show bytea_output")
            .unwrap()
            .parse_statement()
            .unwrap();
        let show_response =
            try_respond_show_statements(&client, &statement, &session_context).await;

        assert!(show_response.is_some());
        assert!(show_response.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_date_style_set_and_show() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Test setting dateStyle
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set dateStyle = 'ISO, DMY'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let set_response =
            try_respond_set_statements(&mut client, &statement, &session_context).await;

        assert!(set_response.is_some());
        assert!(set_response.unwrap().is_ok());

        // Verify the value was set in client metadata
        let bytea_output = client.metadata().get("datestyle").unwrap();
        assert_eq!(bytea_output, "ISO, DMY");

        // Test SHOW dateStyle
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("show dateStyle")
            .unwrap()
            .parse_statement()
            .unwrap();
        let show_response =
            try_respond_show_statements(&client, &statement, &session_context).await;

        assert!(show_response.is_some());
        assert!(show_response.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_statement_timeout_disable() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Set timeout first
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '1000ms'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let resp = try_respond_set_statements(&mut client, &statement, &session_context).await;
        assert!(resp.is_some());
        assert!(resp.unwrap().is_ok());

        // Disable timeout with 0
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '0'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let resp = try_respond_set_statements(&mut client, &statement, &session_context).await;
        assert!(resp.is_some());
        assert!(resp.unwrap().is_ok());

        let timeout = client::get_statement_timeout(&client);
        assert_eq!(timeout, None);
    }

    #[tokio::test]
    async fn test_parameter_status_sent_for_all_set_vars() {
        use pgwire::messages::PgWireBackendMessage;

        let test_cases = vec![
            ("set bytea_output = 'escape'", "bytea_output", "escape"),
            (
                "set intervalstyle = 'postgres'",
                "IntervalStyle",
                "postgres",
            ),
            (
                "set application_name = 'myapp'",
                "application_name",
                "myapp",
            ),
            ("set search_path = 'public'", "search_path", "public"),
            ("set extra_float_digits = '2'", "extra_float_digits", "2"),
            ("set datestyle = 'ISO, MDY'", "DateStyle", "ISO, MDY"),
            (
                "set time zone 'America/New_York'",
                "TimeZone",
                "America/New_York",
            ),
        ];

        for (sql, expected_key, expected_value) in test_cases {
            let session_context = SessionContext::new();
            let mut client = MockClient::new();
            let statement = Parser::new(&PostgreSqlDialect {})
                .try_with_sql(sql)
                .unwrap()
                .parse_statement()
                .unwrap();

            let result =
                try_respond_set_statements(&mut client, &statement, &session_context).await;
            assert!(result.is_some(), "Expected Some for {sql}");
            assert!(result.unwrap().is_ok(), "Expected Ok for {sql}");

            let ps_msgs: Vec<_> = client
                .sent_messages()
                .iter()
                .filter_map(|m| match m {
                    PgWireBackendMessage::ParameterStatus(ps) => Some(ps),
                    _ => None,
                })
                .collect();

            assert_eq!(ps_msgs.len(), 1, "Expected 1 ParameterStatus for {sql}");
            assert_eq!(ps_msgs[0].name, expected_key, "Wrong key for {sql}");
            assert_eq!(ps_msgs[0].value, expected_value, "Wrong value for {sql}");
        }
    }

    #[tokio::test]
    async fn test_set_time_zone_updates_connection_session_only() {
        let (template, session_context, mut client) = connection();

        set(
            &session_context,
            &mut client,
            "set time zone 'America/New_York'",
        )
        .await
        .unwrap();

        // The zone lands on the connection's own session, is reported to the
        // client, and drives the functions that take their zone from the
        // session config (here `now()`) ...
        assert_eq!(
            session_time_zone(&session_context).as_deref(),
            Some("America/New_York")
        );
        assert_eq!(
            parameter_statuses(&client),
            vec![("TimeZone".to_string(), "America/New_York".to_string())]
        );
        assert_eq!(
            shown_time_zone(&client, &session_context).await,
            "America/New_York"
        );
        assert_eq!(
            now_type(&session_context).await,
            timestamp_in(Some("America/New_York"))
        );

        // ... while the session template, shared by every connection, is left
        // untouched.
        assert_eq!(session_time_zone(&template), None);
    }

    #[tokio::test]
    async fn test_time_zone_assignment_forms_are_equivalent() {
        for sql in [
            "set time zone 'Asia/Tokyo'",
            "set time zone = 'Asia/Tokyo'",
            "set session time zone 'Asia/Tokyo'",
            "set timezone to 'Asia/Tokyo'",
            "set TIMEZONE = 'Asia/Tokyo'",
            "set time.zone to 'Asia/Tokyo'",
            "set datafusion.execution.time_zone to 'Asia/Tokyo'",
        ] {
            let (_, session_context, mut client) = connection();
            let response = set(&session_context, &mut client, sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(command_tag(response), "SET", "{sql}");
            assert_eq!(
                session_time_zone(&session_context).as_deref(),
                Some("Asia/Tokyo"),
                "{sql}"
            );
            assert_eq!(
                parameter_statuses(&client),
                vec![("TimeZone".to_string(), "Asia/Tokyo".to_string())],
                "{sql}"
            );
            assert_eq!(
                shown_time_zone(&client, &session_context).await,
                "Asia/Tokyo",
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn test_unknown_time_zone_is_rejected_without_side_effects() {
        let (_, session_context, mut client) = connection();
        set(
            &session_context,
            &mut client,
            "set time zone 'America/New_York'",
        )
        .await
        .unwrap();
        client.sent_messages.clear();

        for sql in [
            "set time zone 'Mars/Olympus_Mons'",
            "set timezone to 'Mars/Olympus_Mons'",
        ] {
            let response = set(&session_context, &mut client, sql).await;
            let Err(PgWireError::UserError(info)) = response else {
                panic!("{sql}: expected an error");
            };
            assert_eq!(info.code, "22023", "{sql}: invalid_parameter_value");
            assert_eq!(
                info.message, "invalid value for parameter \"TimeZone\": \"Mars/Olympus_Mons\"",
                "{sql}"
            );

            // Like Postgres, a rejected value changes nothing: neither the
            // session nor what the client is told.
            assert_eq!(
                session_time_zone(&session_context).as_deref(),
                Some("America/New_York"),
                "{sql}"
            );
            assert!(
                client.sent_messages().is_empty(),
                "{sql}: no ParameterStatus"
            );
            assert_eq!(
                shown_time_zone(&client, &session_context).await,
                "America/New_York",
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn test_reset_time_zone_forms_are_equivalent() {
        for sql in [
            "reset timezone",
            "reset datafusion.execution.time_zone",
            "set time zone default",
            "set timezone to default",
            "set time zone local",
        ] {
            let (template, session_context, mut client) = connection();
            set(
                &session_context,
                &mut client,
                "set time zone 'America/New_York'",
            )
            .await
            .unwrap();
            client.sent_messages.clear();

            let response = if sql.starts_with("reset") {
                reset(&session_context, &mut client, sql).await
            } else {
                set(&session_context, &mut client, sql).await
            }
            .unwrap_or_else(|e| panic!("{sql}: {e}"));

            let expected_tag = if sql.starts_with("reset") {
                "RESET"
            } else {
                "SET"
            };
            assert_eq!(command_tag(response), expected_tag, "{sql}");

            assert_eq!(session_time_zone(&session_context), None, "{sql}");
            assert_eq!(crate::client::get_timezone(&client), None, "{sql}");
            assert_eq!(
                parameter_statuses(&client),
                vec![("TimeZone".to_string(), "UTC".to_string())],
                "{sql}"
            );
            assert_eq!(
                shown_time_zone(&client, &session_context).await,
                "UTC",
                "{sql}"
            );
            assert_eq!(
                now_type(&session_context).await,
                timestamp_in(None),
                "{sql}"
            );
            assert_eq!(session_time_zone(&template), None, "{sql}");
        }
    }

    #[tokio::test]
    async fn test_time_zone_is_reported_only_when_it_changes() {
        let (_, session_context, mut client) = connection();

        // Nothing set: a reset is a no-op and is not worth a report.
        reset(&session_context, &mut client, "reset timezone")
            .await
            .unwrap();
        assert!(client.sent_messages().is_empty());

        // Setting the zone that is already in effect is not a change either.
        set(&session_context, &mut client, "set time zone 'UTC'")
            .await
            .unwrap();
        assert_eq!(
            parameter_statuses(&client),
            vec![("TimeZone".to_string(), "UTC".to_string())]
        );
        client.sent_messages.clear();
        set(&session_context, &mut client, "set time zone 'UTC'")
            .await
            .unwrap();
        assert!(client.sent_messages().is_empty());
    }

    #[tokio::test]
    async fn test_reset_is_handled_at_parse_time_in_the_extended_protocol() {
        // Extended-protocol clients (psycopg, JDBC) send every statement
        // through Parse first; a `RESET` must get a plan from the hook there,
        // as DataFusion's planner only knows its own `RESET` syntax.
        let (_, session_context, client) = connection();
        let plan = SetShowHook
            .handle_extended_parse_query(&parse("reset timezone"), &session_context, &client)
            .await
            .expect("RESET is handled by the hook");
        assert!(plan.is_ok());
    }

    #[tokio::test]
    async fn test_show_time_zone_follows_the_session_setting() {
        let (_, session_context, client) = connection();

        // A change that bypasses the hook still shows up: the reported value
        // is the session's effective setting, not a copy of it.
        session_context
            .sql("set datafusion.execution.time_zone = 'Europe/Paris'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            shown_time_zone(&client, &session_context).await,
            "Europe/Paris"
        );
        let response =
            try_respond_show_statements(&client, &parse("show timezone"), &session_context)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(crate::testing::scalar_text(response).await, "Europe/Paris");

        session_context
            .sql("reset datafusion.execution.time_zone")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(shown_time_zone(&client, &session_context).await, "UTC");
    }

    #[tokio::test]
    async fn test_no_parameter_status_for_statement_timeout() {
        use pgwire::messages::PgWireBackendMessage;

        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '5000ms'")
            .unwrap()
            .parse_statement()
            .unwrap();

        let result = try_respond_set_statements(&mut client, &statement, &session_context).await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());

        let has_ps = client
            .sent_messages()
            .iter()
            .any(|m| matches!(m, PgWireBackendMessage::ParameterStatus(_)));

        assert!(!has_ps, "statement_timeout should not send ParameterStatus");
    }

    #[tokio::test]
    async fn test_supported_show_statements_returned_columns() {
        let session_context = SessionContext::new();
        let client = MockClient::new();

        let tests = [
            ("show time zone", "TimeZone"),
            ("show server_version", "server_version"),
            ("show transaction_isolation", "transaction_isolation"),
            ("show catalogs", "Catalogs"),
            ("show search_path", "search_path"),
            ("show statement_timeout", "statement_timeout"),
            ("show transaction isolation level", "transaction_isolation"),
        ];

        for (query, expected_response_col) in tests {
            let statement = Parser::new(&PostgreSqlDialect {})
                .try_with_sql(&query)
                .unwrap()
                .parse_statement()
                .unwrap();
            let show_response =
                try_respond_show_statements(&client, &statement, &session_context).await;

            dbg!(query);
            dbg!(&show_response);
            let Some(Ok(Response::Query(show_response))) = show_response else {
                panic!("unexpected show response");
            };

            assert_eq!(show_response.command_tag(), "SELECT");

            let row_schema = show_response.row_schema();
            assert_eq!(row_schema.len(), 1);
            assert_eq!(row_schema[0].name(), expected_response_col);
        }
    }
}
