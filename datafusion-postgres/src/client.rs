use datafusion::prelude::{DataFrame, SessionContext};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};

// Metadata keys for session-level settings
const METADATA_STATEMENT_TIMEOUT: &str = "statement_timeout_ms";
const METADATA_TIMEZONE: &str = "timezone";

/// Run `query` against `session_context`, honoring the client's
/// `statement_timeout` (mapping expiry to the `57014` query-canceled error).
///
/// Shared by the default simple-query path and by query hooks that execute a
/// rewritten statement themselves (e.g. the pgvector `INSERT` hook).
pub(crate) async fn execute_statement<C>(
    client: &C,
    session_context: &SessionContext,
    query: &str,
) -> PgWireResult<DataFrame>
where
    C: ClientInfo + ?Sized,
{
    let result = match get_statement_timeout(client) {
        Some(duration) => tokio::time::timeout(duration, session_context.sql(query))
            .await
            .map_err(|_| {
                PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "57014".to_string(), // query_canceled error code
                    "canceling statement due to statement timeout".to_string(),
                )))
            })?,
        None => session_context.sql(query).await,
    };

    result.map_err(|e| PgWireError::ApiError(Box::new(e)))
}

/// Get statement timeout from client metadata
pub fn get_statement_timeout<C>(client: &C) -> Option<std::time::Duration>
where
    C: ClientInfo + ?Sized,
{
    client
        .metadata()
        .get(METADATA_STATEMENT_TIMEOUT)
        .and_then(|s| s.parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
}

/// Set statement timeout in client metadata
pub fn set_statement_timeout<C>(client: &mut C, timeout: Option<std::time::Duration>)
where
    C: ClientInfo + ?Sized,
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

/// Get statement timeout from client metadata
pub fn get_timezone<C>(client: &C) -> Option<&str>
where
    C: ClientInfo + ?Sized,
{
    client.metadata().get(METADATA_TIMEZONE).map(|s| s.as_str())
}

/// Set statement timeout in client metadata
pub fn set_timezone<C>(client: &mut C, timezone: Option<&str>)
where
    C: ClientInfo + ?Sized,
{
    let metadata = client.metadata_mut();
    if let Some(timezone) = timezone {
        metadata.insert(METADATA_TIMEZONE.to_string(), timezone.to_string());
    } else {
        metadata.remove(METADATA_TIMEZONE);
    }
}
