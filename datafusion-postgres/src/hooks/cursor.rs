use std::ops::DerefMut;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::{CloseCursor, DeclareType, FetchDirection};
use futures::StreamExt;
use pgwire::api::ClientInfo;
use pgwire::api::portal::{Format, Portal, PortalExecutionState};
use pgwire::api::results::{QueryResponse, Response, Tag};
use pgwire::api::stmt::StoredStatement;
use pgwire::api::store::{MemPortalStore, PortalStore};
use pgwire::error::{PgWireError, PgWireResult};

use super::{HookClient, QueryHook};
use crate::arrow_pg::datatypes::df;

pub(crate) type DfStatement = (String, Option<(sqlparser::ast::Statement, LogicalPlan)>);

/// Hook for processing cursor-related statements (DECLARE/FETCH/CLOSE)
#[derive(Debug)]
pub struct CursorStatementHook;

#[async_trait]
impl QueryHook for CursorStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &sqlparser::ast::Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let store = client.portal_store();

        match statement {
            sqlparser::ast::Statement::Declare { stmts } => {
                Some(handle_declare(store, stmts, session_context).await)
            }
            sqlparser::ast::Statement::Fetch {
                name, direction, ..
            } => Some(handle_fetch(store, name, direction).await),
            sqlparser::ast::Statement::Close { cursor } => Some(handle_close(store, cursor)),
            _ => None,
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
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

async fn handle_declare(
    store: &MemPortalStore<DfStatement>,
    stmts: &[datafusion::sql::sqlparser::ast::Declare],
    session_context: &SessionContext,
) -> PgWireResult<Response> {
    for declare in stmts {
        if declare.declare_type != Some(DeclareType::Cursor) {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    format!("unsupported DECLARE type: {:?}", declare.declare_type),
                ),
            )));
        }

        let cursor_name = match declare.names.first() {
            Some(name) => name.value.clone(),
            None => {
                return Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_string(),
                        "42601".to_string(),
                        "cursor name is required".to_string(),
                    ),
                )));
            }
        };

        let for_query = match &declare.for_query {
            Some(q) => q.to_string(),
            None => {
                return Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_string(),
                        "42601".to_string(),
                        "DECLARE CURSOR requires a FOR query".to_string(),
                    ),
                )));
            }
        };

        let df = session_context
            .sql(&for_query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let query_response = df::encode_dataframe(df, &Format::UnifiedText, None).await?;

        let stored_stmt = Arc::new(StoredStatement::new(
            cursor_name.clone(),
            (for_query, None),
            vec![],
        ));

        let bind = pgwire::messages::extendedquery::Bind::new(
            Some(cursor_name.clone()),
            None,
            vec![],
            vec![],
            vec![],
        );
        let portal = Portal::try_new(&bind, stored_stmt)?;

        let state = portal.state();
        {
            let mut portal_state = state.lock().await;
            *portal_state = PortalExecutionState::Suspended(query_response);
        }

        store.put_portal(Arc::new(portal));
    }

    Ok(Response::Execution(Tag::new("DECLARE CURSOR")))
}

async fn handle_fetch(
    store: &MemPortalStore<DfStatement>,
    name: &datafusion::sql::sqlparser::ast::Ident,
    direction: &FetchDirection,
) -> PgWireResult<Response> {
    let cursor_name = &name.value;

    let limit = match direction {
        FetchDirection::Next | FetchDirection::Forward { limit: None } => Some(1),
        FetchDirection::Forward { limit: Some(v) } | FetchDirection::Count { limit: v } => {
            parse_value_as_usize(v)
        }
        FetchDirection::ForwardAll | FetchDirection::All => None,
        FetchDirection::Prior | FetchDirection::Backward { .. } | FetchDirection::BackwardAll => {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42000".to_string(),
                    "cursor can only scan forward".to_string(),
                ),
            )));
        }
        FetchDirection::First
        | FetchDirection::Last
        | FetchDirection::Absolute { .. }
        | FetchDirection::Relative { .. } => {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42000".to_string(),
                    "cursor can only scan forward".to_string(),
                ),
            )));
        }
    };

    let portal = store.get_portal(cursor_name).ok_or_else(|| {
        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            "ERROR".to_string(),
            "34000".to_string(),
            format!("cursor \"{cursor_name}\" does not exist"),
        )))
    })?;

    let state = portal.state();
    let mut state = state.lock().await;

    let query_response = match state.deref_mut() {
        PortalExecutionState::Suspended(qr) => qr,
        PortalExecutionState::Finished => {
            return Ok(Response::Execution(Tag::new("FETCH").with_rows(0)));
        }
        PortalExecutionState::Initial => {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "24000".to_string(),
                    "cursor is in invalid state".to_string(),
                ),
            )));
        }
    };

    let schema = query_response.row_schema();
    let mut fetched_rows: Vec<pgwire::messages::data::DataRow> = vec![];
    let mut stream_exhausted = false;

    if let Some(n) = limit {
        for _ in 0..n {
            match query_response.data_rows().next().await {
                Some(Ok(row)) => fetched_rows.push(row),
                Some(Err(e)) => return Err(e),
                None => {
                    stream_exhausted = true;
                    break;
                }
            }
        }
    } else {
        loop {
            match query_response.data_rows().next().await {
                Some(Ok(row)) => fetched_rows.push(row),
                Some(Err(e)) => return Err(e),
                None => {
                    stream_exhausted = true;
                    break;
                }
            }
        }
    }

    if stream_exhausted {
        *state = PortalExecutionState::Finished;
    }

    drop(state);

    if fetched_rows.is_empty() {
        return Ok(Response::Execution(Tag::new("FETCH").with_rows(0)));
    }

    let mut fetched_response = QueryResponse::new(
        schema,
        futures::stream::iter(fetched_rows.into_iter().map(Ok)),
    );
    fetched_response.set_command_tag("FETCH");

    Ok(Response::Query(fetched_response))
}

fn handle_close(
    store: &MemPortalStore<DfStatement>,
    cursor: &CloseCursor,
) -> PgWireResult<Response> {
    match cursor {
        CloseCursor::All => {
            store.clear_portals();
        }
        CloseCursor::Specific { name } => {
            store.rm_portal(&name.value);
        }
    }
    Ok(Response::Execution(Tag::new("CLOSE CURSOR")))
}

fn parse_value_as_usize(value: &datafusion::sql::sqlparser::ast::Value) -> Option<usize> {
    match value {
        datafusion::sql::sqlparser::ast::Value::Number(s, _) => s.parse().ok(),
        _ => None,
    }
}
