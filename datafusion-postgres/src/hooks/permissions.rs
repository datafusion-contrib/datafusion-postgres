use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_pg_catalog::pg_catalog::context::{Permission, ResourceType};
use pgwire::api::results::Response;
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};

use crate::auth::AuthManager;
use crate::QueryHook;

#[derive(Debug)]
pub struct PermissionsHook {
    auth_manager: Arc<AuthManager>,
}

impl PermissionsHook {
    pub fn new(auth_manager: Arc<AuthManager>) -> Self {
        PermissionsHook { auth_manager }
    }

    /// Check if the current user has permission to execute a query
    async fn check_query_permission<C>(&self, client: &C, query: &str) -> PgWireResult<()>
    where
        C: ClientInfo + ?Sized,
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
            (Permission::Select, ResourceType::All)
        } else if query_trimmed.starts_with("insert") {
            (Permission::Insert, ResourceType::All)
        } else if query_trimmed.starts_with("update") {
            (Permission::Update, ResourceType::All)
        } else if query_trimmed.starts_with("delete") {
            (Permission::Delete, ResourceType::All)
        } else if query_trimmed.starts_with("create table")
            || query_trimmed.starts_with("create view")
        {
            (Permission::Create, ResourceType::All)
        } else if query_trimmed.starts_with("drop") {
            (Permission::Drop, ResourceType::All)
        } else if query_trimmed.starts_with("alter") {
            (Permission::Alter, ResourceType::All)
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
}

#[async_trait]
impl QueryHook for PermissionsHook {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        let query_lower = statement.to_string().to_lowercase();

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
            let res = self.check_query_permission(&*client, &query_lower).await;
            if let Err(e) = res {
                return Some(Err(e));
            }
        }

        None
    }

    async fn handle_extended_parse_query(
        &self,
        _stmt: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        let query = statement.to_string().to_lowercase();

        // Check permissions for the query (skip for SET and SHOW statements)
        if !query.starts_with("set") && !query.starts_with("show") {
            let res = self.check_query_permission(&*client, &query).await;
            if let Err(e) = res {
                return Some(Err(e));
            }
        }
        None
    }
}
