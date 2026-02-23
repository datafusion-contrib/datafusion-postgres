pub mod permissions;
pub mod set_show;
pub mod transactions;

use async_trait::async_trait;

use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use pgwire::api::results::Response;
use pgwire::api::ClientInfo;
use pgwire::error::PgWireResult;

#[non_exhaustive]
pub struct HookOutput {
    pub response: Response,
    pub parameter_status: Vec<(String, String)>,
}

impl HookOutput {
    pub fn new(response: Response) -> Self {
        HookOutput {
            response,
            parameter_status: Vec::new(),
        }
    }

    pub fn with_parameter_status(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.parameter_status.push((name.into(), value.into()));
        self
    }
}

#[async_trait]
pub trait QueryHook: Send + Sync {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<HookOutput>>;

    /// called at extended query parse phase, for generating `LogicalPlan`from statement
    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>>;

    /// called at extended query execute phase, for query execution
    async fn handle_extended_query(
        &self,
        statement: &Statement,
        logical_plan: &LogicalPlan,
        params: &ParamValues,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<HookOutput>>;
}
