pub mod cursor;
pub mod permissions;
pub mod set_show;
pub mod transactions;

use async_trait::async_trait;

use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use futures::Sink;
use pgwire::api::ClientInfo;
use pgwire::api::ClientPortalStore;
use pgwire::api::results::Response;
use pgwire::api::store::{MemPortalStore, PortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::hooks::cursor::DfStatement;

#[async_trait]
pub trait HookClient: ClientInfo + Send + Sync {
    fn portal_store(&self) -> &MemPortalStore<DfStatement>;

    async fn send_message(&mut self, item: PgWireBackendMessage) -> PgWireResult<()>;
}

#[async_trait]
impl<S> HookClient for S
where
    S: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Send + Sync + Unpin,
    PgWireError: From<<S as Sink<PgWireBackendMessage>>::Error>,
    S::PortalStore: PortalStore,
{
    fn portal_store(&self) -> &MemPortalStore<DfStatement> {
        self.portal_store()
            .as_any()
            .downcast_ref::<MemPortalStore<DfStatement>>()
            .expect("portal store is not MemPortalStore<DfStatement>")
    }

    async fn send_message(&mut self, item: PgWireBackendMessage) -> PgWireResult<()> {
        use futures::SinkExt;
        self.send(item).await.map_err(PgWireError::from)
    }
}

#[async_trait]
pub trait QueryHook: Send + Sync {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>>;

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
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>>;
}
