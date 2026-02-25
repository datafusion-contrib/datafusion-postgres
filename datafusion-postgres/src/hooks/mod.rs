pub mod permissions;
pub mod set_show;
pub mod transactions;

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;

use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use futures::Sink;
use pgwire::api::results::Response;
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::ParameterStatus;
use pgwire::messages::PgWireBackendMessage;

#[async_trait]
pub trait HookClient: ClientInfo + Send + Sync {
    async fn send_parameter_status(&mut self, name: &str, value: &str) -> PgWireResult<()>;
}

pub struct PgHookClient<'a, C> {
    inner: &'a mut C,
}

impl<'a, C> PgHookClient<'a, C> {
    pub fn new(inner: &'a mut C) -> Self {
        PgHookClient { inner }
    }
}

impl<C: ClientInfo> ClientInfo for PgHookClient<'_, C> {
    fn socket_addr(&self) -> std::net::SocketAddr {
        self.inner.socket_addr()
    }

    fn is_secure(&self) -> bool {
        self.inner.is_secure()
    }

    fn protocol_version(&self) -> pgwire::messages::ProtocolVersion {
        self.inner.protocol_version()
    }

    fn set_protocol_version(&mut self, version: pgwire::messages::ProtocolVersion) {
        self.inner.set_protocol_version(version)
    }

    fn pid_and_secret_key(&self) -> (i32, pgwire::messages::startup::SecretKey) {
        self.inner.pid_and_secret_key()
    }

    fn set_pid_and_secret_key(
        &mut self,
        pid: i32,
        secret_key: pgwire::messages::startup::SecretKey,
    ) {
        self.inner.set_pid_and_secret_key(pid, secret_key)
    }

    fn state(&self) -> pgwire::api::PgWireConnectionState {
        self.inner.state()
    }

    fn set_state(&mut self, new_state: pgwire::api::PgWireConnectionState) {
        self.inner.set_state(new_state)
    }

    fn transaction_status(&self) -> pgwire::messages::response::TransactionStatus {
        self.inner.transaction_status()
    }

    fn set_transaction_status(
        &mut self,
        new_status: pgwire::messages::response::TransactionStatus,
    ) {
        self.inner.set_transaction_status(new_status)
    }

    fn metadata(&self) -> &HashMap<String, String> {
        self.inner.metadata()
    }

    fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        self.inner.metadata_mut()
    }

    fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
        self.inner.client_certificates()
    }

    fn sni_server_name(&self) -> Option<&str> {
        self.inner.sni_server_name()
    }
}

#[async_trait]
impl<C> HookClient for PgHookClient<'_, C>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: std::fmt::Debug,
    PgWireError: From<C::Error>,
{
    async fn send_parameter_status(&mut self, name: &str, value: &str) -> PgWireResult<()> {
        use futures::SinkExt;
        self.inner
            .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                name.to_string(),
                value.to_string(),
            )))
            .await
            .map_err(PgWireError::from)
    }
}

impl<C> Sink<PgWireBackendMessage> for PgHookClient<'_, C>
where
    C: Sink<PgWireBackendMessage> + Unpin,
{
    type Error = C::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: PgWireBackendMessage) -> Result<(), Self::Error> {
        Pin::new(&mut *self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut *self.inner).poll_close(cx)
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
