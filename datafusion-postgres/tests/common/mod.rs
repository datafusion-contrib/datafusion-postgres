use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion_pg_catalog::pg_catalog::setup_pg_catalog;
use datafusion_postgres::{auth::AuthManager, DfSessionService};
use futures::Sink;
use pgwire::{
    api::{ClientInfo, ClientPortalStore, PgWireConnectionState, METADATA_USER},
    messages::{
        response::TransactionStatus, startup::SecretKey, PgWireBackendMessage, ProtocolVersion,
    },
};

pub fn setup_handlers() -> DfSessionService {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("Failed to setup sesession context");

    DfSessionService::new(Arc::new(session_context), Arc::new(AuthManager::new()))
}

#[derive(Debug, Default)]
pub struct MockClient {
    metadata: HashMap<String, String>,
    portal_store: HashMap<String, String>,
}

impl MockClient {
    pub fn new() -> MockClient {
        let mut metadata = HashMap::new();
        metadata.insert(METADATA_USER.to_string(), "postgres".to_string());

        MockClient {
            metadata,
            portal_store: HashMap::default(),
        }
    }
}

impl ClientInfo for MockClient {
    fn socket_addr(&self) -> std::net::SocketAddr {
        "127.0.0.1".parse().unwrap()
    }

    fn is_secure(&self) -> bool {
        false
    }

    fn protocol_version(&self) -> ProtocolVersion {
        ProtocolVersion::PROTOCOL3_0
    }

    fn set_protocol_version(&mut self, _version: ProtocolVersion) {}

    fn pid_and_secret_key(&self) -> (i32, SecretKey) {
        (0, SecretKey::I32(0))
    }

    fn set_pid_and_secret_key(&mut self, _pid: i32, _secret_key: SecretKey) {}

    fn state(&self) -> PgWireConnectionState {
        PgWireConnectionState::ReadyForQuery
    }

    fn set_state(&mut self, _new_state: PgWireConnectionState) {}

    fn transaction_status(&self) -> TransactionStatus {
        TransactionStatus::Idle
    }

    fn set_transaction_status(&mut self, _new_status: TransactionStatus) {}

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

impl ClientPortalStore for MockClient {
    type PortalStore = HashMap<String, String>;
    fn portal_store(&self) -> &Self::PortalStore {
        &self.portal_store
    }
}

impl Sink<PgWireBackendMessage> for MockClient {
    type Error = std::io::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        _item: PgWireBackendMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}
