mod handlers;
pub mod pg_catalog;

use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;

use datafusion::prelude::SessionContext;

pub mod auth;
use getset::{Getters, Setters, WithSetters};
use log::{info, warn};
use pgwire::api::PgWireServerHandlers;
use pgwire::tokio::process_socket;
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::rustls::{self, ServerConfig};
use tokio_rustls::TlsAcceptor;

use crate::auth::AuthManager;
use handlers::HandlerFactory;
pub use handlers::{DfSessionService, Parser};

/// re-exports
pub use arrow_pg;
pub use pgwire;

#[derive(Getters, Setters, WithSetters, Debug)]
#[getset(get = "pub", set = "pub", set_with = "pub")]
pub struct ServerOptions {
    host: String,
    port: u16,
    tls_cert_path: Option<String>,
    tls_key_path: Option<String>,
}

impl ServerOptions {
    pub fn new() -> ServerOptions {
        ServerOptions::default()
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            host: "127.0.0.1".to_string(),
            port: 5432,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

/// Set up TLS configuration if certificate and key paths are provided
fn setup_tls(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, IOError> {
    // Install ring crypto provider for rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert = certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<CertificateDer>, IOError>>()?;

    let key = pkcs8_private_keys(&mut BufReader::new(File::open(key_path)?))
        .map(|key| key.map(PrivateKeyDer::from))
        .collect::<Result<Vec<PrivateKeyDer>, IOError>>()?
        .into_iter()
        .next()
        .ok_or_else(|| IOError::new(ErrorKind::InvalidInput, "No private key found"))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .map_err(|err| IOError::new(ErrorKind::InvalidInput, err))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Serve the Datafusion `SessionContext` with Postgres protocol.
pub async fn serve(
    session_context: Arc<SessionContext>,
    opts: &ServerOptions,
) -> Result<(), std::io::Error> {
    // Create authentication manager
    let auth_manager = Arc::new(AuthManager::new());

    // Create the handler factory with authentication
    let factory = Arc::new(HandlerFactory::new(session_context, auth_manager));

    serve_with_handlers(factory, opts).await
}

/// Serve with custom pgwire handlers
///
/// This function allows you to rewrite some of the built-in logic including
/// authentication and query processing. You can Implement your own
/// `PgWireServerHandlers` by reusing `DfSessionService`.
pub async fn serve_with_handlers(
    handlers: Arc<impl PgWireServerHandlers + Sync + Send + 'static>,
    opts: &ServerOptions,
) -> Result<(), std::io::Error> {
    // Set up TLS if configured
    let tls_acceptor =
        if let (Some(cert_path), Some(key_path)) = (&opts.tls_cert_path, &opts.tls_key_path) {
            match setup_tls(cert_path, key_path) {
                Ok(acceptor) => {
                    info!("TLS enabled using cert: {cert_path} and key: {key_path}");
                    Some(acceptor)
                }
                Err(e) => {
                    warn!("Failed to setup TLS: {e}. Running without encryption.");
                    None
                }
            }
        } else {
            info!("TLS not configured. Running without encryption.");
            None
        };

    // Bind to the specified host and port
    let server_addr = format!("{}:{}", opts.host, opts.port);
    let listener = TcpListener::bind(&server_addr).await?;
    if tls_acceptor.is_some() {
        info!("Listening on {server_addr} with TLS encryption");
    } else {
        info!("Listening on {server_addr} (unencrypted)");
    }

    // Accept incoming connections
    loop {
        match listener.accept().await {
            Ok((socket, _addr)) => {
                let factory_ref = handlers.clone();
                let tls_acceptor_ref = tls_acceptor.clone();

                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket, tls_acceptor_ref, factory_ref).await {
                        warn!("Error processing socket: {e}");
                    }
                });
            }
            Err(e) => {
                warn!("Error accept socket: {e}");
            }
        }
    }
}
