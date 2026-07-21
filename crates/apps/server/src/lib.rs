//! Authenticated HTTP API over canonical Maestro resources.
//!
//! Domain routers stay thin: stores own persistence, operators own behavior,
//! and this application owns transport validation, authentication, masking,
//! and OpenAPI composition.

mod auth;
mod error;
mod mask;
mod mutation;
mod openapi;
mod openapi_commands;
mod resource;
mod routes;
mod settings;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum_server::Handle;
use axum_server::tls_rustls::{RustlsConfig, from_tcp_rustls};
use kernel_api::ClusterId;
use kernel_controller::{RequestDeduplicator, SystemTimestampClock, TimestampClock};
use kernel_store::Store;
use tokio::net::TcpListener;
use tokio::sync::watch;

use auth::AuthPolicy;
pub use auth::OperatorIdentity;
pub use error::{ApiError, ApiErrorBody, ServerError};
pub use openapi::openapi_document;
pub use settings::{ServerSettings, ServerSettingsError, TlsIdentity};

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) cluster_id: ClusterId,
    pub(crate) requests: RequestDeduplicator,
    pub(crate) timestamp_clock: Arc<dyn TimestampClock>,
}

/// Validated API application that has not yet claimed its listener.
pub struct ApiServer {
    settings: ServerSettings,
    router: Router,
}

impl ApiServer {
    /// Composes all domain routers over one cluster store.
    pub fn new(
        store: Arc<dyn Store>,
        cluster_id: ClusterId,
        settings: ServerSettings,
    ) -> Result<Self, ServerError> {
        let settings = settings.validate()?;
        let state = AppState {
            requests: RequestDeduplicator::new(store.clone()),
            timestamp_clock: Arc::new(SystemTimestampClock),
            store,
            cluster_id,
        };
        let router = routes::router(state, AuthPolicy::new(settings.jwt_secret_key.clone()));
        Ok(Self { settings, router })
    }

    /// Returns a cloneable in-process router for composition and tests.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Claims the configured listener before transferring runtime ownership.
    pub async fn bind(self) -> Result<BoundApiServer, ServerError> {
        let tls = match self.settings.tls_identity {
            Some(identity) => {
                let _ = rustls::crypto::ring::default_provider().install_default();
                Some(
                    RustlsConfig::from_pem(
                        identity.certificate_pem.into_bytes(),
                        identity.private_key_pem.expose().as_bytes().to_vec(),
                    )
                    .await
                    .map_err(ServerError::TlsConfiguration)?,
                )
            }
            None => None,
        };
        let listener = TcpListener::bind(self.settings.bind_address)
            .await
            .map_err(|source| ServerError::Bind {
                address: self.settings.bind_address,
                source,
            })?;
        let local_address = listener.local_addr().map_err(ServerError::LocalAddress)?;
        Ok(BoundApiServer {
            listener,
            local_address,
            router: self.router,
            tls,
        })
    }
}

/// Bound API listener whose task lifetime is explicit.
pub struct BoundApiServer {
    listener: TcpListener,
    local_address: SocketAddr,
    router: Router,
    tls: Option<RustlsConfig>,
}

impl BoundApiServer {
    /// Returns the concrete address, including an operating-system-selected port.
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Serves requests until graceful shutdown is requested.
    pub async fn serve(self, shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
        let listener = self
            .listener
            .into_std()
            .map_err(ServerError::ListenerConfiguration)?;
        let handle = Handle::new();
        if let Some(tls) = self.tls {
            let server = from_tcp_rustls(listener, tls)
                .map_err(ServerError::ListenerConfiguration)?
                .handle(handle.clone());
            let serving = server.serve(self.router.into_make_service());
            tokio::pin!(serving);
            wait_for_server(&mut serving, handle, shutdown).await
        } else {
            let server = axum_server::from_tcp(listener)
                .map_err(ServerError::ListenerConfiguration)?
                .handle(handle.clone());
            let serving = server.serve(self.router.into_make_service());
            tokio::pin!(serving);
            wait_for_server(&mut serving, handle, shutdown).await
        }
    }
}

async fn wait_for_server(
    serving: &mut std::pin::Pin<&mut impl Future<Output = std::io::Result<()>>>,
    handle: Handle<SocketAddr>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    if *shutdown.borrow() {
        handle.graceful_shutdown(Some(SHUTDOWN_GRACE));
        return serving.await.map_err(ServerError::Serve);
    }
    tokio::select! {
        result = serving.as_mut() => result.map_err(ServerError::Serve),
        _ = async {
            while shutdown.changed().await.is_ok() {
                if *shutdown.borrow() {
                    return;
                }
            }
        } => {
            handle.graceful_shutdown(Some(SHUTDOWN_GRACE));
            serving.await.map_err(ServerError::Serve)
        }
    }
}

#[cfg(test)]
mod tests;
