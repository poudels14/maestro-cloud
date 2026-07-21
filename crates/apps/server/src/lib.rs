//! Authenticated HTTP API over canonical Maestro resources.
//!
//! Domain routers stay thin: stores own persistence, operators own behavior,
//! and this application owns transport validation, authentication, masking,
//! and OpenAPI composition.

mod auth;
mod error;
mod openapi;
mod resource;
mod routes;
mod settings;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use kernel_api::ClusterId;
use kernel_store::Store;
use tokio::net::TcpListener;
use tokio::sync::watch;

use auth::AuthPolicy;
pub use auth::OperatorIdentity;
pub use error::{ApiError, ApiErrorBody, ServerError};
pub use openapi::openapi_document;
pub use settings::{ServerSettings, ServerSettingsError};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) cluster_id: ClusterId,
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
        let state = AppState { store, cluster_id };
        let router = routes::router(state, AuthPolicy::new(settings.jwt_secret_key.clone()));
        Ok(Self { settings, router })
    }

    /// Returns a cloneable in-process router for composition and tests.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Claims the configured listener before transferring runtime ownership.
    pub async fn bind(self) -> Result<BoundApiServer, ServerError> {
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
        })
    }
}

/// Bound API listener whose task lifetime is explicit.
pub struct BoundApiServer {
    listener: TcpListener,
    local_address: SocketAddr,
    router: Router,
}

impl BoundApiServer {
    /// Returns the concrete address, including an operating-system-selected port.
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Serves requests until graceful shutdown is requested.
    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(async move {
                if *shutdown.borrow() {
                    return;
                }
                while shutdown.changed().await.is_ok() {
                    if *shutdown.borrow() {
                        return;
                    }
                }
            })
            .await
            .map_err(ServerError::Serve)
    }
}

#[cfg(test)]
mod tests;
