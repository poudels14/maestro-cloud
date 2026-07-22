//! Authenticated HTTP API over canonical Maestro resources.
//!
//! Domain routers stay thin: stores own persistence, operators own behavior,
//! and this application owns transport validation, authentication, masking,
//! and OpenAPI composition.

mod auth;
mod error;
mod exec_service;
mod mask;
mod mutation;
mod node_log_client;
mod openapi;
mod openapi_commands;
mod openapi_logs;
mod resource;
mod routes;
mod settings;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum_server::Handle;
use axum_server::accept::Accept;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig, from_tcp_rustls};
use kernel_api::{ClusterId, NodeId};
use kernel_controller::{RequestDeduplicator, SystemTimestampClock, TimestampClock};
use kernel_store::Store;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::sync::watch;

use auth::AuthPolicy;
pub use auth::OperatorIdentity;
pub use error::{ApiError, ApiErrorBody, ServerError};
pub use exec_service::{
    ClusterExecSessions, ExecSessionOpenError, HttpClusterExecSessions, HttpExecClientError,
};
pub use node_log_client::{HttpNodeLogQueryStore, NodeLogClientError};
pub use openapi::openapi_document;
pub use settings::{ServerSettings, ServerSettingsError, TlsIdentity};

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub(crate) struct VerifiedNodeCertificate;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) cluster_id: ClusterId,
    pub(crate) requests: RequestDeduplicator,
    pub(crate) timestamp_clock: Arc<dyn TimestampClock>,
    pub(crate) artifact_archives: Option<Arc<dyn build::ArtifactArchiveStore>>,
    pub(crate) firewall_settings: Option<firewall::FirewallSettings>,
    pub(crate) log_queries: Option<Arc<dyn logs::LogQueryStore>>,
    pub(crate) cluster_log_nodes: Arc<[NodeId]>,
    pub(crate) cluster_log_queries: Option<Arc<logs::ClusterLogQueryCoordinator>>,
    pub(crate) exec_sessions: Option<Arc<dyn ClusterExecSessions>>,
    pub(crate) exec_relays: Arc<Semaphore>,
    pub(crate) webhook_backend: Option<Arc<dyn webhook::WebhookDeliveryBackend>>,
}

/// Validated API application that has not yet claimed its listener.
pub struct ApiServer {
    settings: ServerSettings,
    state: AppState,
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
            artifact_archives: None,
            firewall_settings: None,
            log_queries: None,
            cluster_log_nodes: Arc::from([]),
            cluster_log_queries: None,
            exec_sessions: None,
            exec_relays: Arc::new(Semaphore::new(8)),
            webhook_backend: None,
        };
        let router = routes::router(state.clone(), auth_policy(&settings));
        Ok(Self {
            settings,
            state,
            router,
        })
    }

    /// Enables content-addressed build-context uploads through the configured archive store.
    pub fn with_artifact_archive_store(
        mut self,
        store: Arc<dyn build::ArtifactArchiveStore>,
    ) -> Self {
        self.state.artifact_archives = Some(store);
        self.router = routes::router(self.state.clone(), auth_policy(&self.settings));
        self
    }

    /// Enables firewall dry-runs with the same static settings as the leader operator.
    pub fn with_firewall_settings(mut self, settings: firewall::FirewallSettings) -> Self {
        self.state.firewall_settings = Some(settings);
        self.router = routes::router(self.state.clone(), auth_policy(&self.settings));
        self
    }

    /// Enables node-local normalized-log reads and histograms.
    pub fn with_log_query_store(mut self, store: Arc<dyn logs::LogQueryStore>) -> Self {
        self.state.log_queries = Some(store);
        self.router = routes::router(self.state.clone(), auth_policy(&self.settings));
        self
    }

    /// Enables cluster-wide log fan-out over the declared node topology.
    pub fn with_cluster_log_query_store(
        mut self,
        mut node_ids: Vec<NodeId>,
        nodes: Arc<dyn logs::NodeLogQueryStore>,
    ) -> Self {
        node_ids.sort();
        node_ids.dedup();
        self.state.cluster_log_nodes = Arc::from(node_ids);
        self.state.cluster_log_queries =
            Some(Arc::new(logs::ClusterLogQueryCoordinator::new(nodes)));
        self.router = routes::router(self.state.clone(), auth_policy(&self.settings));
        self
    }

    /// Enables local and cross-node interactive exec session routing.
    pub fn with_exec_sessions(mut self, sessions: Arc<dyn ClusterExecSessions>) -> Self {
        self.state.exec_sessions = Some(sessions);
        self.router = routes::router(self.state.clone(), auth_policy(&self.settings));
        self
    }

    /// Enables webhook test commands through the same delivery seam as the leader operator.
    pub fn with_webhook_backend(
        mut self,
        backend: Arc<dyn webhook::WebhookDeliveryBackend>,
    ) -> Self {
        self.state.webhook_backend = Some(backend);
        self.router = routes::router(self.state.clone(), auth_policy(&self.settings));
        self
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
                Some(tls_config(
                    identity,
                    self.settings.cluster_trust_root_pem.as_deref(),
                )?)
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

fn tls_config(
    identity: TlsIdentity,
    client_trust_root_pem: Option<&str>,
) -> Result<RustlsConfig, ServerError> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let certificates = CertificateDer::pem_slice_iter(identity.certificate_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| tls_configuration("failed to parse API certificate"))?;
    let private_key = PrivateKeyDer::from_pem_slice(identity.private_key_pem.expose().as_bytes())
        .map_err(|_| tls_configuration("failed to parse API private key"))?;
    let builder = rustls::ServerConfig::builder();
    let mut config = match client_trust_root_pem {
        Some(trust_root_pem) => {
            let roots = CertificateDer::pem_slice_iter(trust_root_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| tls_configuration("failed to parse cluster client trust root"))?;
            let mut root_store = rustls::RootCertStore::empty();
            let (accepted, _) = root_store.add_parsable_certificates(roots);
            if accepted == 0 {
                return Err(ServerError::TlsConfiguration(tls_error(
                    "cluster client trust root contains no certificates",
                )));
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                .allow_unauthenticated()
                .build()
                .map_err(|_| {
                    tls_configuration("failed to configure cluster client verification")
                })?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certificates, private_key)
        }
        None => builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key),
    }
    .map_err(|_| tls_configuration("API certificate and private key do not match"))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(RustlsConfig::from_config(Arc::new(config)))
}

fn tls_error(message: &'static str) -> std::io::Error {
    std::io::Error::other(message)
}

fn tls_configuration(message: &'static str) -> ServerError {
    ServerError::TlsConfiguration(tls_error(message))
}

fn auth_policy(settings: &ServerSettings) -> AuthPolicy {
    AuthPolicy::new(
        settings.jwt_secret_key.clone(),
        settings.requires_node_client_certificate(),
    )
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
                .map(VerifiedClientCertificateAcceptor::new)
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

#[derive(Clone)]
struct VerifiedClientCertificateAcceptor {
    inner: RustlsAcceptor,
}

impl VerifiedClientCertificateAcceptor {
    fn new(inner: RustlsAcceptor) -> Self {
        Self { inner }
    }
}

impl<Service> Accept<tokio::net::TcpStream, Service> for VerifiedClientCertificateAcceptor
where
    Service: Send + 'static,
    <RustlsAcceptor as Accept<tokio::net::TcpStream, Service>>::Future: Send + 'static,
{
    type Stream = <RustlsAcceptor as Accept<tokio::net::TcpStream, Service>>::Stream;
    type Service = VerifiedClientCertificateService<Service>;
    type Future = std::pin::Pin<
        Box<dyn Future<Output = std::io::Result<(Self::Stream, Self::Service)>> + Send + 'static>,
    >;

    fn accept(&self, stream: tokio::net::TcpStream, service: Service) -> Self::Future {
        let accepted = self.inner.accept(stream, service);
        Box::pin(async move {
            let (stream, service) = accepted.await?;
            let verified = stream
                .get_ref()
                .1
                .peer_certificates()
                .is_some_and(|certificates| !certificates.is_empty());
            Ok((
                stream,
                VerifiedClientCertificateService { service, verified },
            ))
        })
    }
}

#[derive(Clone)]
struct VerifiedClientCertificateService<Service> {
    service: Service,
    verified: bool,
}

impl<Service, Body> tower::Service<axum::http::Request<Body>>
    for VerifiedClientCertificateService<Service>
where
    Service: tower::Service<axum::http::Request<Body>>,
{
    type Response = Service::Response;
    type Error = Service::Error;
    type Future = Service::Future;

    fn poll_ready(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(context)
    }

    fn call(&mut self, mut request: axum::http::Request<Body>) -> Self::Future {
        if self.verified {
            request.extensions_mut().insert(VerifiedNodeCertificate);
        }
        self.service.call(request)
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
