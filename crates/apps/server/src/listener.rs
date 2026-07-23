use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum_server::Handle;
use axum_server::accept::Accept;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig, from_tcp_rustls};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::{ServerError, ServerSettings, TlsIdentity, VerifiedNodeCertificate};

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Bound API listener whose task lifetime is explicit.
pub struct BoundApiServer {
    listener: TcpListener,
    local_address: SocketAddr,
    router: Router,
    tls: Option<RustlsConfig>,
}

impl BoundApiServer {
    pub(crate) async fn bind(
        settings: ServerSettings,
        router: Router,
    ) -> Result<Self, ServerError> {
        let tls = match settings.tls_identity {
            Some(identity) => {
                let _ = rustls::crypto::ring::default_provider().install_default();
                Some(tls_config(
                    identity,
                    settings.cluster_trust_root_pem.as_deref(),
                )?)
            }
            None => None,
        };
        let listener = TcpListener::bind(settings.bind_address)
            .await
            .map_err(|source| ServerError::Bind {
                address: settings.bind_address,
                source,
            })?;
        let local_address = listener.local_addr().map_err(ServerError::LocalAddress)?;
        Ok(Self {
            listener,
            local_address,
            router,
            tls,
        })
    }

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
            let serving = server.serve(
                self.router
                    .into_make_service_with_connect_info::<SocketAddr>(),
            );
            tokio::pin!(serving);
            wait_for_server(&mut serving, handle, shutdown).await
        } else {
            let server = axum_server::from_tcp(listener)
                .map_err(ServerError::ListenerConfiguration)?
                .handle(handle.clone());
            let serving = server.serve(
                self.router
                    .into_make_service_with_connect_info::<SocketAddr>(),
            );
            tokio::pin!(serving);
            wait_for_server(&mut serving, handle, shutdown).await
        }
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
