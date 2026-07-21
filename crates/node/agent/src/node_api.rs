use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use node_fabric::otlp::{logs, metrics, traces};
use node_fabric::proto::{
    GetIdentityRequest, MutationResult, ResourceMutation, WorkloadIdentity,
    control_server::{Control, ControlServer},
    identity_server::{Identity, IdentityServer},
};
use node_fabric::{SocketPeer, WORKLOAD_TOKEN_HEADER, WorkloadAuthorization, WorkloadClaims};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::server::UdsConnectInfo;
use tonic::{Request, Response, Status};

/// Applies privileged node API mutations after transport authentication.
#[async_trait]
pub trait NodeControlHandler: Send + Sync + 'static {
    /// Applies one mutation on behalf of the authenticated workload.
    async fn mutate(
        &self,
        claims: WorkloadClaims,
        mutation: ResourceMutation,
    ) -> Result<MutationResult, Status>;
}

/// Accepts authenticated standard OTLP exports from one workload.
#[async_trait]
pub trait NodeTelemetryHandler: Send + Sync + 'static {
    /// Accepts an OTLP log batch.
    async fn export_logs(
        &self,
        claims: WorkloadClaims,
        request: logs::ExportLogsServiceRequest,
    ) -> Result<logs::ExportLogsServiceResponse, Status>;

    /// Accepts an OTLP metric batch.
    async fn export_metrics(
        &self,
        claims: WorkloadClaims,
        request: metrics::ExportMetricsServiceRequest,
    ) -> Result<metrics::ExportMetricsServiceResponse, Status>;

    /// Accepts an OTLP trace batch.
    async fn export_traces(
        &self,
        claims: WorkloadClaims,
        request: traces::ExportTraceServiceRequest,
    ) -> Result<traces::ExportTraceServiceResponse, Status>;
}

/// Agent-owned implementations behind a workload's node API socket.
#[derive(Clone)]
pub struct NodeApiServices {
    control: Arc<dyn NodeControlHandler>,
    telemetry: Arc<dyn NodeTelemetryHandler>,
}

impl NodeApiServices {
    /// Constructs the service set shared by per-workload listeners.
    pub fn new(
        control: Arc<dyn NodeControlHandler>,
        telemetry: Arc<dyn NodeTelemetryHandler>,
    ) -> Self {
        Self { control, telemetry }
    }
}

impl Debug for NodeApiServices {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeApiServices")
            .field("control", &"dyn NodeControlHandler")
            .field("telemetry", &"dyn NodeTelemetryHandler")
            .finish()
    }
}

/// Authenticated services bound to one workload-private Unix listener.
#[derive(Clone)]
struct WorkloadNodeApiService {
    authorization: Arc<WorkloadAuthorization>,
    control_allowed: bool,
    services: NodeApiServices,
}

impl WorkloadNodeApiService {
    fn authenticate<T>(&self, request: &Request<T>) -> Result<WorkloadClaims, Status> {
        let encoded_token = request
            .metadata()
            .get(WORKLOAD_TOKEN_HEADER)
            .ok_or_else(|| Status::unauthenticated("workload credentials are invalid"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("workload credentials are invalid"))?;
        let token = URL_SAFE_NO_PAD
            .decode(encoded_token)
            .map_err(|_| Status::unauthenticated("workload credentials are invalid"))?;
        let connect_info = request
            .extensions()
            .get::<UdsConnectInfo>()
            .ok_or_else(|| Status::unauthenticated("Unix peer credentials are missing"))?;
        let credentials = connect_info
            .peer_cred
            .ok_or_else(|| Status::unauthenticated("Unix peer credentials are missing"))?;
        let process_id = credentials
            .pid()
            .and_then(|process_id| u32::try_from(process_id).ok())
            .ok_or_else(|| Status::unauthenticated("Unix peer credentials are invalid"))?;
        let claims = self
            .authorization
            .authorize(
                &token,
                SocketPeer {
                    process_id,
                    user_id: credentials.uid(),
                    group_id: credentials.gid(),
                },
            )
            .map_err(|_| Status::unauthenticated("workload credentials are invalid"))?;
        Ok(claims.clone())
    }
}

#[async_trait]
impl Identity for WorkloadNodeApiService {
    async fn get_identity(
        &self,
        request: Request<GetIdentityRequest>,
    ) -> Result<Response<WorkloadIdentity>, Status> {
        let claims = self.authenticate(&request)?;
        Ok(Response::new(WorkloadIdentity {
            workload_id: claims.workload_id.to_string(),
            assignment_id: claims.assignment_id.to_string(),
            node_id: claims.node_id.to_string(),
            service_id: claims.service_id.to_string(),
            deployment_id: claims.deployment_id.to_string(),
            labels: claims.labels.into_iter().collect(),
        }))
    }
}

#[async_trait]
impl Control for WorkloadNodeApiService {
    async fn mutate(
        &self,
        request: Request<ResourceMutation>,
    ) -> Result<Response<MutationResult>, Status> {
        let claims = self.authenticate(&request)?;
        if !self.control_allowed {
            return Err(Status::permission_denied(
                "workload is not allowed to use the control API",
            ));
        }
        self.services
            .control
            .mutate(claims, request.into_inner())
            .await
            .map(Response::new)
    }
}

#[async_trait]
impl logs::logs_service_server::LogsService for WorkloadNodeApiService {
    async fn export(
        &self,
        request: Request<logs::ExportLogsServiceRequest>,
    ) -> Result<Response<logs::ExportLogsServiceResponse>, Status> {
        let claims = self.authenticate(&request)?;
        self.services
            .telemetry
            .export_logs(claims, request.into_inner())
            .await
            .map(Response::new)
    }
}

#[async_trait]
impl metrics::metrics_service_server::MetricsService for WorkloadNodeApiService {
    async fn export(
        &self,
        request: Request<metrics::ExportMetricsServiceRequest>,
    ) -> Result<Response<metrics::ExportMetricsServiceResponse>, Status> {
        let claims = self.authenticate(&request)?;
        self.services
            .telemetry
            .export_metrics(claims, request.into_inner())
            .await
            .map(Response::new)
    }
}

#[async_trait]
impl traces::trace_service_server::TraceService for WorkloadNodeApiService {
    async fn export(
        &self,
        request: Request<traces::ExportTraceServiceRequest>,
    ) -> Result<Response<traces::ExportTraceServiceResponse>, Status> {
        let claims = self.authenticate(&request)?;
        self.services
            .telemetry
            .export_traces(claims, request.into_inner())
            .await
            .map(Response::new)
    }
}

/// A bound, per-workload node API socket ready to be served.
pub struct BoundWorkloadNodeApi {
    listener: UnixListener,
    socket_path: PathBuf,
    socket_identity: SocketIdentity,
    service: WorkloadNodeApiService,
}

impl BoundWorkloadNodeApi {
    /// Binds one workload-private node API listener.
    pub fn bind(
        socket_path: impl AsRef<Path>,
        authorization: WorkloadAuthorization,
        control_allowed: bool,
        services: NodeApiServices,
    ) -> Result<Self, NodeApiServerError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        let listener = UnixListener::bind(&socket_path)
            .map_err(|source| NodeApiServerError::io("bind", &socket_path, source))?;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o666))
            .map_err(|source| NodeApiServerError::io("set permissions on", &socket_path, source))?;
        let metadata = std::fs::symlink_metadata(&socket_path)
            .map_err(|source| NodeApiServerError::io("inspect", &socket_path, source))?;
        Ok(Self {
            listener,
            socket_path,
            socket_identity: SocketIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            },
            service: WorkloadNodeApiService {
                authorization: Arc::new(authorization),
                control_allowed,
                services,
            },
        })
    }

    /// Returns the host path mounted into the workload.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Serves Identity, Control, and OTLP until shutdown, then removes this socket.
    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<(), NodeApiServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let Self {
            listener,
            socket_path,
            socket_identity,
            service,
        } = self;
        let incoming = UnixListenerStream::new(listener);
        let result = tonic::transport::Server::builder()
            .add_service(IdentityServer::new(service.clone()))
            .add_service(ControlServer::new(service.clone()))
            .add_service(logs::logs_service_server::LogsServiceServer::new(
                service.clone(),
            ))
            .add_service(metrics::metrics_service_server::MetricsServiceServer::new(
                service.clone(),
            ))
            .add_service(traces::trace_service_server::TraceServiceServer::new(
                service,
            ))
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await;
        Self::remove_socket_if_owned(&socket_path, socket_identity)?;
        result.map_err(NodeApiServerError::Transport)
    }

    fn remove_socket_if_owned(
        socket_path: &Path,
        socket_identity: SocketIdentity,
    ) -> Result<(), NodeApiServerError> {
        let metadata = match std::fs::symlink_metadata(socket_path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(NodeApiServerError::io("inspect", socket_path, source));
            }
        };
        if metadata.dev() != socket_identity.device || metadata.ino() != socket_identity.inode {
            return Err(NodeApiServerError::SocketReplaced {
                path: socket_path.to_path_buf(),
            });
        }
        std::fs::remove_file(socket_path)
            .map_err(|source| NodeApiServerError::io("remove", socket_path, source))
    }
}

#[derive(Debug, Clone, Copy)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

/// Failure to bind, serve, or safely clean up a workload node API socket.
#[derive(Debug, thiserror::Error)]
pub enum NodeApiServerError {
    /// A filesystem operation on the socket path failed.
    #[error("failed to {operation} node API socket `{path}`: {source}")]
    Io {
        /// Filesystem action that failed.
        operation: &'static str,
        /// Workload-private socket path.
        path: PathBuf,
        /// Underlying operating-system failure.
        #[source]
        source: io::Error,
    },
    /// Tonic failed while serving the Unix listener.
    #[error("node API transport failed: {0}")]
    Transport(tonic::transport::Error),
    /// The socket path was replaced while this listener was active.
    #[error("node API socket `{path}` was replaced while it was being served")]
    SocketReplaced {
        /// Path whose identity changed.
        path: PathBuf,
    },
}

impl NodeApiServerError {
    fn io(operation: &'static str, path: &Path, source: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_path_buf(),
            source,
        }
    }
}
