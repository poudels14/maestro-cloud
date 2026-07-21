use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kernel_api::{AssignmentId, DeploymentId, NodeId, ServiceId, WorkloadId};
use node_fabric::otlp::{logs, metrics, traces};
use node_fabric::proto::{
    GetIdentityRequest, MutationDisposition, MutationResult, ResourceMutation,
    control_client::ControlClient, identity_client::IdentityClient,
};
use node_fabric::{WORKLOAD_TOKEN_HEADER, WorkloadAuthorization, WorkloadClaims, WorkloadToken};
use tokio::sync::{Mutex, oneshot};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use crate::{
    BoundWorkloadNodeApi, NodeApiServices, NodeApiSocketOwner, NodeControlHandler, NodeLogHandler,
    NodeMetricHandler, NodeTraceHandler,
};

#[derive(Debug, Default)]
struct RecordingHandlers {
    mutations: Mutex<Vec<(WorkloadId, String)>>,
    telemetry: Mutex<Vec<(WorkloadId, &'static str)>>,
}

#[async_trait]
impl NodeControlHandler for RecordingHandlers {
    async fn mutate(
        &self,
        claims: WorkloadClaims,
        mutation: ResourceMutation,
    ) -> Result<MutationResult, Status> {
        self.mutations
            .lock()
            .await
            .push((claims.workload_id, mutation.request_id));
        Ok(MutationResult {
            disposition: MutationDisposition::Committed.into(),
            revision: 42,
        })
    }
}

#[async_trait]
impl NodeLogHandler for RecordingHandlers {
    async fn export_logs(
        &self,
        claims: WorkloadClaims,
        _request: logs::ExportLogsServiceRequest,
    ) -> Result<logs::ExportLogsServiceResponse, Status> {
        self.telemetry
            .lock()
            .await
            .push((claims.workload_id, "logs"));
        Ok(logs::ExportLogsServiceResponse {
            partial_success: None,
        })
    }
}

#[async_trait]
impl NodeMetricHandler for RecordingHandlers {
    async fn export_metrics(
        &self,
        claims: WorkloadClaims,
        _request: metrics::ExportMetricsServiceRequest,
    ) -> Result<metrics::ExportMetricsServiceResponse, Status> {
        self.telemetry
            .lock()
            .await
            .push((claims.workload_id, "metrics"));
        Ok(metrics::ExportMetricsServiceResponse {
            partial_success: None,
        })
    }
}

#[async_trait]
impl NodeTraceHandler for RecordingHandlers {
    async fn export_traces(
        &self,
        claims: WorkloadClaims,
        _request: traces::ExportTraceServiceRequest,
    ) -> Result<traces::ExportTraceServiceResponse, Status> {
        self.telemetry
            .lock()
            .await
            .push((claims.workload_id, "traces"));
        Ok(traces::ExportTraceServiceResponse {
            partial_success: None,
        })
    }
}

#[tokio::test]
async fn uds_server_authenticates_and_routes_every_node_api_service() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let socket_path = temporary.path().join("node.sock");
    let metadata = std::fs::metadata(temporary.path()).expect("directory metadata");
    let handlers = Arc::new(RecordingHandlers::default());
    let services = NodeApiServices::new(
        handlers.clone(),
        handlers.clone(),
        handlers.clone(),
        handlers.clone(),
    );
    let bound = BoundWorkloadNodeApi::bind(
        &socket_path,
        WorkloadAuthorization::new(WorkloadToken::from_bytes([7; 32]), metadata.uid(), claims()),
        NodeApiSocketOwner {
            user_id: metadata.uid(),
            group_id: metadata.gid(),
        },
        true,
        services,
    )
    .expect("bind node API");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(bound.serve_with_shutdown(async move {
        let _ = shutdown_rx.await;
    }));
    let channel = connect(&socket_path).await;

    let mut identity = IdentityClient::new(channel.clone());
    let response = identity
        .get_identity(authenticated(GetIdentityRequest {}, 7))
        .await
        .expect("authenticated identity")
        .into_inner();
    assert_eq!(response.workload_id, "workload-1");
    assert_eq!(response.node_id, "node-1");
    assert_eq!(response.labels.get("environment"), Some(&"test".to_owned()));

    let mut control = ControlClient::new(channel.clone());
    let mutation = control
        .mutate(authenticated(
            ResourceMutation {
                request_id: "request-1".to_owned(),
                ..ResourceMutation::default()
            },
            7,
        ))
        .await
        .expect("authorized control mutation")
        .into_inner();
    assert_eq!(mutation.revision, 42);

    let mut log_client = logs::logs_service_client::LogsServiceClient::new(channel.clone());
    log_client
        .export(authenticated(logs::ExportLogsServiceRequest::default(), 7))
        .await
        .expect("authenticated log export");
    let mut metric_client =
        metrics::metrics_service_client::MetricsServiceClient::new(channel.clone());
    metric_client
        .export(authenticated(
            metrics::ExportMetricsServiceRequest::default(),
            7,
        ))
        .await
        .expect("authenticated metric export");
    let mut trace_client = traces::trace_service_client::TraceServiceClient::new(channel.clone());
    trace_client
        .export(authenticated(
            traces::ExportTraceServiceRequest::default(),
            7,
        ))
        .await
        .expect("authenticated trace export");

    let error = identity
        .get_identity(authenticated(GetIdentityRequest {}, 8))
        .await
        .expect_err("wrong token must fail");
    assert_eq!(error.code(), Code::Unauthenticated);
    assert_eq!(
        handlers.mutations.lock().await.as_slice(),
        &[(workload_id(), "request-1".to_owned())]
    );
    assert_eq!(
        handlers.telemetry.lock().await.as_slice(),
        &[
            (workload_id(), "logs"),
            (workload_id(), "metrics"),
            (workload_id(), "traces"),
        ]
    );

    drop(identity);
    drop(control);
    drop(log_client);
    drop(metric_client);
    drop(trace_client);
    drop(channel);
    shutdown_tx.send(()).expect("request shutdown");
    server
        .await
        .expect("server task")
        .expect("clean server shutdown");
    assert!(!socket_path.exists());
}

#[tokio::test]
async fn log_ingest_service_routes_logs_and_rejects_unconfigured_endpoints() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let socket_path = temporary.path().join("node.sock");
    let metadata = std::fs::metadata(temporary.path()).expect("directory metadata");
    let handlers = Arc::new(RecordingHandlers::default());
    let bound = BoundWorkloadNodeApi::bind(
        &socket_path,
        WorkloadAuthorization::new(WorkloadToken::from_bytes([7; 32]), metadata.uid(), claims()),
        NodeApiSocketOwner {
            user_id: metadata.uid(),
            group_id: metadata.gid(),
        },
        true,
        NodeApiServices::with_log_ingest(handlers.clone()),
    )
    .expect("bind node API");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(bound.serve_with_shutdown(async move {
        let _ = shutdown_rx.await;
    }));
    let channel = connect(&socket_path).await;

    logs::logs_service_client::LogsServiceClient::new(channel.clone())
        .export(authenticated(logs::ExportLogsServiceRequest::default(), 7))
        .await
        .expect("configured log export");
    let metric_error = metrics::metrics_service_client::MetricsServiceClient::new(channel.clone())
        .export(authenticated(
            metrics::ExportMetricsServiceRequest::default(),
            7,
        ))
        .await
        .expect_err("unconfigured metrics must fail");
    assert_eq!(metric_error.code(), Code::Unimplemented);
    let trace_error = traces::trace_service_client::TraceServiceClient::new(channel.clone())
        .export(authenticated(
            traces::ExportTraceServiceRequest::default(),
            7,
        ))
        .await
        .expect_err("unconfigured traces must fail");
    assert_eq!(trace_error.code(), Code::Unimplemented);
    let control_error = ControlClient::new(channel.clone())
        .mutate(authenticated(ResourceMutation::default(), 7))
        .await
        .expect_err("unconfigured control must fail");
    assert_eq!(control_error.code(), Code::Unimplemented);
    assert_eq!(
        handlers.telemetry.lock().await.as_slice(),
        &[(workload_id(), "logs")]
    );
    assert!(handlers.mutations.lock().await.is_empty());

    drop(channel);
    shutdown_tx.send(()).expect("request shutdown");
    server
        .await
        .expect("server task")
        .expect("clean server shutdown");
}

#[tokio::test]
async fn uds_server_rejects_unprivileged_control() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let socket_path = temporary.path().join("node.sock");
    let metadata = std::fs::metadata(temporary.path()).expect("directory metadata");
    let handlers = Arc::new(RecordingHandlers::default());
    let bound = BoundWorkloadNodeApi::bind(
        &socket_path,
        WorkloadAuthorization::new(WorkloadToken::from_bytes([7; 32]), metadata.uid(), claims()),
        NodeApiSocketOwner {
            user_id: metadata.uid(),
            group_id: metadata.gid(),
        },
        false,
        NodeApiServices::new(
            handlers.clone(),
            handlers.clone(),
            handlers.clone(),
            handlers,
        ),
    )
    .expect("bind node API");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(bound.serve_with_shutdown(async move {
        let _ = shutdown_rx.await;
    }));
    let channel = connect(&socket_path).await;
    let error = ControlClient::new(channel.clone())
        .mutate(authenticated(ResourceMutation::default(), 7))
        .await
        .expect_err("unprivileged control must fail");
    assert_eq!(error.code(), Code::PermissionDenied);
    drop(channel);
    shutdown_tx.send(()).expect("request shutdown");
    server
        .await
        .expect("server task")
        .expect("clean server shutdown");
}

async fn connect(socket_path: &std::path::Path) -> Channel {
    Endpoint::from_shared(format!("unix:{}", socket_path.display()))
        .expect("Unix endpoint")
        .connect()
        .await
        .expect("connect to node API")
}

fn authenticated<T>(message: T, token_byte: u8) -> Request<T> {
    let mut request = Request::new(message);
    let encoded = URL_SAFE_NO_PAD.encode([token_byte; 32]);
    request.metadata_mut().insert(
        WORKLOAD_TOKEN_HEADER,
        MetadataValue::try_from(encoded).expect("token metadata"),
    );
    request
}

fn claims() -> WorkloadClaims {
    WorkloadClaims {
        workload_id: workload_id(),
        assignment_id: AssignmentId::new("assignment-1").expect("assignment id"),
        node_id: NodeId::new("node-1").expect("node id"),
        service_id: ServiceId::new("api").expect("service id"),
        deployment_id: DeploymentId::new("deployment-1").expect("deployment id"),
        labels: BTreeMap::from([("environment".to_owned(), "test".to_owned())]),
    }
}

fn workload_id() -> WorkloadId {
    WorkloadId::new("workload-1").expect("workload id")
}
