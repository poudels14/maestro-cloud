use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response as HttpResponse};

use maestro_agent::ImageFetcher;
use maestro_control::{AgentHub, StoreWriter};
use maestro_core::{MaintenanceKind, NodeId, NodeRecord, NodeRole, NodeState};
use maestro_proto::pb::node_client::NodeClient;
use maestro_proto::pb::node_server::Node;
use maestro_proto::pb::{Chunk, Envelope, ExecFrame, ImageRequest, UploadChunk};
use maestro_proto::{CertRole, NodeCredentials, peer_identity};
use maestro_runtime::ContainerdRuntime;
use maestro_store::{Consensus, ConsensusError, Mutation, SecretKey, StateMachine};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

use crate::cluster_service::{decode, encode};
use crate::exec::{ExecLimiter, serve_node_exec};
use crate::node_maintenance;
use crate::unix_http;
use crate::uploads::{self, UploadAccepted, UploadSpec};

const IMAGE_CHUNK_BUFFER: usize = 16;
pub const BUILD_LOGS_DIR: &str = "builds/logs";
const NODE_CALL_TIMEOUT: Duration = Duration::from_secs(10);
const NODE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_LOG_LIMIT: usize = 100;
const MAX_LOG_LIMIT: usize = 1_000;

type ChunkStream = Pin<Box<dyn Stream<Item = Result<Chunk, Status>> + Send>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum NodeRequest {
    QueryLogs {
        query: String,
        limit: usize,
        before_ts: Option<i64>,
        #[serde(default)]
        after_ts: Option<i64>,
    },
    Write {
        mutations: Vec<Mutation>,
    },
    RemoveNode {
        node_id: NodeId,
    },
    Disks,
    Telemetry {
        path: String,
    },
    Info,
    Maintain {
        kind: MaintenanceKind,
        run_id: String,
    },
    BuildLog {
        deployment_id: String,
    },
    ObservedReplicas,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RemovalState {
    Draining,
    Removed,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemovalResponse {
    node_id: NodeId,
    state: RemovalState,
}

#[derive(Clone, Copy)]
enum CallerRole {
    Voter,
    Member,
}

pub struct NodeService {
    telemetry_socket: PathBuf,
    consensus: Option<Arc<Consensus>>,
    runtime: Arc<ContainerdRuntime>,
    uploads: Option<(Arc<SecretKey>, PathBuf)>,
    exec_limiter: ExecLimiter,
    started: std::time::Instant,
    node_dir: PathBuf,
    hub: Option<AgentHub>,
    members: Option<tokio::sync::watch::Receiver<Option<Vec<NodeId>>>>,
}

#[derive(Clone)]
pub struct NodeClients {
    credentials: NodeCredentials,
    voters_only: bool,
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

#[derive(Clone)]
pub struct ClusterAccess {
    pub consensus: Arc<Consensus>,
    pub clients: NodeClients,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterLogsParams {
    query: Option<String>,
    limit: Option<usize>,
    before_ts: Option<i64>,
    after_ts: Option<i64>,
    node_id: Option<u64>,
}

pub struct MeshImageFetcher {
    runtime: Arc<ContainerdRuntime>,
    clients: NodeClients,
}

pub struct ForwardingWriter {
    consensus: Arc<Consensus>,
    clients: NodeClients,
}

impl NodeService {
    pub fn new(
        telemetry_socket: PathBuf,
        consensus: Option<Arc<Consensus>>,
        runtime: Arc<ContainerdRuntime>,
        node_dir: PathBuf,
    ) -> Self {
        Self {
            telemetry_socket,
            consensus,
            runtime,
            uploads: None,
            exec_limiter: ExecLimiter::default(),
            started: std::time::Instant::now(),
            node_dir,
            hub: None,
            members: None,
        }
    }

    pub fn with_members(self, members: tokio::sync::watch::Receiver<Option<Vec<NodeId>>>) -> Self {
        Self {
            members: Some(members),
            ..self
        }
    }

    fn authorize<T>(&self, request: &Request<T>, required: CallerRole) -> Result<(), Status> {
        let identity = request
            .peer_certs()
            .and_then(|certificates| peer_identity(&certificates))
            .ok_or_else(|| Status::unauthenticated("a node certificate is required"))?;
        let role = match &self.consensus {
            Some(consensus) => consensus
                .state()
                .node(NodeId(identity.node_id))
                .map_err(internal)?
                .filter(|node| node.state != NodeState::Removed)
                .map(|node| node.role)
                .ok_or_else(|| {
                    Status::permission_denied(format!(
                        "node {} is not a member of this cluster",
                        identity.node_id
                    ))
                })?,
            None => {
                let current = self
                    .members
                    .as_ref()
                    .and_then(|members| members.borrow().clone());
                let member =
                    current.is_none_or(|members| members.contains(&NodeId(identity.node_id)));
                match (member, identity.role) {
                    (false, _) => Err(Status::permission_denied(format!(
                        "node {} is not a member of this cluster",
                        identity.node_id
                    )))?,
                    (true, Some(CertRole::Voter)) => NodeRole::Voter,
                    (true, _) => NodeRole::Worker,
                }
            }
        };
        match (required, role) {
            (CallerRole::Member, _) | (CallerRole::Voter, NodeRole::Voter) => Ok(()),
            (CallerRole::Voter, NodeRole::Worker) => Err(Status::permission_denied(
                "only voters may make this request",
            )),
        }
    }

    pub fn with_hub(self, hub: AgentHub) -> Self {
        Self {
            hub: Some(hub),
            ..self
        }
    }

    pub fn with_uploads(self, key: Arc<SecretKey>, directory: PathBuf) -> Self {
        Self {
            uploads: Some((key, directory)),
            ..self
        }
    }

    async fn handle(&self, request: NodeRequest) -> Result<Value, Status> {
        match request {
            NodeRequest::QueryLogs {
                query,
                limit,
                before_ts,
                after_ts,
            } => {
                let mut search = url_query(&[("query", query), ("limit", limit.to_string())]);
                if let Some(before_ts) = before_ts {
                    search.push_str(&format!("&beforeTs={before_ts}"));
                }
                if let Some(after_ts) = after_ts {
                    search.push_str(&format!("&afterTs={after_ts}"));
                }
                let (status, body) =
                    unix_http::get(&self.telemetry_socket, &format!("/logs?{search}"))
                        .await
                        .map_err(|error| Status::unavailable(format!("telemetry: {error}")))?;
                match status.is_success() {
                    true => serde_json::from_slice(&body)
                        .map_err(|error| Status::internal(error.to_string())),
                    false => Err(Status::invalid_argument(telemetry_error(&body))),
                }
            }
            NodeRequest::Write { mutations } => match &self.consensus {
                Some(consensus) => consensus
                    .write(mutations)
                    .await
                    .map(Value::from)
                    .map_err(|error| Status::failed_precondition(error.to_string())),
                None => Err(Status::failed_precondition(
                    "this node does not run the control plane",
                )),
            },
            NodeRequest::Disks => encode_value(&maestro_telemetry::disks()),
            NodeRequest::Info => {
                let stats = unix_http::get(&self.telemetry_socket, "/stats")
                    .await
                    .ok()
                    .filter(|(status, _)| status.is_success())
                    .and_then(|(_, body)| serde_json::from_slice::<Value>(&body).ok());
                Ok(json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "uptimeMs": self.started.elapsed().as_millis() as u64,
                    "maintenance": node_maintenance::progress(&self.node_dir),
                    "telemetry": stats.is_some(),
                    "sinks": stats.as_ref().map(|stats| stats["sinks"].clone()).unwrap_or_else(|| json!([])),
                    "backup": stats.map(|stats| stats["backup"].clone()).unwrap_or(Value::Null),
                }))
            }
            NodeRequest::Maintain { kind, run_id } => {
                node_maintenance::start(self.node_dir.clone(), kind, run_id);
                Ok(json!({ "accepted": true }))
            }
            NodeRequest::ObservedReplicas => match &self.hub {
                Some(hub) => encode_value(&hub.snapshot()),
                None => Err(Status::failed_precondition(
                    "this node does not run the control plane",
                )),
            },
            NodeRequest::BuildLog { deployment_id } => Ok(json!({
                "text": read_build_log(&self.node_dir.join(BUILD_LOGS_DIR), &deployment_id).await,
            })),
            NodeRequest::Telemetry { path } => {
                let (status, body) = unix_http::get(&self.telemetry_socket, &path)
                    .await
                    .map_err(|error| Status::unavailable(format!("telemetry: {error}")))?;
                match status.is_success() {
                    true => serde_json::from_slice(&body)
                        .map_err(|error| Status::internal(error.to_string())),
                    false => Err(Status::invalid_argument(telemetry_error(&body))),
                }
            }
            NodeRequest::RemoveNode { node_id } => match &self.consensus {
                Some(consensus) => remove_node(consensus, node_id)
                    .await
                    .and_then(|state| encode_value(&state)),
                None => Err(Status::failed_precondition(
                    "this node does not run the control plane",
                )),
            },
        }
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    type ExecStream = ReceiverStream<Result<ExecFrame, Status>>;
    type FetchImageStream = ChunkStream;

    async fn call(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        self.authorize(&request, CallerRole::Voter)?;
        let node_request: NodeRequest = decode(request.get_ref())?;
        let reply = self.handle(node_request).await?;
        Ok(Response::new(encode(&reply)?))
    }

    async fn fetch_image(
        &self,
        request: Request<ImageRequest>,
    ) -> Result<Response<Self::FetchImageStream>, Status> {
        self.authorize(&request, CallerRole::Member)?;
        let image = request.into_inner().image;
        match self.runtime.has_image(&image).await {
            Ok(false) => Err(Status::not_found(format!(
                "image {image} is not on this node"
            ))),
            Err(error) => Err(Status::unavailable(error.to_string())),
            Ok(true) => {
                let chunks = self
                    .runtime
                    .export_image(&image)
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?;
                let stream: ChunkStream = Box::pin(ReceiverStream::new(chunks).map(|chunk| {
                    chunk
                        .map(|data| Chunk { data })
                        .map_err(|error| Status::internal(error.to_string()))
                }));
                Ok(Response::new(stream))
            }
        }
    }

    async fn upload(
        &self,
        request: Request<Streaming<UploadChunk>>,
    ) -> Result<Response<Envelope>, Status> {
        self.authorize(&request, CallerRole::Voter)?;
        match (&self.consensus, &self.uploads) {
            (Some(consensus), Some((key, directory))) => {
                let mut chunks = request.into_inner();
                let first = chunks
                    .message()
                    .await?
                    .ok_or_else(|| Status::invalid_argument("the upload is empty"))?;
                let upload: UploadSpec = serde_json::from_slice(&first.metadata)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                tokio::fs::create_dir_all(directory)
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?;
                let incoming = uploads::incoming_path(directory);
                let received = receive_chunks(&mut chunks, &incoming).await;
                let accepted = match received {
                    Ok(()) => {
                        uploads::accept_upload(consensus, key, directory, upload, &incoming).await
                    }
                    Err(status) => Err(status),
                };
                let _ = tokio::fs::remove_file(&incoming).await;
                Ok(Response::new(encode(&accepted?)?))
            }
            _ => Err(Status::failed_precondition(
                "this node does not run the control plane",
            )),
        }
    }

    async fn exec(
        &self,
        request: Request<Streaming<ExecFrame>>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        self.authorize(&request, CallerRole::Voter)?;
        serve_node_exec(
            self.runtime.clone(),
            &self.exec_limiter,
            request.into_inner(),
        )
        .await
        .map(Response::new)
    }
}

impl NodeClients {
    pub fn new(credentials: NodeCredentials) -> Self {
        Self {
            credentials,
            voters_only: false,
            channels: Arc::default(),
        }
    }

    pub fn voters(credentials: NodeCredentials) -> Self {
        Self {
            credentials,
            voters_only: true,
            channels: Arc::default(),
        }
    }

    pub async fn call(&self, address: &str, request: &NodeRequest) -> Result<Value, Status> {
        let channel = self.channel(address).await?;
        let reply = NodeClient::new(channel).call(encode(request)?).await?;
        decode(&reply.into_inner())
    }

    pub async fn upload(
        &self,
        address: &str,
        upload: &UploadSpec,
        archive: &std::path::Path,
    ) -> Result<UploadAccepted, Status> {
        let chunks = uploads::stream_chunks(upload, archive).await?;
        let channel = self.channel(address).await?;
        let reply = NodeClient::new(channel)
            .upload(ReceiverStream::new(chunks))
            .await?;
        decode(&reply.into_inner())
    }

    pub async fn exec(
        &self,
        address: &str,
        frames: mpsc::Receiver<ExecFrame>,
    ) -> Result<Streaming<ExecFrame>, Status> {
        let channel = self.channel(address).await?;
        let response = NodeClient::new(channel)
            .exec(ReceiverStream::new(frames))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn fetch_image(
        &self,
        address: &str,
        image: &str,
    ) -> Result<tonic::Streaming<Chunk>, Status> {
        let channel = self.channel(address).await?;
        let response = NodeClient::new(channel)
            .fetch_image(ImageRequest {
                image: image.to_string(),
            })
            .await?;
        Ok(response.into_inner())
    }

    async fn channel(&self, address: &str) -> Result<Channel, Status> {
        let cached = self
            .channels
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(address)
            .cloned();
        match cached {
            Some(channel) => Ok(channel),
            None => {
                let channel = Channel::from_shared(format!("https://{address}"))
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
                    .tls_config(match self.voters_only {
                        true => self.credentials.voter_client_tls(),
                        false => self.credentials.client_tls(),
                    })
                    .map_err(|error| Status::internal(error.to_string()))?
                    .connect_timeout(NODE_CONNECT_TIMEOUT)
                    .connect()
                    .await
                    .map_err(|error| Status::unavailable(format!("{address}: {error}")))?;
                self.channels
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(address.to_string(), channel.clone());
                Ok(channel)
            }
        }
    }
}

impl MeshImageFetcher {
    pub fn new(runtime: Arc<ContainerdRuntime>, clients: NodeClients) -> Self {
        Self { runtime, clients }
    }

    async fn fetch_missing(&self, image: &str, source: &str) -> Result<(), String> {
        let present = self
            .runtime
            .has_image(image)
            .await
            .map_err(|error| error.to_string())?;
        match present {
            true => Ok(()),
            false => {
                tracing::info!(%image, %source, "fetching image from its build node");
                let mut stream =
                    self.clients
                        .fetch_image(source, image)
                        .await
                        .map_err(|status| {
                            format!("fetch {image} from {source}: {}", status.message())
                        })?;
                let (chunks, receiver) = mpsc::channel(IMAGE_CHUNK_BUFFER);
                let forward = tokio::spawn(async move {
                    while let Some(chunk) = stream.message().await? {
                        if chunks.send(chunk.data).await.is_err() {
                            break;
                        }
                    }
                    Ok::<(), Status>(())
                });
                let imported = self
                    .runtime
                    .import_image(image, receiver)
                    .await
                    .map_err(|error| error.to_string());
                let forwarded =
                    forward
                        .await
                        .map_err(|error| error.to_string())?
                        .map_err(|status| {
                            format!("image stream from {source} failed: {}", status.message())
                        });
                forwarded.and(imported)
            }
        }
    }
}

impl ImageFetcher for MeshImageFetcher {
    fn fetch<'a>(
        &'a self,
        image: &'a str,
        source: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(self.fetch_missing(image, source))
    }
}

impl ForwardingWriter {
    pub fn new(consensus: Arc<Consensus>, clients: NodeClients) -> Self {
        Self { consensus, clients }
    }
}

impl StoreWriter for ForwardingWriter {
    async fn write(&self, mutations: Vec<Mutation>) -> Result<u64, ConsensusError> {
        match self.consensus.write(mutations.clone()).await {
            Err(ConsensusError::ForwardToLeader {
                leader_address: Some(address),
                ..
            }) => self
                .clients
                .call(&address, &NodeRequest::Write { mutations })
                .await
                .and_then(|reply| {
                    reply
                        .as_u64()
                        .ok_or_else(|| Status::internal("leader returned no version"))
                })
                .map_err(|status| ConsensusError::Rejected(status.message().to_string())),
            other => other,
        }
    }

    fn state(&self) -> &StateMachine {
        self.consensus.state()
    }
}

pub async fn cluster_logs(
    State(fan_out): State<ClusterAccess>,
    Query(params): Query<ClusterLogsParams>,
) -> HttpResponse {
    match fan_out.consensus.state().nodes() {
        Ok(nodes) => {
            let active = nodes
                .into_iter()
                .filter(|node| node.state != NodeState::Removed)
                .filter(|node| {
                    params
                        .node_id
                        .is_none_or(|node_id| node.id == NodeId(node_id))
                })
                .collect();
            query_nodes(&fan_out.clients, active, params).await
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn remove_cluster_node(
    State(access): State<ClusterAccess>,
    Path(node_id): Path<String>,
) -> HttpResponse {
    let removal = match node_id.parse().map(NodeId) {
        Err(_) => Err(Status::not_found(format!("node {node_id} not found"))),
        Ok(node_id) => match access.consensus.current_leader() {
            Some((leader, _)) if leader == access.consensus.metrics().node_id => {
                remove_node(&access.consensus, node_id).await
            }
            Some((_, address)) => access
                .clients
                .call(&address, &NodeRequest::RemoveNode { node_id })
                .await
                .and_then(|reply| serde_json::from_value(reply).map_err(internal)),
            None => Err(Status::unavailable("the cluster has no leader")),
        }
        .map(|state| (node_id, state)),
    };
    match removal {
        Ok((node_id, state)) => {
            let status = match state {
                RemovalState::Draining => StatusCode::ACCEPTED,
                RemovalState::Removed => StatusCode::OK,
            };
            (status, Json(RemovalResponse { node_id, state })).into_response()
        }
        Err(status) => {
            let code = match status.code() {
                tonic::Code::NotFound => StatusCode::NOT_FOUND,
                tonic::Code::FailedPrecondition => StatusCode::CONFLICT,
                tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (code, Json(json!({ "error": status.message() }))).into_response()
        }
    }
}

async fn remove_node(consensus: &Consensus, node_id: NodeId) -> Result<RemovalState, Status> {
    let state = consensus.state();
    let node = state
        .node(node_id)
        .map_err(internal)?
        .ok_or_else(|| Status::not_found(format!("node {node_id} not found")))?;
    let has_assignments = !state.assignments(node_id).map_err(internal)?.is_empty();
    let metrics = consensus.metrics();
    let in_membership =
        metrics.voters.contains_key(&node_id) || metrics.learners.contains_key(&node_id);
    match (node.state, has_assignments) {
        (NodeState::Removed, _) => Ok(RemovalState::Removed),
        _ if node_id == metrics.node_id => Err(Status::failed_precondition(
            "this node is the leader; stop it so another voter takes over, then remove it",
        )),
        (_, true) => {
            if node.state == NodeState::Active {
                write(
                    consensus,
                    vec![Mutation::PutNode {
                        node: NodeRecord {
                            state: NodeState::Draining,
                            ..node
                        },
                    }],
                )
                .await?;
            }
            Ok(RemovalState::Draining)
        }
        (_, false) => {
            if in_membership {
                consensus
                    .remove_node(node_id)
                    .await
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
            }
            write(
                consensus,
                vec![Mutation::PutNode {
                    node: NodeRecord {
                        state: NodeState::Removed,
                        ..node
                    },
                }],
            )
            .await?;
            Ok(RemovalState::Removed)
        }
    }
}

async fn write(consensus: &Consensus, mutations: Vec<Mutation>) -> Result<u64, Status> {
    consensus
        .write(mutations)
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn encode_value<T: Serialize>(value: &T) -> Result<Value, Status> {
    serde_json::to_value(value).map_err(internal)
}

pub async fn cluster_disks(State(access): State<ClusterAccess>) -> HttpResponse {
    match access.consensus.state().nodes() {
        Ok(nodes) => {
            let active = nodes
                .into_iter()
                .filter(|node| node.state != NodeState::Removed)
                .collect();
            let disks = call_nodes(&access.clients, active, NodeRequest::Disks)
                .await
                .into_iter()
                .filter_map(|(node_id, node_name, reply)| match reply {
                    Ok(disks) => Some((node_id.to_string(), disks)),
                    Err(status) => {
                        tracing::warn!(node = %node_name, error = %status.message(), "node disk query failed");
                        None
                    }
                })
                .collect::<serde_json::Map<_, _>>();
            Json(disks).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn local_disks() -> Json<Vec<maestro_telemetry::Disk>> {
    Json(maestro_telemetry::disks())
}

pub async fn call_nodes(
    clients: &NodeClients,
    nodes: Vec<NodeRecord>,
    request: NodeRequest,
) -> Vec<(NodeId, String, Result<Value, Status>)> {
    let mut calls = JoinSet::new();
    nodes.into_iter().for_each(|node| {
        let clients = clients.clone();
        let request = request.clone();
        calls.spawn(async move {
            let reply =
                tokio::time::timeout(NODE_CALL_TIMEOUT, clients.call(&node.endpoint, &request))
                    .await
                    .unwrap_or_else(|_| Err(Status::deadline_exceeded("node call timed out")));
            (node.id, node.name, reply)
        });
    });
    calls.join_all().await
}

async fn receive_chunks(
    chunks: &mut Streaming<UploadChunk>,
    destination: &std::path::Path,
) -> Result<(), Status> {
    let mut file = tokio::fs::File::create(destination)
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
    let mut received = 0usize;
    while let Some(chunk) = chunks.message().await? {
        received += chunk.data.len();
        if received > uploads::MAX_UPLOAD_BYTES {
            return Err(Status::resource_exhausted(
                "the upload is larger than 1 GiB",
            ));
        }
        file.write_all(&chunk.data)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
    }
    file.flush()
        .await
        .map_err(|error| Status::internal(error.to_string()))
}

async fn query_nodes(
    clients: &NodeClients,
    nodes: Vec<NodeRecord>,
    params: ClusterLogsParams,
) -> HttpResponse {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LOG_LIMIT)
        .clamp(1, MAX_LOG_LIMIT);
    let request = NodeRequest::QueryLogs {
        query: params.query.unwrap_or_default(),
        limit: limit + 1,
        before_ts: params.before_ts,
        after_ts: params.after_ts,
    };
    let mut records = Vec::new();
    let mut unavailable = Vec::new();
    let mut rejection = None;
    for (node_id, node_name, reply) in call_nodes(clients, nodes, request).await {
        match reply {
            Ok(page) => records.extend(
                page["records"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|mut record| {
                        record["nodeId"] = json!(node_id);
                        record["nodeName"] = json!(node_name);
                        record
                    }),
            ),
            Err(status) if status.code() == tonic::Code::InvalidArgument => {
                rejection = Some(status.message().to_string());
            }
            Err(status) => {
                tracing::warn!(node = %node_name, error = %status.message(), "node log query failed");
                unavailable.push(json!({
                    "nodeId": node_id,
                    "nodeName": node_name,
                    "error": status.message(),
                }));
            }
        }
    }
    match rejection {
        Some(message) => {
            (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
        }
        None => {
            records.sort_by_key(|record| std::cmp::Reverse(record_ts(record)));
            let next_cursor = page_boundary(&mut records, limit);
            Json(json!({
                "records": records,
                "nextCursor": next_cursor,
                "partial": !unavailable.is_empty(),
                "unavailableNodes": unavailable,
            }))
            .into_response()
        }
    }
}

pub async fn read_build_log(directory: &std::path::Path, deployment_id: &str) -> Option<String> {
    let valid = !deployment_id.is_empty()
        && deployment_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-');
    match valid {
        true => tokio::fs::read_to_string(directory.join(format!("{deployment_id}.log")))
            .await
            .ok(),
        false => None,
    }
}

fn page_boundary(records: &mut Vec<Value>, limit: usize) -> Option<i64> {
    match records.len() > limit {
        false => None,
        true => {
            let boundary_ts = record_ts(&records[limit]);
            let complete = records[..limit]
                .iter()
                .take_while(|record| record_ts(record) > boundary_ts)
                .count();
            match complete {
                0 => {
                    records.truncate(limit);
                    Some(boundary_ts)
                }
                _ => {
                    records.truncate(complete);
                    Some(boundary_ts + 1)
                }
            }
        }
    }
}

fn telemetry_error(body: &[u8]) -> String {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|error| error["error"].as_str().map(str::to_string))
        .unwrap_or_else(|| String::from_utf8_lossy(body).to_string())
}

fn record_ts(record: &Value) -> i64 {
    record["ts"].as_i64().unwrap_or(0)
}

pub fn url_query(pairs: &[(&str, String)]) -> String {
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={}", percent_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records(timestamps: &[i64]) -> Vec<Value> {
        timestamps.iter().map(|ts| json!({ "ts": ts })).collect()
    }

    #[test]
    fn short_pages_have_no_cursor() {
        let mut page = records(&[30, 20, 10]);
        assert_eq!(page_boundary(&mut page, 3), None);
        assert_eq!(page.len(), 3);
    }

    #[test]
    fn records_sharing_the_boundary_timestamp_move_to_the_next_page() {
        let mut page = records(&[50, 40, 30, 30, 30]);
        assert_eq!(page_boundary(&mut page, 4), Some(31));
        assert_eq!(page, records(&[50, 40]));
    }

    #[test]
    fn a_page_of_identical_timestamps_still_advances() {
        let mut page = records(&[30, 30, 30]);
        assert_eq!(page_boundary(&mut page, 2), Some(30));
        assert_eq!(page.len(), 2);
    }
}
