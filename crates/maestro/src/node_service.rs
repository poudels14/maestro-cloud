use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response as HttpResponse};

use maestro_control::StoreWriter;
use maestro_core::{NodeId, NodeRecord, NodeState};
use maestro_proto::NodeCredentials;
use maestro_proto::pb::Envelope;
use maestro_proto::pb::node_client::NodeClient;
use maestro_proto::pb::node_server::Node;
use maestro_store::{Consensus, ConsensusError, Mutation, StateMachine};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::task::JoinSet;
use tokio_stream::Stream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

use crate::cluster_service::{decode, encode};
use crate::unix_http;

const NODE_LOG_TIMEOUT: Duration = Duration::from_secs(10);
const NODE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_LOG_LIMIT: usize = 100;
const MAX_LOG_LIMIT: usize = 1_000;

type EnvelopeStream = Pin<Box<dyn Stream<Item = Result<Envelope, Status>> + Send>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum NodeRequest {
    QueryLogs {
        query: String,
        limit: usize,
        before_ts: Option<i64>,
    },
    Write {
        mutations: Vec<Mutation>,
    },
    RemoveNode {
        node_id: NodeId,
    },
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

pub struct NodeService {
    telemetry_socket: PathBuf,
    consensus: Option<Arc<Consensus>>,
}

#[derive(Clone)]
pub struct NodeClients {
    credentials: NodeCredentials,
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
}

pub struct ForwardingWriter {
    consensus: Arc<Consensus>,
    clients: NodeClients,
}

impl NodeService {
    pub fn new(telemetry_socket: PathBuf, consensus: Option<Arc<Consensus>>) -> Self {
        Self {
            telemetry_socket,
            consensus,
        }
    }

    async fn handle(&self, request: NodeRequest) -> Result<Value, Status> {
        match request {
            NodeRequest::QueryLogs {
                query,
                limit,
                before_ts,
            } => {
                let mut search = url_query(&[("query", query), ("limit", limit.to_string())]);
                if let Some(before_ts) = before_ts {
                    search.push_str(&format!("&beforeTs={before_ts}"));
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
    type ExecStream = EnvelopeStream;

    async fn call(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        let authenticated = request
            .peer_certs()
            .is_some_and(|certificates| !certificates.is_empty());
        match authenticated {
            false => Err(Status::unauthenticated("a node certificate is required")),
            true => {
                let node_request: NodeRequest = decode(request.get_ref())?;
                let reply = self.handle(node_request).await?;
                Ok(Response::new(encode(&reply)?))
            }
        }
    }

    async fn exec(
        &self,
        _request: Request<Streaming<Envelope>>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        Err(Status::unimplemented("exec is not available yet"))
    }
}

impl NodeClients {
    pub fn new(credentials: NodeCredentials) -> Self {
        Self {
            credentials,
            channels: Arc::default(),
        }
    }

    pub async fn call(&self, address: &str, request: &NodeRequest) -> Result<Value, Status> {
        let channel = self.channel(address).await?;
        let reply = NodeClient::new(channel).call(encode(request)?).await?;
        decode(&reply.into_inner())
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
                    .tls_config(self.credentials.client_tls())
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
    };
    let mut queries = JoinSet::new();
    nodes.into_iter().for_each(|node| {
        let clients = clients.clone();
        let request = request.clone();
        queries.spawn(async move {
            let reply =
                tokio::time::timeout(NODE_LOG_TIMEOUT, clients.call(&node.endpoint, &request))
                    .await
                    .unwrap_or_else(|_| Err(Status::deadline_exceeded("log query timed out")));
            (node.id, node.name, reply)
        });
    });
    let mut records = Vec::new();
    let mut unavailable = Vec::new();
    let mut rejection = None;
    while let Some(Ok((node_id, node_name, reply))) = queries.join_next().await {
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
                unavailable.push(node_name);
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
                "unavailableNodes": unavailable,
            }))
            .into_response()
        }
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

fn url_query(pairs: &[(&str, String)]) -> String {
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
