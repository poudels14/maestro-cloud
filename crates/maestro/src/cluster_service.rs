use std::pin::Pin;
use std::sync::Arc;

use maestro_control::AgentHub;
use maestro_core::{AgentMessage, ClusterSettings, ControlMessage, JoinRequest, NodeId, NodeState};
use maestro_proto::pb::Envelope;
use maestro_proto::pb::cluster_server::Cluster;
use maestro_proto::peer_identity;
use maestro_store::{Consensus, StateMachine};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::join::JoinAuthority;

const OUTBOUND_BUFFER: usize = 64;

type EnvelopeStream = Pin<Box<dyn Stream<Item = Result<Envelope, Status>> + Send>>;

pub struct ClusterService {
    consensus: Arc<Consensus>,
    hub: AgentHub,
    authority: Arc<JoinAuthority>,
    cluster: ClusterSettings,
}

impl ClusterService {
    pub fn new(
        consensus: Arc<Consensus>,
        hub: AgentHub,
        authority: Arc<JoinAuthority>,
        cluster: ClusterSettings,
    ) -> Self {
        Self {
            consensus,
            hub,
            authority,
            cluster,
        }
    }
}

#[tonic::async_trait]
impl Cluster for ClusterService {
    type AttachStream = EnvelopeStream;

    async fn join(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        let join_request: JoinRequest = decode(request.get_ref())?;
        let response = self
            .authority
            .admit(&self.consensus, &self.cluster, join_request)
            .await?;
        Ok(Response::new(encode(&response)?))
    }

    async fn attach(
        &self,
        request: Request<Streaming<Envelope>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let caller = request
            .peer_certs()
            .and_then(|certificates| peer_identity(&certificates))
            .map(|identity| NodeId(identity.node_id));
        match (caller, self.consensus.is_leader()) {
            (None, _) => Err(Status::unauthenticated("a node certificate is required")),
            (Some(_), false) => {
                let leader = self.consensus.current_leader().map(|(_, address)| address);
                let redirect = encode(&ControlMessage::NotLeader { leader })?;
                let stream: EnvelopeStream = Box::pin(tokio_stream::once(Ok(redirect)));
                Ok(Response::new(stream))
            }
            (Some(caller), true) => {
                let inbound = request.into_inner();
                let (outbound_sender, outbound_receiver) = mpsc::channel(OUTBOUND_BUFFER);
                tokio::spawn(serve_agent(
                    self.hub.clone(),
                    self.consensus.state_handle(),
                    caller,
                    inbound,
                    outbound_sender,
                ));
                let stream: EnvelopeStream = Box::pin(ReceiverStream::new(outbound_receiver));
                Ok(Response::new(stream))
            }
        }
    }
}

async fn serve_agent(
    hub: AgentHub,
    state: Arc<StateMachine>,
    caller: NodeId,
    mut inbound: Streaming<Envelope>,
    outbound: mpsc::Sender<Result<Envelope, Status>>,
) {
    let hello = inbound
        .message()
        .await
        .ok()
        .flatten()
        .map(|envelope| decode(&envelope));
    let member = |node_id: NodeId| {
        state
            .node(node_id)
            .ok()
            .flatten()
            .is_some_and(|node| node.state != NodeState::Removed)
    };
    match hello {
        Some(Ok(AgentMessage::Hello { node_id })) if node_id != caller => {
            let _ = outbound
                .send(Err(Status::permission_denied(format!(
                    "this certificate belongs to node {caller}, not node {node_id}"
                ))))
                .await;
        }
        Some(Ok(AgentMessage::Hello { node_id })) if !member(node_id) => {
            let _ = outbound
                .send(Err(Status::permission_denied(format!(
                    "node {node_id} is not a member of this cluster"
                ))))
                .await;
        }
        Some(Ok(AgentMessage::Hello { node_id })) => {
            let mut session = hub.register(node_id);
            let session_id = session.session;
            tokio::spawn(async move {
                while let Some(message) = session.messages.recv().await {
                    if outbound.send(encode(&message)).await.is_err() {
                        break;
                    }
                }
            });
            while let Ok(Some(envelope)) = inbound.message().await {
                if let Ok(AgentMessage::Replicas { statuses }) = decode(&envelope) {
                    let assigned = state
                        .assignments(node_id)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|assignment| assignment.replica_id)
                        .collect::<Vec<_>>();
                    let own = statuses
                        .into_iter()
                        .filter(|status| assigned.contains(&status.replica_id))
                        .collect();
                    hub.record_replicas(node_id, own);
                }
            }
            hub.unregister(node_id, session_id);
        }
        _ => {
            let _ = outbound
                .send(Err(Status::invalid_argument(
                    "the first message must be hello",
                )))
                .await;
        }
    }
}

pub fn encode<T: serde::Serialize>(message: &T) -> Result<Envelope, Status> {
    serde_json::to_vec(message)
        .map(|json| Envelope { json })
        .map_err(|error| Status::internal(error.to_string()))
}

pub fn decode<T: serde::de::DeserializeOwned>(envelope: &Envelope) -> Result<T, Status> {
    serde_json::from_slice(&envelope.json)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}
