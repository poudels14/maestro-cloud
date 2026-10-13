use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maestro_core::{ControlMessage, NodeId, ReplicaId, ReplicaPhase, ReplicaStatus, RouteTable};
use maestro_store::{SecretKey, StateMachine, replica_plans};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::deployer::ObservedReplicas;
use crate::routing::{cluster_dns, cluster_routes};

const AGENT_BUFFER: usize = 64;
const PUBLISH_RESYNC: Duration = Duration::from_secs(10);
const UNREACHABLE_ERROR: &str = "node unreachable";

#[derive(Clone)]
pub struct AgentHub {
    inner: Arc<Mutex<HubState>>,
    changes: Arc<watch::Sender<u64>>,
    node_expiry: Duration,
}

#[derive(Default)]
struct HubState {
    links: HashMap<NodeId, AgentLink>,
    replicas: HashMap<NodeId, NodeReplicas>,
    next_session: u64,
}

struct AgentLink {
    session: u64,
    sender: mpsc::Sender<ControlMessage>,
    last_plans: Option<ControlMessage>,
    last_routes: Option<ControlMessage>,
}

struct NodeReplicas {
    statuses: Vec<ReplicaStatus>,
    disconnected_at: Option<Instant>,
}

pub struct AgentSession {
    pub session: u64,
    pub messages: mpsc::Receiver<ControlMessage>,
}

impl AgentHub {
    pub fn new(node_expiry: Duration) -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            inner: Arc::default(),
            changes: Arc::new(changes),
            node_expiry,
        }
    }

    pub fn register(&self, node_id: NodeId) -> AgentSession {
        let (sender, messages) = mpsc::channel(AGENT_BUFFER);
        let session = {
            let mut state = self.lock();
            state.next_session += 1;
            let session = state.next_session;
            state.links.insert(
                node_id,
                AgentLink {
                    session,
                    sender,
                    last_plans: None,
                    last_routes: None,
                },
            );
            if let Some(replicas) = state.replicas.get_mut(&node_id) {
                replicas.disconnected_at = None;
            }
            session
        };
        self.bump();
        AgentSession { session, messages }
    }

    pub fn unregister(&self, node_id: NodeId, session: u64) {
        let removed = {
            let mut state = self.lock();
            let current = state
                .links
                .get(&node_id)
                .is_some_and(|link| link.session == session);
            if current {
                state.links.remove(&node_id);
                if let Some(replicas) = state.replicas.get_mut(&node_id) {
                    replicas.disconnected_at = Some(Instant::now());
                }
            }
            current
        };
        if removed {
            self.bump();
        }
    }

    pub fn record_replicas(&self, node_id: NodeId, statuses: Vec<ReplicaStatus>) {
        let changed = {
            let mut state = self.lock();
            let previous = state.replicas.insert(
                node_id,
                NodeReplicas {
                    statuses: statuses.clone(),
                    disconnected_at: None,
                },
            );
            previous.is_none_or(|previous| previous.statuses != statuses)
        };
        if changed {
            self.bump();
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub fn connected_nodes(&self) -> Vec<NodeId> {
        self.lock().links.keys().copied().collect()
    }

    pub async fn publish(&self, node_id: NodeId, message: ControlMessage) {
        let sender = {
            let mut state = self.lock();
            state.links.get_mut(&node_id).and_then(|link| {
                let slot = match message {
                    ControlMessage::Plans { .. } => &mut link.last_plans,
                    ControlMessage::Routes { .. } => &mut link.last_routes,
                    ControlMessage::NotLeader { .. } => &mut link.last_routes,
                };
                let unchanged = slot.as_ref() == Some(&message);
                (!unchanged).then(|| {
                    *slot = Some(message.clone());
                    link.sender.clone()
                })
            })
        };
        if let Some(sender) = sender {
            let _ = sender.send(message).await;
        }
    }

    fn statuses(&self) -> Vec<ReplicaStatus> {
        let now = Instant::now();
        let expiry = self.node_expiry;
        self.lock()
            .replicas
            .values()
            .flat_map(|node| {
                let expired = node
                    .disconnected_at
                    .is_some_and(|since| now.duration_since(since) >= expiry);
                node.statuses
                    .iter()
                    .cloned()
                    .map(move |status| match expired {
                        true => ReplicaStatus {
                            phase: ReplicaPhase::Failed,
                            last_error: Some(UNREACHABLE_ERROR.to_string()),
                            ..status
                        },
                        false => status,
                    })
            })
            .collect()
    }

    fn bump(&self) {
        self.changes.send_modify(|version| *version += 1);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HubState> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ObservedReplicas for AgentHub {
    fn replica(&self, replica_id: &ReplicaId) -> Option<ReplicaStatus> {
        self.statuses()
            .into_iter()
            .find(|status| &status.replica_id == replica_id)
    }

    fn replicas(&self) -> Vec<ReplicaStatus> {
        self.statuses()
    }
}

pub async fn run_publisher(
    hub: AgentHub,
    state: Arc<StateMachine>,
    key: Arc<SecretKey>,
    cluster: String,
) {
    let mut hub_changes = hub.subscribe();
    let mut store_changes = state.subscribe();
    let mut previous_routes = RouteTable::default();
    loop {
        let statuses = hub
            .replicas()
            .into_iter()
            .map(|status| (status.replica_id.clone(), status))
            .collect::<BTreeMap<_, _>>();
        let observed = |replica_id: &ReplicaId| statuses.get(replica_id).cloned();
        let routes = cluster_routes(&state, &observed, &previous_routes)
            .and_then(|table| Ok((table, cluster_dns(&state, &observed, &cluster)?)));
        match routes {
            Ok((table, dns)) => {
                for node_id in hub.connected_nodes() {
                    match replica_plans(&state, &key, node_id) {
                        Ok(plans) => hub.publish(node_id, ControlMessage::Plans { plans }).await,
                        Err(error) => tracing::warn!(%node_id, %error, "failed to build plans"),
                    }
                    let message = ControlMessage::Routes {
                        table: table.clone(),
                        dns: dns.clone(),
                    };
                    hub.publish(node_id, message).await;
                }
                previous_routes = table;
            }
            Err(error) => tracing::warn!(%error, "failed to compute routes"),
        }
        tokio::select! {
            _ = hub_changes.changed() => {}
            _ = store_changes.recv() => {}
            _ = tokio::time::sleep(PUBLISH_RESYNC) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use maestro_core::{DeploymentId, ServiceId};

    use super::*;

    fn status(replica: &str, phase: ReplicaPhase) -> ReplicaStatus {
        ReplicaStatus {
            replica_id: ReplicaId::from(replica),
            service_id: ServiceId::from("api"),
            deployment_id: DeploymentId::from("d1"),
            phase,
            ip: None,
            restarts: 0,
            consecutive_failures: 0,
            last_error: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expires_replicas_of_disconnected_nodes() {
        let hub = AgentHub::new(Duration::from_secs(30));
        let session = hub.register(NodeId(1));
        hub.record_replicas(NodeId(1), vec![status("d1-0", ReplicaPhase::Healthy)]);
        hub.unregister(NodeId(1), session.session);

        tokio::time::advance(Duration::from_secs(10)).await;
        let replica_id = ReplicaId::from("d1-0");
        assert_eq!(
            hub.replica(&replica_id).unwrap().phase,
            ReplicaPhase::Healthy
        );

        tokio::time::advance(Duration::from_secs(25)).await;
        let expired = hub.replica(&replica_id).unwrap();
        assert_eq!(expired.phase, ReplicaPhase::Failed);
        assert_eq!(expired.last_error.as_deref(), Some(UNREACHABLE_ERROR));

        hub.register(NodeId(1));
        assert_eq!(
            hub.replica(&replica_id).unwrap().phase,
            ReplicaPhase::Healthy
        );
    }

    #[tokio::test]
    async fn publishes_only_changed_messages_to_the_current_session() {
        let hub = AgentHub::new(Duration::from_secs(30));
        let stale = hub.register(NodeId(1));
        let mut current = hub.register(NodeId(1));
        hub.unregister(NodeId(1), stale.session);

        let plans = ControlMessage::Plans { plans: vec![] };
        hub.publish(NodeId(1), plans.clone()).await;
        hub.publish(NodeId(1), plans.clone()).await;
        assert_eq!(current.messages.recv().await, Some(plans));
        assert!(current.messages.try_recv().is_err());
        assert_eq!(hub.connected_nodes(), [NodeId(1)]);
    }
}
