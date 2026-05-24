//! In-memory [`NodeRegistry`] and [`LeaderElector`] implementations for tests.
//!
//! These share an [`InMemoryCluster`] handle so multiple "nodes" can register
//! against the same fake cluster and run elections against each other —
//! letting us drive multi-node scenarios without etcd.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::watch;

use super::elector::LeaderElector;
use super::registry::NodeRegistry;
use super::types::{LeaderInfo, LeadershipState, NodeId, NodeInfo};

const KEEP_ALIVE_TTL: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
struct ClusterState {
    nodes: HashMap<NodeId, RegisteredNode>,
    leader: Option<LeaderInfo>,
    clock_ms: u64,
    watchers: HashMap<NodeId, watch::Sender<LeadershipState>>,
}

#[derive(Debug, Clone)]
struct RegisteredNode {
    info: NodeInfo,
    expires_at_ms: u64,
}

#[derive(Clone, Default)]
pub struct InMemoryCluster {
    inner: Arc<Mutex<ClusterState>>,
}

impl InMemoryCluster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn advance_clock(&self, delta_ms: u64) {
        let snapshot = {
            let mut state = self.inner.lock().expect("cluster state");
            state.clock_ms = state.clock_ms.saturating_add(delta_ms);
            let now_ms = state.clock_ms;
            let expired: Vec<NodeId> = state
                .nodes
                .iter()
                .filter(|(_, node)| node.expires_at_ms <= now_ms)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &expired {
                state.nodes.remove(id);
            }
            let leader_lost = state
                .leader
                .as_ref()
                .map(|info| !state.nodes.contains_key(&info.node_id))
                .unwrap_or(false);
            if leader_lost {
                state.leader = None;
                let next = state
                    .nodes
                    .values()
                    .min_by_key(|node| node.info.started_at_ms)
                    .map(|node| LeaderInfo {
                        node_id: node.info.node_id.clone(),
                        elected_at_ms: now_ms,
                    });
                state.leader = next;
            }
            state.leader.clone()
        };
        self.broadcast_leadership(snapshot);
    }

    pub fn current_leader(&self) -> Option<LeaderInfo> {
        self.inner.lock().expect("cluster state").leader.clone()
    }

    pub fn registered_node_ids(&self) -> Vec<NodeId> {
        let state = self.inner.lock().expect("cluster state");
        let mut ids: Vec<NodeId> = state.nodes.keys().cloned().collect();
        ids.sort();
        ids
    }

    fn broadcast_leadership(&self, leader: Option<LeaderInfo>) {
        let watchers: Vec<(NodeId, watch::Sender<LeadershipState>)> = {
            let state = self.inner.lock().expect("cluster state");
            state
                .watchers
                .iter()
                .map(|(node_id, sender)| (node_id.clone(), sender.clone()))
                .collect()
        };
        for (node_id, sender) in watchers {
            let next_state = state_for_node(&node_id, leader.as_ref());
            let _ = sender.send(next_state);
        }
    }

    fn subscribe(&self, node_id: &NodeId) -> watch::Receiver<LeadershipState> {
        let mut state = self.inner.lock().expect("cluster state");
        let initial = state_for_node(node_id, state.leader.as_ref());
        let (sender, receiver) = watch::channel(initial);
        state.watchers.insert(node_id.clone(), sender);
        receiver
    }

    fn maybe_elect(&self, candidate: &NodeId) {
        let snapshot = {
            let mut state = self.inner.lock().expect("cluster state");
            let now_ms = state.clock_ms;
            if state.leader.is_some() {
                state.leader.clone()
            } else if !state.nodes.contains_key(candidate) {
                None
            } else {
                let info = LeaderInfo {
                    node_id: candidate.clone(),
                    elected_at_ms: now_ms,
                };
                state.leader = Some(info.clone());
                Some(info)
            }
        };
        self.broadcast_leadership(snapshot);
    }

    fn resign(&self, node_id: &NodeId) {
        let snapshot = {
            let mut state = self.inner.lock().expect("cluster state");
            let is_leader = state
                .leader
                .as_ref()
                .map(|info| &info.node_id == node_id)
                .unwrap_or(false);
            if is_leader {
                state.leader = None;
                let next = state
                    .nodes
                    .values()
                    .filter(|node| &node.info.node_id != node_id)
                    .min_by_key(|node| node.info.started_at_ms)
                    .map(|node| LeaderInfo {
                        node_id: node.info.node_id.clone(),
                        elected_at_ms: state.clock_ms,
                    });
                state.leader = next;
            }
            state.leader.clone()
        };
        self.broadcast_leadership(snapshot);
    }

    fn list_nodes_internal(&self) -> Vec<NodeInfo> {
        let state = self.inner.lock().expect("cluster state");
        let now_ms = state.clock_ms;
        let mut nodes: Vec<NodeInfo> = state
            .nodes
            .values()
            .filter(|node| node.expires_at_ms > now_ms)
            .map(|node| node.info.clone())
            .collect();
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        nodes
    }
}

fn state_for_node(node_id: &NodeId, leader: Option<&LeaderInfo>) -> LeadershipState {
    match leader {
        Some(info) if &info.node_id == node_id => LeadershipState::Leading(info.clone()),
        Some(info) => LeadershipState::Following(Some(info.clone())),
        None => LeadershipState::Following(None),
    }
}

pub struct InMemoryNodeRegistry {
    cluster: InMemoryCluster,
    node_id: NodeId,
}

impl InMemoryNodeRegistry {
    pub fn new(cluster: InMemoryCluster, node_id: NodeId) -> Self {
        Self { cluster, node_id }
    }
}

#[async_trait]
impl NodeRegistry for InMemoryNodeRegistry {
    async fn register(&self, info: &NodeInfo) -> Result<()> {
        if info.node_id != self.node_id {
            return Err(anyhow!(
                "registry initialized for node {} but registering {}",
                self.node_id,
                info.node_id
            ));
        }
        let mut state = self.cluster.inner.lock().expect("cluster state");
        let now_ms = state.clock_ms;
        let expires_at_ms = now_ms + KEEP_ALIVE_TTL.as_millis() as u64;
        state.nodes.insert(
            info.node_id.clone(),
            RegisteredNode {
                info: info.clone(),
                expires_at_ms,
            },
        );
        Ok(())
    }

    async fn keep_alive(&self) -> Result<()> {
        let mut state = self.cluster.inner.lock().expect("cluster state");
        let now_ms = state.clock_ms;
        let entry = state
            .nodes
            .get_mut(&self.node_id)
            .ok_or_else(|| anyhow!("node {} not registered", self.node_id))?;
        entry.expires_at_ms = now_ms + KEEP_ALIVE_TTL.as_millis() as u64;
        Ok(())
    }

    async fn deregister(&self) -> Result<()> {
        self.cluster
            .inner
            .lock()
            .expect("cluster state")
            .nodes
            .remove(&self.node_id);
        Ok(())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        Ok(self.cluster.list_nodes_internal())
    }

    async fn get_node(&self, node_id: &NodeId) -> Result<Option<NodeInfo>> {
        Ok(self
            .cluster
            .list_nodes_internal()
            .into_iter()
            .find(|node| &node.node_id == node_id))
    }

    async fn set_unschedulable(&self, unschedulable: bool) -> Result<()> {
        let mut state = self.cluster.inner.lock().expect("cluster state");
        let entry = state
            .nodes
            .get_mut(&self.node_id)
            .ok_or_else(|| anyhow!("node {} not registered", self.node_id))?;
        entry.info.unschedulable = unschedulable;
        Ok(())
    }
}

pub struct InMemoryLeaderElector {
    cluster: InMemoryCluster,
    node_id: NodeId,
    receiver: watch::Receiver<LeadershipState>,
}

impl InMemoryLeaderElector {
    pub fn new(cluster: InMemoryCluster, node_id: NodeId) -> Self {
        let receiver = cluster.subscribe(&node_id);
        Self {
            cluster,
            node_id,
            receiver,
        }
    }
}

#[async_trait]
impl LeaderElector for InMemoryLeaderElector {
    async fn campaign(&self) -> Result<()> {
        let mut receiver = self.receiver.clone();
        self.cluster.maybe_elect(&self.node_id);
        loop {
            let current = receiver.borrow().clone();
            if let LeadershipState::Leading(_) = current {
                return Ok(());
            }
            self.cluster.maybe_elect(&self.node_id);
            if receiver.changed().await.is_err() {
                return Ok(());
            }
        }
    }

    async fn resign(&self) -> Result<()> {
        self.cluster.resign(&self.node_id);
        Ok(())
    }

    fn state(&self) -> LeadershipState {
        self.receiver.borrow().clone()
    }

    fn subscribe(&self) -> watch::Receiver<LeadershipState> {
        self.receiver.clone()
    }

    fn this_node(&self) -> &NodeId {
        &self.node_id
    }
}
