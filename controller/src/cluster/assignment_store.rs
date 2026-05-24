//! Desired-state distribution: leader writes assignments, each node watches
//! its own slice.
//!
//! Etcd layout:
//!   assignments/{node-id}/{service-id}/{replica-index} = Assignment (json)
//!
//! This is the "remote execution" surface — each worker reconciles its slice
//! into actual running replicas via the existing [`Engine`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

use super::scheduling::Assignment;
use super::types::NodeId;

pub const ASSIGNMENTS_PREFIX: &str = "cluster/assignments/";

#[async_trait]
pub trait AssignmentStore: Send + Sync {
    /// Replace the entire assignment list for a single node atomically.
    async fn replace_for_node(&self, node_id: &NodeId, assignments: &[Assignment]) -> Result<()>;

    /// Read current assignments for a node.
    async fn list_for_node(&self, node_id: &NodeId) -> Result<Vec<Assignment>>;

    /// Read every assignment in the cluster (used by leader to compute deltas).
    async fn list_all(&self) -> Result<Vec<Assignment>>;
}

pub struct InMemoryAssignmentStore {
    inner: Arc<Mutex<InMemoryAssignmentState>>,
}

#[derive(Default)]
struct InMemoryAssignmentState {
    per_node: HashMap<NodeId, Vec<Assignment>>,
    watchers: HashMap<NodeId, watch::Sender<Vec<Assignment>>>,
}

impl InMemoryAssignmentStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemoryAssignmentState::default())),
        }
    }

    /// Subscribe to changes for a specific node. Used by the assignment
    /// watcher on that node to reconcile its replicas.
    pub fn subscribe(&self, node_id: &NodeId) -> watch::Receiver<Vec<Assignment>> {
        let mut state = self.inner.lock().expect("assignment state");
        let initial = state.per_node.get(node_id).cloned().unwrap_or_default();
        let (sender, receiver) = watch::channel(initial);
        state.watchers.insert(node_id.clone(), sender);
        receiver
    }
}

#[async_trait]
impl AssignmentStore for InMemoryAssignmentStore {
    async fn replace_for_node(&self, node_id: &NodeId, assignments: &[Assignment]) -> Result<()> {
        let sender = {
            let mut state = self.inner.lock().expect("assignment state");
            state.per_node.insert(node_id.clone(), assignments.to_vec());
            state.watchers.get(node_id).cloned()
        };
        if let Some(sender) = sender {
            let _ = sender.send(assignments.to_vec());
        }
        Ok(())
    }

    async fn list_for_node(&self, node_id: &NodeId) -> Result<Vec<Assignment>> {
        let state = self.inner.lock().expect("assignment state");
        Ok(state.per_node.get(node_id).cloned().unwrap_or_default())
    }

    async fn list_all(&self) -> Result<Vec<Assignment>> {
        let state = self.inner.lock().expect("assignment state");
        let mut all: Vec<Assignment> = state
            .per_node
            .values()
            .flat_map(|list| list.iter().cloned())
            .collect();
        all.sort_by(|left, right| {
            left.service_id
                .cmp(&right.service_id)
                .then_with(|| left.replica_index.cmp(&right.replica_index))
        });
        Ok(all)
    }
}

/// Action emitted by the [`AssignmentReconciler`] when the desired state for
/// this node changes. Consumed by whatever runs replicas locally — in
/// production that's the existing [`Engine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileAction {
    Start(Assignment),
    Stop {
        service_id: String,
        replica_index: u32,
    },
}

/// Computes the delta between previously-running and newly-desired assignments
/// for a single node. Pure function — testable.
pub fn diff_assignments(previous: &[Assignment], desired: &[Assignment]) -> Vec<ReconcileAction> {
    let mut actions: Vec<ReconcileAction> = Vec::new();
    let mut prev_by_slot: HashMap<(String, u32), &Assignment> = previous
        .iter()
        .map(|assignment| {
            (
                (assignment.service_id.clone(), assignment.replica_index),
                assignment,
            )
        })
        .collect();

    for assignment in desired {
        let key = (assignment.service_id.clone(), assignment.replica_index);
        let needs_restart = prev_by_slot
            .remove(&key)
            .map(|prev| {
                prev.deployment_id != assignment.deployment_id
                    || prev.node_id != assignment.node_id
                    || prev.port != assignment.port
            })
            .unwrap_or(true);
        if needs_restart {
            actions.push(ReconcileAction::Start(assignment.clone()));
        }
    }
    for ((service_id, replica_index), _) in prev_by_slot {
        actions.push(ReconcileAction::Stop {
            service_id,
            replica_index,
        });
    }
    actions.sort_by(|left, right| match (left, right) {
        (ReconcileAction::Stop { .. }, ReconcileAction::Start(_)) => std::cmp::Ordering::Less,
        (ReconcileAction::Start(_), ReconcileAction::Stop { .. }) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    });
    actions
}

/// Long-lived watcher that reconciles a node's slice of assignments into
/// real-world replicas via a pluggable [`ReplicaExecutor`].
pub struct AssignmentReconciler<E: ReplicaExecutor> {
    pub node_id: NodeId,
    pub store: Arc<dyn AssignmentStore>,
    pub executor: Arc<E>,
    pub poll_interval: Duration,
}

#[async_trait]
pub trait ReplicaExecutor: Send + Sync {
    async fn start(&self, assignment: &Assignment) -> Result<()>;
    async fn stop(&self, service_id: &str, replica_index: u32) -> Result<()>;
}

impl<E: ReplicaExecutor + 'static> AssignmentReconciler<E> {
    pub async fn reconcile_once(&self, last_seen: &mut Vec<Assignment>) -> Result<()> {
        let desired = self.store.list_for_node(&self.node_id).await?;
        let actions = diff_assignments(last_seen, &desired);
        for action in actions {
            match action {
                ReconcileAction::Start(assignment) => {
                    if let Err(err) = self.executor.start(&assignment).await {
                        eprintln!(
                            "assignment reconcile: start {}/{} on {} failed: {err}",
                            assignment.service_id, assignment.replica_index, self.node_id
                        );
                    }
                }
                ReconcileAction::Stop {
                    service_id,
                    replica_index,
                } => {
                    if let Err(err) = self.executor.stop(&service_id, replica_index).await {
                        eprintln!(
                            "assignment reconcile: stop {service_id}/{replica_index} on {} failed: {err}",
                            self.node_id
                        );
                    }
                }
            }
        }
        *last_seen = desired;
        Ok(())
    }
}
