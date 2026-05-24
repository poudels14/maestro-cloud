//! Cluster upgrade orchestrator. Walks the cluster through a zero-downtime
//! upgrade:
//!
//!   1. freeze: prevent new deploys
//!   2. for each worker (in series): drain → upgrade → verify
//!   3. transfer leadership to a healthy worker
//!   4. upgrade self (former leader) → rejoin as follower
//!   5. unfreeze
//!
//! The orchestrator is parameterized over a [`NodeUpgrader`] trait so we can
//! test the state machine without touching real binaries.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::Mutex;

use super::elector::LeaderElector;
use super::registry::NodeRegistry;
use super::types::{LeadershipState, NodeId, NodeInfo};

#[async_trait]
pub trait NodeUpgrader: Send + Sync {
    /// Drain a worker: stop scheduling new replicas, allow in-flight to migrate.
    async fn drain(&self, node_id: &NodeId) -> Result<()>;

    /// Upgrade the node to `target_version`. Returns when the node binary has
    /// been replaced and re-registered (or fails to come back).
    async fn upgrade(&self, node_id: &NodeId, target_version: &str) -> Result<()>;

    /// Confirm the node is healthy at `target_version`.
    async fn verify(&self, node_id: &NodeId, target_version: &str) -> Result<()>;

    /// Reverse a drain — start scheduling replicas to this node again.
    async fn restore(&self, node_id: &NodeId) -> Result<()>;
}

#[async_trait]
pub trait FreezeGate: Send + Sync {
    async fn freeze(&self) -> Result<()>;
    async fn unfreeze(&self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeStepStatus {
    Started,
    Drained,
    Upgraded,
    Verified,
    Restored,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeStep {
    pub node_id: NodeId,
    pub status: UpgradeStepStatus,
}

pub struct ClusterUpgradeOrchestrator {
    pub target_version: String,
    pub upgrader: Arc<dyn NodeUpgrader>,
    pub freeze_gate: Arc<dyn FreezeGate>,
    pub registry: Arc<dyn NodeRegistry>,
    pub elector: Arc<dyn LeaderElector>,
    pub step_history: Arc<Mutex<Vec<UpgradeStep>>>,
    pub leadership_transfer_timeout: Duration,
}

impl ClusterUpgradeOrchestrator {
    pub fn new(
        target_version: String,
        upgrader: Arc<dyn NodeUpgrader>,
        freeze_gate: Arc<dyn FreezeGate>,
        registry: Arc<dyn NodeRegistry>,
        elector: Arc<dyn LeaderElector>,
    ) -> Self {
        Self {
            target_version,
            upgrader,
            freeze_gate,
            registry,
            elector,
            step_history: Arc::new(Mutex::new(Vec::new())),
            leadership_transfer_timeout: Duration::from_secs(30),
        }
    }

    pub async fn history(&self) -> Vec<UpgradeStep> {
        self.step_history.lock().await.clone()
    }

    pub async fn run(&self) -> Result<()> {
        self.freeze_gate.freeze().await?;
        let outcome = self.run_inner().await;
        if let Err(err) = self.freeze_gate.unfreeze().await {
            eprintln!("upgrade: failed to unfreeze cluster: {err}");
        }
        outcome
    }

    async fn run_inner(&self) -> Result<()> {
        let nodes = self.registry.list_nodes().await?;
        let this_node = self.elector.this_node().clone();
        let workers = workers_excluding_self(&nodes, &this_node);
        for node in &workers {
            self.upgrade_node(node).await?;
        }
        if needs_self_upgrade(&this_node, &nodes) {
            let successor = pick_successor(&workers, &this_node)
                .ok_or_else(|| anyhow!("no healthy worker available to transfer leadership to"))?;
            self.transfer_leadership(&successor).await?;
            let this_info = nodes
                .iter()
                .find(|node| node.node_id == this_node)
                .cloned()
                .ok_or_else(|| anyhow!("self node not in registry"))?;
            self.upgrade_node(&this_info).await?;
        }
        Ok(())
    }

    async fn upgrade_node(&self, node: &NodeInfo) -> Result<()> {
        self.record(node.node_id.clone(), UpgradeStepStatus::Started)
            .await;
        if let Err(err) = self.upgrader.drain(&node.node_id).await {
            self.record_failure(node, format!("drain failed: {err}"))
                .await;
            return Err(err);
        }
        self.record(node.node_id.clone(), UpgradeStepStatus::Drained)
            .await;
        if let Err(err) = self
            .upgrader
            .upgrade(&node.node_id, &self.target_version)
            .await
        {
            self.record_failure(node, format!("upgrade failed: {err}"))
                .await;
            let _ = self.upgrader.restore(&node.node_id).await;
            self.record(node.node_id.clone(), UpgradeStepStatus::Restored)
                .await;
            return Err(err);
        }
        self.record(node.node_id.clone(), UpgradeStepStatus::Upgraded)
            .await;
        if let Err(err) = self
            .upgrader
            .verify(&node.node_id, &self.target_version)
            .await
        {
            self.record_failure(node, format!("verify failed: {err}"))
                .await;
            return Err(err);
        }
        self.record(node.node_id.clone(), UpgradeStepStatus::Verified)
            .await;
        self.upgrader.restore(&node.node_id).await?;
        self.record(node.node_id.clone(), UpgradeStepStatus::Restored)
            .await;
        Ok(())
    }

    async fn transfer_leadership(&self, _successor: &NodeInfo) -> Result<()> {
        self.elector.resign().await?;
        let mut receiver = self.elector.subscribe();
        let deadline = tokio::time::Instant::now() + self.leadership_transfer_timeout;
        loop {
            let snapshot = receiver.borrow().clone();
            if matches!(snapshot, LeadershipState::Following(Some(_))) {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(anyhow!("timed out waiting for leadership transfer"));
            }
            match tokio::time::timeout(remaining, receiver.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    return Err(anyhow!("leader watcher closed before transfer completed"));
                }
            }
        }
    }

    async fn record(&self, node_id: NodeId, status: UpgradeStepStatus) {
        self.step_history
            .lock()
            .await
            .push(UpgradeStep { node_id, status });
    }

    async fn record_failure(&self, node: &NodeInfo, message: String) {
        self.record(node.node_id.clone(), UpgradeStepStatus::Failed(message))
            .await;
    }
}

fn workers_excluding_self(nodes: &[NodeInfo], this_node: &NodeId) -> Vec<NodeInfo> {
    let mut workers: Vec<NodeInfo> = nodes
        .iter()
        .filter(|node| &node.node_id != this_node)
        .cloned()
        .collect();
    workers.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    workers
}

fn needs_self_upgrade(this_node: &NodeId, nodes: &[NodeInfo]) -> bool {
    nodes.iter().any(|node| &node.node_id == this_node)
}

fn pick_successor(workers: &[NodeInfo], _avoid: &NodeId) -> Option<NodeInfo> {
    workers.iter().find(|node| node.role.can_lead()).cloned()
}
