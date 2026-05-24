//! State-machine tests for [`ClusterUpgradeOrchestrator`] using an in-memory
//! cluster + recording NodeUpgrader.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;

use crate::cluster::elector::LeaderElector;
use crate::cluster::in_memory::{InMemoryCluster, InMemoryLeaderElector, InMemoryNodeRegistry};
use crate::cluster::registry::NodeRegistry;
use crate::cluster::types::{NodeId, NodeInfo, NodeRole};
use crate::cluster::upgrade::{
    ClusterUpgradeOrchestrator, FreezeGate, NodeUpgrader, UpgradeStepStatus,
};

fn node(id: &str, started_at_ms: u64) -> NodeInfo {
    NodeInfo {
        node_id: id.to_string(),
        hostname: format!("{id}.local"),
        role: NodeRole::Both,
        tailscale_ip: None,
        api_port: 3001,
        version: "v1".to_string(),
        started_at_ms,
        labels: BTreeMap::new(),
        unschedulable: false,
    }
}

#[derive(Debug, Default, Clone)]
struct RecordingUpgrader {
    drained: Arc<Mutex<Vec<NodeId>>>,
    upgraded: Arc<Mutex<Vec<NodeId>>>,
    verified: Arc<Mutex<Vec<NodeId>>>,
    restored: Arc<Mutex<Vec<NodeId>>>,
    fail_upgrade: Arc<Mutex<Vec<NodeId>>>,
    fail_verify: Arc<Mutex<Vec<NodeId>>>,
}

#[async_trait]
impl NodeUpgrader for RecordingUpgrader {
    async fn drain(&self, node_id: &NodeId) -> Result<()> {
        self.drained.lock().unwrap().push(node_id.clone());
        Ok(())
    }

    async fn upgrade(&self, node_id: &NodeId, _target_version: &str) -> Result<()> {
        if self.fail_upgrade.lock().unwrap().contains(node_id) {
            return Err(anyhow::anyhow!("simulated upgrade failure for {node_id}"));
        }
        self.upgraded.lock().unwrap().push(node_id.clone());
        Ok(())
    }

    async fn verify(&self, node_id: &NodeId, _target_version: &str) -> Result<()> {
        if self.fail_verify.lock().unwrap().contains(node_id) {
            return Err(anyhow::anyhow!("simulated verify failure for {node_id}"));
        }
        self.verified.lock().unwrap().push(node_id.clone());
        Ok(())
    }

    async fn restore(&self, node_id: &NodeId) -> Result<()> {
        self.restored.lock().unwrap().push(node_id.clone());
        Ok(())
    }
}

#[derive(Default)]
struct RecordingFreezeGate {
    frozen: Mutex<bool>,
    freeze_count: Mutex<u32>,
    unfreeze_count: Mutex<u32>,
}

#[async_trait]
impl FreezeGate for RecordingFreezeGate {
    async fn freeze(&self) -> Result<()> {
        *self.frozen.lock().unwrap() = true;
        *self.freeze_count.lock().unwrap() += 1;
        Ok(())
    }

    async fn unfreeze(&self) -> Result<()> {
        *self.frozen.lock().unwrap() = false;
        *self.unfreeze_count.lock().unwrap() += 1;
        Ok(())
    }
}

async fn register_node(cluster: &InMemoryCluster, info: NodeInfo) -> Arc<InMemoryNodeRegistry> {
    let registry = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        info.node_id.clone(),
    ));
    registry.register(&info).await.unwrap();
    registry
}

async fn build_orchestrator(
    cluster: &InMemoryCluster,
    this_node: &str,
    upgrader: Arc<RecordingUpgrader>,
    freeze: Arc<RecordingFreezeGate>,
) -> (ClusterUpgradeOrchestrator, Arc<InMemoryLeaderElector>) {
    let registry = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        this_node.to_string(),
    ));
    let elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        this_node.to_string(),
    ));
    elector.campaign().await.unwrap();
    let orchestrator = ClusterUpgradeOrchestrator::new(
        "v2".to_string(),
        upgrader,
        freeze,
        registry as Arc<dyn NodeRegistry>,
        elector.clone() as Arc<dyn LeaderElector>,
    );
    (orchestrator, elector)
}

#[tokio::test]
async fn upgrades_workers_before_leader_self_upgrade() {
    let cluster = InMemoryCluster::new();
    let _node_a = register_node(&cluster, node("node-a", 100)).await;
    let _node_b = register_node(&cluster, node("node-b", 200)).await;
    let _node_c = register_node(&cluster, node("node-c", 300)).await;

    let upgrader = Arc::new(RecordingUpgrader::default());
    let freeze = Arc::new(RecordingFreezeGate::default());
    let (orchestrator, _elector) =
        build_orchestrator(&cluster, "node-a", upgrader.clone(), freeze.clone()).await;

    orchestrator.run().await.expect("upgrade should succeed");

    let drained = upgrader.drained.lock().unwrap().clone();
    let upgraded = upgrader.upgraded.lock().unwrap().clone();
    let verified = upgrader.verified.lock().unwrap().clone();
    assert_eq!(drained, vec!["node-b", "node-c", "node-a"]);
    assert_eq!(upgraded, vec!["node-b", "node-c", "node-a"]);
    assert_eq!(verified, vec!["node-b", "node-c", "node-a"]);
}

#[tokio::test]
async fn freezes_and_unfreezes_around_run() {
    let cluster = InMemoryCluster::new();
    let _node_a = register_node(&cluster, node("node-a", 100)).await;
    let _node_b = register_node(&cluster, node("node-b", 200)).await;

    let upgrader = Arc::new(RecordingUpgrader::default());
    let freeze = Arc::new(RecordingFreezeGate::default());
    let (orchestrator, _) =
        build_orchestrator(&cluster, "node-a", upgrader.clone(), freeze.clone()).await;
    orchestrator.run().await.expect("upgrade succeeds");

    assert_eq!(*freeze.freeze_count.lock().unwrap(), 1);
    assert_eq!(*freeze.unfreeze_count.lock().unwrap(), 1);
    assert!(!*freeze.frozen.lock().unwrap());
}

#[tokio::test]
async fn upgrade_failure_still_unfreezes_cluster() {
    let cluster = InMemoryCluster::new();
    let _node_a = register_node(&cluster, node("node-a", 100)).await;
    let _node_b = register_node(&cluster, node("node-b", 200)).await;

    let upgrader = Arc::new(RecordingUpgrader::default());
    upgrader
        .fail_upgrade
        .lock()
        .unwrap()
        .push("node-b".to_string());
    let freeze = Arc::new(RecordingFreezeGate::default());
    let (orchestrator, _) =
        build_orchestrator(&cluster, "node-a", upgrader.clone(), freeze.clone()).await;

    let result = orchestrator.run().await;
    assert!(result.is_err());
    assert_eq!(*freeze.unfreeze_count.lock().unwrap(), 1);
    let history = orchestrator.history().await;
    assert!(
        history
            .iter()
            .any(|step| matches!(step.status, UpgradeStepStatus::Failed(_)))
    );
    assert!(
        history
            .iter()
            .any(|step| step.node_id == "node-b"
                && matches!(step.status, UpgradeStepStatus::Restored))
    );
}

#[tokio::test]
async fn verify_failure_aborts_subsequent_nodes() {
    let cluster = InMemoryCluster::new();
    let _node_a = register_node(&cluster, node("node-a", 100)).await;
    let _node_b = register_node(&cluster, node("node-b", 200)).await;
    let _node_c = register_node(&cluster, node("node-c", 300)).await;

    let upgrader = Arc::new(RecordingUpgrader::default());
    upgrader
        .fail_verify
        .lock()
        .unwrap()
        .push("node-b".to_string());
    let freeze = Arc::new(RecordingFreezeGate::default());
    let (orchestrator, _) =
        build_orchestrator(&cluster, "node-a", upgrader.clone(), freeze.clone()).await;

    let result = orchestrator.run().await;
    assert!(result.is_err());
    let upgraded = upgrader.upgraded.lock().unwrap().clone();
    assert!(upgraded.contains(&"node-b".to_string()));
    assert!(
        !upgraded.contains(&"node-c".to_string()),
        "c should not be upgraded after b fails verify"
    );
}

#[tokio::test]
async fn history_records_status_transitions_in_order() {
    let cluster = InMemoryCluster::new();
    let _node_a = register_node(&cluster, node("node-a", 100)).await;
    let _node_b = register_node(&cluster, node("node-b", 200)).await;

    let upgrader = Arc::new(RecordingUpgrader::default());
    let freeze = Arc::new(RecordingFreezeGate::default());
    let (orchestrator, _) =
        build_orchestrator(&cluster, "node-a", upgrader.clone(), freeze.clone()).await;
    orchestrator.run().await.unwrap();

    let history = orchestrator.history().await;
    let node_b_history: Vec<UpgradeStepStatus> = history
        .iter()
        .filter(|step| step.node_id == "node-b")
        .map(|step| step.status.clone())
        .collect();
    assert_eq!(
        node_b_history,
        vec![
            UpgradeStepStatus::Started,
            UpgradeStepStatus::Drained,
            UpgradeStepStatus::Upgraded,
            UpgradeStepStatus::Verified,
            UpgradeStepStatus::Restored,
        ]
    );
}
