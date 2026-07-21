use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::{
    FixtureInstanceId, FixtureNodeName, FixtureVersion, MaintenanceAttempt, MaintenanceCompletion,
    MaintenanceFreeze, MaintenanceNodeRole, MaintenanceNodeSnapshot, MaintenanceTopology,
    SchedulingEligibility, SelectedRestartObservation, TargetRetention, UpgradeCluster,
    UpgradeFault, UpgradeObservation,
    scenarios::{
        all_node_upgrade_restores_nodes_as_one_batch,
        rolling_upgrade_retries_and_restores_nodes_serially,
    },
};

struct UpgradeWorld {
    topology: MaintenanceTopology,
}

impl UpgradeWorld {
    fn new() -> Self {
        let voter_one = FixtureNodeName::new("node-a");
        let voter_two = FixtureNodeName::new("node-b");
        let leader = FixtureNodeName::new("node-c");
        let mut nodes = BTreeMap::new();
        for (node, role) in [
            (voter_one, MaintenanceNodeRole::Voter),
            (voter_two, MaintenanceNodeRole::Voter),
            (leader.clone(), MaintenanceNodeRole::Voter),
        ] {
            nodes.insert(
                node.clone(),
                MaintenanceNodeSnapshot {
                    role,
                    version: FixtureVersion::new("1.0.0"),
                    instance_id: FixtureInstanceId::new(format!("{}-initial", node.as_str())),
                    scheduling: SchedulingEligibility::Eligible,
                },
            );
        }
        Self {
            topology: MaintenanceTopology { nodes, leader },
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("upgrade world failed")]
struct UpgradeWorldError;

#[async_trait]
impl UpgradeCluster for UpgradeWorld {
    type Error = UpgradeWorldError;

    async fn topology(&mut self) -> Result<MaintenanceTopology, Self::Error> {
        Ok(self.topology.clone())
    }

    async fn rolling_upgrade(
        &mut self,
        target: FixtureVersion,
        fault: UpgradeFault,
    ) -> Result<UpgradeObservation, Self::Error> {
        let UpgradeFault::FailFirstAttempt { node: failed } = fault;
        let mut planned = self
            .topology
            .nodes
            .iter()
            .filter(|(_, node)| node.role == MaintenanceNodeRole::Worker)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        planned.extend(
            self.topology
                .nodes
                .iter()
                .filter(|(name, node)| {
                    node.role == MaintenanceNodeRole::Voter && **name != self.topology.leader
                })
                .map(|(name, _)| name.clone()),
        );
        planned.push(self.topology.leader.clone());
        let mut attempt_nodes = vec![failed.clone(), failed];
        attempt_nodes.extend(planned.iter().skip(1).cloned());
        let attempts = attempt_nodes
            .into_iter()
            .map(|node| MaintenanceAttempt {
                drained_nodes: BTreeSet::from([node.clone()]),
                node,
            })
            .collect();
        for (node, state) in &mut self.topology.nodes {
            state.version = target.clone();
            state.instance_id = FixtureInstanceId::new(format!("{}-upgraded", node.as_str()));
            state.scheduling = SchedulingEligibility::Eligible;
        }
        Ok(UpgradeObservation {
            planned_nodes: planned,
            attempts,
            completion: MaintenanceCompletion::Succeeded,
            target_retention: TargetRetention::RetainedUntilCompletion,
            final_freeze: MaintenanceFreeze::Cleared,
            final_nodes: self.topology.nodes.clone(),
        })
    }

    async fn all_node_upgrade(
        &mut self,
        target: FixtureVersion,
    ) -> Result<UpgradeObservation, Self::Error> {
        let planned = self.topology.nodes.keys().cloned().collect::<Vec<_>>();
        let drained_nodes = planned.iter().cloned().collect::<BTreeSet<_>>();
        let attempts = planned
            .iter()
            .cloned()
            .map(|node| MaintenanceAttempt {
                node,
                drained_nodes: drained_nodes.clone(),
            })
            .collect();
        for (node, state) in &mut self.topology.nodes {
            state.version = target.clone();
            state.instance_id = FixtureInstanceId::new(format!("{}-all-node", node.as_str()));
            state.scheduling = SchedulingEligibility::Eligible;
        }
        Ok(UpgradeObservation {
            planned_nodes: planned,
            attempts,
            completion: MaintenanceCompletion::Succeeded,
            target_retention: TargetRetention::RetainedUntilCompletion,
            final_freeze: MaintenanceFreeze::Cleared,
            final_nodes: self.topology.nodes.clone(),
        })
    }

    async fn restart_node(
        &mut self,
        node: &FixtureNodeName,
    ) -> Result<SelectedRestartObservation, Self::Error> {
        Ok(SelectedRestartObservation {
            planned_nodes: vec![node.clone()],
            requested_nodes: vec![node.clone()],
            completion: MaintenanceCompletion::Succeeded,
            final_freeze: MaintenanceFreeze::Cleared,
        })
    }
}

#[tokio::test]
async fn upgrade_scenario_retries_serially_and_cleans_freezes() {
    let mut cluster = UpgradeWorld::new();

    rolling_upgrade_retries_and_restores_nodes_serially(&mut cluster)
        .await
        .expect("rolling upgrade scenario");
    all_node_upgrade_restores_nodes_as_one_batch(&mut cluster)
        .await
        .expect("all-node upgrade scenario");
}
