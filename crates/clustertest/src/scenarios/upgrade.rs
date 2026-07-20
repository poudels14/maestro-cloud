use std::collections::BTreeSet;

use crate::{
    FixtureNodeName, FixtureVersion, MaintenanceCompletion, MaintenanceFreeze, MaintenanceNodeRole,
    ScenarioError, SchedulingEligibility, TargetRetention, UpgradeCluster, UpgradeFault,
};

/// Proves safe rolling order, retry recovery, restoration, and selected restart.
pub async fn rolling_upgrade_retries_and_restores_nodes_serially<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: UpgradeCluster,
{
    let topology = cluster
        .topology()
        .await
        .map_err(|error| driver_error("observe maintenance topology", error))?;
    if topology.nodes.len() != 4 {
        return Err(ScenarioError::Assertion(format!(
            "rolling upgrade requires four live nodes, found {}",
            topology.nodes.len()
        )));
    }
    let workers = nodes_with_role(&topology, MaintenanceNodeRole::Worker);
    let voters = nodes_with_role(&topology, MaintenanceNodeRole::Voter);
    let [failed_worker] = workers.as_slice() else {
        return Err(ScenarioError::Assertion(format!(
            "rolling upgrade requires one worker, found {}",
            workers.len()
        )));
    };
    if voters.len() != 3 || !voters.contains(&topology.leader) {
        return Err(ScenarioError::Assertion(
            "rolling upgrade requires three voters including the leader".to_string(),
        ));
    }

    let mut planned = workers.clone();
    planned.extend(
        voters
            .iter()
            .filter(|node| **node != topology.leader)
            .cloned(),
    );
    planned.push(topology.leader.clone());
    let target = FixtureVersion::new("2.0.0");
    let observation = cluster
        .rolling_upgrade(
            target.clone(),
            UpgradeFault::FailFirstAttempt {
                node: failed_worker.clone(),
            },
        )
        .await
        .map_err(|error| driver_error("run faulted rolling upgrade", error))?;
    let mut expected_attempts = planned.clone();
    expected_attempts.push(failed_worker.clone());
    let actual_attempts = observation
        .attempts
        .iter()
        .map(|attempt| attempt.node.clone())
        .collect::<Vec<_>>();
    if observation.planned_nodes != planned || actual_attempts != expected_attempts {
        return Err(ScenarioError::Assertion(format!(
            "rolling order or retry order was unsafe: {observation:?}"
        )));
    }
    for attempt in &observation.attempts {
        if attempt.drained_nodes != BTreeSet::from([attempt.node.clone()]) {
            return Err(ScenarioError::Assertion(format!(
                "upgrade request for {:?} observed concurrent drains: {:?}",
                attempt.node, attempt.drained_nodes
            )));
        }
    }
    if observation.completion != MaintenanceCompletion::Succeeded
        || observation.target_retention != TargetRetention::RetainedUntilCompletion
        || observation.final_freeze != MaintenanceFreeze::Cleared
        || observation.final_nodes.keys().ne(topology.nodes.keys())
    {
        return Err(ScenarioError::Assertion(format!(
            "rolling upgrade did not complete cleanly: {observation:?}"
        )));
    }
    for (node, final_state) in &observation.final_nodes {
        let Some(initial_state) = topology.nodes.get(node) else {
            return Err(ScenarioError::Assertion(format!(
                "upgrade introduced unknown node {node:?}"
            )));
        };
        if final_state.role != initial_state.role
            || final_state.version != target
            || final_state.instance_id == initial_state.instance_id
            || final_state.scheduling != SchedulingEligibility::Eligible
        {
            return Err(ScenarioError::Assertion(format!(
                "node {node:?} was not upgraded and restored: {final_state:?}"
            )));
        }
    }

    let restarted = cluster
        .restart_node(failed_worker)
        .await
        .map_err(|error| driver_error("restart selected node", error))?;
    let selected = vec![failed_worker.clone()];
    if restarted.planned_nodes == selected
        && restarted.requested_nodes == selected
        && restarted.completion == MaintenanceCompletion::Succeeded
        && restarted.final_freeze == MaintenanceFreeze::Cleared
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "selected-node restart was not isolated: {restarted:?}"
        )))
    }
}

fn nodes_with_role(
    topology: &crate::MaintenanceTopology,
    role: MaintenanceNodeRole,
) -> Vec<FixtureNodeName> {
    topology
        .nodes
        .iter()
        .filter(|(_, node)| node.role == role)
        .map(|(name, _)| name.clone())
        .collect()
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
