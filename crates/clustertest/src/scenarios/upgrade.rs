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
    let workers = nodes_with_role(&topology, MaintenanceNodeRole::Worker);
    let voters = nodes_with_role(&topology, MaintenanceNodeRole::Voter);
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
    let failed_node = planned.first().cloned().ok_or_else(|| {
        ScenarioError::Assertion("rolling upgrade has no planned nodes".to_string())
    })?;
    let target = FixtureVersion::new("2.0.0");
    let observation = cluster
        .rolling_upgrade(
            target.clone(),
            UpgradeFault::FailFirstAttempt {
                node: failed_node.clone(),
            },
        )
        .await
        .map_err(|error| driver_error("run faulted rolling upgrade", error))?;
    let actual_attempts = observation
        .attempts
        .iter()
        .map(|attempt| attempt.node.clone())
        .collect::<Vec<_>>();
    let first_attempts = first_occurrences(&actual_attempts);
    if observation.planned_nodes != planned
        || first_attempts != planned
        || actual_attempts
            .iter()
            .filter(|node| **node == failed_node)
            .count()
            != 2
        || planned.iter().skip(1).any(|node| {
            actual_attempts
                .iter()
                .filter(|attempted| *attempted == node)
                .count()
                != 1
        })
    {
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

    let before_restart = cluster
        .topology()
        .await
        .map_err(|error| driver_error("observe selected restart baseline", error))?;
    let restarted = cluster
        .restart_node(&failed_node)
        .await
        .map_err(|error| driver_error("restart selected node", error))?;
    let after_restart = cluster
        .topology()
        .await
        .map_err(|error| driver_error("observe selected restart result", error))?;
    let selected = vec![failed_node.clone()];
    let before = before_restart.nodes.get(&failed_node).ok_or_else(|| {
        ScenarioError::Assertion("selected restart baseline omitted its node".to_string())
    })?;
    let after = after_restart.nodes.get(&failed_node).ok_or_else(|| {
        ScenarioError::Assertion("selected restart result omitted its node".to_string())
    })?;
    if restarted.planned_nodes == selected
        && restarted.requested_nodes == selected
        && restarted.completion == MaintenanceCompletion::Succeeded
        && restarted.final_freeze == MaintenanceFreeze::Cleared
        && after.version == before.version
        && after.instance_id != before.instance_id
        && after.scheduling == SchedulingEligibility::Eligible
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "selected-node restart was not isolated: {restarted:?}"
        )))
    }
}

/// Proves all selected nodes drain, dispatch, verify, and restore as one batch.
pub async fn all_node_upgrade_restores_nodes_as_one_batch<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: UpgradeCluster,
{
    let topology = cluster
        .topology()
        .await
        .map_err(|error| driver_error("observe all-node upgrade topology", error))?;
    let voters = nodes_with_role(&topology, MaintenanceNodeRole::Voter);
    if voters.len() != 3 || !voters.contains(&topology.leader) {
        return Err(ScenarioError::Assertion(
            "all-node upgrade requires three voters including the leader".to_string(),
        ));
    }
    let target = FixtureVersion::new("3.0.0");
    let observation = cluster
        .all_node_upgrade(target.clone())
        .await
        .map_err(|error| driver_error("run all-node upgrade", error))?;
    let planned = observation
        .planned_nodes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected = topology.nodes.keys().cloned().collect::<BTreeSet<_>>();
    let attempted = observation
        .attempts
        .iter()
        .map(|attempt| attempt.node.clone())
        .collect::<BTreeSet<_>>();
    if planned != expected
        || attempted != expected
        || observation.attempts.len() != expected.len()
        || observation
            .attempts
            .iter()
            .any(|attempt| attempt.drained_nodes != expected)
        || observation.completion != MaintenanceCompletion::Succeeded
        || observation.target_retention != TargetRetention::RetainedUntilCompletion
        || observation.final_freeze != MaintenanceFreeze::Cleared
        || observation.final_nodes.keys().ne(topology.nodes.keys())
    {
        return Err(ScenarioError::Assertion(format!(
            "all-node upgrade did not complete as one restored batch: {observation:?}"
        )));
    }
    for (node, final_state) in &observation.final_nodes {
        let Some(initial_state) = topology.nodes.get(node) else {
            return Err(ScenarioError::Assertion(format!(
                "all-node upgrade introduced unknown node {node:?}"
            )));
        };
        if final_state.role != initial_state.role
            || final_state.version != target
            || final_state.instance_id == initial_state.instance_id
            || final_state.scheduling != SchedulingEligibility::Eligible
        {
            return Err(ScenarioError::Assertion(format!(
                "node {node:?} was not upgraded and restored in the all-node batch: {final_state:?}"
            )));
        }
    }
    Ok(())
}

fn first_occurrences(nodes: &[FixtureNodeName]) -> Vec<FixtureNodeName> {
    let mut seen = BTreeSet::new();
    nodes
        .iter()
        .filter(|node| seen.insert((*node).clone()))
        .cloned()
        .collect()
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
