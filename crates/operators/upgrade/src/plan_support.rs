use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kernel_api::{Node, NodeId, Timestamp, UpgradeMode, UpgradePhase};
use semver::Version;

use crate::{UpgradePlan, UpgradePlanAction, UpgradePlanError};

pub(crate) fn selected_nodes<'a>(
    run: &kernel_api::UpgradeRun,
    nodes: &'a BTreeMap<NodeId, Node>,
) -> Result<Vec<&'a Node>, UpgradePlanError> {
    if run.spec.node_ids.is_empty() {
        return Ok(nodes.values().collect());
    }
    let unique = run.spec.node_ids.iter().collect::<BTreeSet<_>>();
    if unique.len() != run.spec.node_ids.len() {
        return Err(UpgradePlanError::DuplicateSelection);
    }
    run.spec
        .node_ids
        .iter()
        .map(|node_id| {
            nodes
                .get(node_id)
                .ok_or_else(|| UpgradePlanError::NodeMissing {
                    node_id: node_id.clone(),
                })
        })
        .collect()
}

pub(crate) fn validate_quorum(
    run: &kernel_api::UpgradeRun,
    nodes: &BTreeMap<NodeId, Node>,
    pending: &[&Node],
) -> Result<(), UpgradePlanError> {
    if run.spec.mode != UpgradeMode::Rolling
        || !pending.iter().any(|node| node.spec.role.is_control_plane())
    {
        return Ok(());
    }
    let voters = nodes
        .values()
        .filter(|node| node.spec.role.is_control_plane())
        .count();
    if voters == 2 {
        Err(UpgradePlanError::UnsafeTwoVoterRollingUpgrade)
    } else {
        Ok(())
    }
}

pub(crate) fn maintenance_order(node: &Node, leader_id: &NodeId) -> u8 {
    if &node.meta.id == leader_id {
        2
    } else if node.spec.role.is_control_plane() {
        1
    } else {
        0
    }
}

pub(crate) fn start_next_batch(
    run: &mut kernel_api::UpgradeRun,
) -> Result<Vec<NodeId>, UpgradePlanError> {
    let pending = status_indices(run, UpgradePhase::Pending);
    if pending.is_empty() {
        return Err(UpgradePlanError::EmptyPendingBatch);
    }
    let selected = match run.spec.mode {
        UpgradeMode::Rolling => pending.into_iter().take(1).collect::<Vec<_>>(),
        UpgradeMode::AllNodes => pending,
    };
    let ids = status_ids(run, &selected)?.into_iter().collect::<Vec<_>>();
    transition_statuses(run, &selected, UpgradePhase::Draining)?;
    Ok(ids)
}

pub(crate) fn transition_statuses(
    run: &mut kernel_api::UpgradeRun,
    indices: &[usize],
    target: UpgradePhase,
) -> Result<(), UpgradePlanError> {
    for index in indices {
        let status = run
            .status
            .nodes
            .get_mut(*index)
            .ok_or(UpgradePlanError::CorruptStatusIndex)?;
        status.phase = transition(status.phase, target)?;
        if target != UpgradePhase::Applying {
            status.retry_at = None;
        }
    }
    Ok(())
}

pub(crate) fn transition(
    from: UpgradePhase,
    to: UpgradePhase,
) -> Result<UpgradePhase, UpgradePlanError> {
    if from.can_transition_to(to) {
        Ok(to)
    } else {
        Err(UpgradePlanError::InvalidTransition { from, to })
    }
}

pub(crate) fn status_indices(run: &kernel_api::UpgradeRun, phase: UpgradePhase) -> Vec<usize> {
    run.status
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(index, status)| (status.phase == phase).then_some(index))
        .collect()
}

pub(crate) fn status_ids(
    run: &kernel_api::UpgradeRun,
    indices: &[usize],
) -> Result<BTreeSet<NodeId>, UpgradePlanError> {
    indices
        .iter()
        .map(|index| {
            run.status
                .nodes
                .get(*index)
                .map(|status| status.node_id.clone())
                .ok_or(UpgradePlanError::CorruptStatusIndex)
        })
        .collect()
}

pub(crate) fn index_nodes(nodes: &[Node]) -> Result<BTreeMap<NodeId, Node>, UpgradePlanError> {
    let mut indexed = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|node| node.meta.deletion_timestamp.is_none())
    {
        if indexed.insert(node.meta.id.clone(), node.clone()).is_some() {
            return Err(UpgradePlanError::DuplicateNode {
                node_id: node.meta.id.clone(),
            });
        }
    }
    Ok(indexed)
}

pub(crate) fn validate_live_nodes(
    nodes: &BTreeMap<NodeId, Node>,
    live_nodes: &BTreeSet<NodeId>,
) -> Result<(), UpgradePlanError> {
    let offline = nodes
        .keys()
        .filter(|node_id| !live_nodes.contains(*node_id))
        .cloned()
        .collect::<Vec<_>>();
    if offline.is_empty() {
        Ok(())
    } else {
        Err(UpgradePlanError::OfflineNodes { node_ids: offline })
    }
}

pub(crate) fn parse_target(value: &str) -> Result<Version, UpgradePlanError> {
    Version::parse(value.trim()).map_err(|error| UpgradePlanError::InvalidTargetVersion {
        value: value.to_string(),
        message: error.to_string(),
    })
}

pub(crate) fn parse_node_version(node: &Node) -> Result<Version, UpgradePlanError> {
    Version::parse(node.status.version.trim()).map_err(|error| {
        UpgradePlanError::InvalidNodeVersion {
            node_id: node.meta.id.clone(),
            value: node.status.version.clone(),
            message: error.to_string(),
        }
    })
}

pub(crate) fn add_duration(now: Timestamp, duration: Duration) -> Timestamp {
    let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
    Timestamp(now.0.saturating_add(millis))
}

pub(crate) fn duration_between(now: Timestamp, future: Timestamp) -> Duration {
    Duration::from_millis(u64::try_from(future.0.saturating_sub(now.0)).unwrap_or(u64::MAX))
}

pub(crate) fn plan(
    run: kernel_api::UpgradeRun,
    node_updates: Vec<Node>,
    action: UpgradePlanAction,
) -> UpgradePlan {
    UpgradePlan {
        run,
        node_updates,
        action,
    }
}
