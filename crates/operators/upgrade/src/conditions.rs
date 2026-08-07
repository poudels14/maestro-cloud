use std::collections::BTreeMap;

use kernel_api::{
    Condition, ConditionReason, ConditionState, ConditionType, Node, NodeId, Timestamp, UpgradeRun,
};

use crate::UpgradePlanError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaintenanceAction {
    Reserve,
    Release,
}

pub(crate) fn set_maintenance(
    updates: &mut BTreeMap<NodeId, Node>,
    nodes: &BTreeMap<NodeId, Node>,
    run: &UpgradeRun,
    node_id: &NodeId,
    action: MaintenanceAction,
    now: Timestamp,
) -> Result<(), UpgradePlanError> {
    let current = updates
        .get(node_id)
        .or_else(|| nodes.get(node_id))
        .ok_or_else(|| UpgradePlanError::NodeMissing {
            node_id: node_id.clone(),
        })?;
    let owner = maintenance_owner(run);
    let existing = current
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::Maintenance);
    if action == MaintenanceAction::Reserve
        && existing.is_some_and(|condition| {
            condition.state == ConditionState::True && condition.reason.0 != owner
        })
    {
        return Err(UpgradePlanError::NodeAlreadyMaintained {
            node_id: node_id.clone(),
        });
    }
    if action == MaintenanceAction::Release
        && existing.is_none_or(|condition| {
            condition.state != ConditionState::True || condition.reason.0 != owner
        })
    {
        return Ok(());
    }
    let mut desired = current.clone();
    desired
        .status
        .conditions
        .retain(|condition| condition.condition_type != ConditionType::Maintenance);
    let (state, reason, message) = match action {
        MaintenanceAction::Reserve => (
            ConditionState::True,
            owner,
            format!("node reserved by upgrade run `{}`", run.meta.id),
        ),
        MaintenanceAction::Release => (
            ConditionState::False,
            "UpgradeReleased".to_string(),
            format!("node released by upgrade run `{}`", run.meta.id),
        ),
    };
    let last_transition_time = existing
        .filter(|condition| condition.state == state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    desired.status.conditions.push(Condition {
        condition_type: ConditionType::Maintenance,
        state,
        reason: ConditionReason(reason),
        message,
        observed_generation: desired.meta.generation,
        last_transition_time,
    });
    updates.insert(node_id.clone(), desired);
    Ok(())
}

pub(crate) fn has_foreign_maintenance(node: &Node, run: &UpgradeRun) -> bool {
    node.status.conditions.iter().any(|condition| {
        condition.condition_type == ConditionType::Maintenance
            && condition.state == ConditionState::True
            && condition.reason.0 != maintenance_owner(run)
    })
}

fn maintenance_owner(run: &UpgradeRun) -> String {
    format!("UpgradeRun:{}", run.meta.id)
}

pub(crate) fn set_ready_condition(
    run: &mut UpgradeRun,
    state: ConditionState,
    reason: &str,
    message: &str,
    now: Timestamp,
) {
    let existing = run
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::Ready);
    let transitioned = existing
        .filter(|condition| condition.state == state)
        .map_or(now, |condition| condition.last_transition_time);
    run.status
        .conditions
        .retain(|condition| condition.condition_type != ConditionType::Ready);
    run.status.conditions.push(Condition {
        condition_type: ConditionType::Ready,
        state,
        reason: ConditionReason(reason.to_string()),
        message: message.to_string(),
        observed_generation: run.meta.generation,
        last_transition_time: transitioned,
    });
}
