use std::collections::BTreeMap;

use kernel_api::{
    Condition, ConditionReason, ConditionState, ConditionType, Node, NodeId, Timestamp, UpgradeRun,
};

use crate::UpgradePlanError;

const MAINTENANCE_CONDITION: &str = "Maintenance";
const READY_CONDITION: &str = "Ready";

pub(crate) fn set_maintenance(
    updates: &mut BTreeMap<NodeId, Node>,
    nodes: &BTreeMap<NodeId, Node>,
    run: &UpgradeRun,
    node_id: &NodeId,
    enabled: bool,
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
        .find(|condition| condition.condition_type.0 == MAINTENANCE_CONDITION);
    if enabled
        && existing.is_some_and(|condition| {
            condition.state == ConditionState::True && condition.reason.0 != owner
        })
    {
        return Err(UpgradePlanError::NodeAlreadyMaintained {
            node_id: node_id.clone(),
        });
    }
    if !enabled
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
        .retain(|condition| condition.condition_type.0 != MAINTENANCE_CONDITION);
    let state = if enabled {
        ConditionState::True
    } else {
        ConditionState::False
    };
    let reason = if enabled {
        owner
    } else {
        "UpgradeReleased".to_string()
    };
    let last_transition_time = existing
        .filter(|condition| condition.state == state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    desired.status.conditions.push(Condition {
        condition_type: ConditionType(MAINTENANCE_CONDITION.to_string()),
        state,
        reason: ConditionReason(reason),
        message: if enabled {
            format!("node reserved by upgrade run `{}`", run.meta.id)
        } else {
            format!("node released by upgrade run `{}`", run.meta.id)
        },
        observed_generation: desired.meta.generation,
        last_transition_time,
    });
    updates.insert(node_id.clone(), desired);
    Ok(())
}

pub(crate) fn reject_foreign_maintenance(
    node: &Node,
    run: &UpgradeRun,
) -> Result<(), UpgradePlanError> {
    if node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == MAINTENANCE_CONDITION
            && condition.state == ConditionState::True
            && condition.reason.0 != maintenance_owner(run)
    }) {
        Err(UpgradePlanError::NodeAlreadyMaintained {
            node_id: node.meta.id.clone(),
        })
    } else {
        Ok(())
    }
}

fn maintenance_owner(run: &UpgradeRun) -> String {
    format!("UpgradeRun:{}", run.meta.id)
}

pub(crate) fn set_ready_condition(
    run: &mut UpgradeRun,
    ready: bool,
    reason: &str,
    message: &str,
    now: Timestamp,
) {
    let state = if ready {
        ConditionState::True
    } else {
        ConditionState::False
    };
    let existing = run
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type.0 == READY_CONDITION);
    let transitioned = existing
        .filter(|condition| condition.state == state)
        .map_or(now, |condition| condition.last_transition_time);
    run.status
        .conditions
        .retain(|condition| condition.condition_type.0 != READY_CONDITION);
    run.status.conditions.push(Condition {
        condition_type: ConditionType(READY_CONDITION.to_string()),
        state,
        reason: ConditionReason(reason.to_string()),
        message: message.to_string(),
        observed_generation: run.meta.generation,
        last_transition_time: transitioned,
    });
}
