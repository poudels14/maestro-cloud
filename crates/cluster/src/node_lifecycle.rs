use kernel_api::{Condition, ConditionReason, ConditionState, ConditionType, Node, Timestamp};

const DRAINING_CONDITION: &str = "Draining";
const DRAIN_REQUEST_REASON: &str = "ReplicatingArtifacts";

/// Applies the canonical drain or restore condition and reports whether storage must change.
pub fn set_node_draining(node: &mut Node, draining: bool, now: Timestamp) -> bool {
    let (desired, reason, message) = if draining && node.spec.role.runs_workloads() {
        (
            ConditionState::Unknown,
            DRAIN_REQUEST_REASON,
            "node drain requested; waiting for retained artifacts to acquire peer copies",
        )
    } else if draining {
        (
            ConditionState::True,
            "Requested",
            "node drain requested; this node does not run workloads",
        )
    } else {
        (
            ConditionState::False,
            "Restored",
            "node restored to scheduling",
        )
    };
    let mut existing = node
        .status
        .conditions
        .iter()
        .filter(|condition| condition.condition_type.0 == DRAINING_CONDITION);
    let canonical = existing.next().is_some_and(|condition| {
        (draining && condition.state == ConditionState::True)
            || (condition.state == desired && condition.reason.0 == reason)
    }) && existing.next().is_none();
    if !canonical {
        node.status
            .conditions
            .retain(|condition| condition.condition_type.0 != DRAINING_CONDITION);
        node.status.conditions.push(Condition {
            condition_type: ConditionType(DRAINING_CONDITION.to_string()),
            state: desired,
            reason: ConditionReason(reason.to_string()),
            message: message.to_string(),
            observed_generation: node.meta.generation,
            last_transition_time: now,
        });
    }
    !canonical
}
