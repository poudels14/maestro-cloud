use kernel_api::{Condition, ConditionReason, ConditionState, ConditionType, Node, Timestamp};

const DRAIN_REQUEST_REASON: &str = "ReplicatingArtifacts";
const CUTOVER_PENDING_REASON: &str = "CutoverPending";
const LEGACY_NODE_RECORD_ANNOTATION: &str = "migration.maestro.dev/legacy-node-record";

/// Requested change to a node's workload scheduling eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeSchedulingAction {
    /// Stop accepting new workloads and prepare existing workloads to move.
    Drain,
    /// Allow the node to accept workloads again.
    Restore,
}

/// Applies the canonical drain or restore condition and reports whether storage must change.
pub fn set_node_scheduling(node: &mut Node, action: NodeSchedulingAction, now: Timestamp) -> bool {
    let (desired, reason, message) = match action {
        NodeSchedulingAction::Drain if node.spec.role.runs_workloads() => (
            ConditionState::Unknown,
            DRAIN_REQUEST_REASON,
            "node drain requested; waiting for retained artifacts to acquire peer copies",
        ),
        NodeSchedulingAction::Drain => (
            ConditionState::True,
            "Requested",
            "node drain requested; this node does not run workloads",
        ),
        NodeSchedulingAction::Restore => (
            ConditionState::False,
            "Restored",
            "node restored to scheduling",
        ),
    };
    let mut existing = node
        .status
        .conditions
        .iter()
        .filter(|condition| condition.condition_type == ConditionType::Draining);
    let canonical = existing.next().is_some_and(|condition| match action {
        NodeSchedulingAction::Drain => {
            condition.state == ConditionState::True
                || (condition.state == desired && condition.reason.0 == reason)
        }
        NodeSchedulingAction::Restore => condition.state == desired && condition.reason.0 == reason,
    }) && existing.next().is_none();
    let migrated = node
        .meta
        .annotations
        .keys()
        .any(|key| key.0 == LEGACY_NODE_RECORD_ANNOTATION);
    let conditions_before_restore = node.status.conditions.len();
    if action == NodeSchedulingAction::Restore && migrated {
        node.status.conditions.retain(|condition| {
            condition.condition_type != ConditionType::Maintenance
                || condition.reason.0 != CUTOVER_PENDING_REASON
        });
    }
    let released_cutover = node.status.conditions.len() != conditions_before_restore;
    if !canonical {
        node.status
            .conditions
            .retain(|condition| condition.condition_type != ConditionType::Draining);
        node.status.conditions.push(Condition {
            condition_type: ConditionType::Draining,
            state: desired,
            reason: ConditionReason(reason.to_string()),
            message: message.to_string(),
            observed_generation: node.meta.generation,
            last_transition_time: now,
        });
    }
    !canonical || released_cutover
}
