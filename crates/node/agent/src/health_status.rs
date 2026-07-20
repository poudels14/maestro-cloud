use kernel_api::{
    Assignment, Condition, ConditionReason, ConditionState, ConditionType, DeploymentPhase,
    HealthCheckSpec, ReplicaState, ReplicaStateStatus, Timestamp,
};

const HEALTH_READY_CONDITION: &str = "HealthReady";

#[derive(Clone, Copy)]
pub(crate) enum HealthObservation<'a> {
    NotConfigured,
    Healthy,
    Unhealthy(&'a str),
}

pub(crate) fn desired_health_status(
    replica: &ReplicaState,
    assignment: &Assignment,
    health_check: Option<&HealthCheckSpec>,
    observation: HealthObservation<'_>,
    now: Timestamp,
) -> ReplicaStateStatus {
    let (phase, failures, state, reason, message) = match observation {
        HealthObservation::NotConfigured => (
            DeploymentPhase::Ready,
            0,
            ConditionState::True,
            "HealthCheckNotConfigured",
            "workload is ready because no health check is configured".to_owned(),
        ),
        HealthObservation::Healthy => (
            DeploymentPhase::Ready,
            0,
            ConditionState::True,
            "ProbeSucceeded",
            "workload health probe succeeded".to_owned(),
        ),
        HealthObservation::Unhealthy(message) => {
            let failures = replica.status.healthcheck_failures.saturating_add(1);
            let threshold = health_check.map_or(1, |check| check.unhealthy_threshold.max(1));
            let phase = if failures >= threshold {
                DeploymentPhase::Crashed
            } else {
                DeploymentPhase::PendingReady
            };
            (
                phase,
                failures,
                ConditionState::False,
                if phase == DeploymentPhase::Crashed {
                    "UnhealthyThresholdReached"
                } else {
                    "ProbeFailed"
                },
                message.to_owned(),
            )
        }
    };
    let previous = replica
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type.0 == HEALTH_READY_CONDITION);
    let last_transition_time = previous
        .filter(|condition| condition.state == state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    ReplicaStateStatus {
        phase,
        node_id: Some(assignment.spec.node_id.clone()),
        workload_id: assignment.status.workload_id.clone(),
        healthcheck_failures: failures,
        restart_attempts: replica.status.restart_attempts,
        conditions: vec![Condition {
            condition_type: ConditionType(HEALTH_READY_CONDITION.to_owned()),
            state,
            reason: ConditionReason(reason.to_owned()),
            message,
            observed_generation: replica.meta.generation,
            last_transition_time,
        }],
    }
}
