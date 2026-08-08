use std::time::Duration;

use kernel_api::{
    Assignment, Condition, ConditionReason, ConditionState, ConditionType, Deployment,
    DeploymentGoal, DeploymentPhase, DeploymentStatus, Service, Timestamp,
};

use crate::readiness::{drain_elapsed, has_assignments};

pub(crate) fn requested_status(
    service: &Service,
    deployment: &Deployment,
    assignments: &[Assignment],
    now: Timestamp,
    drain_grace: Duration,
) -> Option<DeploymentStatus> {
    if service.meta.deletion_timestamp.is_some() || deployment.spec.goal == DeploymentGoal::Remove {
        Some(removal_status(
            service,
            deployment,
            assignments,
            now,
            drain_grace,
        ))
    } else if deployment.spec.goal == DeploymentGoal::Cancel {
        Some(cancellation_status(deployment))
    } else {
        None
    }
}

fn cancellation_status(deployment: &Deployment) -> DeploymentStatus {
    let mut desired = deployment.status.clone();
    if matches!(
        desired.phase,
        DeploymentPhase::Queued | DeploymentPhase::Preparing | DeploymentPhase::Building
    ) {
        desired.phase = DeploymentPhase::Canceled;
    }
    desired
}

fn removal_status(
    service: &Service,
    deployment: &Deployment,
    assignments: &[Assignment],
    now: Timestamp,
    drain_grace: Duration,
) -> DeploymentStatus {
    let mut desired = deployment.status.clone();
    match desired.phase {
        DeploymentPhase::Queued => desired.phase = DeploymentPhase::Canceled,
        DeploymentPhase::Preparing
        | DeploymentPhase::Building
        | DeploymentPhase::Publishing
        | DeploymentPhase::Starting
        | DeploymentPhase::PendingReady
        | DeploymentPhase::Retrying
        | DeploymentPhase::Ready
        | DeploymentPhase::Recovering
        | DeploymentPhase::Stopping
        | DeploymentPhase::Stopped => {
            desired.phase = DeploymentPhase::Draining;
            desired.draining_at.get_or_insert(now);
        }
        DeploymentPhase::Crashed => {
            let complete = !has_assignments(&deployment.meta.id, assignments);
            set_cleanup_condition(deployment, &mut desired, complete, now);
            if service.meta.deletion_timestamp.is_some() && complete {
                desired.phase = DeploymentPhase::Removed;
            }
        }
        DeploymentPhase::Draining => {
            desired.draining_at.get_or_insert(now);
            if drain_elapsed(desired.draining_at, now, drain_grace)
                && !has_assignments(&deployment.meta.id, assignments)
            {
                desired.phase = DeploymentPhase::Removed;
            }
        }
        DeploymentPhase::Canceled => {
            if !has_assignments(&deployment.meta.id, assignments) {
                desired.phase = DeploymentPhase::Removed;
            }
        }
        DeploymentPhase::Removed => {}
    }
    desired
}

pub(crate) fn set_cleanup_condition(
    deployment: &Deployment,
    desired: &mut DeploymentStatus,
    complete: bool,
    now: Timestamp,
) {
    let state = if complete {
        ConditionState::True
    } else {
        ConditionState::False
    };
    let reason = ConditionReason(if complete {
        "RuntimeResourcesRemoved".to_owned()
    } else {
        "RuntimeCleanupPending".to_owned()
    });
    let previous = desired
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::CleanupComplete);
    let last_transition_time = previous
        .filter(|condition| condition.state == state && condition.reason == reason)
        .map_or(now, |condition| condition.last_transition_time);
    desired
        .conditions
        .retain(|condition| condition.condition_type != ConditionType::CleanupComplete);
    desired.conditions.push(Condition {
        condition_type: ConditionType::CleanupComplete,
        state,
        reason,
        message: if complete {
            "runtime resources have been removed".to_owned()
        } else {
            "waiting for runtime resources to be removed".to_owned()
        },
        observed_generation: deployment.meta.generation,
        last_transition_time,
    });
}
