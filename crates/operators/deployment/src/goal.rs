use std::time::Duration;

use kernel_api::{
    Assignment, Deployment, DeploymentGoal, DeploymentPhase, DeploymentStatus, Service, Timestamp,
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
        Some(removal_status(deployment, assignments, now, drain_grace))
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
        DeploymentPhase::Queued | DeploymentPhase::Building
    ) {
        desired.phase = DeploymentPhase::Canceled;
    }
    desired
}

fn removal_status(
    deployment: &Deployment,
    assignments: &[Assignment],
    now: Timestamp,
    drain_grace: Duration,
) -> DeploymentStatus {
    let mut desired = deployment.status.clone();
    match desired.phase {
        DeploymentPhase::Queued => desired.phase = DeploymentPhase::Canceled,
        DeploymentPhase::Building
        | DeploymentPhase::Publishing
        | DeploymentPhase::PendingReady
        | DeploymentPhase::Ready => {
            desired.phase = DeploymentPhase::Draining;
            desired.draining_at.get_or_insert(now);
        }
        DeploymentPhase::Crashed => desired.phase = DeploymentPhase::Terminated,
        DeploymentPhase::Draining => {
            desired.draining_at.get_or_insert(now);
            if drain_elapsed(desired.draining_at, now, drain_grace)
                && !has_assignments(&deployment.meta.id, assignments)
            {
                desired.phase = DeploymentPhase::Removed;
            }
        }
        DeploymentPhase::Terminated | DeploymentPhase::Canceled => {
            if !has_assignments(&deployment.meta.id, assignments) {
                desired.phase = DeploymentPhase::Removed;
            }
        }
        DeploymentPhase::Removed => {}
    }
    desired
}
