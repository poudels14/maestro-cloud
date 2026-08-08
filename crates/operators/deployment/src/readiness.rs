use std::collections::BTreeMap;
use std::time::Duration;

use kernel_api::{
    Assignment, AssignmentPhase, Deployment, DeploymentId, DeploymentPhase, NodeId, ReplicaState,
    Timestamp,
};

pub(crate) fn current_slots<'a>(
    deployment_id: &DeploymentId,
    assignments: &'a [Assignment],
    count: u32,
) -> BTreeMap<u32, &'a Assignment> {
    let mut slots = BTreeMap::new();
    for assignment in assignments.iter().filter(|assignment| {
        assignment.meta.deletion_timestamp.is_none()
            && &assignment.spec.deployment_id == deployment_id
            && assignment.spec.replica_index < count
    }) {
        let current: &mut &Assignment = slots
            .entry(assignment.spec.replica_index)
            .or_insert(assignment);
        if assignment.spec.placement_epoch > current.spec.placement_epoch {
            *current = assignment;
        }
    }
    slots
}

pub(crate) fn all_ready(
    deployment: &Deployment,
    slots: &BTreeMap<u32, &Assignment>,
    replicas: &[ReplicaState],
    live_nodes: &std::collections::BTreeSet<NodeId>,
    count: u32,
) -> bool {
    (0..count).all(|index| {
        slots.get(&index).is_some_and(|assignment| {
            assignment.status.phase == AssignmentPhase::Running
                && live_nodes.contains(&assignment.spec.node_id)
                && exact_replica(deployment, assignment, replicas)
                    .is_some_and(|replica| replica.status.phase == DeploymentPhase::Ready)
        })
    })
}

pub(crate) fn all_started(
    deployment: &Deployment,
    slots: &BTreeMap<u32, &Assignment>,
    replicas: &[ReplicaState],
    live_nodes: &std::collections::BTreeSet<NodeId>,
    count: u32,
) -> bool {
    (0..count).all(|index| {
        slots.get(&index).is_some_and(|assignment| {
            assignment.status.phase == AssignmentPhase::Running
                && live_nodes.contains(&assignment.spec.node_id)
                && exact_replica(deployment, assignment, replicas).is_some_and(|replica| {
                    matches!(
                        replica.status.phase,
                        DeploymentPhase::PendingReady | DeploymentPhase::Ready
                    )
                })
        })
    })
}

pub(crate) fn all_stopped(
    deployment: &Deployment,
    slots: &BTreeMap<u32, &Assignment>,
    replicas: &[ReplicaState],
    count: u32,
) -> bool {
    count > 0
        && (0..count).all(|index| {
            slots.get(&index).is_some_and(|assignment| {
                assignment.status.phase == AssignmentPhase::Stopped
                    && exact_replica(deployment, assignment, replicas)
                        .is_some_and(|replica| replica.status.phase == DeploymentPhase::Stopped)
            })
        })
}

pub(crate) fn any_stopping(
    deployment: &Deployment,
    slots: &BTreeMap<u32, &Assignment>,
    replicas: &[ReplicaState],
    count: u32,
) -> bool {
    (0..count).any(|index| {
        slots.get(&index).is_some_and(|assignment| {
            assignment.status.phase == AssignmentPhase::Stopping
                || exact_replica(deployment, assignment, replicas)
                    .is_some_and(|replica| replica.status.phase == DeploymentPhase::Stopping)
        })
    })
}

pub(crate) fn any_unreachable(
    slots: &BTreeMap<u32, &Assignment>,
    live_nodes: &std::collections::BTreeSet<NodeId>,
    count: u32,
) -> bool {
    (0..count).any(|index| {
        slots
            .get(&index)
            .is_none_or(|assignment| !live_nodes.contains(&assignment.spec.node_id))
    })
}

pub(crate) fn has_retrying(
    deployment: &Deployment,
    slots: &BTreeMap<u32, &Assignment>,
    replicas: &[ReplicaState],
    count: u32,
) -> bool {
    (0..count).any(|index| {
        slots.get(&index).is_some_and(|assignment| {
            exact_replica(deployment, assignment, replicas)
                .is_some_and(|replica| replica.status.phase == DeploymentPhase::Crashed)
        })
    })
}

fn exact_replica<'a>(
    deployment: &Deployment,
    assignment: &Assignment,
    replicas: &'a [ReplicaState],
) -> Option<&'a ReplicaState> {
    replicas.iter().find(|replica| {
        replica.meta.deletion_timestamp.is_none()
            && replica.spec.service_id == deployment.spec.service_id
            && replica.spec.deployment_id == deployment.meta.id
            && replica.spec.replica_index == assignment.spec.replica_index
            && replica.spec.assignment_id == assignment.meta.id
    })
}

pub(crate) fn has_assignments(deployment_id: &DeploymentId, assignments: &[Assignment]) -> bool {
    assignments.iter().any(|assignment| {
        assignment.meta.deletion_timestamp.is_none()
            && &assignment.spec.deployment_id == deployment_id
    })
}

pub(crate) fn drain_elapsed(
    draining_at: Option<Timestamp>,
    now: Timestamp,
    grace: Duration,
) -> bool {
    draining_at.is_some_and(|started| {
        let grace = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
        now.0 >= started.0.saturating_add(grace)
    })
}
