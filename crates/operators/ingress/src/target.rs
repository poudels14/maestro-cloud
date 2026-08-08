use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use kernel_api::{
    Assignment, AssignmentPhase, Deployment, DeploymentId, DeploymentPhase, NodeId, ReplicaState,
    Service, TrafficGenerationSpec, TrafficRoute, TrafficTarget, assignment_workload_address,
};

use crate::IngressPlanError;

pub(crate) fn desired_spec(
    service: &Service,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    routes: &[TrafficRoute],
    assignments: &[Assignment],
    live_nodes: &BTreeSet<NodeId>,
    replicas: &[ReplicaState],
) -> Result<Option<TrafficGenerationSpec>, IngressPlanError> {
    let Some(deployment_id) = service.status.active_deployment_id.as_ref() else {
        return Ok(None);
    };
    let deployment = deployments.get(deployment_id).ok_or_else(|| {
        IngressPlanError::MissingActiveDeployment {
            service_id: service.meta.id.clone(),
            deployment_id: deployment_id.clone(),
        }
    })?;
    if deployment.spec.service_id != service.meta.id {
        return Err(IngressPlanError::ActiveDeploymentOwnershipMismatch {
            service_id: service.meta.id.clone(),
            deployment_id: deployment.meta.id.clone(),
        });
    }
    if !matches!(
        deployment.status.phase,
        DeploymentPhase::Ready
            | DeploymentPhase::Recovering
            | DeploymentPhase::Stopping
            | DeploymentPhase::Stopped
    ) {
        return Ok(None);
    }
    let targets = if routes.is_empty() {
        Vec::new()
    } else {
        let assignments = ready_assignments(service, deployment, assignments, live_nodes, replicas);
        let assignment_count = assignments.len();
        let ports = routes
            .iter()
            .map(|route| route.target_port)
            .collect::<BTreeSet<_>>();
        let addressed = assignments
            .into_iter()
            .filter_map(|assignment| {
                assignment_workload_address(assignment).map(|address| (assignment, address))
            })
            .collect::<Vec<_>>();
        if addressed.len() != assignment_count {
            return Ok(None);
        }
        addressed
            .into_iter()
            .flat_map(|(assignment, address)| {
                ports.iter().map(move |port| TrafficTarget {
                    assignment_id: assignment.meta.id.clone(),
                    node_id: assignment.spec.node_id.clone(),
                    endpoint: SocketAddr::new(address, *port),
                })
            })
            .collect()
    };
    Ok(Some(TrafficGenerationSpec {
        service_id: service.meta.id.clone(),
        deployment_id: deployment.meta.id.clone(),
        epoch: 0,
        routes: routes.to_vec(),
        targets,
    }))
}

fn ready_assignments<'a>(
    service: &Service,
    deployment: &Deployment,
    assignments: &'a [Assignment],
    live_nodes: &BTreeSet<NodeId>,
    replicas: &[ReplicaState],
) -> Vec<&'a Assignment> {
    let count = service
        .status
        .replica_override
        .unwrap_or(service.spec.replicas);
    let mut slots = BTreeMap::new();
    for assignment in assignments.iter().filter(|assignment| {
        assignment.meta.deletion_timestamp.is_none()
            && assignment.spec.deployment_id == deployment.meta.id
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
        .into_values()
        .filter(|assignment| {
            assignment.status.phase == AssignmentPhase::Running
                && live_nodes.contains(&assignment.spec.node_id)
                && replicas.iter().any(|replica| {
                    replica.meta.deletion_timestamp.is_none()
                        && replica.spec.service_id == service.meta.id
                        && replica.spec.deployment_id == deployment.meta.id
                        && replica.spec.assignment_id == assignment.meta.id
                        && replica.spec.replica_index == assignment.spec.replica_index
                        && replica.status.phase == DeploymentPhase::Ready
                })
        })
        .collect()
}
