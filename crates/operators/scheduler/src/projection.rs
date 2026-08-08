use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, ConditionState, ConditionType, Deployment,
    DeploymentPhase, NodeId, PlacementConstraint, ServiceId, Timestamp, TrafficGenerationPhase,
    VolumeSource, desired_service_replicas,
};

use crate::SchedulerError;
use crate::model::{
    DeploymentGroup, NodeSchedulingState, ScheduleInput, ScheduleNode, ServiceSchedule,
    UnschedulableReason, UnschedulableReplica,
};
use crate::resource::ResourceSnapshot;

pub(crate) struct Projection {
    pub(crate) input: ScheduleInput,
    pub(crate) validation_errors: Vec<UnschedulableReplica>,
    pub(crate) retained_assignments: Vec<Assignment>,
}

pub(crate) fn project(
    cluster_id: kernel_api::ClusterId,
    snapshot: &ResourceSnapshot,
    live_nodes: &BTreeSet<NodeId>,
    now: Timestamp,
    replacement_grace: Duration,
    deployment_drain_grace: Duration,
) -> Result<Projection, SchedulerError> {
    let (nodes, held) = schedule_nodes(snapshot, live_nodes, now, replacement_grace)?;
    let (services, validation_errors, mut retained_assignments) =
        schedule_services(snapshot, now, deployment_drain_grace);
    retained_assignments.extend(active_traffic_assignments(snapshot));
    retained_assignments.extend(deleting_service_assignments(
        snapshot,
        now,
        deployment_drain_grace,
    ));
    Ok(Projection {
        input: ScheduleInput {
            cluster_id,
            services,
            nodes,
            current: snapshot.assignments.values().cloned().collect(),
            held,
        },
        validation_errors,
        retained_assignments,
    })
}

fn active_traffic_assignments(snapshot: &ResourceSnapshot) -> Vec<Assignment> {
    let assignment_ids = snapshot
        .traffic_generations
        .values()
        .filter(|generation| generation.status.phase == TrafficGenerationPhase::Active)
        .flat_map(|generation| {
            generation
                .spec
                .targets
                .iter()
                .map(|target| target.assignment_id.clone())
        })
        .collect::<BTreeSet<_>>();
    snapshot
        .assignments
        .values()
        .filter(|assignment| assignment_ids.contains(&assignment.meta.id))
        .cloned()
        .collect()
}

fn deleting_service_assignments(
    snapshot: &ResourceSnapshot,
    now: Timestamp,
    deployment_drain_grace: Duration,
) -> Vec<Assignment> {
    let deleting_services = snapshot
        .services
        .values()
        .filter_map(|service| {
            service
                .meta
                .deletion_timestamp
                .map(|deleted_at| (service.meta.id.clone(), deleted_at))
        })
        .collect::<BTreeMap<_, _>>();
    let draining_deployments = snapshot
        .deployments
        .values()
        .filter(|deployment| {
            let Some(deleted_at) = deleting_services.get(&deployment.spec.service_id) else {
                return false;
            };
            if deployment.status.phase == DeploymentPhase::Draining {
                active_deployment(deployment, now, deployment_drain_grace)
            } else {
                matches!(
                    deployment.status.phase,
                    DeploymentPhase::Preparing
                        | DeploymentPhase::Building
                        | DeploymentPhase::Publishing
                        | DeploymentPhase::Starting
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Retrying
                        | DeploymentPhase::Recovering
                        | DeploymentPhase::Stopping
                        | DeploymentPhase::Stopped
                        | DeploymentPhase::Ready
                ) && within_grace(now, *deleted_at, deployment_drain_grace)
            }
        })
        .map(|deployment| deployment.meta.id.clone())
        .collect::<BTreeSet<_>>();
    snapshot
        .assignments
        .values()
        .filter(|assignment| draining_deployments.contains(&assignment.spec.deployment_id))
        .cloned()
        .collect()
}

fn schedule_nodes(
    snapshot: &ResourceSnapshot,
    live_nodes: &BTreeSet<NodeId>,
    now: Timestamp,
    replacement_grace: Duration,
) -> Result<(Vec<ScheduleNode>, BTreeSet<AssignmentId>), SchedulerError> {
    let mut networks = BTreeMap::new();
    for network in snapshot
        .networks
        .values()
        .filter(|network| network.meta.deletion_timestamp.is_none())
    {
        if networks
            .insert(network.spec.node_id.clone(), network)
            .is_some()
        {
            return Err(SchedulerError::DuplicateNodeNetwork {
                node_id: network.spec.node_id.clone(),
            });
        }
    }

    let mut nodes = Vec::new();
    let mut held_nodes = BTreeSet::new();
    for node in snapshot
        .nodes
        .values()
        .filter(|node| node.meta.deletion_timestamp.is_none())
    {
        let network = networks.get(&node.meta.id).copied();
        let explicitly_unschedulable = node.status.conditions.iter().any(|condition| {
            (condition.condition_type == ConditionType::Schedulable
                && condition.state == ConditionState::False)
                || (matches!(
                    condition.condition_type,
                    ConditionType::Draining | ConditionType::Maintenance
                ) && condition.state == ConditionState::True)
        });
        let mesh_ready = network.is_some_and(|network| {
            network.status.applied_generation == network.meta.generation
                && network.status.conditions.iter().any(|condition| {
                    condition.condition_type == ConditionType::MeshReady
                        && condition.state == ConditionState::True
                })
        });
        let network_ready = match node.spec.workload_network_mode {
            kernel_api::WorkloadNetworkMode::ClusterRouted => mesh_ready,
            kernel_api::WorkloadNetworkMode::RuntimeDelegated => true,
        };
        let live = live_nodes.contains(&node.meta.id);
        let state = if explicitly_unschedulable {
            NodeSchedulingState::Unschedulable
        } else if live && network_ready {
            NodeSchedulingState::Available
        } else {
            NodeSchedulingState::Unavailable
        };
        if state == NodeSchedulingState::Unavailable
            && within_grace(
                now,
                unavailable_since(node.status.last_seen, network),
                replacement_grace,
            )
        {
            held_nodes.insert(node.meta.id.clone());
        }
        nodes.push(ScheduleNode {
            node_id: node.meta.id.clone(),
            role: node.spec.role,
            labels: node.spec.scheduling_labels.clone(),
            workload_network_mode: node.spec.workload_network_mode,
            workload_subnet: network.map(|network| network.spec.workload_subnet.clone()),
            state,
        });
    }
    let held = snapshot
        .assignments
        .values()
        .filter(|assignment| held_nodes.contains(&assignment.spec.node_id))
        .map(|assignment| assignment.meta.id.clone())
        .collect();
    Ok((nodes, held))
}

fn unavailable_since(
    node_last_seen: Timestamp,
    network: Option<&kernel_api::NodeNetwork>,
) -> Timestamp {
    network
        .and_then(|network| {
            network.status.conditions.iter().find_map(|condition| {
                (condition.condition_type == ConditionType::MeshReady
                    && condition.state != ConditionState::True)
                    .then_some(condition.last_transition_time)
            })
        })
        .unwrap_or(node_last_seen)
}

fn within_grace(now: Timestamp, since: Timestamp, grace: Duration) -> bool {
    let elapsed = now.0.saturating_sub(since.0);
    let grace_millis = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
    elapsed < grace_millis
}

fn schedule_services(
    snapshot: &ResourceSnapshot,
    now: Timestamp,
    deployment_drain_grace: Duration,
) -> (
    Vec<ServiceSchedule>,
    Vec<UnschedulableReplica>,
    Vec<Assignment>,
) {
    let mut deployments = snapshot
        .deployments
        .values()
        .filter(|deployment| deployment.meta.deletion_timestamp.is_none())
        .filter(|deployment| active_deployment(deployment, now, deployment_drain_grace))
        .fold(
            BTreeMap::<ServiceId, Vec<&Deployment>>::new(),
            |mut by_service, deployment| {
                by_service
                    .entry(deployment.spec.service_id.clone())
                    .or_default()
                    .push(deployment);
                by_service
            },
        );
    for values in deployments.values_mut() {
        values.sort_by(|left, right| {
            deployment_order(left.status.phase)
                .cmp(&deployment_order(right.status.phase))
                .then_with(|| left.status.created_at.cmp(&right.status.created_at))
                .then_with(|| left.meta.id.cmp(&right.meta.id))
        });
    }

    let mut services = Vec::new();
    let mut validation_errors = Vec::new();
    let mut retained_on_error = Vec::new();
    for service in snapshot
        .services
        .values()
        .filter(|service| service.meta.deletion_timestamp.is_none())
    {
        let Some(groups) = deployments.get(&service.meta.id) else {
            continue;
        };
        let replicas = desired_service_replicas(service);
        let deployment_groups = groups
            .iter()
            .map(|deployment| DeploymentGroup {
                deployment_id: deployment.meta.id.clone(),
                restart_generation: deployment.spec.restart_generation,
                replicas,
            })
            .collect::<Vec<_>>();
        let placement = match effective_placement(service) {
            Ok(placement) => placement,
            Err(reason) => {
                validation_errors.extend(deployment_groups.iter().flat_map(|group| {
                    (0..group.replicas).map(|replica_index| UnschedulableReplica {
                        service_id: service.meta.id.clone(),
                        deployment_id: group.deployment_id.clone(),
                        replica_index,
                        reason: reason.clone(),
                    })
                }));
                retained_on_error.extend(
                    snapshot
                        .assignments
                        .values()
                        .filter(|assignment| assignment.spec.service_id == service.meta.id)
                        .cloned(),
                );
                continue;
            }
        };
        services.push(ServiceSchedule {
            service_id: service.meta.id.clone(),
            groups: deployment_groups,
            placement,
        });
    }
    (services, validation_errors, retained_on_error)
}

fn active_deployment(
    deployment: &Deployment,
    now: Timestamp,
    deployment_drain_grace: Duration,
) -> bool {
    matches!(
        deployment.status.phase,
        DeploymentPhase::Building
            | DeploymentPhase::Publishing
            | DeploymentPhase::Starting
            | DeploymentPhase::PendingReady
            | DeploymentPhase::Retrying
            | DeploymentPhase::Ready
            | DeploymentPhase::Recovering
            | DeploymentPhase::Stopping
            | DeploymentPhase::Stopped
            | DeploymentPhase::Draining
    ) && (deployment.status.phase != DeploymentPhase::Draining
        || deployment
            .status
            .draining_at
            .is_none_or(|started| within_grace(now, started, deployment_drain_grace)))
        && (matches!(
            deployment.spec.service.artifact,
            ArtifactTemplate::Image { .. }
        ) || deployment.status.image_digest.is_some())
}

fn deployment_order(phase: DeploymentPhase) -> u8 {
    match phase {
        DeploymentPhase::Draining => 0,
        DeploymentPhase::Ready => 1,
        DeploymentPhase::Recovering => 2,
        DeploymentPhase::Retrying => 3,
        DeploymentPhase::PendingReady => 4,
        DeploymentPhase::Starting => 5,
        DeploymentPhase::Publishing => 6,
        DeploymentPhase::Stopping => 7,
        DeploymentPhase::Stopped => 8,
        DeploymentPhase::Building => 9,
        DeploymentPhase::Preparing => 10,
        _ => 11,
    }
}

fn effective_placement(
    service: &kernel_api::Service,
) -> Result<PlacementConstraint, UnschedulableReason> {
    let volume_nodes = service
        .spec
        .volumes
        .iter()
        .filter_map(|volume| match &volume.source {
            VolumeSource::HostPath { node_id, .. } => Some(node_id.clone()),
            VolumeSource::Managed { .. } | VolumeSource::ReplicaManaged { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    let volume_node = if volume_nodes.is_empty() {
        None
    } else if volume_nodes.len() == 1 {
        volume_nodes.first().cloned()
    } else {
        return Err(UnschedulableReason::ConflictingHostVolumeNodes);
    };
    if let (Some(volume_node_id), Some(placement_node_id)) =
        (&volume_node, &service.spec.placement.node_id)
        && volume_node_id != placement_node_id
    {
        return Err(UnschedulableReason::HostVolumePlacementMismatch {
            volume_node_id: volume_node_id.clone(),
            placement_node_id: placement_node_id.clone(),
        });
    }
    let mut placement = service.spec.placement.clone();
    placement.node_id = placement.node_id.or(volume_node);
    Ok(placement)
}
