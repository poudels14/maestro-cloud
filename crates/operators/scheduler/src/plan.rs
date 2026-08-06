use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus, Generation,
    NodeId, NodeRole, ObjectMeta, ReplicaSpread, ResourceRevision,
};
use sha2::{Digest, Sha256};

use crate::address::allocate_addresses;
use crate::model::{
    NodeSchedulingState, ScheduleInput, ScheduleNode, SchedulePlan, ServiceSchedule,
    UnschedulableReason, UnschedulableReplica,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CandidateAvailability {
    None,
    Available,
}

/// Computes stable placements without reading the store or performing side effects.
pub fn plan(mut input: ScheduleInput) -> SchedulePlan {
    input.services.sort_by(|left, right| {
        right
            .placement
            .node_id
            .is_some()
            .cmp(&left.placement.node_id.is_some())
            .then_with(|| replica_total(right).cmp(&replica_total(left)))
            .then_with(|| left.service_id.cmp(&right.service_id))
    });
    input
        .nodes
        .sort_by(|left, right| left.node_id.cmp(&right.node_id));

    let held_nodes = input
        .current
        .iter()
        .filter(|assignment| input.held.contains(&assignment.meta.id))
        .map(|assignment| assignment.spec.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut current_by_slot = BTreeMap::new();
    for assignment in &input.current {
        if assignment.meta.deletion_timestamp.is_none() {
            current_by_slot
                .entry((
                    assignment.spec.service_id.clone(),
                    assignment.spec.deployment_id.clone(),
                    assignment.spec.replica_index,
                ))
                .or_insert_with(Vec::new)
                .push(assignment);
        }
    }
    for assignments in current_by_slot.values_mut() {
        assignments.sort_by(|left, right| {
            input
                .held
                .contains(&right.meta.id)
                .cmp(&input.held.contains(&left.meta.id))
                .then_with(|| right.spec.placement_epoch.cmp(&left.spec.placement_epoch))
                .then_with(|| left.meta.id.cmp(&right.meta.id))
        });
    }

    let mut planned = Vec::new();
    let mut unschedulable = Vec::new();
    let mut node_load = BTreeMap::<NodeId, usize>::new();
    let mut deployment_load = BTreeMap::new();
    let mut rollout_preference = BTreeMap::new();
    for service in &input.services {
        plan_service(
            &input,
            service,
            &held_nodes,
            &current_by_slot,
            &mut planned,
            &mut unschedulable,
            &mut node_load,
            &mut deployment_load,
            &mut rollout_preference,
        );
    }

    let address_failures = allocate_addresses(&mut planned, &input.nodes, &input.current);
    for (index, reason) in &address_failures {
        if let Some(assignment) = planned.get(*index) {
            unschedulable.push(UnschedulableReplica {
                service_id: assignment.service_id.clone(),
                deployment_id: assignment.deployment_id.clone(),
                replica_index: assignment.replica_index,
                reason: reason.clone(),
            });
        }
    }
    let failed = address_failures
        .into_iter()
        .map(|(index, _reason)| index)
        .collect::<BTreeSet<_>>();
    let mut assignments = planned
        .into_iter()
        .enumerate()
        .filter_map(|(index, assignment)| (!failed.contains(&index)).then_some(assignment))
        .filter_map(PlannedAssignment::into_resource)
        .collect::<Vec<_>>();
    assignments.sort_by(|left, right| {
        left.spec
            .node_id
            .cmp(&right.spec.node_id)
            .then_with(|| left.spec.service_id.cmp(&right.spec.service_id))
            .then_with(|| left.spec.deployment_id.cmp(&right.spec.deployment_id))
            .then_with(|| left.spec.replica_index.cmp(&right.spec.replica_index))
            .then_with(|| left.meta.id.cmp(&right.meta.id))
    });
    unschedulable.sort_by(|left, right| {
        left.service_id
            .cmp(&right.service_id)
            .then_with(|| left.deployment_id.cmp(&right.deployment_id))
            .then_with(|| left.replica_index.cmp(&right.replica_index))
    });
    SchedulePlan {
        assignments,
        unschedulable,
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_service<'a>(
    input: &'a ScheduleInput,
    service: &ServiceSchedule,
    held_nodes: &BTreeSet<NodeId>,
    current_by_slot: &BTreeMap<
        (kernel_api::ServiceId, kernel_api::DeploymentId, u32),
        Vec<&'a Assignment>,
    >,
    planned: &mut Vec<PlannedAssignment>,
    unschedulable: &mut Vec<UnschedulableReplica>,
    node_load: &mut BTreeMap<NodeId, usize>,
    deployment_load: &mut BTreeMap<
        (kernel_api::ServiceId, kernel_api::DeploymentId, NodeId),
        usize,
    >,
    rollout_preference: &mut BTreeMap<(kernel_api::ServiceId, u32), NodeId>,
) {
    let candidates = eligible_nodes(service, &input.nodes, held_nodes);
    for (group_index, group) in service.groups.iter().enumerate() {
        for replica_index in 0..group.replicas {
            let slot = (
                service.service_id.clone(),
                group.deployment_id.clone(),
                replica_index,
            );
            let existing = current_by_slot
                .get(&slot)
                .and_then(|assignments| assignments.first())
                .copied();
            let existing_is_eligible = existing.is_some_and(|assignment| {
                assignment.spec.restart_generation == group.restart_generation
                    && (input.held.contains(&assignment.meta.id)
                        || candidates
                            .iter()
                            .any(|candidate| candidate.node_id == assignment.spec.node_id))
            });
            if let Some(existing) = existing
                && existing_is_eligible
                && spread_allows_retention(
                    service,
                    &group.deployment_id,
                    existing,
                    &candidates,
                    deployment_load,
                    &input.held,
                )
            {
                retain(existing, planned, deployment_load, node_load);
                if group_index == 0 {
                    rollout_preference.insert(
                        (service.service_id.clone(), replica_index),
                        existing.spec.node_id.clone(),
                    );
                }
                continue;
            }

            let preferred_node = (group_index > 0)
                .then(|| rollout_preference.get(&(service.service_id.clone(), replica_index)))
                .flatten();
            let selected = candidates.iter().min_by_key(|node| {
                let deployment_node_load = deployment_load
                    .get(&(
                        service.service_id.clone(),
                        group.deployment_id.clone(),
                        node.node_id.clone(),
                    ))
                    .copied()
                    .unwrap_or_default();
                let spreads_replicas =
                    service.placement.replica_spread == ReplicaSpread::BestEffort;
                (
                    spreads_replicas.then_some(deployment_node_load),
                    preferred_node != Some(&node.node_id),
                    deployment_node_load,
                    node_load.get(&node.node_id).copied().unwrap_or_default(),
                    node.role != NodeRole::Worker,
                    &node.node_id,
                )
            });
            if let Some(node) = selected {
                let placement_epoch = existing
                    .map(|assignment| assignment.spec.placement_epoch.saturating_add(1))
                    .unwrap_or(1);
                let assignment = PlannedAssignment::new(
                    assignment_id(
                        &input.cluster_id,
                        &service.service_id,
                        &group.deployment_id,
                        replica_index,
                        &node.node_id,
                        placement_epoch,
                    ),
                    placement_epoch,
                    service.service_id.clone(),
                    group.deployment_id.clone(),
                    group.restart_generation,
                    replica_index,
                    node.node_id.clone(),
                    existing.map(|assignment| assignment.meta.id.clone()),
                );
                retain_planned(assignment, planned, deployment_load, node_load);
                if group_index == 0 {
                    rollout_preference.insert(
                        (service.service_id.clone(), replica_index),
                        node.node_id.clone(),
                    );
                }
            } else {
                if let Some(existing) = existing {
                    retain(existing, planned, deployment_load, node_load);
                }
                unschedulable.push(UnschedulableReplica {
                    service_id: service.service_id.clone(),
                    deployment_id: group.deployment_id.clone(),
                    replica_index,
                    reason: unschedulable_reason(
                        service,
                        &input.nodes,
                        if candidates.is_empty() {
                            CandidateAvailability::None
                        } else {
                            CandidateAvailability::Available
                        },
                    ),
                });
            }
        }
    }
}

fn spread_allows_retention(
    service: &ServiceSchedule,
    deployment_id: &kernel_api::DeploymentId,
    existing: &Assignment,
    candidates: &[&ScheduleNode],
    deployment_load: &BTreeMap<(kernel_api::ServiceId, kernel_api::DeploymentId, NodeId), usize>,
    held: &BTreeSet<AssignmentId>,
) -> bool {
    if service.placement.replica_spread == ReplicaSpread::Stable || held.contains(&existing.meta.id)
    {
        return true;
    }
    let load = |node_id: &NodeId| {
        deployment_load
            .get(&(
                service.service_id.clone(),
                deployment_id.clone(),
                node_id.clone(),
            ))
            .copied()
            .unwrap_or_default()
    };
    let existing_load = load(&existing.spec.node_id);
    candidates
        .iter()
        .map(|candidate| load(&candidate.node_id))
        .min()
        .is_none_or(|minimum_load| existing_load <= minimum_load)
}

fn eligible_nodes<'a>(
    service: &ServiceSchedule,
    nodes: &'a [ScheduleNode],
    held_nodes: &BTreeSet<NodeId>,
) -> Vec<&'a ScheduleNode> {
    nodes
        .iter()
        .filter(|node| node.role.runs_workloads())
        .filter(|node| node.state == NodeSchedulingState::Available)
        .filter(|node| !held_nodes.contains(&node.node_id))
        .filter(|node| {
            service
                .placement
                .node_id
                .as_ref()
                .is_none_or(|node_id| node_id == &node.node_id)
                && service
                    .placement
                    .labels
                    .iter()
                    .all(|(key, value)| node.labels.get(key) == Some(value))
        })
        .collect()
}

fn unschedulable_reason(
    service: &ServiceSchedule,
    nodes: &[ScheduleNode],
    candidate_availability: CandidateAvailability,
) -> UnschedulableReason {
    let constrained = service.placement.node_id.is_some() || !service.placement.labels.is_empty();
    let affinity_matches = nodes.iter().any(|node| {
        service
            .placement
            .node_id
            .as_ref()
            .is_none_or(|node_id| node_id == &node.node_id)
            && service
                .placement
                .labels
                .iter()
                .all(|(key, value)| node.labels.get(key) == Some(value))
    });
    if constrained && !affinity_matches {
        UnschedulableReason::AffinityMatchesNoNode
    } else if candidate_availability == CandidateAvailability::Available {
        UnschedulableReason::NoAlternateNode
    } else {
        UnschedulableReason::NoSchedulableNode
    }
}

fn retain(
    assignment: &Assignment,
    planned: &mut Vec<PlannedAssignment>,
    deployment_load: &mut BTreeMap<
        (kernel_api::ServiceId, kernel_api::DeploymentId, NodeId),
        usize,
    >,
    node_load: &mut BTreeMap<NodeId, usize>,
) {
    retain_planned(
        PlannedAssignment::from_existing(assignment.clone()),
        planned,
        deployment_load,
        node_load,
    );
}

fn retain_planned(
    assignment: PlannedAssignment,
    planned: &mut Vec<PlannedAssignment>,
    deployment_load: &mut BTreeMap<
        (kernel_api::ServiceId, kernel_api::DeploymentId, NodeId),
        usize,
    >,
    node_load: &mut BTreeMap<NodeId, usize>,
) {
    *deployment_load
        .entry((
            assignment.service_id.clone(),
            assignment.deployment_id.clone(),
            assignment.node_id.clone(),
        ))
        .or_default() += 1;
    *node_load.entry(assignment.node_id.clone()).or_default() += 1;
    planned.push(assignment);
}

fn replica_total(service: &ServiceSchedule) -> u64 {
    service
        .groups
        .iter()
        .map(|group| u64::from(group.replicas))
        .sum()
}

fn assignment_id(
    cluster_id: &kernel_api::ClusterId,
    service_id: &kernel_api::ServiceId,
    deployment_id: &kernel_api::DeploymentId,
    replica_index: u32,
    node_id: &NodeId,
    placement_epoch: u64,
) -> AssignmentId {
    let mut hasher = Sha256::new();
    for value in [
        cluster_id.as_str().to_owned(),
        service_id.as_str().to_owned(),
        deployment_id.as_str().to_owned(),
        replica_index.to_string(),
        node_id.as_str().to_owned(),
        placement_epoch.to_string(),
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    AssignmentId::from_sha256(hasher.finalize().into())
}

pub(crate) struct PlannedAssignment {
    assignment_id: AssignmentId,
    placement_epoch: u64,
    pub(crate) service_id: kernel_api::ServiceId,
    pub(crate) deployment_id: kernel_api::DeploymentId,
    restart_generation: Generation,
    pub(crate) replica_index: u32,
    pub(crate) node_id: NodeId,
    pub(crate) workload_address: Option<Ipv4Addr>,
    replaces_assignment_id: Option<AssignmentId>,
    existing: Option<Assignment>,
}

impl PlannedAssignment {
    #[allow(clippy::too_many_arguments)]
    fn new(
        assignment_id: AssignmentId,
        placement_epoch: u64,
        service_id: kernel_api::ServiceId,
        deployment_id: kernel_api::DeploymentId,
        restart_generation: Generation,
        replica_index: u32,
        node_id: NodeId,
        replaces_assignment_id: Option<AssignmentId>,
    ) -> Self {
        Self {
            assignment_id,
            placement_epoch,
            service_id,
            deployment_id,
            restart_generation,
            replica_index,
            node_id,
            workload_address: None,
            replaces_assignment_id,
            existing: None,
        }
    }

    fn from_existing(resource: Assignment) -> Self {
        Self {
            assignment_id: resource.meta.id.clone(),
            placement_epoch: resource.spec.placement_epoch,
            service_id: resource.spec.service_id.clone(),
            deployment_id: resource.spec.deployment_id.clone(),
            restart_generation: resource.spec.restart_generation,
            replica_index: resource.spec.replica_index,
            node_id: resource.spec.node_id.clone(),
            workload_address: resource
                .spec
                .workload_address
                .and_then(|address| match address {
                    IpAddr::V4(address) => Some(address),
                    IpAddr::V6(_) => None,
                }),
            replaces_assignment_id: resource.spec.replaces_assignment_id.clone(),
            existing: Some(resource),
        }
    }

    fn into_resource(self) -> Option<Assignment> {
        if let Some(resource) = self.existing {
            return Some(resource);
        }
        Some(Assignment {
            meta: ObjectMeta {
                id: self.assignment_id,
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: AssignmentSpec {
                service_id: self.service_id,
                deployment_id: self.deployment_id,
                restart_generation: self.restart_generation,
                replica_index: self.replica_index,
                node_id: self.node_id,
                placement_epoch: self.placement_epoch,
                workload_address: self.workload_address.map(IpAddr::V4),
                replaces_assignment_id: self.replaces_assignment_id,
            },
            status: AssignmentStatus {
                phase: AssignmentPhase::Pending,
                workload_id: None,
                workload_address: None,
                conditions: Vec::new(),
            },
        })
    }
}
