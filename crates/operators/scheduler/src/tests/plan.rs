use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus, ClusterId,
    DeploymentId, Generation, NodeId, NodeRole, ObjectMeta, PlacementConstraint, ResourceRevision,
    ServiceId,
};

use crate::{
    DeploymentGroup, NodeSchedulingState, ScheduleInput, ScheduleNode, ServiceSchedule,
    UnhealthySlot, UnschedulableReason, plan,
};

#[test]
fn plan_is_deterministic_and_spreads_replicas() {
    let first = plan(input(5));
    let second = plan(input(5));
    assert_eq!(first, second);
    assert!(first.unschedulable.is_empty());
    assert_eq!(first.assignments.len(), 5);
    let counts = first.assignments.iter().fold(
        BTreeMap::<NodeId, usize>::new(),
        |mut counts, assignment| {
            *counts.entry(assignment.spec.node_id.clone()).or_default() += 1;
            counts
        },
    );
    assert_eq!(counts.get(&node_id("node-a")), Some(&3));
    assert_eq!(counts.get(&node_id("node-b")), Some(&2));
}

#[test]
fn generated_assignment_ids_are_stable_content_addresses() {
    let output = plan(input(1));

    assert_eq!(
        output.assignments[0].meta.id.as_str(),
        "00efef54e3f84f49abb455c6"
    );
}

#[test]
fn plan_preserves_existing_assignments_during_scale_changes() {
    let initial = plan(input(2));
    let initial_ids = initial
        .assignments
        .iter()
        .map(|assignment| assignment.meta.id.clone())
        .collect::<BTreeSet<_>>();
    let mut scaled = input(5);
    scaled.current = initial.assignments;
    let scaled = plan(scaled);
    assert_eq!(scaled.assignments.len(), 5);
    assert!(initial_ids.iter().all(|assignment_id| {
        scaled
            .assignments
            .iter()
            .any(|assignment| &assignment.meta.id == assignment_id)
    }));

    let survivors = scaled
        .assignments
        .iter()
        .filter(|assignment| assignment.spec.replica_index < 2)
        .map(|assignment| assignment.meta.id.clone())
        .collect::<BTreeSet<_>>();
    let mut reduced = input(2);
    reduced.current = scaled.assignments;
    assert_eq!(
        plan(reduced)
            .assignments
            .into_iter()
            .map(|assignment| assignment.meta.id)
            .collect::<BTreeSet<_>>(),
        survivors
    );
}

#[test]
fn rollout_groups_prefer_the_same_node_for_each_replica_slot() {
    let mut rollout = input(2);
    rollout.services[0].groups.push(DeploymentGroup {
        deployment_id: deployment_id("dep-2"),
        restart_generation: Generation(1),
        replicas: 2,
    });
    let output = plan(rollout);
    assert_eq!(output.assignments.len(), 4);
    for replica_index in 0..2 {
        let nodes = output
            .assignments
            .iter()
            .filter(|assignment| assignment.spec.replica_index == replica_index)
            .map(|assignment| assignment.spec.node_id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(nodes.len(), 1);
    }
}

#[test]
fn restart_generation_replaces_the_assignment_in_place() {
    let baseline = plan(input(1));
    let previous = baseline.assignments.first().unwrap().clone();
    let mut restarted = input(1);
    restarted.current = vec![previous.clone()];
    restarted.services[0].groups[0].restart_generation = Generation(2);

    let output = plan(restarted);
    let replacement = output.assignments.first().unwrap();

    assert_eq!(output.assignments.len(), 1);
    assert_ne!(replacement.meta.id, previous.meta.id);
    assert_eq!(replacement.spec.deployment_id, previous.spec.deployment_id);
    assert_eq!(replacement.spec.node_id, previous.spec.node_id);
    assert_eq!(replacement.spec.restart_generation, Generation(2));
    assert_eq!(replacement.spec.placement_epoch, 2);
    assert_eq!(
        replacement.spec.replaces_assignment_id.as_ref(),
        Some(&previous.meta.id)
    );
}

#[test]
fn unhealthy_assignment_moves_and_stale_health_does_not_move_successor() {
    let old = plan(input(1)).assignments.remove(0);
    let unhealthy = UnhealthySlot {
        deployment_id: old.spec.deployment_id.clone(),
        node_id: old.spec.node_id.clone(),
        replica_index: old.spec.replica_index,
        assignment_id: old.meta.id.clone(),
    };
    let mut replacement_input = input(1);
    replacement_input.current = vec![old.clone()];
    replacement_input.services[0]
        .unhealthy_slots
        .insert(unhealthy.clone());
    let replacement = plan(replacement_input).assignments.remove(0);
    assert_ne!(replacement.spec.node_id, old.spec.node_id);
    assert_eq!(replacement.spec.placement_epoch, 2);
    assert_eq!(
        replacement.spec.replaces_assignment_id.as_ref(),
        Some(&old.meta.id)
    );

    let mut stale_input = input(1);
    stale_input.current = vec![replacement.clone()];
    stale_input.services[0].unhealthy_slots.insert(unhealthy);
    assert_eq!(plan(stale_input).assignments, vec![replacement]);
}

#[test]
fn exhausted_slot_keeps_terminal_assignment() {
    let failed = plan(input(1)).assignments.remove(0);
    let mut next = input(1);
    next.current = vec![failed.clone()];
    next.services[0]
        .exhausted_slots
        .insert((failed.spec.deployment_id.clone(), failed.spec.replica_index));
    let output = plan(next);
    assert_eq!(output.assignments, vec![failed]);
    assert!(output.unschedulable.is_empty());
}

#[test]
fn held_assignment_survives_node_loss_without_attracting_new_work() {
    let held = plan(input(1)).assignments.remove(0);
    let mut next = input(2);
    next.nodes.clear();
    next.held.insert(held.meta.id.clone());
    next.current = vec![held.clone()];
    let output = plan(next);
    assert_eq!(output.assignments, vec![held]);
    assert_eq!(output.unschedulable.len(), 1);
    assert_eq!(
        output.unschedulable[0].reason,
        UnschedulableReason::NoSchedulableNode
    );
}

#[test]
fn plan_honors_affinity_and_prefers_workers_after_spreading() {
    let mut value = input(2);
    value.nodes[0]
        .labels
        .insert("zone".to_owned(), "west".to_owned());
    value.nodes[0].role = NodeRole::Worker;
    value.services[0].placement = PlacementConstraint {
        node_id: None,
        labels: BTreeMap::from([("zone".to_owned(), "west".to_owned())]),
    };
    let output = plan(value);
    assert_eq!(output.assignments.len(), 2);
    assert!(
        output
            .assignments
            .iter()
            .all(|assignment| assignment.spec.node_id == node_id("node-b"))
    );

    let mut worker = input(1);
    worker.nodes[1].role = NodeRole::Worker;
    assert_eq!(plan(worker).assignments[0].spec.node_id, node_id("node-a"));
}

#[test]
fn plan_reports_affinity_and_alternate_node_failures() {
    let mut affinity = input(1);
    affinity.services[0].placement.node_id = Some(node_id("missing"));
    assert_eq!(
        plan(affinity).unschedulable[0].reason,
        UnschedulableReason::AffinityMatchesNoNode
    );

    let old = plan(input(1)).assignments.remove(0);
    let mut no_alternate = input(1);
    no_alternate
        .nodes
        .retain(|node| node.node_id == old.spec.node_id);
    no_alternate.current = vec![old.clone()];
    no_alternate.services[0]
        .unhealthy_slots
        .insert(UnhealthySlot {
            deployment_id: old.spec.deployment_id.clone(),
            node_id: old.spec.node_id.clone(),
            replica_index: 0,
            assignment_id: old.meta.id,
        });
    assert_eq!(
        plan(no_alternate).unschedulable[0].reason,
        UnschedulableReason::NoAlternateNode
    );
}

#[test]
fn workload_addresses_are_stable_unique_and_do_not_reuse_draining_addresses() {
    let initial = plan(input(2));
    let addresses = initial
        .assignments
        .iter()
        .map(|assignment| assignment.spec.workload_address)
        .collect::<BTreeSet<_>>();
    assert_eq!(addresses.len(), 2);
    assert!(addresses.iter().all(|address| match address {
        Some(IpAddr::V4(address)) => address.octets()[3] >= 2 && address.octets()[3] < 200,
        Some(IpAddr::V6(_)) | None => false,
    }));

    let old = assignment("old", "dep-old", 0, "node-a", 1, [10, 42, 1, 2]);
    let mut replacement = input(1);
    replacement.current = vec![old];
    replacement.services[0].groups[0].deployment_id = deployment_id("dep-new");
    assert_eq!(
        plan(replacement).assignments[0].spec.workload_address,
        Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 3)))
    );
}

#[test]
fn invalid_and_exhausted_subnets_are_isolated_as_unschedulable() {
    let mut invalid = input(1);
    invalid.nodes.truncate(1);
    invalid.nodes[0].workload_subnet = Some("not-a-cidr".to_owned());
    let invalid = plan(invalid);
    assert!(invalid.assignments.is_empty());
    assert!(matches!(
        invalid.unschedulable[0].reason,
        UnschedulableReason::InvalidWorkloadSubnet { .. }
    ));

    let mut exhausted = input(1);
    exhausted.nodes.truncate(1);
    exhausted.nodes[0].workload_subnet = Some("10.42.1.0/27".to_owned());
    let exhausted = plan(exhausted);
    assert!(exhausted.assignments.is_empty());
    assert!(matches!(
        exhausted.unschedulable[0].reason,
        UnschedulableReason::WorkloadAddressCapacityExhausted { .. }
    ));
}

#[test]
fn runtime_delegated_nodes_schedule_without_preselecting_an_address() {
    let mut delegated = input(1);
    delegated.nodes.truncate(1);
    delegated.nodes[0].workload_network_mode = kernel_api::WorkloadNetworkMode::RuntimeDelegated;
    delegated.nodes[0].workload_subnet = None;

    let planned = plan(delegated);
    assert!(planned.unschedulable.is_empty());
    assert_eq!(planned.assignments.len(), 1);
    assert_eq!(planned.assignments[0].spec.workload_address, None);
}

fn input(replicas: u32) -> ScheduleInput {
    ScheduleInput {
        cluster_id: ClusterId::new("cluster-1").unwrap(),
        services: vec![ServiceSchedule {
            service_id: service_id("web"),
            groups: vec![DeploymentGroup {
                deployment_id: deployment_id("dep-1"),
                restart_generation: Generation(1),
                replicas,
            }],
            placement: PlacementConstraint::default(),
            unhealthy_slots: BTreeSet::new(),
            exhausted_slots: BTreeSet::new(),
        }],
        nodes: vec![
            node("node-b", "10.42.2.0/24"),
            node("node-a", "10.42.1.0/24"),
        ],
        current: Vec::new(),
        held: BTreeSet::new(),
    }
}

fn node(id: &str, subnet: &str) -> ScheduleNode {
    ScheduleNode {
        node_id: node_id(id),
        role: NodeRole::Hybrid,
        labels: BTreeMap::new(),
        workload_network_mode: kernel_api::WorkloadNetworkMode::ClusterRouted,
        workload_subnet: Some(subnet.to_owned()),
        state: NodeSchedulingState::Available,
    }
}

fn assignment(
    id: &str,
    deployment: &str,
    replica_index: u32,
    node: &str,
    placement_epoch: u64,
    address: [u8; 4],
) -> Assignment {
    Assignment {
        meta: ObjectMeta {
            id: AssignmentId::new(id).unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: AssignmentSpec {
            service_id: service_id("web"),
            deployment_id: deployment_id(deployment),
            restart_generation: Generation(1),
            replica_index,
            node_id: node_id(node),
            placement_epoch,
            workload_address: Some(IpAddr::V4(Ipv4Addr::from(address))),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Running,
            workload_id: None,
            workload_address: Some(IpAddr::V4(Ipv4Addr::from(address))),
            conditions: Vec::new(),
        },
    }
}

fn service_id(value: &str) -> ServiceId {
    ServiceId::new(value).unwrap()
}

fn deployment_id(value: &str) -> DeploymentId {
    DeploymentId::new(value).unwrap()
}

fn node_id(value: &str) -> NodeId {
    NodeId::new(value).unwrap()
}
