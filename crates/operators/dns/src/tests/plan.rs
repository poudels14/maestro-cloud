use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    ClusterId, DeploymentId, DeploymentPhase, DnsRecord, ExecPolicy, Generation, NodeApiAccess,
    NodeId, Object, ObjectMeta, PlacementConstraint, ReplicaState, ReplicaStateId,
    ReplicaStateSpec, ReplicaStateStatus, ResourceRevision, RolloutState, Service, ServiceId,
    ServiceSpec, ServiceStatus, Timestamp,
};

use crate::{DnsInput, DnsPlanError, DnsSettings, plan};

#[test]
fn ready_service_generates_stable_and_replica_record_sets() {
    let world = World::ready();
    let output = plan(world.input()).expect("DNS plan");
    assert!(output.replace_records.is_empty());
    assert!(output.delete_records.is_empty());
    insta::assert_json_snapshot!(output.create_records);
}

#[test]
fn record_projection_is_independent_of_assignment_input_order() {
    let world = World::ready();
    let first = plan(world.input()).expect("first plan").create_records;
    let mut reversed = world.input();
    reversed.assignments.reverse();
    reversed.replicas.reverse();
    let second = plan(reversed).expect("second plan").create_records;
    assert_eq!(first, second);
}

#[test]
fn incomplete_readiness_preserves_the_last_published_set() {
    let mut world = World::ready();
    let existing = plan(world.input()).unwrap().create_records;
    world.records = existing;
    world.replicas.pop();
    let output = plan(world.input()).expect("held plan");
    assert_eq!(output, Default::default());
}

#[test]
fn address_change_increments_generation_and_clears_node_acknowledgements() {
    let mut world = World::ready();
    let mut records = plan(world.input()).unwrap().create_records;
    for record in &mut records {
        record.status.applied_generation = record.meta.generation;
        record.status.published_nodes = vec![NodeId::new("node-1").unwrap()];
    }
    world.records = records;
    world.assignments[0].spec.workload_address = IpAddr::V4(Ipv4Addr::new(10, 42, 1, 99));
    let output = plan(world.input()).expect("replacement plan");
    assert_eq!(output.replace_records.len(), 2);
    assert!(
        output
            .replace_records
            .iter()
            .all(|record| record.meta.generation == Generation(2))
    );
    assert!(
        output
            .replace_records
            .iter()
            .all(|record| record.status.published_nodes.is_empty())
    );
}

#[test]
fn zero_replicas_and_service_deletion_collect_managed_records() {
    let mut world = World::ready();
    world.records = plan(world.input()).unwrap().create_records;
    world.service.spec.replicas = 0;
    world.service.status.replica_override = Some(0);
    assert_eq!(plan(world.input()).unwrap().delete_records.len(), 4);

    world.service.meta.deletion_timestamp = Some(Timestamp(50_000));
    assert_eq!(plan(world.input()).unwrap().delete_records.len(), 4);
}

#[test]
fn deterministic_identity_collision_does_not_overwrite_user_records() {
    let mut world = World::ready();
    let mut record = plan(world.input()).unwrap().create_records.remove(0);
    record.meta.annotations.clear();
    record.meta.owner_refs.clear();
    world.records.push(record);
    assert!(matches!(
        plan(world.input()),
        Err(DnsPlanError::RecordIdentityCollision { .. })
    ));
}

#[test]
fn invalid_cluster_labels_and_zero_ttl_fail_closed() {
    let mut input = World::ready().input();
    input.cluster_id = ClusterId::new("Cluster_1").unwrap();
    assert!(matches!(
        plan(input),
        Err(DnsPlanError::InvalidDnsName { .. })
    ));

    let mut input = World::ready().input();
    input.settings.ttl_secs = 0;
    assert_eq!(plan(input), Err(DnsPlanError::ZeroTtl));
}

#[test]
fn ambiguous_current_assignments_fail_closed() {
    let mut world = World::ready();
    let mut duplicate = world.assignments[0].clone();
    duplicate.meta.id = AssignmentId::new("assignment-other").unwrap();
    world.assignments.push(duplicate);
    assert!(matches!(
        plan(world.input()),
        Err(DnsPlanError::AmbiguousAssignment { .. })
    ));
}

struct World {
    service: Service,
    assignments: Vec<Assignment>,
    replicas: Vec<ReplicaState>,
    records: Vec<DnsRecord>,
}

impl World {
    fn ready() -> Self {
        let service_id = ServiceId::new("api").unwrap();
        let deployment_id = DeploymentId::new("deployment-1").unwrap();
        let mut service = Object {
            meta: metadata(service_id.clone()),
            spec: service_spec(),
            status: ServiceStatus {
                active_deployment_id: Some(deployment_id.clone()),
                replica_override: None,
                rollout: RolloutState::Active,
                conditions: Vec::new(),
            },
        };
        service.spec.replicas = 2;
        let assignments = vec![
            assignment(
                &service_id,
                &deployment_id,
                "assignment-0",
                0,
                IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)),
            ),
            assignment(
                &service_id,
                &deployment_id,
                "assignment-1",
                1,
                IpAddr::V6(Ipv6Addr::new(0xfd00, 0x42, 0, 0, 0, 0, 0, 11)),
            ),
        ];
        let replicas = assignments
            .iter()
            .map(|assignment| replica(&service_id, &deployment_id, assignment))
            .collect();
        Self {
            service,
            assignments,
            replicas,
            records: Vec::new(),
        }
    }

    fn input(&self) -> DnsInput {
        DnsInput {
            cluster_id: ClusterId::new("cluster-1").unwrap(),
            settings: DnsSettings { ttl_secs: 5 },
            services: vec![self.service.clone()],
            assignments: self.assignments.clone(),
            replicas: self.replicas.clone(),
            records: self.records.clone(),
        }
    }
}

fn assignment(
    service_id: &ServiceId,
    deployment_id: &DeploymentId,
    id: &str,
    replica_index: u32,
    workload_address: IpAddr,
) -> Assignment {
    Object {
        meta: metadata(AssignmentId::new(id).unwrap()),
        spec: AssignmentSpec {
            service_id: service_id.clone(),
            deployment_id: deployment_id.clone(),
            replica_index,
            node_id: NodeId::new(format!("node-{replica_index}")).unwrap(),
            placement_epoch: 1,
            workload_address,
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Running,
            workload_id: None,
            conditions: Vec::new(),
        },
    }
}

fn replica(
    service_id: &ServiceId,
    deployment_id: &DeploymentId,
    assignment: &Assignment,
) -> ReplicaState {
    Object {
        meta: metadata(
            ReplicaStateId::new(format!("replica-{}", assignment.spec.replica_index)).unwrap(),
        ),
        spec: ReplicaStateSpec {
            service_id: service_id.clone(),
            deployment_id: deployment_id.clone(),
            assignment_id: assignment.meta.id.clone(),
            replica_index: assignment.spec.replica_index,
        },
        status: ReplicaStateStatus {
            phase: DeploymentPhase::Ready,
            node_id: Some(assignment.spec.node_id.clone()),
            workload_id: None,
            healthcheck_failures: 0,
            restart_attempts: 0,
            restart_pending_attempt: None,
            restart_not_before: None,
            conditions: Vec::new(),
        },
    }
}

fn service_spec() -> ServiceSpec {
    ServiceSpec {
        name: "API".to_string(),
        version: "1.0.0".to_string(),
        artifact: ArtifactTemplate::Image {
            reference: "registry.test/api:latest".to_string(),
        },
        command: None,
        replicas: 1,
        exposed_ports: vec![8080],
        health_check: None,
        max_restarts: Some(3),
        environment: BTreeMap::new(),
        user: None,
        node_api: NodeApiAccess::Disabled,
        secrets: None,
        volumes: Vec::new(),
        placement: PlacementConstraint::default(),
        exec: ExecPolicy::Allowed,
    }
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision(5),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}
