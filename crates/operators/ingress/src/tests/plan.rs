use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    ClusterId, Deployment, DeploymentId, DeploymentPhase, DeploymentSpec, DeploymentStatus,
    ExecPolicy, Generation, IngressBlocklist, IngressBlocklistId, IngressBlocklistSpec,
    IngressBlocklistStatus, IngressRoute, IngressRouteId, IngressRouteSpec, IngressRouteStatus,
    NodeApiAccess, NodeId, Object, ObjectMeta, PlacementConstraint, ReplicaState, ReplicaStateId,
    ReplicaStateSpec, ReplicaStateStatus, ResourceRevision, RolloutState, Service, ServiceId,
    ServiceSpec, ServiceStatus, SessionAffinity, Timestamp, TrafficGeneration,
    TrafficGenerationPhase,
};

use crate::{IngressInput, IngressPlanError, IngressSettings, plan};

#[test]
fn ready_service_stages_one_stable_immutable_generation() {
    let world = World::ready();
    let first = plan(world.input()).expect("first plan");
    let second = plan(world.input()).expect("second plan");
    assert_eq!(first.create_generations, second.create_generations);
    let generation = first.create_generations.first().expect("generation");
    assert_eq!(generation.status.phase, TrafficGenerationPhase::Staged);
    assert_eq!(generation.spec.epoch, 1);
    assert_eq!(generation.spec.routes.len(), 1);
    assert_eq!(generation.spec.targets.len(), 1);
    assert_eq!(generation.spec.targets[0].endpoint.port(), 8080);
    assert!(first.backend_changes[0].active.is_none());
}

#[test]
fn staged_generation_activates_and_acknowledges_routes_on_the_next_pass() {
    let mut world = World::ready();
    let mut staged = plan(world.input()).expect("stage").create_generations[0].clone();
    staged.meta.revision = ResourceRevision(11);
    world.generations.push(staged.clone());

    let activated = plan(world.input()).expect("activate");
    assert!(activated.create_generations.is_empty());
    assert_eq!(activated.generation_updates.len(), 1);
    assert_eq!(
        activated.generation_updates[0].status.phase,
        TrafficGenerationPhase::Active
    );
    assert_eq!(
        activated.generation_updates[0].status.activated_at,
        Some(Timestamp(40_000))
    );
    assert_eq!(activated.route_updates.len(), 1);
    assert_eq!(
        activated.backend_changes[0]
            .active
            .as_ref()
            .expect("published generation")
            .generation_id,
        staged.meta.id
    );
}

#[test]
fn target_change_stages_before_retiring_the_active_generation() {
    let mut world = World::ready();
    let mut active = plan(world.input()).expect("stage old").create_generations[0].clone();
    active.status.phase = TrafficGenerationPhase::Active;
    active.status.activated_at = Some(Timestamp(1_000));
    active.meta.revision = ResourceRevision(7);
    world.generations.push(active.clone());
    world.assignments[0].meta.id = AssignmentId::new("assignment-new").unwrap();
    world.assignments[0].spec.placement_epoch = 2;
    world.replicas[0].meta.id = ReplicaStateId::new("replica-new").unwrap();
    world.replicas[0].spec.assignment_id = world.assignments[0].meta.id.clone();

    let staged = plan(world.input()).expect("stage replacement");
    assert_eq!(staged.create_generations.len(), 1);
    assert_eq!(staged.create_generations[0].spec.epoch, 2);
    assert!(staged.generation_updates.is_empty());
    assert_eq!(
        staged.backend_changes[0]
            .active
            .as_ref()
            .expect("old active")
            .generation_id,
        active.meta.id
    );

    let replacement = staged.create_generations[0].clone();
    world.generations.push(replacement.clone());
    let cutover = plan(world.input()).expect("cutover replacement");
    assert_eq!(cutover.generation_updates.len(), 2);
    assert_eq!(
        update_phase(&cutover, &active.meta.id),
        TrafficGenerationPhase::Retired
    );
    assert_eq!(
        update_phase(&cutover, &replacement.meta.id),
        TrafficGenerationPhase::Active
    );
    assert_eq!(
        cutover.backend_changes[0]
            .active
            .as_ref()
            .expect("new active")
            .generation_id,
        replacement.meta.id
    );
}

#[test]
fn temporarily_unready_targets_preserve_the_last_active_generation() {
    let mut world = World::ready();
    let mut active = plan(world.input()).expect("stage").create_generations[0].clone();
    active.status.phase = TrafficGenerationPhase::Active;
    active.status.activated_at = Some(Timestamp(1_000));
    world.generations.push(active.clone());
    world.replicas[0].status.phase = DeploymentPhase::PendingReady;

    let held = plan(world.input()).expect("hold active");
    assert!(held.create_generations.is_empty());
    assert!(held.generation_updates.is_empty());
    assert_eq!(
        held.backend_changes[0]
            .active
            .as_ref()
            .expect("active generation")
            .generation_id,
        active.meta.id
    );
}

#[test]
fn runtime_delegated_assignment_waits_for_an_observed_address() {
    let mut world = World::ready();
    world.assignments[0].spec.workload_address = None;
    world.assignments[0].status.workload_address = None;

    let held = plan(world.input()).expect("hold addressless assignment");
    assert!(held.create_generations.is_empty());
    assert!(held.backend_changes[0].active.is_none());
}

#[test]
fn service_without_active_deployment_retires_serving_generation() {
    let mut world = World::ready();
    let mut active = plan(world.input()).expect("stage").create_generations[0].clone();
    active.status.phase = TrafficGenerationPhase::Active;
    active.status.activated_at = Some(Timestamp(1_000));
    world.generations.push(active.clone());
    world.service.status.active_deployment_id = None;

    let retiring = plan(world.input()).expect("retire inactive service");
    assert!(retiring.backend_changes[0].active.is_none());
    assert_eq!(
        update_phase(&retiring, &active.meta.id),
        TrafficGenerationPhase::Retired
    );
}

#[test]
fn no_external_routes_still_acknowledges_the_active_deployment() {
    let mut world = World::ready();
    world.routes.clear();
    world.assignments.clear();
    world.replicas.clear();
    let staged = plan(world.input()).expect("route-free generation");
    assert_eq!(staged.create_generations.len(), 1);
    assert!(staged.create_generations[0].spec.routes.is_empty());
    assert!(staged.create_generations[0].spec.targets.is_empty());
}

#[test]
fn retired_generation_is_collected_at_the_exact_grace_boundary() {
    let mut world = World::ready();
    let mut retired = plan(world.input()).expect("stage").create_generations[0].clone();
    retired.status.phase = TrafficGenerationPhase::Retired;
    retired.status.activated_at = Some(Timestamp(1_000));
    retired.status.retired_at = Some(Timestamp(10_000));
    world.generations.push(retired.clone());
    world.service.status.active_deployment_id = None;
    world.now = Timestamp(39_999);
    assert!(
        plan(world.input())
            .expect("before grace")
            .delete_generations
            .is_empty()
    );

    world.now = Timestamp(40_000);
    let removed = plan(world.input()).expect("at grace");
    assert_eq!(removed.delete_generations, vec![retired.meta.id.clone()]);
    assert_eq!(removed.backend_changes[0].remove, vec![retired.meta.id]);
}

#[test]
fn deleting_service_retires_then_collects_active_traffic() {
    let mut world = World::ready();
    let mut active = plan(world.input()).expect("stage").create_generations[0].clone();
    active.status.phase = TrafficGenerationPhase::Active;
    active.status.activated_at = Some(Timestamp(1_000));
    world.generations.push(active.clone());
    world.service.meta.deletion_timestamp = Some(Timestamp(40_000));

    let retiring = plan(world.input()).expect("retire");
    assert!(retiring.backend_changes[0].active.is_none());
    assert_eq!(
        retiring.generation_updates[0].status.phase,
        TrafficGenerationPhase::Retired
    );
    assert_eq!(
        retiring.generation_updates[0].status.retired_at,
        Some(Timestamp(40_000))
    );
    assert_eq!(retiring.requeue_at, Some(Timestamp(70_000)));

    active.status = retiring.generation_updates[0].status.clone();
    world.generations = vec![active.clone()];
    world.now = Timestamp(70_000);
    assert_eq!(
        plan(world.input()).expect("collect").delete_generations,
        vec![active.meta.id]
    );
}

#[test]
fn retired_matching_configuration_uses_a_new_epoch_and_identity() {
    let mut world = World::ready();
    let mut retired = plan(world.input()).expect("stage").create_generations[0].clone();
    retired.status.phase = TrafficGenerationPhase::Retired;
    retired.status.activated_at = Some(Timestamp(1_000));
    retired.status.retired_at = Some(Timestamp(20_000));
    world.generations.push(retired.clone());
    let repeated = plan(world.input()).expect("restage").create_generations[0].clone();
    assert_eq!(repeated.spec.epoch, 2);
    assert_ne!(repeated.meta.id, retired.meta.id);
}

#[test]
fn malformed_and_ambiguous_routes_fail_closed() {
    let mut invalid_host = World::ready();
    invalid_host.routes[0].spec.hosts = vec!["HTTPS://API.example.test".to_string()];
    assert!(matches!(
        plan(invalid_host.input()),
        Err(IngressPlanError::InvalidRouteHost { .. })
    ));

    let mut unexposed = World::ready();
    unexposed.routes[0].spec.target_port = 9090;
    assert!(matches!(
        plan(unexposed.input()),
        Err(IngressPlanError::UnexposedTargetPort { .. })
    ));

    let mut conflict = World::ready();
    let mut duplicate = conflict.routes[0].clone();
    duplicate.meta.id = IngressRouteId::new("route-duplicate").unwrap();
    conflict.routes.push(duplicate);
    assert!(matches!(
        plan(conflict.input()),
        Err(IngressPlanError::ConflictingRoute { .. })
    ));
}

#[test]
fn blocklist_plan_is_stable_and_acknowledges_the_exact_generation() {
    let mut world = World::ready();
    world.blocklists.push(blocklist(vec![
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 9)),
    ]));

    let planned = plan(world.input()).expect("blocklist plan");
    let change = planned.blocklist_change.as_ref().expect("publication");
    assert_eq!(change.generation, Generation(2));
    assert_eq!(change.addresses, world.blocklists[0].spec.addresses);
    assert_eq!(planned.blocklist_updates.len(), 1);
    assert_eq!(
        planned.blocklist_updates[0].status.configuration_digest,
        Some(change.configuration_digest.clone())
    );

    world.blocklists[0].status = planned.blocklist_updates[0].status.clone();
    let stable = plan(world.input()).expect("stable blocklist plan");
    assert!(stable.blocklist_change.is_none());
    assert!(stable.blocklist_updates.is_empty());
}

#[test]
fn blocklist_plan_rejects_non_singleton_and_noncanonical_state() {
    let mut duplicate = World::ready();
    duplicate.blocklists.push(blocklist(vec![
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
    ]));
    assert!(matches!(
        plan(duplicate.input()),
        Err(IngressPlanError::NonCanonicalBlocklist)
    ));

    let mut wrong_id = World::ready();
    let mut blocklist = blocklist(Vec::new());
    blocklist.meta.id = IngressBlocklistId::new("other").unwrap();
    wrong_id.blocklists.push(blocklist);
    assert!(matches!(
        plan(wrong_id.input()),
        Err(IngressPlanError::UnexpectedBlocklistId { .. })
    ));
}

fn update_phase(
    plan: &crate::IngressPlan,
    id: &kernel_api::TrafficGenerationId,
) -> TrafficGenerationPhase {
    plan.generation_updates
        .iter()
        .find(|update| &update.id == id)
        .expect("generation update")
        .status
        .phase
}

pub(super) struct World {
    now: Timestamp,
    pub(super) service: Service,
    pub(super) deployment: Deployment,
    pub(super) routes: Vec<IngressRoute>,
    pub(super) assignments: Vec<Assignment>,
    pub(super) replicas: Vec<ReplicaState>,
    pub(super) generations: Vec<TrafficGeneration>,
    pub(super) blocklists: Vec<kernel_api::IngressBlocklist>,
}

impl World {
    pub(super) fn ready() -> Self {
        let service_id = ServiceId::new("api").unwrap();
        let deployment_id = DeploymentId::new("deployment-1").unwrap();
        let assignment_id = AssignmentId::new("assignment-1").unwrap();
        let service = Object {
            meta: metadata(service_id.clone(), Generation(1)),
            spec: service_spec(),
            status: ServiceStatus {
                active_deployment_id: Some(deployment_id.clone()),
                replica_override: None,
                rollout: RolloutState::Active,
                rollout_bypass_generation: None,
                conditions: Vec::new(),
            },
        };
        let deployment = Object {
            meta: metadata(deployment_id.clone(), Generation(1)),
            spec: DeploymentSpec {
                service_id: service_id.clone(),
                service_generation: Generation(1),
                restart_generation: Generation(1),
                bypass_rollout_freeze: false,
                service: service.spec.clone(),
                goal: kernel_api::DeploymentGoal::Run,
                build_id: None,
            },
            status: DeploymentStatus {
                phase: DeploymentPhase::Ready,
                created_at: Timestamp(1_000),
                ready_at: Some(Timestamp(2_000)),
                draining_at: None,
                image_digest: None,
                git_commit: None,
                conditions: Vec::new(),
            },
        };
        let route = Object {
            meta: metadata(IngressRouteId::new("route-api").unwrap(), Generation(3)),
            spec: IngressRouteSpec {
                service_id: service_id.clone(),
                hosts: vec!["api.example.test".to_string()],
                path_prefix: Some("/v1".to_string()),
                target_port: 8080,
                session_affinity: Some(SessionAffinity {
                    header: "X-Maestro-Affinity".to_string(),
                }),
            },
            status: IngressRouteStatus {
                applied_generation: Generation(0),
                conditions: Vec::new(),
            },
        };
        let assignment = Object {
            meta: metadata(assignment_id.clone(), Generation(1)),
            spec: AssignmentSpec {
                service_id: service_id.clone(),
                deployment_id: deployment_id.clone(),
                restart_generation: Generation(1),
                replica_index: 0,
                node_id: NodeId::new("node-1").unwrap(),
                placement_epoch: 1,
                workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10))),
                replaces_assignment_id: None,
            },
            status: AssignmentStatus {
                phase: AssignmentPhase::Running,
                workload_id: None,
                workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10))),
                conditions: Vec::new(),
            },
        };
        let replica = Object {
            meta: metadata(ReplicaStateId::new("replica-1").unwrap(), Generation(1)),
            spec: ReplicaStateSpec {
                service_id,
                deployment_id,
                assignment_id,
                replica_index: 0,
            },
            status: ReplicaStateStatus {
                phase: DeploymentPhase::Ready,
                node_id: Some(NodeId::new("node-1").unwrap()),
                workload_id: None,
                healthcheck_failures: 0,
                restart_attempts: 0,
                restart_pending_attempt: None,
                restart_not_before: None,
                conditions: Vec::new(),
            },
        };
        Self {
            now: Timestamp(40_000),
            service,
            deployment,
            routes: vec![route],
            assignments: vec![assignment],
            replicas: vec![replica],
            generations: Vec::new(),
            blocklists: Vec::new(),
        }
    }

    pub(super) fn input(&self) -> IngressInput {
        IngressInput {
            cluster_id: ClusterId::new("cluster-1").unwrap(),
            now: self.now,
            settings: IngressSettings {
                retirement_grace: Duration::from_secs(30),
            },
            services: vec![self.service.clone()],
            deployments: vec![self.deployment.clone()],
            routes: self.routes.clone(),
            assignments: self.assignments.clone(),
            replicas: self.replicas.clone(),
            traffic_generations: self.generations.clone(),
            blocklists: self.blocklists.clone(),
        }
    }
}

fn service_spec() -> ServiceSpec {
    ServiceSpec {
        name: "API".to_string(),
        version: "1.0.0".to_string(),
        artifact: ArtifactTemplate::Image {
            reference: "registry.test/api:latest".to_string(),
        },
        preview: None,
        command: None,
        replicas: 1,
        exposed_ports: vec![8080],
        health_check: None,
        max_restarts: Some(3),
        environment: BTreeMap::new(),
        environment_sources: Vec::new(),
        user: None,
        node_api: NodeApiAccess::Disabled,
        secrets: None,
        volumes: Vec::new(),
        placement: PlacementConstraint::default(),
        exec: ExecPolicy::Allowed,
    }
}

fn blocklist(addresses: Vec<IpAddr>) -> IngressBlocklist {
    Object {
        meta: metadata(IngressBlocklistId::new("global").unwrap(), Generation(2)),
        spec: IngressBlocklistSpec { addresses },
        status: IngressBlocklistStatus {
            applied_generation: Generation::default(),
            configuration_digest: None,
            conditions: Vec::new(),
        },
    }
}

fn metadata<Id>(id: Id, generation: Generation) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision(5),
        generation,
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}
