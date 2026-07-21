use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    Build, BuildPhase, BuildSource, BuildStatus, BuildTemplate, ClusterId, Deployment,
    DeploymentId, DeploymentPhase, ExecPolicy, Generation, IngressRouteId, NodeApiAccess, NodeId,
    Object, ObjectMeta, PlacementConstraint, ReplicaState, ReplicaStateId, ReplicaStateSpec,
    ReplicaStateStatus, ResourceRevision, RolloutState, Service, ServiceId, ServiceSpec,
    ServiceStatus, Timestamp, TrafficGeneration, TrafficGenerationId, TrafficGenerationPhase,
    TrafficGenerationSpec, TrafficGenerationStatus,
};

use crate::{DeploymentInput, LifecycleSettings, plan};

#[test]
fn creates_one_stable_deployment_per_service_generation() {
    let input = input(service(Generation(7), RolloutState::Active), Vec::new());
    let first = plan(input.clone()).expect("first plan");
    let second = plan(input).expect("second plan");
    assert_eq!(first.create_deployments, second.create_deployments);
    let deployment = first.create_deployments.first().expect("deployment");
    assert_eq!(deployment.spec.service_generation, Generation(7));
    assert_eq!(deployment.status.phase, DeploymentPhase::Queued);
    assert!(deployment.spec.build_id.is_none());
}

#[test]
fn frozen_build_stays_queued_then_unfreeze_creates_its_build() {
    let mut frozen = service(Generation(1), RolloutState::Frozen);
    frozen.spec.artifact = build_artifact();
    let created = plan(input(frozen.clone(), Vec::new())).expect("create deployment");
    let deployment = created.create_deployments[0].clone();
    assert!(deployment.spec.build_id.is_some());

    let frozen_plan = plan(input(frozen.clone(), vec![deployment.clone()])).expect("frozen plan");
    assert!(frozen_plan.deployment_updates.is_empty());
    assert!(frozen_plan.create_builds.is_empty());

    frozen.status.rollout = RolloutState::Active;
    let active = plan(input(frozen, vec![deployment])).expect("active plan");
    assert_eq!(
        active.deployment_updates[0].status.phase,
        DeploymentPhase::Building
    );
    assert_eq!(active.create_builds.len(), 1);
}

#[test]
fn successful_build_publishes_digest_without_skipping_assignment_readiness() {
    let mut svc = service(Generation(1), RolloutState::Active);
    svc.spec.artifact = build_artifact();
    let mut deployment = deployment(&svc, DeploymentPhase::Building);
    let mut build = Build {
        meta: metadata(
            deployment.spec.build_id.clone().expect("build id"),
            Generation(1),
        ),
        spec: kernel_api::BuildSpec {
            service_id: svc.meta.id.clone(),
            deployment_id: deployment.meta.id.clone(),
            template: build_template(),
        },
        status: BuildStatus {
            phase: BuildPhase::Succeeded,
            image_digest: Some("registry.test/api@sha256:abc".to_string()),
            source_revision: Some("abc".to_string()),
            conditions: Vec::new(),
        },
    };
    let mut snapshot = input(svc, vec![deployment.clone()]);
    snapshot.builds = vec![build.clone()];
    let waiting = plan(snapshot).expect("build output plan");
    assert_eq!(
        waiting.deployment_updates[0].status.image_digest.as_deref(),
        Some("registry.test/api@sha256:abc")
    );
    assert_eq!(
        waiting.deployment_updates[0].status.phase,
        DeploymentPhase::Building
    );

    deployment.status = waiting.deployment_updates[0].status.clone();
    build.status.phase = BuildPhase::Succeeded;
    let assigned = assignment(&deployment, "assignment-1", 1);
    let mut snapshot = input(
        service(Generation(1), RolloutState::Active),
        vec![deployment.clone()],
    );
    snapshot.services[0].spec.artifact = build_artifact();
    snapshot.assignments = vec![assigned.clone()];
    snapshot.replicas = vec![replica(&deployment, &assigned, DeploymentPhase::Ready, 0)];
    snapshot.builds = vec![build];
    assert_eq!(
        plan(snapshot).expect("ready build plan").deployment_updates[0]
            .status
            .phase,
        DeploymentPhase::Ready
    );
}

#[test]
fn canceled_deployment_remains_in_history_until_service_deletion() {
    let service = service(Generation(1), RolloutState::Active);
    let canceled = deployment(&service, DeploymentPhase::Canceled);
    assert!(
        plan(input(service, vec![canceled]))
            .expect("canceled history")
            .deployment_updates
            .is_empty()
    );
}

#[test]
fn readiness_requires_the_exact_current_assignment() {
    let service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Building);
    let current = assignment(&deployment, "assignment-current", 2);
    let stale = assignment(&deployment, "assignment-stale", 1);
    let mut snapshot = input(service.clone(), vec![deployment.clone()]);
    snapshot.assignments = vec![stale.clone(), current.clone()];
    snapshot.replicas = vec![replica(&deployment, &stale, DeploymentPhase::Ready, 0)];
    let pending = plan(snapshot).expect("pending plan");
    assert_eq!(
        pending.deployment_updates[0].status.phase,
        DeploymentPhase::PendingReady
    );

    deployment.status.phase = DeploymentPhase::PendingReady;
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.assignments = vec![stale, current.clone()];
    snapshot.replicas = vec![replica(&deployment, &current, DeploymentPhase::Ready, 0)];
    let ready = plan(snapshot).expect("ready plan");
    assert_eq!(
        ready.deployment_updates[0].status.phase,
        DeploymentPhase::Ready
    );
    assert_eq!(
        ready.deployment_updates[0].status.ready_at,
        Some(Timestamp(40_000))
    );
}

#[test]
fn every_current_slot_must_exhaust_its_configured_restart_budget() {
    let mut service = service(Generation(1), RolloutState::Active);
    service.spec.replicas = 2;
    service.spec.max_restarts = Some(3);
    let deployment = deployment(&service, DeploymentPhase::PendingReady);
    let first = assignment_slot(&deployment, "assignment-0", 0, 1);
    let second = assignment_slot(&deployment, "assignment-1", 1, 1);
    let mut snapshot = input(service.clone(), vec![deployment.clone()]);
    snapshot.assignments = vec![first.clone(), second.clone()];
    snapshot.replicas = vec![
        replica(&deployment, &first, DeploymentPhase::Crashed, 3),
        replica(&deployment, &second, DeploymentPhase::Crashed, 2),
    ];
    assert!(
        plan(snapshot)
            .expect("not exhausted")
            .deployment_updates
            .is_empty()
    );

    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.assignments = vec![first.clone(), second.clone()];
    snapshot.replicas = vec![
        replica(&deployment, &first, DeploymentPhase::Crashed, 3),
        replica(&deployment, &second, DeploymentPhase::Crashed, 3),
    ];
    assert_eq!(
        plan(snapshot).expect("exhausted").deployment_updates[0]
            .status
            .phase,
        DeploymentPhase::Crashed
    );
}

#[test]
fn active_traffic_acknowledgement_drains_only_superseded_deployments() {
    let mut service = service(Generation(2), RolloutState::Active);
    let old = deployment_generation(
        &service,
        "deployment-old",
        Generation(1),
        DeploymentPhase::Ready,
    );
    let incoming = deployment_generation(
        &service,
        "deployment-new",
        Generation(2),
        DeploymentPhase::Ready,
    );
    service.status.active_deployment_id = Some(old.meta.id.clone());
    let mut first = input(service.clone(), vec![old.clone(), incoming.clone()]);
    first.traffic_generations = Vec::new();
    let activated = plan(first).expect("activate plan");
    assert_eq!(
        activated.service_updates[0].status.active_deployment_id,
        Some(incoming.meta.id.clone())
    );
    assert!(activated.deployment_updates.is_empty());

    service.status.active_deployment_id = Some(incoming.meta.id.clone());
    let mut acknowledged = input(service, vec![old.clone(), incoming.clone()]);
    acknowledged.traffic_generations = vec![traffic(&incoming)];
    let drained = plan(acknowledged).expect("drain plan");
    assert_eq!(drained.deployment_updates.len(), 1);
    assert_eq!(drained.deployment_updates[0].id, old.meta.id);
    assert_eq!(
        drained.deployment_updates[0].status.phase,
        DeploymentPhase::Draining
    );
    assert_eq!(
        drained.deployment_updates[0].status.draining_at,
        Some(Timestamp(40_000))
    );
}

#[test]
fn draining_waits_for_grace_and_assignment_removal() {
    let service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Draining);
    deployment.status.draining_at = Some(Timestamp(10_000));
    let assignment = assignment(&deployment, "assignment-1", 1);
    let mut held = input(service.clone(), vec![deployment.clone()]);
    held.assignments = vec![assignment];
    assert!(plan(held).expect("held").deployment_updates.is_empty());

    let removed = plan(input(service, vec![deployment])).expect("removed");
    assert_eq!(
        removed.deployment_updates[0].status.phase,
        DeploymentPhase::Removed
    );
}

#[test]
fn deleting_service_cancels_queued_and_drains_serving_deployments() {
    let mut service = service(Generation(2), RolloutState::Active);
    service.meta.deletion_timestamp = Some(Timestamp(39_000));
    let queued = deployment_generation(
        &service,
        "deployment-queued",
        Generation(2),
        DeploymentPhase::Queued,
    );
    let ready = deployment_generation(
        &service,
        "deployment-ready",
        Generation(1),
        DeploymentPhase::Ready,
    );
    let result = plan(input(service, vec![queued.clone(), ready.clone()])).expect("delete plan");
    assert_eq!(result.create_deployments.len(), 0);
    assert_eq!(result.deployment_updates.len(), 2);
    assert_eq!(
        result
            .deployment_updates
            .iter()
            .find(|update| update.id == queued.meta.id)
            .expect("queued update")
            .status
            .phase,
        DeploymentPhase::Canceled
    );
    assert_eq!(
        result
            .deployment_updates
            .iter()
            .find(|update| update.id == ready.meta.id)
            .expect("ready update")
            .status
            .phase,
        DeploymentPhase::Draining
    );
}

#[test]
fn removed_children_are_collected_before_service_finalization() {
    let mut service = service(Generation(1), RolloutState::Active);
    service.spec.artifact = build_artifact();
    let created = plan(input(service.clone(), Vec::new())).expect("deployment creation");
    let mut deployment = created.create_deployments[0].clone();
    let building = plan(input(service.clone(), vec![deployment.clone()])).expect("build creation");
    let build = building.create_builds[0].clone();
    deployment.status.phase = DeploymentPhase::Removed;
    service.meta.deletion_timestamp = Some(Timestamp(40_000));
    let assignment = assignment(&deployment, "assignment-old", 1);
    let replica = replica(&deployment, &assignment, DeploymentPhase::Ready, 0);
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.builds = vec![build.clone()];
    snapshot.replicas = vec![replica.clone()];

    let collected = plan(snapshot).expect("child collection");
    assert_eq!(collected.delete_deployments, vec![deployment.meta.id]);
    assert_eq!(collected.delete_builds, vec![build.meta.id]);
    assert_eq!(collected.delete_replicas, vec![replica.meta.id]);
    assert!(collected.deployment_updates.is_empty());
}

fn input(service: Service, deployments: Vec<Deployment>) -> DeploymentInput {
    DeploymentInput {
        cluster_id: ClusterId::new("cluster-1").unwrap(),
        now: Timestamp(40_000),
        settings: LifecycleSettings {
            drain_grace: Duration::from_secs(30),
        },
        services: vec![service],
        deployments,
        builds: Vec::new(),
        assignments: Vec::new(),
        replicas: Vec::new(),
        traffic_generations: Vec::new(),
    }
}

fn service(generation: Generation, rollout: RolloutState) -> Service {
    Object {
        meta: metadata(ServiceId::new("api").unwrap(), generation),
        spec: ServiceSpec {
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
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout,
            conditions: Vec::new(),
        },
    }
}

fn build_artifact() -> ArtifactTemplate {
    ArtifactTemplate::Build {
        template: build_template(),
    }
}

fn build_template() -> BuildTemplate {
    BuildTemplate {
        source: BuildSource::Git {
            repository: "https://example.test/repo.git".to_string(),
            revision: "main".to_string(),
        },
        dockerfile: "Dockerfile".to_string(),
        environment: BTreeMap::new(),
        secrets: BTreeMap::new(),
    }
}

fn deployment(service: &Service, phase: DeploymentPhase) -> Deployment {
    deployment_generation(service, "deployment-1", service.meta.generation, phase)
}

fn deployment_generation(
    service: &Service,
    id: &str,
    generation: Generation,
    phase: DeploymentPhase,
) -> Deployment {
    Object {
        meta: metadata(DeploymentId::new(id).unwrap(), Generation(1)),
        spec: kernel_api::DeploymentSpec {
            service_id: service.meta.id.clone(),
            service_generation: generation,
            service: service.spec.clone(),
            build_id: matches!(service.spec.artifact, ArtifactTemplate::Build { .. })
                .then(|| kernel_api::BuildId::new(format!("build-{id}")).unwrap()),
        },
        status: kernel_api::DeploymentStatus {
            phase,
            created_at: Timestamp(i64::from(generation.0 as u32)),
            ready_at: (phase == DeploymentPhase::Ready).then_some(Timestamp(1_000)),
            draining_at: None,
            image_digest: None,
            conditions: Vec::new(),
        },
    }
}

pub(super) fn assignment(deployment: &Deployment, id: &str, epoch: u64) -> Assignment {
    assignment_slot(deployment, id, 0, epoch)
}

fn assignment_slot(
    deployment: &Deployment,
    id: &str,
    replica_index: u32,
    epoch: u64,
) -> Assignment {
    Object {
        meta: metadata(AssignmentId::new(id).unwrap(), Generation(1)),
        spec: AssignmentSpec {
            service_id: deployment.spec.service_id.clone(),
            deployment_id: deployment.meta.id.clone(),
            replica_index,
            node_id: NodeId::new("node-1").unwrap(),
            placement_epoch: epoch,
            workload_address: IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)),
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
    deployment: &Deployment,
    assignment: &Assignment,
    phase: DeploymentPhase,
    restart_attempts: u32,
) -> ReplicaState {
    Object {
        meta: metadata(
            ReplicaStateId::new(format!("replica-{}", assignment.meta.id)).unwrap(),
            Generation(1),
        ),
        spec: ReplicaStateSpec {
            service_id: deployment.spec.service_id.clone(),
            deployment_id: deployment.meta.id.clone(),
            assignment_id: assignment.meta.id.clone(),
            replica_index: assignment.spec.replica_index,
        },
        status: ReplicaStateStatus {
            phase,
            node_id: Some(assignment.spec.node_id.clone()),
            workload_id: None,
            healthcheck_failures: 0,
            restart_attempts,
            restart_pending_attempt: None,
            restart_not_before: None,
            conditions: Vec::new(),
        },
    }
}

fn traffic(deployment: &Deployment) -> TrafficGeneration {
    Object {
        meta: metadata(
            TrafficGenerationId::new("traffic-1").unwrap(),
            Generation(1),
        ),
        spec: TrafficGenerationSpec {
            service_id: deployment.spec.service_id.clone(),
            deployment_id: deployment.meta.id.clone(),
            epoch: 1,
            routes: vec![kernel_api::TrafficRoute {
                route_id: IngressRouteId::new("route-1").unwrap(),
                route_generation: Generation(1),
                hosts: vec!["api.example.test".to_string()],
                path_prefix: None,
                target_port: 8080,
                session_affinity: None,
            }],
            targets: vec![kernel_api::TrafficTarget {
                assignment_id: AssignmentId::new("assignment-new").unwrap(),
                endpoint: SocketAddr::from(([10, 42, 1, 10], 8080)),
            }],
        },
        status: TrafficGenerationStatus {
            phase: TrafficGenerationPhase::Active,
            staged_at: Timestamp(900),
            activated_at: Some(Timestamp(1_000)),
            retired_at: None,
            conditions: Vec::new(),
        },
    }
}

fn metadata<Id>(id: Id, generation: Generation) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision(7),
        generation,
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}

#[allow(dead_code)]
fn succeeded(build: &mut Build) {
    build.status = BuildStatus {
        phase: BuildPhase::Succeeded,
        image_digest: Some("registry.test/api@sha256:abc".to_string()),
        source_revision: Some("abc".to_string()),
        conditions: Vec::new(),
    };
}
