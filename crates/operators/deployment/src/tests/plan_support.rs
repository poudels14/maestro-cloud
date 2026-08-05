use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    BuildSource, BuildTemplate, ClusterId, Deployment, DeploymentId, DeploymentPhase, ExecPolicy,
    Generation, IngressRouteId, NodeApiAccess, NodeId, Object, ObjectMeta, PlacementConstraint,
    ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceRevision,
    RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp, TrafficGeneration,
    TrafficGenerationId, TrafficGenerationPhase, TrafficGenerationSpec, TrafficGenerationStatus,
};

use crate::{DeploymentInput, LifecycleSettings};

pub(super) fn input(service: Service, deployments: Vec<Deployment>) -> DeploymentInput {
    DeploymentInput {
        cluster_id: ClusterId::new("cluster-1").unwrap(),
        now: Timestamp(40_000),
        settings: LifecycleSettings {
            drain_grace: Duration::from_secs(30),
        },
        services: vec![service],
        ingress_routes: Vec::new(),
        deployments,
        builds: Vec::new(),
        assignments: Vec::new(),
        replicas: Vec::new(),
        traffic_generations: Vec::new(),
    }
}

pub(super) fn service(generation: Generation, rollout: RolloutState) -> Service {
    Object {
        meta: metadata(ServiceId::new("api").unwrap(), generation),
        spec: ServiceSpec {
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
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout,
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    }
}

pub(super) fn build_artifact() -> ArtifactTemplate {
    ArtifactTemplate::Build {
        template: build_template(),
    }
}

pub(super) fn build_template() -> BuildTemplate {
    BuildTemplate {
        source: BuildSource::Git {
            repository: "https://example.test/repo.git".to_string(),
            revision: "main".to_string(),
        },
        dockerfile: "Dockerfile".to_string(),
        watch: false,
        registry: None,
        depot: None,
        environment: BTreeMap::new(),
        environment_source: None,
        secrets: BTreeMap::new(),
        secrets_source: None,
    }
}

pub(super) fn deployment(service: &Service, phase: DeploymentPhase) -> Deployment {
    deployment_generation(service, "deployment-1", service.meta.generation, phase)
}

pub(super) fn deployment_generation(
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
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            service: service.spec.clone(),
            environment_template: Default::default(),
            goal: kernel_api::DeploymentGoal::Run,
            build_id: matches!(service.spec.artifact, ArtifactTemplate::Build { .. })
                .then(|| kernel_api::BuildId::new(format!("build-{id}")).unwrap()),
        },
        status: kernel_api::DeploymentStatus {
            phase,
            created_at: Timestamp(i64::from(generation.0 as u32)),
            ready_at: (phase == DeploymentPhase::Ready).then_some(Timestamp(1_000)),
            draining_at: None,
            image_digest: None,
            git_commit: None,
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    }
}

pub(super) fn assignment(deployment: &Deployment, id: &str, epoch: u64) -> Assignment {
    assignment_slot(deployment, id, 0, epoch)
}

pub(super) fn assignment_slot(
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
            restart_generation: deployment.spec.restart_generation,
            replica_index,
            node_id: NodeId::new("node-1").unwrap(),
            placement_epoch: epoch,
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10))),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Running,
            workload_id: None,
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10))),
            conditions: Vec::new(),
        },
    }
}

pub(super) fn replica(
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
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    }
}

pub(super) fn traffic(deployment: &Deployment) -> TrafficGeneration {
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
                node_id: NodeId::new("node-1").unwrap(),
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

pub(super) fn metadata<Id>(id: Id, generation: Generation) -> ObjectMeta<Id> {
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
