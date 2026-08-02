use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    ArtifactTemplate, BuildSource, BuildTemplate, ExecPolicy, Generation, IngressRoute,
    IngressRouteId, IngressRouteSpec, IngressRouteStatus, NodeApiAccess, Object, ObjectMeta,
    PlacementConstraint, Preview, PreviewId, PreviewPhase, PreviewPolicy, PreviewSpec,
    PreviewStatus, ResourceRevision, RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus,
    Timestamp, VolumeAccess, VolumeMountSpec, VolumeSource,
};

pub(super) fn base_service() -> Service {
    Object {
        meta: metadata(ServiceId::new("api").unwrap()),
        spec: ServiceSpec {
            name: "API".to_string(),
            version: "base-v1".to_string(),
            artifact: ArtifactTemplate::Build {
                template: BuildTemplate {
                    source: BuildSource::Git {
                        repository: "https://github.com/acme/api.git".to_string(),
                        revision: "main".to_string(),
                    },
                    dockerfile: "Dockerfile".to_string(),
                    watch: true,
                    registry: None,
                    depot: None,
                    environment: BTreeMap::new(),
                    environment_source: None,
                    secrets: BTreeMap::new(),
                    secrets_source: None,
                },
            },
            preview: Some(PreviewPolicy {
                close_grace_period_secs: 10,
                lifetime_secs: 3_600,
                replicas: 1,
                environment: BTreeMap::from([("PREVIEW".to_string(), "true".to_string())]),
                environment_source: None,
            }),
            command: None,
            replicas: 3,
            exposed_ports: vec![8080],
            health_check: None,
            max_restarts: Some(3),
            environment: BTreeMap::from([("BASE".to_string(), "true".to_string())]),
            environment_sources: Vec::new(),
            user: None,
            node_api: NodeApiAccess::Disabled,
            secrets: None,
            volumes: vec![VolumeMountSpec {
                source: VolumeSource::Managed {
                    name: "base-data".to_string(),
                },
                target: "/data".to_string(),
                access: VolumeAccess::ReadWrite,
            }],
            placement: PlacementConstraint::default(),
            exec: ExecPolicy::Allowed,
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: Some(5),
            rollout: RolloutState::Frozen,
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    }
}

pub(super) fn base_route() -> IngressRoute {
    Object {
        meta: metadata(IngressRouteId::new("api-route").unwrap()),
        spec: IngressRouteSpec {
            service_id: ServiceId::new("api").unwrap(),
            hosts: vec!["api.example.test".to_string()],
            path_prefix: Some("/v1".to_string()),
            target_port: 8080,
            session_affinity: None,
        },
        status: IngressRouteStatus {
            applied_generation: Generation::default(),
            conditions: Vec::new(),
        },
    }
}

pub(super) fn preview() -> Preview {
    Object {
        meta: metadata(PreviewId::new("api-pr-42").unwrap()),
        spec: PreviewSpec {
            base_service_id: ServiceId::new("api").unwrap(),
            repository: "acme/api".to_string(),
            pull_request_number: 42,
            title: "Add pagination".to_string(),
            head_revision: "0123456789abcdef0123456789abcdef01234567".to_string(),
            service_id: ServiceId::new("api-pr-42").unwrap(),
            close_grace_period_secs: 10,
            expires_at: Timestamp(3_600_000),
        },
        status: PreviewStatus {
            phase: PreviewPhase::Pending,
            teardown_at: None,
            conditions: Vec::new(),
        },
    }
}

pub(super) fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}
