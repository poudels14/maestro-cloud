use std::collections::{BTreeMap, BTreeSet};

use cluster::ClusterConfig;
use kernel_api::{
    AnnotationKey, ArtifactTemplate, CommandSpec, ExecPolicy, Generation, HealthCheckSpec,
    HealthProbe, NodeApiAccess, Object, ObjectMeta, PlacementConstraint, ResourceRevision,
    RolloutState, SecretMountSpec, Service, ServiceId, ServiceSpec, ServiceStatus,
    WorkloadUserSpec,
};

pub(crate) const CLOUDFLARE_SERVICE_ID: &str = "maestro-system-cloudflared";
pub(crate) const CLOUDFLARE_MANAGED_OWNER: &str = "cloudflare-tunnel";
pub(crate) const CLOUDFLARE_IMAGE: &str = "cloudflare/cloudflared:2026.7.2@sha256:4f6655284ab3d252b7f28fedb19fe6c8fc82ee5b1295c20ac74d475e5398a52d";
const CLOUDFLARE_VERSION: &str = "cloudflared-2026.7.2";
const MANAGED_ANNOTATION: &str = "system.maestro.dev/owner";
const METRICS_PORT: u16 = 20_241;
const SECRET_DIRECTORY: &str = "/run/secrets/cloudflare";
const TOKEN_FILE: &str = "/run/secrets/cloudflare/token";

/// Ordinary system Service providing optional Cloudflare Tunnel connectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CloudflareSystemResources {
    pub(crate) service: Service,
}

impl CloudflareSystemResources {
    pub(crate) fn from_cluster(
        cluster: &ClusterConfig,
    ) -> Result<Option<Self>, CloudflareResourceError> {
        let Some(config) = &cluster.cloudflare else {
            return Ok(None);
        };
        let service = Object {
            meta: ObjectMeta {
                id: ServiceId::new(CLOUDFLARE_SERVICE_ID)?,
                labels: BTreeMap::new(),
                annotations: BTreeMap::from([(
                    AnnotationKey(MANAGED_ANNOTATION.to_owned()),
                    CLOUDFLARE_MANAGED_OWNER.to_owned(),
                )]),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: ServiceSpec {
                name: "Maestro Cloudflare Tunnel".to_owned(),
                version: CLOUDFLARE_VERSION.to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: CLOUDFLARE_IMAGE.to_owned(),
                },
                preview: None,
                command: Some(CommandSpec {
                    executable: "/usr/local/bin/cloudflared".to_owned(),
                    arguments: vec![
                        "tunnel".to_owned(),
                        "--no-autoupdate".to_owned(),
                        "--metrics".to_owned(),
                        format!("0.0.0.0:{METRICS_PORT}"),
                        "run".to_owned(),
                        "--token-file".to_owned(),
                        TOKEN_FILE.to_owned(),
                    ],
                }),
                replicas: config.replicas,
                exposed_ports: vec![METRICS_PORT],
                health_check: Some(HealthCheckSpec {
                    probe: HealthProbe::Http {
                        port: METRICS_PORT,
                        path: "/ready".to_owned(),
                    },
                    interval_secs: 10,
                    unhealthy_threshold: 3,
                }),
                max_restarts: None,
                environment: BTreeMap::new(),
                // Node secret mounts are root-owned and mode 0600.
                user: Some(WorkloadUserSpec {
                    user_id: 0,
                    group_id: 0,
                }),
                node_api: NodeApiAccess::Disabled,
                secrets: Some(SecretMountSpec::Files {
                    mount_path: SECRET_DIRECTORY.to_owned(),
                    files: BTreeMap::from([("token".to_owned(), config.token.clone())]),
                }),
                volumes: Vec::new(),
                placement: PlacementConstraint::default(),
                exec: ExecPolicy::Denied,
            },
            status: ServiceStatus {
                active_deployment_id: None,
                replica_override: None,
                rollout: RolloutState::Active,
                rollout_bypass_generation: None,
                conditions: Vec::new(),
            },
        };
        service.spec.validate()?;
        Ok(Some(Self { service }))
    }
}

/// Invalid built-in Cloudflare Tunnel resource construction.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CloudflareResourceError {
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    #[error(transparent)]
    Service(#[from] kernel_api::ServiceSpecError),
}
