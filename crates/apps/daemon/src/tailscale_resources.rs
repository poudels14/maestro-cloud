use std::collections::{BTreeMap, BTreeSet};

use cluster::ClusterConfig;
use kernel_api::{
    AnnotationKey, ArtifactTemplate, CommandSpec, ExecPolicy, FirewallDirection, FirewallPolicy,
    FirewallPolicyId, FirewallPolicySpec, FirewallPolicyStatus, FirewallRule, FirewallSubject,
    FirewallVerdict, Generation, HealthCheckSpec, HealthProbe, NodeApiAccess, Object, ObjectMeta,
    OwnerReference, Ownership, PlacementConstraint, ResourceId, ResourceKind, ResourceName,
    ResourceRevision, RolloutState, SecretMountSpec, SecretValue, Service, ServiceId, ServiceSpec,
    ServiceStatus, TransportProtocol, VolumeAccess, VolumeMountSpec, VolumeSource,
};

const GATEWAY_SERVICE_ID: &str = "maestro-system-tailscale-gateway";
const GATEWAY_POLICY_ID: &str = "maestro-system-tailscale-egress";
const MANAGED_ANNOTATION: &str = "system.maestro.dev/owner";
const MANAGED_VALUE: &str = "tailscale-gateway";
pub(crate) const TAILSCALE_IMAGE: &str = "ghcr.io/tailscale/tailscale:v1.98.8@sha256:d54b2e6a9c09f0e5ec52e82b9ad4af3d446b54a7c08075e92f11c39dd410105f";
const TAILSCALE_VERSION: &str = "tailscale-1.98.8";
pub(crate) const AUTH_SCRIPT: &str = "if [ ! -s /state/tailscaled.state ]; then\n  set -a\n  . /run/secrets/tailscale.env\n  set +a\nfi\nexec /usr/local/bin/containerboot";

/// Ordinary resources that provide optional operator access through Tailscale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TailscaleSystemResources {
    /// Highly available subnet-router service.
    pub(crate) service: Service,
    /// Workload egress policy attached to the subnet routers.
    pub(crate) firewall_policy: FirewallPolicy,
}

impl TailscaleSystemResources {
    pub(crate) fn from_cluster(
        cluster: &ClusterConfig,
    ) -> Result<Option<Self>, TailscaleResourceError> {
        let Some(config) = &cluster.tailscale else {
            return Ok(None);
        };
        let service_id = ServiceId::new(GATEWAY_SERVICE_ID)?;
        let routes = config
            .advertised_routes(cluster.cluster_cidr)
            .into_iter()
            .map(|route| route.to_string())
            .collect::<Vec<_>>();
        let environment = BTreeMap::from([
            ("TS_ACCEPT_DNS".to_owned(), "false".to_owned()),
            ("TS_AUTH_ONCE".to_owned(), "true".to_owned()),
            ("TS_ENABLE_HEALTH_CHECK".to_owned(), "true".to_owned()),
            (
                "TS_EXTRA_ARGS".to_owned(),
                format!("--advertise-tags={}", config.tags.join(",")),
            ),
            ("TS_KUBE_SECRET".to_owned(), String::new()),
            ("TS_LOCAL_ADDR_PORT".to_owned(), ":9002".to_owned()),
            ("TS_ROUTES".to_owned(), routes.join(",")),
            ("TS_STATE_DIR".to_owned(), "/state".to_owned()),
            ("TS_USERSPACE".to_owned(), "true".to_owned()),
        ]);
        let annotations = BTreeMap::from([(managed_annotation(), MANAGED_VALUE.to_owned())]);
        let service = Object {
            meta: ObjectMeta {
                id: service_id.clone(),
                labels: BTreeMap::new(),
                annotations: annotations.clone(),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: ServiceSpec {
                name: "Maestro Tailscale Gateway".to_owned(),
                version: TAILSCALE_VERSION.to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: TAILSCALE_IMAGE.to_owned(),
                },
                preview: None,
                command: Some(CommandSpec {
                    executable: "/bin/sh".to_owned(),
                    arguments: vec!["-ceu".to_owned(), AUTH_SCRIPT.to_owned()],
                }),
                replicas: config.replicas,
                exposed_ports: vec![9_002],
                health_check: Some(HealthCheckSpec {
                    probe: HealthProbe::Http {
                        port: 9_002,
                        path: "/healthz".to_owned(),
                    },
                    interval_secs: 10,
                    unhealthy_threshold: 3,
                }),
                max_restarts: None,
                environment,
                user: None,
                node_api: NodeApiAccess::Disabled,
                secrets: Some(SecretMountSpec {
                    mount_path: "/run/secrets/tailscale.env".to_owned(),
                    items: BTreeMap::from([("TS_AUTHKEY".to_owned(), config.auth_key.clone())]),
                }),
                volumes: vec![VolumeMountSpec {
                    source: VolumeSource::ReplicaManaged {
                        name: "tailscale-state".to_owned(),
                    },
                    target: "/state".to_owned(),
                    access: VolumeAccess::ReadWrite,
                }],
                placement: PlacementConstraint::default(),
                exec: ExecPolicy::Denied,
            },
            status: ServiceStatus {
                active_deployment_id: None,
                replica_override: None,
                rollout: RolloutState::Active,
                conditions: Vec::new(),
            },
        };
        service.spec.validate()?;

        let firewall_policy = Object {
            meta: ObjectMeta {
                id: FirewallPolicyId::new(GATEWAY_POLICY_ID)?,
                labels: BTreeMap::new(),
                annotations,
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: vec![OwnerReference {
                    resource: ResourceId::new(
                        ResourceKind::new("Service")?,
                        ResourceName::from(service_id.clone()),
                    ),
                    ownership: Ownership::Controller,
                }],
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: FirewallPolicySpec {
                direction: FirewallDirection::Egress,
                subject: FirewallSubject::Service(service_id),
                rules: vec![FirewallRule {
                    cidr: cluster.cluster_cidr.to_string(),
                    protocol: TransportProtocol::Any,
                    ports: Vec::new(),
                    verdict: FirewallVerdict::Allow,
                }],
                // Tailscale coordination, DERP, STUN, and direct peers use a
                // changing public endpoint set, so the remaining egress stays open.
                default_verdict: FirewallVerdict::Allow,
            },
            status: FirewallPolicyStatus {
                applied_generation: Generation::default(),
                ruleset_digest: None,
                conditions: Vec::new(),
            },
        };
        Ok(Some(Self {
            service,
            firewall_policy,
        }))
    }

    pub(crate) fn with_auth_key(
        mut self,
        auth_key: SecretValue,
    ) -> Result<Self, TailscaleResourceError> {
        let secrets = self
            .service
            .spec
            .secrets
            .as_mut()
            .ok_or(TailscaleResourceError::MissingAuthSecretMount)?;
        secrets.items.insert("TS_AUTHKEY".to_owned(), auth_key);
        Ok(self)
    }
}

pub(crate) fn is_managed(annotations: &BTreeMap<AnnotationKey, String>) -> bool {
    annotations
        .get(&managed_annotation())
        .is_some_and(|value| value == MANAGED_VALUE)
}

pub(crate) fn resource_ids() -> Result<(ServiceId, FirewallPolicyId), kernel_api::InvalidIdentifier>
{
    Ok((
        ServiceId::new(GATEWAY_SERVICE_ID)?,
        FirewallPolicyId::new(GATEWAY_POLICY_ID)?,
    ))
}

fn managed_annotation() -> AnnotationKey {
    AnnotationKey(MANAGED_ANNOTATION.to_owned())
}

/// Invalid built-in Tailscale resource construction.
#[derive(Debug, thiserror::Error)]
pub enum TailscaleResourceError {
    /// A fixed built-in identifier no longer satisfies the resource contract.
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    /// The generated service violated the public workload contract.
    #[error(transparent)]
    Service(#[from] kernel_api::ServiceSpecError),
    /// An internal resource template lost the private auth-key mount.
    #[error("built-in Tailscale service is missing its auth-key secret mount")]
    MissingAuthSecretMount,
}
