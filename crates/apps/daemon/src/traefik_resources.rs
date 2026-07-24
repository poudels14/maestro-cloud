use std::collections::BTreeMap;
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use std::collections::BTreeSet;
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use std::net::{IpAddr, Ipv4Addr};

#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use cluster::{ClusterConfig, NodeCertificateBundle};
use kernel_api::{AnnotationKey, Service, ServiceId};
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use kernel_api::{
    ArtifactTemplate, CommandSpec, ExecPolicy, Generation, HealthCheckSpec, HealthProbe,
    NodeApiAccess, NodeId, Object, ObjectMeta, PlacementConstraint, ResourceRevision, RolloutState,
    SecretMountSpec, SecretValue, ServiceSpec, ServiceStatus, WorkloadUserSpec,
};
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use kernel_store::Keyspace;
use runtime::HostPortPublication;
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use runtime::PortProtocol;

pub(crate) const TRAEFIK_SERVICE_ID: &str = "maestro-system-traefik";
const MANAGED_ANNOTATION: &str = "system.maestro.dev/owner";
const MANAGED_VALUE: &str = "traefik";
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
pub(crate) const TRAEFIK_IMAGE: &str =
    "traefik:v3.6.23@sha256:d85749d4d10d970ed2b3a7cb2406d9b9da1cdd6ea975a39c727aab809d73136a";
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
const TRAEFIK_VERSION: &str = "traefik-3.6.23";
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
const ETCD_SECRET_DIRECTORY: &str = "/run/secrets/etcd";

/// Ordinary service and daemon-only host publications for cluster ingress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraefikSystemResources {
    pub(crate) service: Service,
    host_ports: Vec<HostPortPublication>,
}

impl TraefikSystemResources {
    #[cfg(any(target_os = "macos", feature = "macos-platform", test))]
    pub(crate) fn for_docker_node(
        cluster: &ClusterConfig,
        node_id: &NodeId,
        security: &NodeCertificateBundle,
    ) -> Result<Self, TraefikResourceError> {
        if !cluster.nodes.contains_key(node_id) {
            return Err(TraefikResourceError::UnknownNode {
                node_id: node_id.to_string(),
            });
        }
        let service_id = ServiceId::new(TRAEFIK_SERVICE_ID)?;
        let root_key = Keyspace::new(&cluster.cluster_id)
            .traefik()
            .as_str()
            .trim_end_matches('/')
            .to_owned();
        let endpoints = cluster
            .nodes
            .values()
            .filter(|node| node.role.is_control_plane())
            .map(|node| {
                format!(
                    "{}:{}",
                    node.endpoint.host_address, cluster.ports.store_client
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let command = CommandSpec {
            executable: "/traefik".to_owned(),
            arguments: vec![
                "--providers.etcd=true".to_owned(),
                format!("--providers.etcd.rootKey={root_key}"),
                format!("--providers.etcd.endpoints={endpoints}"),
                format!("--providers.etcd.tls.ca={ETCD_SECRET_DIRECTORY}/ca.pem"),
                format!("--providers.etcd.tls.cert={ETCD_SECRET_DIRECTORY}/client.pem"),
                format!("--providers.etcd.tls.key={ETCD_SECRET_DIRECTORY}/client-key.pem"),
                "--entrypoints.web.address=:80".to_owned(),
                "--entrypoints.websecure.address=:443".to_owned(),
                "--ping=true".to_owned(),
                "--ping.entrypoint=web".to_owned(),
                "--accesslog=true".to_owned(),
                "--accesslog.format=json".to_owned(),
                "--accesslog.fields.defaultmode=keep".to_owned(),
                "--accesslog.fields.headers.defaultmode=drop".to_owned(),
            ],
        };
        let service = Object {
            meta: ObjectMeta {
                id: service_id.clone(),
                labels: BTreeMap::new(),
                annotations: BTreeMap::from([(managed_annotation(), MANAGED_VALUE.to_owned())]),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: ServiceSpec {
                name: "Maestro Traefik".to_owned(),
                version: TRAEFIK_VERSION.to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: TRAEFIK_IMAGE.to_owned(),
                },
                preview: None,
                command: Some(command),
                replicas: 1,
                exposed_ports: vec![80, 443],
                health_check: Some(HealthCheckSpec {
                    probe: HealthProbe::Http {
                        port: 80,
                        path: "/ping".to_owned(),
                    },
                    interval_secs: 10,
                    unhealthy_threshold: 3,
                }),
                max_restarts: None,
                environment: BTreeMap::new(),
                user: Some(WorkloadUserSpec {
                    user_id: 0,
                    group_id: 0,
                }),
                node_api: NodeApiAccess::IdentityAndTelemetry,
                secrets: Some(SecretMountSpec::Files {
                    mount_path: ETCD_SECRET_DIRECTORY.to_owned(),
                    files: BTreeMap::from([
                        (
                            "ca.pem".to_owned(),
                            SecretValue::new(security.trust_root_pem.clone()),
                        ),
                        (
                            "client.pem".to_owned(),
                            SecretValue::new(security.identity.certificate_pem.clone()),
                        ),
                        (
                            "client-key.pem".to_owned(),
                            security.identity.private_key_pem.clone(),
                        ),
                    ]),
                }),
                volumes: Vec::new(),
                placement: PlacementConstraint {
                    node_id: Some(node_id.clone()),
                    labels: BTreeMap::new(),
                },
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
        Ok(Self {
            service,
            host_ports: vec![host_port(80), host_port(443)],
        })
    }

    pub(crate) fn host_port_grants(&self) -> BTreeMap<ServiceId, Vec<HostPortPublication>> {
        BTreeMap::from([(self.service.meta.id.clone(), self.host_ports.clone())])
    }
}

pub(crate) fn is_managed(annotations: &BTreeMap<AnnotationKey, String>) -> bool {
    annotations
        .get(&managed_annotation())
        .is_some_and(|value| value == MANAGED_VALUE)
}

fn managed_annotation() -> AnnotationKey {
    AnnotationKey(MANAGED_ANNOTATION.to_owned())
}

#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
fn host_port(port: u16) -> HostPortPublication {
    HostPortPublication {
        container_port: port,
        host_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        host_port: port,
        protocol: PortProtocol::Tcp,
    }
}

/// Invalid built-in Traefik resource construction.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TraefikResourceError {
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    #[cfg(any(target_os = "macos", feature = "macos-platform", test))]
    #[error(transparent)]
    Service(#[from] kernel_api::ServiceSpecError),
    #[cfg(any(target_os = "macos", feature = "macos-platform", test))]
    #[error("local Traefik node `{node_id}` is absent from the cluster topology")]
    UnknownNode { node_id: String },
}
