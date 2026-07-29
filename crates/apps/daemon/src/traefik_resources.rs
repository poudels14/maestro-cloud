use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use cluster::{ClusterConfig, NodeCertificateBundle};
use kernel_api::{
    AnnotationKey, ArtifactTemplate, CommandSpec, ExecPolicy, Generation, HealthCheckSpec,
    HealthProbe, NodeApiAccess, Object, ObjectMeta, PlacementConstraint, ReplicaSpread,
    ResourceRevision, RolloutState, SecretMountSpec, SecretValue, Service, ServiceId, ServiceSpec,
    ServiceStatus, WorkloadUserSpec,
};
use kernel_store::Keyspace;
use runtime::{HostPortPublication, PortProtocol};

pub(crate) const TRAEFIK_SERVICE_ID: &str = "maestro-system-traefik";
const MANAGED_ANNOTATION: &str = "system.maestro.dev/owner";
pub(crate) const TRAEFIK_MANAGED_OWNER: &str = "traefik";
pub(crate) const TRAEFIK_IMAGE: &str =
    "traefik:v3.6.23@sha256:f5dba1e65167778cd5f8d1b463fc5d200f49d40c6458fc9f4b391a68ebfb9534";
const TRAEFIK_VERSION: &str = "traefik-3.6.23";
const ETCD_SECRET_DIRECTORY: &str = "/run/secrets/etcd";
const ETCD_CA_FILE: &str = "ca.pem";
const ETCD_CLIENT_CERTIFICATE_FILE: &str = "client.pem";
const ETCD_CLIENT_KEY_FILE: &str = "client-key.pem";

/// Ordinary service and daemon-only host publications for cluster ingress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraefikSystemResources {
    pub(crate) service: Service,
    host_ports: Vec<HostPortPublication>,
}

impl TraefikSystemResources {
    pub(crate) fn for_cluster(
        cluster: &ClusterConfig,
        security: &NodeCertificateBundle,
    ) -> Result<Self, TraefikResourceError> {
        let replicas = u32::try_from(
            cluster
                .nodes
                .values()
                .filter(|node| node.role.runs_workloads())
                .count(),
        )
        .map_err(|_| TraefikResourceError::TooManyWorkloadNodes)?;
        if replicas == 0 {
            return Err(TraefikResourceError::NoWorkloadNodes);
        }
        let service_id = ServiceId::new(TRAEFIK_SERVICE_ID)?;
        // kvtools/etcdv3 strips one leading slash from every key it reads.
        // Select the fenced compatibility mirror rather than the canonical
        // `/maestro/` copy retained for snapshots and migration.
        let root_key = Keyspace::new(&cluster.cluster_id)
            .traefik_provider()
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
            executable: "/usr/local/bin/traefik".to_owned(),
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
                annotations: BTreeMap::from([(
                    managed_annotation(),
                    TRAEFIK_MANAGED_OWNER.to_owned(),
                )]),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: ServiceSpec {
                name: "Traefik".to_owned(),
                version: TRAEFIK_VERSION.to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: TRAEFIK_IMAGE.to_owned(),
                },
                preview: None,
                command: Some(command),
                replicas,
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
                            ETCD_CA_FILE.to_owned(),
                            SecretValue::new(security.trust_root_pem.clone()),
                        ),
                        (
                            ETCD_CLIENT_CERTIFICATE_FILE.to_owned(),
                            SecretValue::new(security.identity.certificate_pem.clone()),
                        ),
                        (
                            ETCD_CLIENT_KEY_FILE.to_owned(),
                            security.identity.private_key_pem.clone(),
                        ),
                    ]),
                }),
                volumes: Vec::new(),
                placement: PlacementConstraint {
                    replica_spread: ReplicaSpread::BestEffort,
                    ..PlacementConstraint::default()
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

    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    pub(crate) fn firewall_routes(&self) -> Vec<firewall::HostPortRoute> {
        self.host_ports
            .iter()
            .map(|publication| firewall::HostPortRoute {
                service_id: self.service.meta.id.clone(),
                host_port: publication.host_port,
                workload_port: publication.container_port,
                protocol: match publication.protocol {
                    PortProtocol::Tcp => firewall::HostPortProtocol::Tcp,
                    PortProtocol::Udp => firewall::HostPortProtocol::Udp,
                },
            })
            .collect()
    }
}

/// Retains the cluster-wide Traefik client identity across controller leaders.
///
/// Every control-plane node has a distinct etcd client certificate. The first
/// leader seeds the shared Traefik Service with one valid identity; subsequent
/// leaders retain it while the cluster trust root remains unchanged. A trust
/// root rotation replaces the complete credential set and rolls the Service.
pub(crate) fn preserve_client_identity(current: &Service, desired: &mut Service) {
    let (
        Some(SecretMountSpec::Files {
            mount_path: current_mount_path,
            files: current_files,
        }),
        Some(SecretMountSpec::Files {
            mount_path: desired_mount_path,
            files: desired_files,
        }),
    ) = (&current.spec.secrets, &mut desired.spec.secrets)
    else {
        return;
    };
    if current_mount_path != desired_mount_path
        || current_files.get(ETCD_CA_FILE) != desired_files.get(ETCD_CA_FILE)
    {
        return;
    }
    let Some(certificate) = current_files.get(ETCD_CLIENT_CERTIFICATE_FILE) else {
        return;
    };
    let Some(private_key) = current_files.get(ETCD_CLIENT_KEY_FILE) else {
        return;
    };
    if !desired_files.contains_key(ETCD_CLIENT_CERTIFICATE_FILE)
        || !desired_files.contains_key(ETCD_CLIENT_KEY_FILE)
    {
        return;
    }
    desired_files.insert(ETCD_CLIENT_CERTIFICATE_FILE.to_owned(), certificate.clone());
    desired_files.insert(ETCD_CLIENT_KEY_FILE.to_owned(), private_key.clone());
}

fn managed_annotation() -> AnnotationKey {
    AnnotationKey(MANAGED_ANNOTATION.to_owned())
}

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
    #[error(transparent)]
    Service(#[from] kernel_api::ServiceSpecError),
    #[error("cluster has no workload-capable node for Traefik")]
    NoWorkloadNodes,
    #[error("cluster has too many workload nodes to represent Traefik replicas")]
    TooManyWorkloadNodes,
}
