use std::collections::BTreeMap;
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use std::collections::BTreeSet;

#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use cluster::{ClusterConfig, NodeCertificateBundle};
use kernel_api::{AnnotationKey, Service};
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use kernel_api::{
    ArtifactTemplate, CommandSpec, ExecPolicy, Generation, HealthCheckSpec, HealthProbe,
    NodeApiAccess, NodeId, Object, ObjectMeta, PlacementConstraint, ResourceRevision, RolloutState,
    SecretMountSpec, SecretValue, ServiceId, ServiceSpec, ServiceStatus, WorkloadUserSpec,
};
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
use semver::Version;

pub(crate) const DNS_RESOLVER_SERVICE_ID: &str = "maestro-system-dns";
const MANAGED_ANNOTATION: &str = "system.maestro.dev/owner";
const MANAGED_VALUE: &str = "dns-resolver";
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
const CREDENTIAL_DIRECTORY: &str = "/run/secrets/dns";
#[cfg(any(target_os = "macos", feature = "macos-platform", test))]
const DNS_PORT: u16 = 53;

/// Ordinary Service resource running Maestro's resolver on delegated networking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DnsResolverSystemResources {
    pub(crate) service: Service,
}

impl DnsResolverSystemResources {
    #[cfg(any(target_os = "macos", feature = "macos-platform", test))]
    pub(crate) fn for_docker_node(
        cluster: &ClusterConfig,
        node_id: &NodeId,
        security: &NodeCertificateBundle,
        store_encryption_secret: &SecretValue,
        daemon_version: &Version,
    ) -> Result<Self, DnsResolverResourceError> {
        if !cluster.nodes.contains_key(node_id) {
            return Err(DnsResolverResourceError::UnknownNode {
                node_id: node_id.to_string(),
            });
        }
        let service_id = ServiceId::new(DNS_RESOLVER_SERVICE_ID)?;
        let mut arguments = vec![
            "dns".to_owned(),
            "--cluster-id".to_owned(),
            cluster.cluster_id.to_string(),
            "--node-id".to_owned(),
            node_id.to_string(),
        ];
        for node in cluster
            .nodes
            .values()
            .filter(|node| node.role.is_control_plane())
        {
            arguments.push("--endpoint".to_owned());
            arguments.push(format!(
                "https://{}:{}",
                node.endpoint.host_address, cluster.ports.store_client
            ));
        }
        arguments.extend([
            "--certificate-authority".to_owned(),
            format!("{CREDENTIAL_DIRECTORY}/ca.pem"),
            "--client-certificate".to_owned(),
            format!("{CREDENTIAL_DIRECTORY}/client.pem"),
            "--client-private-key".to_owned(),
            format!("{CREDENTIAL_DIRECTORY}/client-key.pem"),
            "--store-encryption-secret".to_owned(),
            format!("{CREDENTIAL_DIRECTORY}/store-key"),
        ]);
        let service = Object {
            meta: ObjectMeta {
                id: service_id,
                labels: BTreeMap::new(),
                annotations: BTreeMap::from([(managed_annotation(), MANAGED_VALUE.to_owned())]),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: ServiceSpec {
                name: "Maestro DNS Resolver".to_owned(),
                version: format!("maestro-dns-{daemon_version}"),
                artifact: ArtifactTemplate::Image {
                    reference: format!("maestro-daemon:{daemon_version}"),
                },
                preview: None,
                command: Some(CommandSpec {
                    executable: "/bin/maestro-daemon".to_owned(),
                    arguments,
                }),
                replicas: 1,
                exposed_ports: vec![DNS_PORT],
                health_check: Some(HealthCheckSpec {
                    probe: HealthProbe::Tcp { port: DNS_PORT },
                    interval_secs: 10,
                    unhealthy_threshold: 3,
                }),
                max_restarts: None,
                environment: BTreeMap::new(),
                user: Some(WorkloadUserSpec {
                    user_id: 0,
                    group_id: 0,
                }),
                node_api: NodeApiAccess::Disabled,
                secrets: Some(SecretMountSpec::Files {
                    mount_path: CREDENTIAL_DIRECTORY.to_owned(),
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
                        ("store-key".to_owned(), store_encryption_secret.clone()),
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
        Ok(Self { service })
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

/// Invalid built-in delegated DNS resource construction.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DnsResolverResourceError {
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    #[cfg(any(target_os = "macos", feature = "macos-platform", test))]
    #[error(transparent)]
    Service(#[from] kernel_api::ServiceSpecError),
    #[cfg(any(target_os = "macos", feature = "macos-platform", test))]
    #[error("local DNS resolver node `{node_id}` is absent from the cluster topology")]
    UnknownNode { node_id: String },
}
