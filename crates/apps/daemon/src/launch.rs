use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster::{
    ClusterConfig, EmbeddedEtcdProvider, EmbeddedEtcdSettings, NodeCertificateBundle,
    StoreJoinTicket, StoreMember, StoreProviderConfig, StoreStartMode,
};
use kernel_api::{NodeId, NodeInstanceId, NodeRole};
use kernel_controller::SystemTimestampClock;
use kernel_store::{EtcdStore, EtcdTlsConfig, Store, TokioClock};
use node_agent::{
    HickoryDnsServerBinder, LinuxMeshBackend, LinuxWorkloadBridgeBackend, MeshIdentity,
    NetworkHealthProber, NftablesFirewallBackend, SystemStatusClock,
};
use runtime::{ContainerdRuntime, ContainerdRuntimeSettings, TokioRuntimeClock};
use serde::{Deserialize, Serialize};

use crate::{
    AgentStore, Daemon, DaemonPlan, DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings,
    OperatorLeaderWorkload, OperatorSettings, RunningDaemon,
};

/// Store process decision supplied explicitly on every daemon start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", deny_unknown_fields)]
pub enum StoreLaunchMode {
    /// Create the designated master's first store member.
    Bootstrap,
    /// Start one member previously staged by the active cluster leader.
    Join { ticket: StoreJoinTicket },
    /// Reopen the member already persisted in this node's data directory.
    Restart,
    /// Connect a worker agent without starting a local store member.
    Client,
}

impl StoreLaunchMode {
    fn provider_mode(&self) -> Option<StoreStartMode> {
        match self {
            Self::Bootstrap => Some(StoreStartMode::Bootstrap),
            Self::Join { ticket } => Some(StoreStartMode::Join(ticket.clone())),
            Self::Restart => Some(StoreStartMode::Restart),
            Self::Client => None,
        }
    }
}

/// Protected launch document consumed by the control-plane daemon executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DaemonLaunchConfig {
    /// Authoritative topology and fixed cluster settings.
    pub cluster: ClusterConfig,
    /// Stable local node selected from the topology.
    pub node_id: NodeId,
    /// Root of all role and provider persistence.
    pub data_directory: PathBuf,
    /// Exact etcd executable on control-plane nodes; absent on workers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etcd_binary: Option<PathBuf>,
    /// Explicit local store initialization decision.
    pub store_mode: StoreLaunchMode,
    /// Node-specific mutual TLS identity granted during bootstrap or join.
    pub security: NodeCertificateBundle,
    /// Optional deterministic process identity, primarily for cluster tests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<NodeInstanceId>,
}

impl DaemonLaunchConfig {
    /// Validates all launch choices before local state or processes are touched.
    pub fn validate(&self) -> Result<(), DaemonLaunchError> {
        self.cluster.preflight()?;
        if !self.data_directory.is_absolute() {
            return Err(invalid("data directory must be an absolute path"));
        }
        let node = self.cluster.nodes.get(&self.node_id).ok_or_else(|| {
            invalid(format!(
                "node `{}` is absent from the cluster topology",
                self.node_id
            ))
        })?;
        match (&self.etcd_binary, node.role.is_control_plane()) {
            (Some(path), true) if path.is_absolute() => {}
            (Some(_), true) => {
                return Err(invalid("embedded etcd binary must be an absolute path"));
            }
            (None, true) => {
                return Err(invalid(
                    "control-plane nodes require an embedded etcd binary",
                ));
            }
            (None, false) => {}
            (Some(_), false) => {
                return Err(invalid("worker nodes must not configure an etcd binary"));
            }
        }
        match &self.store_mode {
            StoreLaunchMode::Client if node.role.is_control_plane() => Err(invalid(
                "control-plane nodes must start their declared local store member",
            )),
            mode if !node.role.is_control_plane() && !matches!(mode, StoreLaunchMode::Client) => {
                Err(invalid("worker nodes must use client-only store access"))
            }
            StoreLaunchMode::Bootstrap if node.role != NodeRole::Master => Err(invalid(
                "only the designated master may bootstrap the cluster store",
            )),
            StoreLaunchMode::Join { ticket }
                if node.role == NodeRole::Master || ticket.node_id() != &self.node_id =>
            {
                Err(invalid(
                    "join mode requires a non-master ticket bound to the local node",
                ))
            }
            _ => Ok(()),
        }
    }
}

/// Reads a secret-bearing launch document after enforcing owner-only permissions.
pub fn load_launch_config(path: &Path) -> Result<DaemonLaunchConfig, DaemonLaunchError> {
    validate_private_permissions(path)?;
    let bytes = std::fs::read(path).map_err(|source| DaemonLaunchError::Io {
        action: "read",
        path: path.to_path_buf(),
        source,
    })?;
    let config = serde_json::from_slice::<DaemonLaunchConfig>(&bytes).map_err(|source| {
        DaemonLaunchError::InvalidDocument {
            path: path.to_path_buf(),
            source,
        }
    })?;
    config.validate()?;
    Ok(config)
}

/// Builds production adapters and starts one daemon instance for its declared node role.
pub async fn launch_daemon(config: DaemonLaunchConfig) -> Result<RunningDaemon, DaemonLaunchError> {
    config.validate()?;
    let DaemonLaunchConfig {
        cluster,
        node_id,
        data_directory,
        etcd_binary,
        store_mode,
        security,
        instance_id,
    } = config;
    let known_members = control_plane_members(&cluster);
    let clock = Arc::new(TokioClock::new());
    let local_node = cluster
        .nodes
        .get(&node_id)
        .ok_or_else(|| invalid("local node disappeared from validated topology"))?;
    let agent_store = if local_node.role.is_control_plane() {
        let local_member = known_members
            .get(&node_id)
            .cloned()
            .ok_or_else(|| invalid("local node is absent from control-plane membership"))?;
        let provider_config = StoreProviderConfig::new(
            cluster.cluster_id.clone(),
            local_member,
            known_members.clone(),
            cluster.ports,
            data_directory.join("store"),
            security,
        )?;
        let provider = Arc::new(EmbeddedEtcdProvider::new(
            provider_config,
            etcd_binary
                .ok_or_else(|| invalid("control-plane nodes require an embedded etcd binary"))?,
            clock.clone(),
            EmbeddedEtcdSettings::default(),
        )?);
        AgentStore::Managed {
            provider,
            start_mode: store_mode.provider_mode().ok_or_else(|| {
                invalid("control-plane nodes cannot use client-only store access")
            })?,
        }
    } else {
        AgentStore::Remote(connect_worker_store(&cluster, &known_members, &security).await?)
    };
    let mesh_identity = MeshIdentity::load_or_generate(&data_directory.join("agent").join("mesh"))?;
    let containerd = Arc::new(
        ContainerdRuntime::connect(
            ContainerdRuntimeSettings {
                namespace: format!("maestro-{}", cluster.cluster_id),
                state_root: data_directory.join("runtime").join("containerd"),
                ..ContainerdRuntimeSettings::default()
            },
            Arc::new(TokioRuntimeClock::new()),
        )
        .await?,
    );
    let volatile_root = PathBuf::from("/run/maestro")
        .join(cluster.cluster_id.as_str())
        .join(node_id.as_str());
    let instance_id = match instance_id {
        Some(instance_id) => instance_id,
        None => generate_instance_id()?,
    };
    let operator_workload = Arc::new(OperatorLeaderWorkload::new(
        cluster.cluster_id.clone(),
        clock.clone(),
        Arc::new(SystemTimestampClock),
        OperatorSettings::production(&cluster)?,
    ));
    let plan = DaemonPlan::new(cluster, node_id, data_directory)?;
    let factory = DaemonRoleFactory::new(
        DaemonRoleDependencies {
            agent_store,
            mesh_backend: LinuxMeshBackend::new(),
            firewall_backend: NftablesFirewallBackend::new(),
            bridge_backend: LinuxWorkloadBridgeBackend::new(),
            dns_server_binder: Arc::new(HickoryDnsServerBinder),
            workload_runtime: containerd.clone(),
            network_provider: containerd,
            health_prober: Arc::new(NetworkHealthProber::new(Duration::from_secs(5))?),
            volatile_root,
            mesh_identity,
            instance_id,
            monotonic_clock: clock,
            status_clock: Arc::new(SystemStatusClock),
        },
        DaemonRoleSettings::default(),
    )
    .with_leader_workload(operator_workload);
    Daemon::new(plan, factory).start().await.map_err(Into::into)
}

async fn connect_worker_store(
    cluster: &ClusterConfig,
    members: &BTreeMap<NodeId, StoreMember>,
    security: &NodeCertificateBundle,
) -> Result<Arc<dyn Store>, DaemonLaunchError> {
    let endpoints = members
        .values()
        .map(|member| {
            format!(
                "https://{}:{}",
                member.host_address, cluster.ports.store_client
            )
        })
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(DaemonLaunchError::RemoteStore {
            detail: "topology declares no control-plane store endpoints".to_owned(),
        });
    }
    let tls = EtcdTlsConfig::for_endpoints(
        security.trust_root_pem.as_bytes().to_vec(),
        security.identity.certificate_pem.as_bytes().to_vec(),
        security
            .identity
            .private_key_pem
            .expose()
            .as_bytes()
            .to_vec(),
    );
    EtcdStore::connect_with_tls(endpoints, tls)
        .await
        .map(|store| Arc::new(store) as Arc<dyn Store>)
        .map_err(|error| DaemonLaunchError::RemoteStore {
            detail: error.to_string(),
        })
}

fn control_plane_members(config: &ClusterConfig) -> BTreeMap<NodeId, StoreMember> {
    config
        .nodes
        .iter()
        .filter(|(_, node)| node.role.is_control_plane())
        .map(|(node_id, node)| {
            (
                node_id.clone(),
                StoreMember {
                    node_id: node_id.clone(),
                    host_address: node.endpoint.host_address,
                },
            )
        })
        .collect()
}

fn generate_instance_id() -> Result<NodeInstanceId, DaemonLaunchError> {
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    NodeInstanceId::new(format!("{}-{entropy}", std::process::id())).map_err(Into::into)
}

fn validate_private_permissions(path: &Path) -> Result<(), DaemonLaunchError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|source| DaemonLaunchError::Io {
                action: "inspect permissions of",
                path: path.to_path_buf(),
                source,
            })?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(DaemonLaunchError::InsecurePermissions {
                path: path.to_path_buf(),
                mode,
            });
        }
    }
    Ok(())
}

fn invalid(detail: impl Into<String>) -> DaemonLaunchError {
    DaemonLaunchError::InvalidConfiguration {
        detail: detail.into(),
    }
}

/// Why a protected launch document or production daemon start failed.
#[derive(Debug, thiserror::Error)]
pub enum DaemonLaunchError {
    /// Topology preflight rejected authoritative cluster settings.
    #[error(transparent)]
    InvalidTopology(#[from] cluster::ClusterPreflightError),
    /// Launch-specific mode, node, or path selection was invalid.
    #[error("invalid daemon launch configuration: {detail}")]
    InvalidConfiguration { detail: String },
    /// A secret-bearing launch document was accessible by other users.
    #[error("daemon launch document `{}` has insecure permissions {mode:#o}", path.display())]
    InsecurePermissions { path: PathBuf, mode: u32 },
    /// Launch document filesystem access failed.
    #[error("failed to {action} daemon launch document `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A launch document did not match its strict JSON schema.
    #[error("invalid daemon launch document `{}`: {source}", path.display())]
    InvalidDocument {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    /// Provider configuration or store lifecycle failed.
    #[error(transparent)]
    StoreProvider(#[from] cluster::StoreProviderError),
    /// A worker could not connect to any declared control-plane store endpoint.
    #[error("worker store connection failed: {detail}")]
    RemoteStore { detail: String },
    /// The node-local WireGuard identity could not be loaded safely.
    #[error(transparent)]
    MeshIdentity(#[from] node_agent::MeshIdentityError),
    /// The production workload health probe adapter could not be constructed.
    #[error(transparent)]
    HealthProbe(#[from] node_agent::HealthProbeError),
    /// The native workload runtime could not be configured or reached.
    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),
    /// A generated process identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// Static operator views could not be constructed from cluster settings.
    #[error(transparent)]
    OperatorSettings(#[from] crate::OperatorSuiteError),
    /// Role planning, startup, or rollback failed.
    #[error(transparent)]
    Daemon(#[from] crate::DaemonError),
}
