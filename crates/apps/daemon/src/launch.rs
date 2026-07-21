use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use build::LocalBuildSourceProvider;
use cluster::{
    ClusterConfig, EmbeddedEtcdProvider, EmbeddedEtcdSettings, NodeCertificateBundle,
    StoreJoinTicket, StoreMember, StoreProviderConfig, StoreStartMode,
};
use kernel_api::{NodeId, NodeInstanceId, NodeRole};
use kernel_controller::SystemTimestampClock;
use kernel_store::{EtcdStore, EtcdTlsConfig, Store, TokioClock};
use logstore::{DuckLogStoreRuntime, DuckMetricStoreRuntime, DuckStoreSettings};
use node_agent::{
    CgroupV2StatsReader, HickoryDnsServerBinder, HostNetworkStatsReader, LinuxHostDiskReader,
    LinuxHostStatsReader, LinuxMeshBackend, LinuxWorkloadBridgeBackend, MeshIdentity,
    NetworkHealthProber, NftablesFirewallBackend, SystemStatusClock,
};
use runtime::{ContainerdRuntime, ContainerdRuntimeSettings, TokioRuntimeClock};
use serde::{Deserialize, Serialize};
use upgrade::{StoreNodeUpgradeBackendSettings, UpgradeSettings};

use crate::datadog::{build_datadog_sinks, configure_datadog};
use crate::launch_error::{DaemonLaunchError, invalid};
use crate::log_backup_config::configure_log_maintenance;
use crate::{
    AgentStore, BuildOperatorBackends, Daemon, DaemonPlan, DaemonRoleDependencies,
    DaemonRoleFactory, DaemonRoleSettings, DatadogLaunchConfig, LogBackupLaunchConfig,
    NodeUpgradeDependencies, OperatorLeaderWorkload, OperatorSettings, RunningDaemon,
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
    /// Optional node-local Datadog log delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datadog: Option<DatadogLaunchConfig>,
    /// Optional node-local S3 log backup and retention target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_backup: Option<LogBackupLaunchConfig>,
    /// Optional cluster-wide GitHub pull-request previews.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<crate::PreviewLaunchConfig>,
    /// Optional NixOS staging and reboot policy; absence disables cluster upgrades.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nixos_upgrade: Option<crate::NixosUpgradeLaunchConfig>,
}

impl DaemonLaunchConfig {
    /// Validates all launch choices before local state or processes are touched.
    pub fn validate(&self) -> Result<(), DaemonLaunchError> {
        self.cluster.preflight()?;
        if let Some(datadog) = &self.datadog {
            datadog.validate()?;
        }
        if let Some(log_backup) = &self.log_backup {
            log_backup.validate(&self.cluster.name, &self.node_id)?;
        }
        if let Some(preview) = &self.preview {
            preview.validate()?;
        }
        if let Some(upgrade) = &self.nixos_upgrade {
            upgrade.validate()?;
        }
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
        datadog,
        log_backup,
        preview,
        nixos_upgrade,
    } = config;
    let known_members = control_plane_members(&cluster);
    let clock = Arc::new(TokioClock::new());
    let local_node = cluster
        .nodes
        .get(&node_id)
        .ok_or_else(|| invalid("local node disappeared from validated topology"))?;
    let configured_datadog =
        configure_datadog(datadog.as_ref(), &cluster.name, &local_node.hostname)?;
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
    let timestamp_clock = Arc::new(SystemTimestampClock);
    let build_root = data_directory.join("build");
    let build_source = Arc::new(LocalBuildSourceProvider::new(
        build_root.join("workspaces"),
        build_root.join("archives"),
    )?);
    let configured_preview = preview
        .as_ref()
        .map(|preview| preview.configure())
        .transpose()?;
    let configured_upgrade = nixos_upgrade
        .as_ref()
        .map(crate::NixosUpgradeLaunchConfig::configure)
        .transpose()?;
    let mut operator_settings = OperatorSettings::production(&cluster)?;
    operator_settings.preview = configured_preview
        .as_ref()
        .map(|preview| preview.settings.clone());
    operator_settings.upgrade = configured_upgrade
        .as_ref()
        .map(|_| {
            UpgradeSettings::new(Duration::from_secs(30), Duration::from_secs(5), 3)
                .map_err(|error| invalid(error.to_string()))
        })
        .transpose()?;
    let store_upgrades = configured_upgrade
        .as_ref()
        .map(|_| {
            StoreNodeUpgradeBackendSettings::new(
                Duration::from_secs(60 * 60),
                Duration::from_secs(2),
            )
            .map_err(|error| invalid(error.to_string()))
        })
        .transpose()?;
    let operator_workload = Arc::new(OperatorLeaderWorkload::new(
        cluster.cluster_id.clone(),
        clock.clone(),
        timestamp_clock.clone(),
        operator_settings,
        BuildOperatorBackends {
            source: build_source.clone(),
            revisions: build_source,
            artifacts: containerd.clone(),
            pull_requests: configured_preview.map(|preview| preview.pull_requests),
            upgrades: None,
            store_upgrades,
        },
    ));
    let plan = DaemonPlan::new(cluster, node_id, data_directory)?;
    let health_prober = Arc::new(NetworkHealthProber::new(Duration::from_secs(5))?);
    let (log_store_runtime, metric_store_runtime) =
        open_observability_stores(plan.data_directory()).await?;
    let log_maintenance = configure_log_maintenance(
        log_backup.as_ref(),
        &plan.cluster().name,
        plan.node_id(),
        log_store_runtime.store(),
        clock.clone(),
        timestamp_clock,
    )
    .await?;
    let datadog_sinks = build_datadog_sinks(configured_datadog, &log_store_runtime);
    let network_stats_reader = Arc::new(HostNetworkStatsReader::production(containerd.clone()));
    let factory = DaemonRoleFactory::new(
        DaemonRoleDependencies {
            agent_store,
            mesh_backend: LinuxMeshBackend::new(),
            firewall_backend: NftablesFirewallBackend::new(),
            bridge_backend: LinuxWorkloadBridgeBackend::new(),
            dns_server_binder: Arc::new(HickoryDnsServerBinder),
            workload_runtime: containerd.clone(),
            log_store_runtime: Box::new(log_store_runtime),
            log_sinks: datadog_sinks.logs,
            metric_sinks: datadog_sinks.metrics,
            host_metric_sinks: datadog_sinks.host_metrics,
            metric_store_runtime: Box::new(metric_store_runtime),
            stats_reader: Arc::new(CgroupV2StatsReader),
            network_stats_reader,
            host_stats_reader: Arc::new(LinuxHostStatsReader::production()),
            host_disk_reader: Arc::new(LinuxHostDiskReader::production()),
            network_provider: containerd,
            health_prober,
            volatile_root,
            mesh_identity,
            instance_id,
            monotonic_clock: clock,
            status_clock: Arc::new(SystemStatusClock),
            node_upgrade: configured_upgrade.map(|upgrade| NodeUpgradeDependencies {
                stager: upgrade.stager,
                rebooter: upgrade.rebooter,
                running_version: upgrade.running_version,
            }),
        },
        DaemonRoleSettings::default(),
    )
    .with_log_maintenance(log_maintenance)
    .with_leader_workload(operator_workload);
    Daemon::new(plan, factory).start().await.map_err(Into::into)
}

async fn open_observability_stores(
    data_directory: &Path,
) -> Result<(DuckLogStoreRuntime, DuckMetricStoreRuntime), DaemonLaunchError> {
    let agent_directory = data_directory.join("agent");
    let log_settings = DuckStoreSettings::new(agent_directory.join("logs.duckdb"), 1_024)?;
    let metric_settings = DuckStoreSettings::new(agent_directory.join("metrics.duckdb"), 1_024)?;
    let logs = DuckLogStoreRuntime::open(log_settings).await?;
    match DuckMetricStoreRuntime::open(metric_settings).await {
        Ok(metrics) => Ok((logs, metrics)),
        Err(error) => match logs.shutdown().await {
            Ok(()) => Err(error.into()),
            Err(rollback_error) => Err(DaemonLaunchError::ObservabilityStoreRollback {
                startup: error.to_string(),
                rollback: rollback_error.to_string(),
            }),
        },
    }
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
