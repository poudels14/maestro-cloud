use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cluster::{StoreProvider, StoreStartMode};
use kernel_api::{NodeInstanceId, ServiceId, WorkloadNetworkMode};
use kernel_controller::{FencedStore, LeaderElector, LeaderIdentity, StoreLeaderElector};
use kernel_store::{Clock, Keyspace, Store};
use logs::{LogSink, LogStoreRuntime, SinkRuntimeRegistry};
use metrics::{HostMetricSink, MetricSink, MetricStoreRuntime};
use node_agent::{
    CgroupStatsReader, DnsServerBinder, FirewallBackend, HealthProber, HostDiskReader,
    HostStatsReader, MeshBackend, MeshIdentity, StatusClock, SystemDnsPluginSettings,
    TailscaleDnsPluginSettings, WorkloadBridgeBackend, WorkloadNetworkStatsReader,
};
use runtime::{
    ArtifactStore, HostPortPublication, NetworkProvider, ValueSourceResolver, WorkloadRuntime,
};
use semver::Version;
use server::ServerSettings;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use upgrade::{NixosUpgradeStager, NodeRebooter};

pub use self::settings::DaemonRoleSettings;
use crate::admission::AdmissionDependencies;
use crate::agent_role::start_agent;
use crate::leadership::run_leadership;
use crate::role_tasks::{shutdown_role_tasks, wait_for_role_task};
use crate::{
    DaemonPlan, DaemonRole, LogMaintenanceWorker, RoleError, RoleFactory, RoleRuntime, RoleSpec,
};

mod settings;

/// Store access owned by an agent role, with local-process lifetime kept explicit.
pub enum AgentStore {
    /// Control-plane node that starts and owns one provider member.
    Managed {
        /// Provisioning boundary for the node-local cluster store member.
        provider: Arc<dyn StoreProvider>,
        /// Explicit bootstrap, join, or restart decision for the local member.
        start_mode: StoreStartMode,
    },
    /// Worker node that connects to an already-running cluster store.
    Remote(Arc<dyn Store>),
}

/// Node-local seams used by coordinated restart and optional NixOS upgrades.
pub struct NodeUpgradeDependencies {
    /// Prepares a validated NixOS boot generation, when upgrades are enabled.
    pub stager: Option<Arc<dyn NixosUpgradeStager>>,
    /// Requests host reboot only after collective leader release.
    pub rebooter: Arc<dyn NodeRebooter>,
}

/// Host telemetry adapters exposed only on platforms with native host readers.
pub enum HostTelemetryDependencies {
    /// Direct host resource and mount readers are available.
    Available {
        /// Aggregate CPU, memory, and network reader.
        resource_reader: Arc<dyn HostStatsReader>,
        /// Host mount identity and capacity reader.
        disk_reader: Arc<dyn HostDiskReader>,
    },
    /// The platform intentionally omits host telemetry.
    Unavailable,
}

/// One leader-owned workload bound to the exact fence for an election term.
#[async_trait]
pub trait LeaderWorkload: Send + Sync {
    /// Runs until shutdown, returning only after every fenced worker has stopped.
    async fn run(
        &self,
        store: Arc<FencedStore>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), RoleError>;
}

/// Production adapters and identities required by the concrete role factory.
pub struct DaemonRoleDependencies<MeshBackendType, FirewallBackendType, BridgeBackendType> {
    /// Local provider ownership or a remote worker store connection.
    pub agent_store: AgentStore,
    /// Host-network adapter that applies exact WireGuard and route state.
    pub mesh_backend: MeshBackendType,
    /// Host-network adapter that applies complete node-local nftables state.
    pub firewall_backend: FirewallBackendType,
    /// Host-network adapter that owns the node-local workload bridge.
    pub bridge_backend: BridgeBackendType,
    /// Address ownership and node-local network agents enabled for workloads.
    pub workload_network_mode: WorkloadNetworkMode,
    /// Host-port grants reserved for daemon-owned system services.
    pub system_host_ports: BTreeMap<ServiceId, Vec<HostPortPublication>>,
    /// UDP/TCP listener binder for the node-local authoritative DNS server.
    pub dns_server_binder: Arc<dyn DnsServerBinder>,
    /// Optional scoped forwarding path for explicitly configured remote cluster suffixes.
    pub dns_plugin_settings: Option<TailscaleDnsPluginSettings>,
    /// Host resolver path used for names outside Maestro-owned DNS zones.
    pub dns_upstream_settings: Option<SystemDnsPluginSettings>,
    /// Native backend used for workload lifecycle, adoption, and events.
    pub workload_runtime: Arc<dyn WorkloadRuntime>,
    /// Native backend used for build artifacts and peer export/import streams.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Node-local durable store for content-addressed build context uploads.
    pub artifact_archives: Arc<dyn build::ArtifactArchiveStore>,
    /// Owned normalized-log storage runtime for this node.
    pub log_store_runtime: Box<dyn LogStoreRuntime>,
    /// Independently checkpointed normalized-log destinations owned by this node.
    pub log_sinks: Vec<Arc<dyn LogSink>>,
    /// Owned normalized-metric storage runtime for this node.
    pub metric_store_runtime: Box<dyn MetricStoreRuntime>,
    /// Independently checkpointed normalized-metric destinations owned by this node.
    pub metric_sinks: Vec<Arc<dyn MetricSink>>,
    /// Independently checkpointed host-metric destinations owned by this node.
    pub host_metric_sinks: Vec<Arc<dyn HostMetricSink>>,
    /// Direct cgroup v2 reader used for backend-neutral workload samples.
    pub stats_reader: Arc<dyn CgroupStatsReader>,
    /// Runtime-aware reader for optional cumulative workload network counters.
    pub network_stats_reader: Arc<dyn WorkloadNetworkStatsReader>,
    /// Platform-specific host telemetry capability and adapters.
    pub host_telemetry: HostTelemetryDependencies,
    /// Host-owned workload address allocator and attachment backend.
    pub network_provider: Arc<dyn NetworkProvider>,
    /// Bounded HTTP and TCP probe adapter for local workload readiness.
    pub health_prober: Arc<dyn HealthProber>,
    /// Volatile tmpfs-backed root for workload secrets and node API sockets.
    pub volatile_root: PathBuf,
    /// Persisted node-local WireGuard identity.
    pub mesh_identity: MeshIdentity,
    /// Unique identity of this daemon process for leader election.
    pub instance_id: NodeInstanceId,
    /// Semantic version reported in node status and upgrade observations.
    pub running_version: Version,
    /// Monotonic clock shared by resync, leadership, and shutdown deadlines.
    pub monotonic_clock: Arc<dyn Clock>,
    /// Wall clock used only for status condition transition timestamps.
    pub status_clock: Arc<dyn StatusClock>,
    /// Optional paired node-local NixOS staging and reboot seams.
    pub node_upgrade: Option<NodeUpgradeDependencies>,
    /// Validated node-local operator API transport and authentication policy.
    pub api_settings: ServerSettings,
    /// Optional bridge-only plaintext listener reached through the managed operator gateway.
    pub admin_api_settings: Option<ServerSettings>,
    /// Static firewall compiler settings exposed to read-only API dry-runs.
    pub firewall_settings: firewall::FirewallSettings,
}

/// Concrete daemon factory composing node agents and control-plane leader work when declared.
pub struct DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType> {
    pub(crate) agent_store: AgentStore,
    pub(crate) mesh_backend: Mutex<Option<MeshBackendType>>,
    pub(crate) firewall_backend: Mutex<Option<FirewallBackendType>>,
    pub(crate) bridge_backend: Mutex<Option<BridgeBackendType>>,
    pub(crate) workload_network_mode: WorkloadNetworkMode,
    pub(crate) system_host_ports: BTreeMap<ServiceId, Vec<HostPortPublication>>,
    pub(crate) dns_server_binder: Arc<dyn DnsServerBinder>,
    pub(crate) dns_plugin_settings: Option<TailscaleDnsPluginSettings>,
    pub(crate) dns_upstream_settings: Option<SystemDnsPluginSettings>,
    pub(crate) workload_runtime: Arc<dyn WorkloadRuntime>,
    pub(crate) artifact_store: Arc<dyn ArtifactStore>,
    pub(crate) artifact_archives: Arc<dyn build::ArtifactArchiveStore>,
    pub(crate) value_sources: Option<Arc<dyn ValueSourceResolver>>,
    pub(crate) log_store_runtime: Mutex<Option<Box<dyn LogStoreRuntime>>>,
    pub(crate) log_sinks: Vec<Arc<dyn LogSink>>,
    pub(crate) sink_runtime: SinkRuntimeRegistry,
    pub(crate) log_maintenance: Mutex<Option<LogMaintenanceWorker>>,
    pub(crate) metric_store_runtime: Mutex<Option<Box<dyn MetricStoreRuntime>>>,
    pub(crate) metric_sinks: Vec<Arc<dyn MetricSink>>,
    pub(crate) host_metric_sinks: Vec<Arc<dyn HostMetricSink>>,
    pub(crate) stats_reader: Arc<dyn CgroupStatsReader>,
    pub(crate) network_stats_reader: Arc<dyn WorkloadNetworkStatsReader>,
    pub(crate) host_telemetry: HostTelemetryDependencies,
    pub(crate) network_provider: Arc<dyn NetworkProvider>,
    pub(crate) health_prober: Arc<dyn HealthProber>,
    pub(crate) volatile_root: PathBuf,
    pub(crate) mesh_identity: MeshIdentity,
    instance_id: NodeInstanceId,
    pub(crate) running_version: Version,
    pub(crate) monotonic_clock: Arc<dyn Clock>,
    pub(crate) status_clock: Arc<dyn StatusClock>,
    pub(crate) node_upgrade: Option<NodeUpgradeDependencies>,
    pub(crate) api_settings: ServerSettings,
    pub(crate) admin_api_settings: Option<ServerSettings>,
    pub(crate) firewall_settings: firewall::FirewallSettings,
    pub(crate) launch_config_admin: Option<Arc<dyn server::LaunchConfigAdmin>>,
    pub(crate) webhook_backend: Option<Arc<dyn webhook::WebhookDeliveryBackend>>,
    pub(crate) admission: Option<AdmissionDependencies>,
    pub(crate) settings: DaemonRoleSettings,
    pub(crate) store: Mutex<Option<Arc<dyn Store>>>,
    leader_workload: Option<Arc<dyn LeaderWorkload>>,
}

impl<MeshBackendType, FirewallBackendType, BridgeBackendType>
    DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>
{
    /// Binds all production adapters without starting tasks or processes.
    pub fn new(
        dependencies: DaemonRoleDependencies<
            MeshBackendType,
            FirewallBackendType,
            BridgeBackendType,
        >,
        settings: DaemonRoleSettings,
    ) -> Self {
        Self {
            agent_store: dependencies.agent_store,
            mesh_backend: Mutex::new(Some(dependencies.mesh_backend)),
            firewall_backend: Mutex::new(Some(dependencies.firewall_backend)),
            bridge_backend: Mutex::new(Some(dependencies.bridge_backend)),
            workload_network_mode: dependencies.workload_network_mode,
            system_host_ports: dependencies.system_host_ports,
            dns_server_binder: dependencies.dns_server_binder,
            dns_plugin_settings: dependencies.dns_plugin_settings,
            dns_upstream_settings: dependencies.dns_upstream_settings,
            workload_runtime: dependencies.workload_runtime,
            artifact_store: dependencies.artifact_store,
            artifact_archives: dependencies.artifact_archives,
            value_sources: None,
            log_store_runtime: Mutex::new(Some(dependencies.log_store_runtime)),
            log_sinks: dependencies.log_sinks,
            sink_runtime: SinkRuntimeRegistry::default(),
            log_maintenance: Mutex::new(None),
            metric_store_runtime: Mutex::new(Some(dependencies.metric_store_runtime)),
            metric_sinks: dependencies.metric_sinks,
            host_metric_sinks: dependencies.host_metric_sinks,
            stats_reader: dependencies.stats_reader,
            network_stats_reader: dependencies.network_stats_reader,
            host_telemetry: dependencies.host_telemetry,
            network_provider: dependencies.network_provider,
            health_prober: dependencies.health_prober,
            volatile_root: dependencies.volatile_root,
            mesh_identity: dependencies.mesh_identity,
            instance_id: dependencies.instance_id,
            running_version: dependencies.running_version,
            monotonic_clock: dependencies.monotonic_clock,
            status_clock: dependencies.status_clock,
            node_upgrade: dependencies.node_upgrade,
            api_settings: dependencies.api_settings,
            admin_api_settings: dependencies.admin_api_settings,
            firewall_settings: dependencies.firewall_settings,
            launch_config_admin: None,
            webhook_backend: None,
            admission: None,
            settings,
            store: Mutex::new(None),
            leader_workload: None,
        }
    }

    /// Enables deployment-time resolution of external environment and secret references.
    pub fn with_value_source_resolver(mut self, resolver: Arc<dyn ValueSourceResolver>) -> Self {
        self.value_sources = Some(resolver);
        self
    }

    /// Attaches the workload started for each successfully fenced leadership term.
    pub fn with_leader_workload(mut self, workload: Arc<dyn LeaderWorkload>) -> Self {
        self.leader_workload = Some(workload);
        self
    }

    /// Shares the outbound webhook transport with the authenticated test endpoint.
    pub fn with_webhook_backend(
        mut self,
        backend: Arc<dyn webhook::WebhookDeliveryBackend>,
    ) -> Self {
        self.webhook_backend = Some(backend);
        self
    }

    /// Enables node-local protected launch-document updates through Admin.
    pub fn with_launch_config_admin(mut self, admin: Arc<dyn server::LaunchConfigAdmin>) -> Self {
        self.launch_config_admin = Some(admin);
        self
    }

    /// Attaches control-plane certificate and cluster-secret material for joins.
    pub fn with_admission_dependencies(mut self, dependencies: AdmissionDependencies) -> Self {
        self.admission = Some(dependencies);
        self
    }

    /// Attaches node-local log rollover, backup, and retention to the agent lifetime.
    pub fn with_log_maintenance(mut self, worker: LogMaintenanceWorker) -> Self {
        self.log_maintenance = Mutex::new(Some(worker));
        self
    }

    /// Returns the shared node-local log-delivery health registry for stats APIs.
    pub fn sink_runtime_registry(&self) -> SinkRuntimeRegistry {
        self.sink_runtime.clone()
    }

    pub(crate) fn instance_id(&self) -> &NodeInstanceId {
        &self.instance_id
    }
}

#[async_trait]
impl<MeshBackendType, FirewallBackendType, BridgeBackendType> RoleFactory
    for DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>
where
    MeshBackendType: MeshBackend + 'static,
    FirewallBackendType: FirewallBackend + 'static,
    BridgeBackendType: WorkloadBridgeBackend + 'static,
{
    async fn start(
        &self,
        plan: &DaemonPlan,
        spec: &RoleSpec,
    ) -> Result<Box<dyn RoleRuntime>, RoleError> {
        match spec.role {
            DaemonRole::Agent => start_agent(self, plan, spec).await,
            DaemonRole::Controller if spec.node_role.is_control_plane() => {
                self.start_controller(spec).await
            }
            DaemonRole::Controller => Err(RoleError::new(
                "controller role requires a control-plane-capable node",
            )),
        }
    }
}

impl<MeshBackendType, FirewallBackendType, BridgeBackendType>
    DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>
where
    MeshBackendType: MeshBackend + 'static,
    FirewallBackendType: FirewallBackend + 'static,
    BridgeBackendType: WorkloadBridgeBackend + 'static,
{
    async fn start_controller(&self, spec: &RoleSpec) -> Result<Box<dyn RoleRuntime>, RoleError> {
        let store = self
            .store
            .lock()
            .map_err(|_| RoleError::new("shared store lock was poisoned"))?
            .clone()
            .ok_or_else(|| RoleError::new("controller started before the agent store"))?;
        let identity = LeaderIdentity {
            node_id: spec.node_id.clone(),
            instance_id: self.instance_id.clone(),
        };
        let leader_key = Keyspace::new(&spec.cluster_id).leader();
        let elector: Arc<dyn LeaderElector> =
            Arc::new(StoreLeaderElector::new(store.clone(), leader_key.clone()));
        let lease = elector
            .campaign(identity.clone(), self.settings.leadership_ttl)
            .await
            .map_err(|error| role_error("campaign for initial controller leadership", error))?;
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let clock = self.monotonic_clock.clone();
        let task_clock = clock.clone();
        let settings = self.settings;
        let workload = self.leader_workload.clone();
        let task = tokio::spawn(async move {
            run_leadership(
                store,
                leader_key,
                elector,
                identity,
                lease,
                workload,
                task_clock,
                settings,
                shutdown_receiver,
            )
            .await
        });
        Ok(Box::new(ControllerRoleRuntime {
            shutdown,
            tasks: vec![task],
            clock,
            shutdown_grace: settings.role_shutdown_grace,
        }))
    }
}

struct ControllerRoleRuntime {
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<Result<(), RoleError>>>,
    clock: Arc<dyn Clock>,
    shutdown_grace: Duration,
}

#[async_trait]
impl RoleRuntime for ControllerRoleRuntime {
    async fn wait_for_failure(&mut self) -> RoleError {
        wait_for_role_task(&mut self.tasks).await
    }

    async fn shutdown(mut self: Box<Self>) -> Result<(), RoleError> {
        let _ = self.shutdown.send(true);
        let failures =
            shutdown_role_tasks(&mut self.tasks, self.clock.as_ref(), self.shutdown_grace).await;
        finish_shutdown(failures)
    }
}

impl Drop for ControllerRoleRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn finish_shutdown(failures: Vec<String>) -> Result<(), RoleError> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(RoleError::new(failures.join("; ")))
    }
}

pub(crate) fn role_error(action: &str, error: impl std::fmt::Display) -> RoleError {
    RoleError::new(format!("failed to {action}: {error}"))
}
