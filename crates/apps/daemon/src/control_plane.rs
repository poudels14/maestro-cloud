use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cluster::{StoreProvider, StoreStartMode};
use kernel_api::NodeInstanceId;
use kernel_controller::{FencedStore, LeaderElector, LeaderIdentity, StoreLeaderElector};
use kernel_store::{Clock, Keyspace, Store};
use logs::LogStoreRuntime;
use node_agent::{
    DnsServerBinder, FirewallBackend, HealthProber, MeshBackend, MeshIdentity, StatusClock,
    WorkloadBridgeBackend,
};
use runtime::{NetworkProvider, WorkloadRuntime};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::agent_role::start_agent;
use crate::leadership::run_leadership;
use crate::{DaemonPlan, DaemonRole, RoleError, RoleFactory, RoleRuntime, RoleSpec};

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

/// Time bounds for node resync, leadership, and graceful store shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonRoleSettings {
    pub(crate) bridge_resync_interval: Duration,
    pub(crate) mesh_resync_interval: Duration,
    pub(crate) dns_resync_interval: Duration,
    pub(crate) firewall_resync_interval: Duration,
    pub(crate) assignment_resync_interval: Duration,
    pub(crate) health_poll_interval: Duration,
    pub(crate) log_poll_interval: Duration,
    pub(crate) max_log_frames_per_workload: usize,
    pub(crate) workload_stop_timeout: Duration,
    pub(crate) restart_backoff_base: Duration,
    pub(crate) restart_backoff_max: Duration,
    pub(crate) leadership_ttl: Duration,
    pub(crate) leadership_keepalive_interval: Duration,
    pub(crate) campaign_retry_interval: Duration,
    pub(crate) store_shutdown_grace: Duration,
}

impl DaemonRoleSettings {
    /// Creates bounded settings and rejects hot loops or expired leadership.
    pub fn new(
        bridge_resync_interval: Duration,
        mesh_resync_interval: Duration,
        dns_resync_interval: Duration,
        firewall_resync_interval: Duration,
        health_poll_interval: Duration,
        log_poll_interval: Duration,
        max_log_frames_per_workload: usize,
        leadership_ttl: Duration,
        leadership_keepalive_interval: Duration,
        campaign_retry_interval: Duration,
        store_shutdown_grace: Duration,
    ) -> Result<Self, RoleError> {
        if bridge_resync_interval.is_zero()
            || mesh_resync_interval.is_zero()
            || dns_resync_interval.is_zero()
            || firewall_resync_interval.is_zero()
            || health_poll_interval.is_zero()
            || log_poll_interval.is_zero()
            || max_log_frames_per_workload == 0
            || leadership_ttl.is_zero()
            || leadership_keepalive_interval.is_zero()
            || campaign_retry_interval.is_zero()
            || store_shutdown_grace.is_zero()
            || leadership_keepalive_interval >= leadership_ttl
        {
            return Err(RoleError::new(
                "daemon intervals and log frame bounds must be non-zero, and leadership keepalive must precede TTL",
            ));
        }
        Ok(Self {
            bridge_resync_interval,
            mesh_resync_interval,
            dns_resync_interval,
            firewall_resync_interval,
            assignment_resync_interval: Duration::from_secs(30),
            health_poll_interval,
            log_poll_interval,
            max_log_frames_per_workload,
            workload_stop_timeout: Duration::from_secs(10),
            restart_backoff_base: Duration::from_secs(5),
            restart_backoff_max: Duration::from_secs(60),
            leadership_ttl,
            leadership_keepalive_interval,
            campaign_retry_interval,
            store_shutdown_grace,
        })
    }
}

impl Default for DaemonRoleSettings {
    fn default() -> Self {
        Self {
            bridge_resync_interval: Duration::from_secs(30),
            mesh_resync_interval: Duration::from_secs(30),
            dns_resync_interval: Duration::from_secs(30),
            firewall_resync_interval: Duration::from_secs(30),
            assignment_resync_interval: Duration::from_secs(30),
            health_poll_interval: Duration::from_secs(5),
            log_poll_interval: Duration::from_secs(1),
            max_log_frames_per_workload: 1_000,
            workload_stop_timeout: Duration::from_secs(10),
            restart_backoff_base: Duration::from_secs(5),
            restart_backoff_max: Duration::from_secs(60),
            leadership_ttl: Duration::from_secs(15),
            leadership_keepalive_interval: Duration::from_secs(5),
            campaign_retry_interval: Duration::from_secs(1),
            store_shutdown_grace: Duration::from_secs(10),
        }
    }
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
    /// UDP/TCP listener binder for the node-local authoritative DNS server.
    pub dns_server_binder: Arc<dyn DnsServerBinder>,
    /// Native backend used for workload lifecycle, adoption, and events.
    pub workload_runtime: Arc<dyn WorkloadRuntime>,
    /// Owned normalized-log storage runtime for this node.
    pub log_store_runtime: Box<dyn LogStoreRuntime>,
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
    /// Monotonic clock shared by resync, leadership, and shutdown deadlines.
    pub monotonic_clock: Arc<dyn Clock>,
    /// Wall clock used only for status condition transition timestamps.
    pub status_clock: Arc<dyn StatusClock>,
}

/// Concrete daemon factory composing node agents and control-plane leader work when declared.
pub struct DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType> {
    pub(crate) agent_store: AgentStore,
    pub(crate) mesh_backend: Mutex<Option<MeshBackendType>>,
    pub(crate) firewall_backend: Mutex<Option<FirewallBackendType>>,
    pub(crate) bridge_backend: Mutex<Option<BridgeBackendType>>,
    pub(crate) dns_server_binder: Arc<dyn DnsServerBinder>,
    pub(crate) workload_runtime: Arc<dyn WorkloadRuntime>,
    pub(crate) log_store_runtime: Mutex<Option<Box<dyn LogStoreRuntime>>>,
    pub(crate) network_provider: Arc<dyn NetworkProvider>,
    pub(crate) health_prober: Arc<dyn HealthProber>,
    pub(crate) volatile_root: PathBuf,
    pub(crate) mesh_identity: MeshIdentity,
    instance_id: NodeInstanceId,
    pub(crate) monotonic_clock: Arc<dyn Clock>,
    pub(crate) status_clock: Arc<dyn StatusClock>,
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
            dns_server_binder: dependencies.dns_server_binder,
            workload_runtime: dependencies.workload_runtime,
            log_store_runtime: Mutex::new(Some(dependencies.log_store_runtime)),
            network_provider: dependencies.network_provider,
            health_prober: dependencies.health_prober,
            volatile_root: dependencies.volatile_root,
            mesh_identity: dependencies.mesh_identity,
            instance_id: dependencies.instance_id,
            monotonic_clock: dependencies.monotonic_clock,
            status_clock: dependencies.status_clock,
            settings,
            store: Mutex::new(None),
            leader_workload: None,
        }
    }

    /// Attaches the workload started for each successfully fenced leadership term.
    pub fn with_leader_workload(mut self, workload: Arc<dyn LeaderWorkload>) -> Self {
        self.leader_workload = Some(workload);
        self
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
                clock,
                settings,
                shutdown_receiver,
            )
            .await
        });
        Ok(Box::new(ControllerRoleRuntime {
            shutdown,
            task: Some(task),
        }))
    }
}

struct ControllerRoleRuntime {
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<Result<(), RoleError>>>,
}

#[async_trait]
impl RoleRuntime for ControllerRoleRuntime {
    async fn shutdown(mut self: Box<Self>) -> Result<(), RoleError> {
        let _ = self.shutdown.send(true);
        let failures = match self.task.take() {
            Some(task) => match task.await {
                Ok(Ok(())) => Vec::new(),
                Ok(Err(error)) => vec![error.to_string()],
                Err(error) => vec![format!("leadership task failed: {error}")],
            },
            None => Vec::new(),
        };
        finish_shutdown(failures)
    }
}

impl Drop for ControllerRoleRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.as_ref() {
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
