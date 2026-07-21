use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cluster::{StoreProvider, StoreRuntime, StoreShutdown, StoreStartMode};
use kernel_api::NodeInstanceId;
use kernel_controller::{FencedStore, LeaderElector, LeaderIdentity, StoreLeaderElector};
use kernel_store::{Clock, Keyspace, Store};
use node_agent::{
    FirewallBackend, MeshBackend, MeshIdentity, MeshPlanner, MeshResourceAgent, NodeFirewallAgent,
    StatusClock,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::leadership::run_leadership;
use crate::{DaemonPlan, DaemonRole, RoleError, RoleFactory, RoleRuntime, RoleSpec};

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
pub struct ControlPlaneRoleSettings {
    mesh_resync_interval: Duration,
    firewall_resync_interval: Duration,
    pub(crate) leadership_ttl: Duration,
    pub(crate) leadership_keepalive_interval: Duration,
    pub(crate) campaign_retry_interval: Duration,
    store_shutdown_grace: Duration,
}

impl ControlPlaneRoleSettings {
    /// Creates bounded settings and rejects hot loops or expired leadership.
    pub fn new(
        mesh_resync_interval: Duration,
        firewall_resync_interval: Duration,
        leadership_ttl: Duration,
        leadership_keepalive_interval: Duration,
        campaign_retry_interval: Duration,
        store_shutdown_grace: Duration,
    ) -> Result<Self, RoleError> {
        if mesh_resync_interval.is_zero()
            || firewall_resync_interval.is_zero()
            || leadership_ttl.is_zero()
            || leadership_keepalive_interval.is_zero()
            || campaign_retry_interval.is_zero()
            || store_shutdown_grace.is_zero()
            || leadership_keepalive_interval >= leadership_ttl
        {
            return Err(RoleError::new(
                "control-plane intervals must be non-zero and leadership keepalive must precede TTL",
            ));
        }
        Ok(Self {
            mesh_resync_interval,
            firewall_resync_interval,
            leadership_ttl,
            leadership_keepalive_interval,
            campaign_retry_interval,
            store_shutdown_grace,
        })
    }
}

impl Default for ControlPlaneRoleSettings {
    fn default() -> Self {
        Self {
            mesh_resync_interval: Duration::from_secs(30),
            firewall_resync_interval: Duration::from_secs(30),
            leadership_ttl: Duration::from_secs(15),
            leadership_keepalive_interval: Duration::from_secs(5),
            campaign_retry_interval: Duration::from_secs(1),
            store_shutdown_grace: Duration::from_secs(10),
        }
    }
}

/// Production adapters and identities required by the concrete role factory.
pub struct ControlPlaneRoleDependencies<MeshBackendType, FirewallBackendType> {
    /// Provisioning boundary for the node-local cluster store member.
    pub provider: Arc<dyn StoreProvider>,
    /// Explicit bootstrap, join, or restart decision for the local member.
    pub store_start_mode: StoreStartMode,
    /// Host-network adapter that applies exact WireGuard and route state.
    pub mesh_backend: MeshBackendType,
    /// Host-network adapter that applies complete node-local nftables state.
    pub firewall_backend: FirewallBackendType,
    /// Persisted node-local WireGuard identity.
    pub mesh_identity: MeshIdentity,
    /// Unique identity of this daemon process for leader election.
    pub instance_id: NodeInstanceId,
    /// Monotonic clock shared by resync, leadership, and shutdown deadlines.
    pub monotonic_clock: Arc<dyn Clock>,
    /// Wall clock used only for status condition transition timestamps.
    pub status_clock: Arc<dyn StatusClock>,
}

/// Concrete control-plane factory composing a store provider, mesh agent, and leader lease.
pub struct ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType> {
    provider: Arc<dyn StoreProvider>,
    store_start_mode: StoreStartMode,
    mesh_backend: Mutex<Option<MeshBackendType>>,
    firewall_backend: Mutex<Option<FirewallBackendType>>,
    mesh_identity: MeshIdentity,
    instance_id: NodeInstanceId,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
    settings: ControlPlaneRoleSettings,
    store: Mutex<Option<Arc<dyn Store>>>,
    leader_workload: Option<Arc<dyn LeaderWorkload>>,
}

impl<MeshBackendType, FirewallBackendType>
    ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType>
{
    /// Binds all production adapters without starting tasks or processes.
    pub fn new(
        dependencies: ControlPlaneRoleDependencies<MeshBackendType, FirewallBackendType>,
        settings: ControlPlaneRoleSettings,
    ) -> Self {
        Self {
            provider: dependencies.provider,
            store_start_mode: dependencies.store_start_mode,
            mesh_backend: Mutex::new(Some(dependencies.mesh_backend)),
            firewall_backend: Mutex::new(Some(dependencies.firewall_backend)),
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
impl<MeshBackendType, FirewallBackendType> RoleFactory
    for ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType>
where
    MeshBackendType: MeshBackend + 'static,
    FirewallBackendType: FirewallBackend + 'static,
{
    async fn start(
        &self,
        plan: &DaemonPlan,
        spec: &RoleSpec,
    ) -> Result<Box<dyn RoleRuntime>, RoleError> {
        if !spec.node_role.is_control_plane() {
            return Err(RoleError::new(
                "control-plane factory cannot start a worker-only node",
            ));
        }
        match spec.role {
            DaemonRole::Agent => self.start_agent(plan, spec).await,
            DaemonRole::Controller => self.start_controller(spec).await,
        }
    }
}

impl<MeshBackendType, FirewallBackendType>
    ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType>
where
    MeshBackendType: MeshBackend + 'static,
    FirewallBackendType: FirewallBackend + 'static,
{
    async fn start_agent(
        &self,
        plan: &DaemonPlan,
        spec: &RoleSpec,
    ) -> Result<Box<dyn RoleRuntime>, RoleError> {
        let backend = self
            .mesh_backend
            .lock()
            .map_err(|_| RoleError::new("mesh backend lock was poisoned"))?
            .take()
            .ok_or_else(|| RoleError::new("agent role was already started"))?;
        let firewall_backend = self
            .firewall_backend
            .lock()
            .map_err(|_| RoleError::new("firewall backend lock was poisoned"))?
            .take()
            .ok_or_else(|| RoleError::new("agent role was already started"))?;
        let runtime = self
            .provider
            .start(self.store_start_mode.clone())
            .await
            .map_err(|error| role_error("start local store provider", error))?;
        let store = runtime.store();

        let agent = match build_mesh_agent(self, plan, spec, store.clone(), backend) {
            Ok(agent) => agent,
            Err(error) => return fail_after_store_start(runtime, error).await,
        };
        let firewall_agent =
            match build_firewall_agent(self, plan, spec, store.clone(), firewall_backend) {
                Ok(agent) => agent,
                Err(error) => return fail_after_store_start(runtime, error).await,
            };
        if let Err(error) = agent.reconcile_once().await {
            return fail_after_store_start(
                runtime,
                role_error("establish initial mesh snapshot", error),
            )
            .await;
        }
        if let Err(error) = firewall_agent.reconcile_once().await {
            return fail_after_store_start(
                runtime,
                role_error("establish initial firewall snapshot", error),
            )
            .await;
        }

        *self
            .store
            .lock()
            .map_err(|_| RoleError::new("shared store lock was poisoned"))? = Some(store);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let firewall_shutdown = shutdown_receiver.clone();
        let mesh_task = tokio::spawn(async move {
            agent
                .run(shutdown_receiver)
                .await
                .map_err(|error| role_error("run mesh agent", error))
        });
        let firewall_task = tokio::spawn(async move {
            firewall_agent
                .run(firewall_shutdown)
                .await
                .map_err(|error| role_error("run firewall agent", error))
        });
        Ok(Box::new(AgentRoleRuntime {
            shutdown,
            tasks: vec![mesh_task, firewall_task],
            store_runtime: Some(runtime),
            clock: self.monotonic_clock.clone(),
            shutdown_grace: self.settings.store_shutdown_grace,
        }))
    }

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

fn build_mesh_agent<MeshBackendType, FirewallBackendType>(
    factory: &ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
    backend: MeshBackendType,
) -> Result<MeshResourceAgent<MeshBackendType>, RoleError>
where
    MeshBackendType: MeshBackend,
{
    let node = plan
        .cluster()
        .nodes
        .get(&spec.node_id)
        .ok_or_else(|| RoleError::new("local node disappeared from validated topology"))?;
    let planner = MeshPlanner::new(
        spec.node_id.clone(),
        factory.mesh_identity.clone(),
        plan.cluster().ports.wireguard,
    )
    .map_err(|error| role_error("build local mesh planner", error))?;
    let publication = planner
        .publication(
            node.endpoint.host_address,
            node.workload_subnet
                .to_string()
                .parse()
                .map_err(|error| role_error("convert local workload subnet", error))?,
        )
        .map_err(|error| role_error("build local mesh publication", error))?;
    MeshResourceAgent::new(
        store,
        &plan.cluster().cluster_id,
        planner,
        publication,
        backend,
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
        factory.settings.mesh_resync_interval,
    )
    .map_err(|error| role_error("construct mesh resource agent", error))
}

fn build_firewall_agent<MeshBackendType, FirewallBackendType>(
    factory: &ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
    backend: FirewallBackendType,
) -> Result<NodeFirewallAgent<FirewallBackendType>, RoleError>
where
    FirewallBackendType: FirewallBackend,
{
    NodeFirewallAgent::new(
        store,
        &plan.cluster().cluster_id,
        spec.node_id.clone(),
        backend,
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
        factory.settings.firewall_resync_interval,
    )
    .map_err(|error| role_error("construct firewall resource agent", error))
}

async fn fail_after_store_start<T>(
    runtime: Box<dyn StoreRuntime>,
    error: RoleError,
) -> Result<T, RoleError> {
    match runtime.shutdown(StoreShutdown::Immediate).await {
        Ok(()) => Err(error),
        Err(shutdown_error) => Err(RoleError::new(format!(
            "{}; failed to roll back local store: {shutdown_error}",
            error.detail()
        ))),
    }
}

struct AgentRoleRuntime {
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<Result<(), RoleError>>>,
    store_runtime: Option<Box<dyn StoreRuntime>>,
    clock: Arc<dyn Clock>,
    shutdown_grace: Duration,
}

#[async_trait]
impl RoleRuntime for AgentRoleRuntime {
    async fn shutdown(mut self: Box<Self>) -> Result<(), RoleError> {
        let _ = self.shutdown.send(true);
        let mut failures = Vec::new();
        for task in self.tasks.drain(..) {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error.to_string()),
                Err(error) => failures.push(format!("node agent task failed: {error}")),
            }
        }
        if let Some(runtime) = self.store_runtime.take() {
            let deadline = self.clock.now().saturating_add(self.shutdown_grace);
            if let Err(error) = runtime.shutdown(StoreShutdown::Graceful { deadline }).await {
                failures.push(format!("store shutdown failed: {error}"));
            }
        }
        finish_shutdown(failures)
    }
}

impl Drop for AgentRoleRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        for task in &self.tasks {
            task.abort();
        }
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
