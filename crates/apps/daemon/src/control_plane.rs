use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cluster::{StoreProvider, StoreRuntime, StoreShutdown, StoreStartMode};
use kernel_api::NodeInstanceId;
use kernel_controller::{LeaderElector, LeaderIdentity, LeadershipLease, StoreLeaderElector};
use kernel_store::{Clock, Keyspace, Store};
use node_agent::{MeshBackend, MeshIdentity, MeshPlanner, MeshResourceAgent, StatusClock};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{DaemonPlan, DaemonRole, RoleError, RoleFactory, RoleRuntime, RoleSpec};

/// Time bounds for mesh resync, leadership, and graceful store shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneRoleSettings {
    mesh_resync_interval: Duration,
    leadership_ttl: Duration,
    leadership_keepalive_interval: Duration,
    campaign_retry_interval: Duration,
    store_shutdown_grace: Duration,
}

impl ControlPlaneRoleSettings {
    /// Creates bounded settings and rejects hot loops or expired leadership.
    pub fn new(
        mesh_resync_interval: Duration,
        leadership_ttl: Duration,
        leadership_keepalive_interval: Duration,
        campaign_retry_interval: Duration,
        store_shutdown_grace: Duration,
    ) -> Result<Self, RoleError> {
        if mesh_resync_interval.is_zero()
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
            leadership_ttl: Duration::from_secs(15),
            leadership_keepalive_interval: Duration::from_secs(5),
            campaign_retry_interval: Duration::from_secs(1),
            store_shutdown_grace: Duration::from_secs(10),
        }
    }
}

/// Production adapters and identities required by the concrete role factory.
pub struct ControlPlaneRoleDependencies<Backend> {
    /// Provisioning boundary for the node-local cluster store member.
    pub provider: Arc<dyn StoreProvider>,
    /// Explicit bootstrap, join, or restart decision for the local member.
    pub store_start_mode: StoreStartMode,
    /// Host-network adapter that applies exact WireGuard and route state.
    pub mesh_backend: Backend,
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
pub struct ControlPlaneRoleFactory<Backend> {
    provider: Arc<dyn StoreProvider>,
    store_start_mode: StoreStartMode,
    mesh_backend: Mutex<Option<Backend>>,
    mesh_identity: MeshIdentity,
    instance_id: NodeInstanceId,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
    settings: ControlPlaneRoleSettings,
    store: Mutex<Option<Arc<dyn Store>>>,
}

impl<Backend> ControlPlaneRoleFactory<Backend> {
    /// Binds all production adapters without starting tasks or processes.
    pub fn new(
        dependencies: ControlPlaneRoleDependencies<Backend>,
        settings: ControlPlaneRoleSettings,
    ) -> Self {
        Self {
            provider: dependencies.provider,
            store_start_mode: dependencies.store_start_mode,
            mesh_backend: Mutex::new(Some(dependencies.mesh_backend)),
            mesh_identity: dependencies.mesh_identity,
            instance_id: dependencies.instance_id,
            monotonic_clock: dependencies.monotonic_clock,
            status_clock: dependencies.status_clock,
            settings,
            store: Mutex::new(None),
        }
    }
}

#[async_trait]
impl<Backend> RoleFactory for ControlPlaneRoleFactory<Backend>
where
    Backend: MeshBackend + 'static,
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

impl<Backend> ControlPlaneRoleFactory<Backend>
where
    Backend: MeshBackend + 'static,
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
        if let Err(error) = agent.reconcile_once().await {
            return fail_after_store_start(
                runtime,
                role_error("establish initial mesh snapshot", error),
            )
            .await;
        }

        *self
            .store
            .lock()
            .map_err(|_| RoleError::new("shared store lock was poisoned"))? = Some(store);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let task = tokio::spawn(async move { agent.run(shutdown_receiver).await });
        Ok(Box::new(AgentRoleRuntime {
            shutdown,
            task: Some(task),
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
        let elector = StoreLeaderElector::new(store, Keyspace::new(&spec.cluster_id).leader());
        let lease = elector
            .campaign(identity.clone(), self.settings.leadership_ttl)
            .await
            .map_err(|error| role_error("campaign for initial controller leadership", error))?;
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let clock = self.monotonic_clock.clone();
        let settings = self.settings;
        let task = tokio::spawn(async move {
            run_leadership(elector, identity, lease, clock, settings, shutdown_receiver).await
        });
        Ok(Box::new(ControllerRoleRuntime {
            shutdown,
            task: Some(task),
        }))
    }
}

fn build_mesh_agent<Backend>(
    factory: &ControlPlaneRoleFactory<Backend>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
    backend: Backend,
) -> Result<MeshResourceAgent<Backend>, RoleError>
where
    Backend: MeshBackend,
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

async fn run_leadership(
    elector: StoreLeaderElector,
    identity: LeaderIdentity,
    mut lease: Option<Box<dyn LeadershipLease>>,
    clock: Arc<dyn Clock>,
    settings: ControlPlaneRoleSettings,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RoleError> {
    loop {
        if *shutdown.borrow() {
            return resign(lease).await;
        }
        if let Some(active) = lease.as_ref() {
            let keepalive_at = clock
                .now()
                .saturating_add(settings.leadership_keepalive_interval);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return resign(lease).await;
                    }
                }
                () = clock.sleep_until(keepalive_at) => {
                    if active.keep_alive().await.is_err() {
                        lease = None;
                    }
                }
            }
        } else {
            let retry_at = clock.now().saturating_add(settings.campaign_retry_interval);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = clock.sleep_until(retry_at) => {
                    lease = elector
                        .campaign(identity.clone(), settings.leadership_ttl)
                        .await
                        .map_err(|error| role_error("retry controller leadership campaign", error))?;
                }
            }
        }
    }
}

async fn resign(lease: Option<Box<dyn LeadershipLease>>) -> Result<(), RoleError> {
    if let Some(lease) = lease {
        lease
            .resign()
            .await
            .map_err(|error| role_error("resign controller leadership", error))?;
    }
    Ok(())
}

struct AgentRoleRuntime {
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<Result<(), node_agent::MeshResourceError>>>,
    store_runtime: Option<Box<dyn StoreRuntime>>,
    clock: Arc<dyn Clock>,
    shutdown_grace: Duration,
}

#[async_trait]
impl RoleRuntime for AgentRoleRuntime {
    async fn shutdown(mut self: Box<Self>) -> Result<(), RoleError> {
        let _ = self.shutdown.send(true);
        let mut failures = Vec::new();
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(format!("mesh agent failed: {error}")),
                Err(error) => failures.push(format!("mesh agent task failed: {error}")),
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
        if let Some(task) = self.task.as_ref() {
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

fn role_error(action: &str, error: impl std::fmt::Display) -> RoleError {
    RoleError::new(format!("failed to {action}: {error}"))
}
