use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cluster::{StoreRuntime, StoreShutdown};
use kernel_store::{Clock, Store};
use node_agent::{FirewallBackend, MeshBackend, MeshPlanner, MeshResourceAgent, NodeFirewallAgent};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::control_plane::{ControlPlaneRoleFactory, role_error};
use crate::{DaemonPlan, RoleError, RoleRuntime, RoleSpec};

pub(crate) async fn start_agent<MeshBackendType, FirewallBackendType>(
    factory: &ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
) -> Result<Box<dyn RoleRuntime>, RoleError>
where
    MeshBackendType: MeshBackend + 'static,
    FirewallBackendType: FirewallBackend + 'static,
{
    let mesh_backend = factory
        .mesh_backend
        .lock()
        .map_err(|_| RoleError::new("mesh backend lock was poisoned"))?
        .take()
        .ok_or_else(|| RoleError::new("agent role was already started"))?;
    let firewall_backend = factory
        .firewall_backend
        .lock()
        .map_err(|_| RoleError::new("firewall backend lock was poisoned"))?
        .take()
        .ok_or_else(|| RoleError::new("agent role was already started"))?;
    let runtime = factory
        .provider
        .start(factory.store_start_mode.clone())
        .await
        .map_err(|error| role_error("start local store provider", error))?;
    let store = runtime.store();

    let mesh_agent = match build_mesh_agent(factory, plan, spec, store.clone(), mesh_backend) {
        Ok(agent) => agent,
        Err(error) => return fail_after_store_start(runtime, error).await,
    };
    let firewall_agent =
        match build_firewall_agent(factory, plan, spec, store.clone(), firewall_backend) {
            Ok(agent) => agent,
            Err(error) => return fail_after_store_start(runtime, error).await,
        };
    if let Err(error) = mesh_agent.reconcile_once().await {
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

    *factory
        .store
        .lock()
        .map_err(|_| RoleError::new("shared store lock was poisoned"))? = Some(store);
    let (shutdown, mesh_shutdown) = watch::channel(false);
    let firewall_shutdown = mesh_shutdown.clone();
    let mesh_task = tokio::spawn(async move {
        mesh_agent
            .run(mesh_shutdown)
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
        clock: factory.monotonic_clock.clone(),
        shutdown_grace: factory.settings.store_shutdown_grace,
    }))
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

fn finish_shutdown(failures: Vec<String>) -> Result<(), RoleError> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(RoleError::new(failures.join("; ")))
    }
}
