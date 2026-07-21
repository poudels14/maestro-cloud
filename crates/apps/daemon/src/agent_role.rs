use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cluster::{StoreRuntime, StoreShutdown, WIREGUARD_MTU_BYTES};
use kernel_store::{Clock, Store};
use node_agent::{
    AUTHORITATIVE_DNS_PORT, AuthoritativeDnsResolver, DnsResourceAgent, DnsServerSettings,
    FirewallBackend, MeshBackend, MeshPlanner, MeshResourceAgent, NodeFirewallAgent,
    WorkloadBridge, WorkloadBridgeAgent, WorkloadBridgeBackend,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::control_plane::{ControlPlaneRoleFactory, role_error};
use crate::{DaemonPlan, RoleError, RoleRuntime, RoleSpec};

pub(crate) async fn start_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
) -> Result<Box<dyn RoleRuntime>, RoleError>
where
    MeshBackendType: MeshBackend + 'static,
    FirewallBackendType: FirewallBackend + 'static,
    BridgeBackendType: WorkloadBridgeBackend + 'static,
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
    let bridge_backend = factory
        .bridge_backend
        .lock()
        .map_err(|_| RoleError::new("workload bridge backend lock was poisoned"))?
        .take()
        .ok_or_else(|| RoleError::new("agent role was already started"))?;
    let runtime = factory
        .provider
        .start(factory.store_start_mode.clone())
        .await
        .map_err(|error| role_error("start local store provider", error))?;
    let store = runtime.store();

    let bridge_agent = match build_bridge_agent(factory, plan, spec, bridge_backend) {
        Ok(agent) => agent,
        Err(error) => return fail_after_store_start(runtime, error).await,
    };
    let mesh_agent = match build_mesh_agent(factory, plan, spec, store.clone(), mesh_backend) {
        Ok(agent) => agent,
        Err(error) => return fail_after_store_start(runtime, error).await,
    };
    let firewall_agent =
        match build_firewall_agent(factory, plan, spec, store.clone(), firewall_backend) {
            Ok(agent) => agent,
            Err(error) => return fail_after_store_start(runtime, error).await,
        };
    let resolver = match AuthoritativeDnsResolver::new() {
        Ok(resolver) => resolver,
        Err(error) => {
            return fail_after_store_start(
                runtime,
                role_error("construct authoritative DNS resolver", error),
            )
            .await;
        }
    };
    let dns_agent = match DnsResourceAgent::new(
        store.clone(),
        &plan.cluster().cluster_id,
        spec.node_id.clone(),
        resolver.clone(),
        factory.monotonic_clock.clone(),
        factory.settings.dns_resync_interval,
    ) {
        Ok(agent) => agent,
        Err(error) => {
            return fail_after_store_start(
                runtime,
                role_error("construct DNS resource agent", error),
            )
            .await;
        }
    };
    if let Err(error) = bridge_agent.reconcile_once().await {
        return fail_after_store_start(runtime, role_error("establish workload bridge", error))
            .await;
    }
    if let Err(error) = mesh_agent.reconcile_once().await {
        return fail_after_store_start(
            runtime,
            role_error("establish initial mesh snapshot", error),
        )
        .await;
    }
    if let Err(error) = dns_agent.reconcile_once().await {
        return fail_after_store_start(
            runtime,
            role_error("establish initial DNS snapshot", error),
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

    let dns_settings = match DnsServerSettings::new(SocketAddr::new(
        IpAddr::V4(bridge_agent.desired().gateway),
        AUTHORITATIVE_DNS_PORT,
    )) {
        Ok(settings) => settings,
        Err(error) => {
            return fail_after_store_start(
                runtime,
                role_error("validate authoritative DNS listener", error),
            )
            .await;
        }
    };
    let dns_server = match factory.dns_server_binder.bind(dns_settings, resolver).await {
        Ok(server) => server,
        Err(error) => {
            return fail_after_store_start(
                runtime,
                role_error("bind authoritative DNS listener", error),
            )
            .await;
        }
    };

    let publish_store_error = {
        match factory.store.lock() {
            Ok(mut shared_store) => {
                *shared_store = Some(store);
                None
            }
            Err(_) => Some(RoleError::new("shared store lock was poisoned")),
        }
    };
    if let Some(error) = publish_store_error {
        return fail_after_store_start(runtime, error).await;
    }
    let (shutdown, bridge_shutdown) = watch::channel(false);
    let mesh_shutdown = bridge_shutdown.clone();
    let dns_resource_shutdown = bridge_shutdown.clone();
    let firewall_shutdown = bridge_shutdown.clone();
    let dns_server_shutdown = bridge_shutdown.clone();
    let bridge_task = tokio::spawn(async move {
        bridge_agent
            .run(bridge_shutdown)
            .await
            .map_err(|error| role_error("run workload bridge agent", error))
    });
    let mesh_task = tokio::spawn(async move {
        mesh_agent
            .run(mesh_shutdown)
            .await
            .map_err(|error| role_error("run mesh agent", error))
    });
    let dns_resource_task = tokio::spawn(async move {
        dns_agent
            .run(dns_resource_shutdown)
            .await
            .map_err(|error| role_error("run DNS resource agent", error))
    });
    let firewall_task = tokio::spawn(async move {
        firewall_agent
            .run(firewall_shutdown)
            .await
            .map_err(|error| role_error("run firewall agent", error))
    });
    let dns_server_task = tokio::spawn(async move {
        dns_server
            .serve(dns_server_shutdown)
            .await
            .map_err(|error| role_error("serve authoritative DNS", error))
    });
    Ok(Box::new(AgentRoleRuntime {
        shutdown,
        tasks: vec![
            bridge_task,
            mesh_task,
            dns_resource_task,
            firewall_task,
            dns_server_task,
        ],
        store_runtime: Some(runtime),
        clock: factory.monotonic_clock.clone(),
        shutdown_grace: factory.settings.store_shutdown_grace,
    }))
}

fn build_bridge_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    backend: BridgeBackendType,
) -> Result<WorkloadBridgeAgent<BridgeBackendType>, RoleError>
where
    BridgeBackendType: WorkloadBridgeBackend,
{
    let node = plan
        .cluster()
        .nodes
        .get(&spec.node_id)
        .ok_or_else(|| RoleError::new("local node disappeared from validated topology"))?;
    let gateway = node.workload_subnet.gateway_address().ok_or_else(|| {
        RoleError::new("local workload subnet has no usable workload bridge gateway")
    })?;
    let desired = WorkloadBridge::new(gateway, node.workload_subnet.prefix(), WIREGUARD_MTU_BYTES)
        .map_err(|error| role_error("build workload bridge state", error))?;
    WorkloadBridgeAgent::new(
        desired,
        backend,
        factory.monotonic_clock.clone(),
        factory.settings.bridge_resync_interval,
    )
    .map_err(|error| role_error("construct workload bridge agent", error))
}

fn build_mesh_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
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

fn build_firewall_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &ControlPlaneRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
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
