use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use cluster::WIREGUARD_MTU_BYTES;
use kernel_store::Store;
use node_agent::{
    AUTHORITATIVE_DNS_PORT, AuthoritativeDnsResolver, DnsResourceAgent, DnsServerSettings,
    FirewallBackend, MeshBackend, MeshPlanner, MeshResourceAgent, NodeFirewallAgent,
    WorkloadBridge, WorkloadBridgeAgent, WorkloadBridgeBackend,
};
use tokio::sync::watch;

use crate::agent_lifecycle::{AgentRoleRuntime, AgentStartupRuntimes};
use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::log_delivery::build_sink_workers;
use crate::metric_delivery::{build_host_metric_sink_workers, build_metric_sink_workers};
use crate::workload_agents::{
    build_assignment_agent, build_health_agent, build_host_telemetry_agent, build_log_agent,
    build_stats_agent,
};
use crate::{AgentStore, DaemonPlan, RoleError, RoleRuntime, RoleSpec};

pub(crate) async fn start_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
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
    let log_store_runtime = factory
        .log_store_runtime
        .lock()
        .map_err(|_| RoleError::new("log-store runtime lock was poisoned"))?
        .take()
        .ok_or_else(|| RoleError::new("agent role was already started"))?;
    let metric_store_runtime = factory
        .metric_store_runtime
        .lock()
        .map_err(|_| RoleError::new("metric-store runtime lock was poisoned"))?
        .take()
        .ok_or_else(|| RoleError::new("agent role was already started"))?;
    let log_maintenance = factory
        .log_maintenance
        .lock()
        .map_err(|_| RoleError::new("log-maintenance lock was poisoned"))?
        .take();
    let (store, store_runtime) = match &factory.agent_store {
        AgentStore::Managed {
            provider,
            start_mode,
        } => {
            let runtime = match provider.start(start_mode.clone()).await {
                Ok(runtime) => runtime,
                Err(error) => {
                    return AgentStartupRuntimes::new(
                        None,
                        log_store_runtime,
                        metric_store_runtime,
                    )
                    .fail(role_error("start local store provider", error))
                    .await;
                }
            };
            (runtime.store(), Some(runtime))
        }
        AgentStore::Remote(store) => (store.clone(), None),
    };
    let runtimes =
        AgentStartupRuntimes::new(store_runtime, log_store_runtime, metric_store_runtime);
    let sink_workers = match build_sink_workers(
        &factory.log_sinks,
        runtimes.log_delivery_store(),
        factory.settings.sink_worker_settings,
        factory.sink_runtime.clone(),
    ) {
        Ok(workers) => workers,
        Err(error) => return runtimes.fail(error).await,
    };
    let metric_sink_workers = match build_metric_sink_workers(
        &factory.metric_sinks,
        runtimes.metric_delivery_store(),
        factory.settings.metric_sink_worker_settings,
    ) {
        Ok(workers) => workers,
        Err(error) => return runtimes.fail(error).await,
    };
    let host_metric_sink_workers = match build_host_metric_sink_workers(
        &factory.host_metric_sinks,
        runtimes.host_metric_delivery_store(),
        factory.settings.metric_sink_worker_settings,
    ) {
        Ok(workers) => workers,
        Err(error) => return runtimes.fail(error).await,
    };

    let bridge_agent = match build_bridge_agent(factory, plan, spec, bridge_backend) {
        Ok(agent) => agent,
        Err(error) => return runtimes.fail(error).await,
    };
    let mesh_agent = match build_mesh_agent(factory, plan, spec, store.clone(), mesh_backend) {
        Ok(agent) => agent,
        Err(error) => return runtimes.fail(error).await,
    };
    let firewall_agent =
        match build_firewall_agent(factory, plan, spec, store.clone(), firewall_backend) {
            Ok(agent) => agent,
            Err(error) => return runtimes.fail(error).await,
        };
    let resolver = match AuthoritativeDnsResolver::new() {
        Ok(resolver) => resolver,
        Err(error) => {
            return runtimes
                .fail(role_error("construct authoritative DNS resolver", error))
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
            return runtimes
                .fail(role_error("construct DNS resource agent", error))
                .await;
        }
    };
    let assignment_agent = if spec.workload_enabled {
        match build_assignment_agent(factory, plan, spec, store.clone()) {
            Ok(agent) => Some(agent),
            Err(error) => return runtimes.fail(error).await,
        }
    } else {
        None
    };
    let health_agent = if spec.workload_enabled {
        match build_health_agent(factory, plan, spec, store.clone()) {
            Ok(agent) => Some(agent),
            Err(error) => return runtimes.fail(error).await,
        }
    } else {
        None
    };
    let log_agent = if spec.workload_enabled {
        match build_log_agent(factory, plan, spec, runtimes.log_store()) {
            Ok(agent) => Some(agent),
            Err(error) => return runtimes.fail(error).await,
        }
    } else {
        None
    };
    let stats_agent = if spec.workload_enabled {
        match build_stats_agent(factory, plan, spec, runtimes.metric_store()) {
            Ok(agent) => Some(agent),
            Err(error) => return runtimes.fail(error).await,
        }
    } else {
        None
    };
    let host_telemetry_agent =
        match build_host_telemetry_agent(factory, plan, spec, runtimes.host_metric_store()) {
            Ok(agent) => agent,
            Err(error) => return runtimes.fail(error).await,
        };
    if let Err(error) = bridge_agent.reconcile_once().await {
        return runtimes
            .fail(role_error("establish workload bridge", error))
            .await;
    }
    if let Err(error) = mesh_agent.reconcile_once().await {
        return runtimes
            .fail(role_error("establish initial mesh snapshot", error))
            .await;
    }
    if let Err(error) = dns_agent.reconcile_once().await {
        return runtimes
            .fail(role_error("establish initial DNS snapshot", error))
            .await;
    }
    if let Err(error) = firewall_agent.reconcile_once().await {
        return runtimes
            .fail(role_error("establish initial firewall snapshot", error))
            .await;
    }
    let dns_settings = match DnsServerSettings::new(SocketAddr::new(
        IpAddr::V4(bridge_agent.desired().gateway),
        AUTHORITATIVE_DNS_PORT,
    )) {
        Ok(settings) => settings,
        Err(error) => {
            return runtimes
                .fail(role_error("validate authoritative DNS listener", error))
                .await;
        }
    };
    let dns_server = match factory.dns_server_binder.bind(dns_settings, resolver).await {
        Ok(server) => server,
        Err(error) => {
            return runtimes
                .fail(role_error("bind authoritative DNS listener", error))
                .await;
        }
    };
    if let Some(agent) = assignment_agent.as_ref()
        && let Err(error) = agent.reconcile_once().await
    {
        return runtimes
            .fail(role_error("establish initial workload assignments", error))
            .await;
    }
    if let Some(agent) = health_agent.as_ref()
        && let Err(error) = agent.reconcile_once().await
    {
        return runtimes
            .fail(role_error("establish initial workload health", error))
            .await;
    }
    if let Some(agent) = log_agent.as_ref()
        && let Err(error) = agent.collect_once().await
    {
        return runtimes
            .fail(role_error(
                "establish initial runtime log collection",
                error,
            ))
            .await;
    }
    if let Some(agent) = stats_agent.as_ref()
        && let Err(error) = agent.collect().await
    {
        return runtimes
            .fail(role_error(
                "establish initial workload stats collection",
                error,
            ))
            .await;
    }
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
        return runtimes.fail(error).await;
    }
    let (shutdown, bridge_shutdown) = watch::channel(false);
    let mesh_shutdown = bridge_shutdown.clone();
    let dns_resource_shutdown = bridge_shutdown.clone();
    let firewall_shutdown = bridge_shutdown.clone();
    let dns_server_shutdown = bridge_shutdown.clone();
    let assignment_shutdown = bridge_shutdown.clone();
    let health_shutdown = bridge_shutdown.clone();
    let log_shutdown = bridge_shutdown.clone();
    let stats_shutdown = bridge_shutdown.clone();
    let host_telemetry_shutdown = bridge_shutdown.clone();
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
    let mut tasks = vec![
        bridge_task,
        mesh_task,
        dns_resource_task,
        firewall_task,
        dns_server_task,
    ];
    if let Some(agent) = assignment_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(assignment_shutdown)
                .await
                .map_err(|error| role_error("run assignment agent", error))
        }));
    }
    if let Some(agent) = health_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(health_shutdown)
                .await
                .map_err(|error| role_error("run workload health agent", error))
        }));
    }
    if let Some(agent) = log_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(log_shutdown)
                .await
                .map_err(|error| role_error("run runtime log agent", error))
        }));
    }
    if let Some(agent) = stats_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(stats_shutdown)
                .await
                .map_err(|error| role_error("run workload stats agent", error))
        }));
    }
    tasks.push(tokio::spawn(async move {
        host_telemetry_agent
            .run(host_telemetry_shutdown)
            .await
            .map_err(|error| role_error("run host telemetry agent", error))
    }));
    for worker in sink_workers {
        let sink_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            worker.run(sink_shutdown).await;
            Ok(())
        }));
    }
    for worker in metric_sink_workers {
        let sink_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            worker.run(sink_shutdown).await;
            Ok(())
        }));
    }
    for worker in host_metric_sink_workers {
        let sink_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            worker.run(sink_shutdown).await;
            Ok(())
        }));
    }
    if let Some(worker) = log_maintenance {
        let maintenance_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            worker.run(maintenance_shutdown).await;
            Ok(())
        }));
    }
    let owned_runtimes = runtimes.into_owned();
    Ok(Box::new(AgentRoleRuntime::new(
        shutdown,
        tasks,
        owned_runtimes,
        factory.monotonic_clock.clone(),
        factory.settings.store_shutdown_grace,
    )))
}

fn build_bridge_agent<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
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
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
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
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
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
