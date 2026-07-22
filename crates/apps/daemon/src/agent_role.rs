use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use logs::{NodeLogQueryStore, NodeTrafficQueryStore};
use node_agent::{
    AUTHORITATIVE_DNS_PORT, AuthoritativeDnsResolver, DnsResourceAgent, DnsServerSettings,
    FirewallBackend, MeshBackend, NodeRegistration, WorkloadBridgeBackend,
};
use tokio::sync::watch;

use crate::agent_lifecycle::{AgentRoleRuntime, AgentStartupRuntimes};
use crate::agent_network::{build_bridge_agent, build_firewall_agent, build_mesh_agent};
use crate::artifact_replication::build_artifact_replication_agent;
use crate::cluster_query_clients;
use crate::config_view::masked_cluster_config;
use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::log_delivery::build_sink_workers;
use crate::metric_delivery::{build_host_metric_sink_workers, build_metric_sink_workers};
use crate::stats_metric_sampler::StatsMetricSampler;
use crate::workload_agents::{
    build_assignment_agent, build_health_agent, build_host_telemetry_agent, build_log_agent,
    build_node_registry_agent, build_node_upgrade_agent, build_stats_agent,
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
    let local_log_queries = runtimes.log_query_store();
    let local_traffic_queries = runtimes.traffic_query_store();
    let controller_stats = Arc::new(logs::LiveControllerStats::new(
        runtimes.log_stats_store(),
        factory
            .log_sinks
            .iter()
            .map(|sink| sink.id().clone())
            .collect(),
        factory.sink_runtime.clone(),
        env!("CARGO_PKG_VERSION"),
    )) as Arc<dyn logs::ControllerStatsProvider>;
    let backup_stats = log_maintenance
        .as_ref()
        .map(|worker| Arc::new(worker.stats_handle()) as Arc<dyn logs::BackupStatsProvider>);
    let stats_metric_queries = runtimes.stats_metric_store();
    let stats_metric_sampler = match StatsMetricSampler::new(
        spec.node_id.clone(),
        stats_metric_queries.clone(),
        controller_stats.clone(),
        backup_stats.clone(),
        factory.monotonic_clock.clone(),
        factory.status_clock.clone(),
        factory.settings.stats_poll_interval,
    ) {
        Ok(sampler) => sampler,
        Err(error) => {
            return runtimes
                .fail(role_error("construct operational stats sampler", error))
                .await;
        }
    };
    if let Err(error) = stats_metric_sampler.collect_once().await {
        return runtimes
            .fail(role_error(
                "establish initial operational stats sample",
                error,
            ))
            .await;
    }
    let workload_metric_queries = runtimes.metric_query_store();
    let host_metric_queries = runtimes.host_metric_query_store();
    let cluster_query_client = match cluster_query_clients::log_query_store(
        plan,
        &factory.api_settings,
        local_log_queries.clone(),
        local_traffic_queries.clone(),
    ) {
        Ok(queries) => Arc::new(queries),
        Err(error) => return runtimes.fail(error).await,
    };
    let cluster_log_queries = cluster_query_client.clone() as Arc<dyn NodeLogQueryStore>;
    let cluster_traffic_queries = cluster_query_client as Arc<dyn NodeTrafficQueryStore>;
    let cluster_log_nodes = plan.cluster().nodes.keys().cloned().collect::<Vec<_>>();
    let cluster_traffic_nodes = cluster_log_nodes.clone();
    let cluster_metric_queries = match cluster_query_clients::metric_query_store(
        plan,
        &factory.api_settings,
        workload_metric_queries.clone(),
        host_metric_queries.clone(),
    ) {
        Ok(queries) => Arc::new(queries) as Arc<dyn server::NodeMetricQueryStore>,
        Err(error) => return runtimes.fail(error).await,
    };
    let cluster_metric_nodes = cluster_log_nodes.clone();
    let cluster_stats_queries = match cluster_query_clients::stats_query_store(
        plan,
        &factory.api_settings,
        controller_stats.clone(),
        stats_metric_queries.clone(),
    ) {
        Ok(queries) => Arc::new(queries) as Arc<dyn server::NodeStatsQueryStore>,
        Err(error) => return runtimes.fail(error).await,
    };
    let cluster_stats_nodes = cluster_log_nodes.clone();
    let exec_sessions = if spec.workload_enabled {
        match cluster_query_clients::exec_sessions(factory, plan, spec, store.clone()) {
            Ok(sessions) => Some(Arc::new(sessions) as Arc<dyn server::ClusterExecSessions>),
            Err(error) => return runtimes.fail(error).await,
        }
    } else {
        None
    };
    let api_server = match server::ApiServer::new(
        store.clone(),
        plan.cluster().cluster_id.clone(),
        factory.api_settings.clone(),
    )
    .map(|server| {
        let server = server
            .with_cluster_config(masked_cluster_config(plan.cluster(), &spec.node_id))
            .with_artifact_archive_store(factory.artifact_archives.clone())
            .with_artifact_store(factory.artifact_store.clone())
            .with_firewall_settings(factory.firewall_settings.clone())
            .with_log_query_store(local_log_queries)
            .with_cluster_log_query_store(cluster_log_nodes, cluster_log_queries)
            .with_traffic_query_stores(
                local_traffic_queries,
                cluster_traffic_nodes,
                cluster_traffic_queries,
            )
            .with_metric_query_stores(
                spec.node_id.clone(),
                workload_metric_queries,
                host_metric_queries,
            )
            .with_cluster_metric_query_store(cluster_metric_nodes, cluster_metric_queries)
            .with_stats_providers(
                controller_stats,
                backup_stats,
                stats_metric_queries,
                cluster_stats_nodes,
                cluster_stats_queries,
            );
        let server = match exec_sessions {
            Some(sessions) => server.with_exec_sessions(sessions),
            None => server,
        };
        match &factory.webhook_backend {
            Some(backend) => server.with_webhook_backend(backend.clone()),
            None => server,
        }
    }) {
        Ok(server) => match server.bind().await {
            Ok(server) => server,
            Err(error) => {
                return runtimes.fail(role_error("bind operator API", error)).await;
            }
        },
        Err(error) => {
            return runtimes
                .fail(role_error("construct operator API", error))
                .await;
        }
    };
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
    let mut assignment_agent = if spec.workload_enabled {
        match build_assignment_agent(factory, plan, spec, store.clone(), runtimes.log_store()) {
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
    let node_upgrade_agent = match build_node_upgrade_agent(factory, plan, spec, store.clone()) {
        Ok(agent) => agent,
        Err(error) => return runtimes.fail(error).await,
    };
    let node_registry_agent = match build_node_registry_agent(factory, plan, spec, store.clone()) {
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
    let node_registration = match node_registry_agent.register().await {
        Ok(registration) => registration,
        Err(error) => {
            return runtimes
                .fail(role_error("register node liveness", error))
                .await;
        }
    };
    let artifact_replication_agent = match build_artifact_replication_agent(
        factory,
        plan,
        spec,
        store.clone(),
        node_registration.session_id(),
    ) {
        Ok(agent) => agent,
        Err(error) => {
            return fail_after_registration(runtimes, node_registration, error).await;
        }
    };
    if let Err(error) = artifact_replication_agent.reconcile_once().await {
        return fail_after_registration(
            runtimes,
            node_registration,
            role_error("establish initial artifact replication", error),
        )
        .await;
    }
    assignment_agent = assignment_agent
        .map(|agent| agent.with_artifact_replication(artifact_replication_agent.clone()));
    if let Some(agent) = assignment_agent.as_ref()
        && let Err(error) = agent.reconcile_once().await
    {
        return fail_after_registration(
            runtimes,
            node_registration,
            role_error("establish initial workload assignments", error),
        )
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
        return fail_after_registration(runtimes, node_registration, error).await;
    }
    let (shutdown, bridge_shutdown) = watch::channel(false);
    let mesh_shutdown = bridge_shutdown.clone();
    let dns_resource_shutdown = bridge_shutdown.clone();
    let firewall_shutdown = bridge_shutdown.clone();
    let dns_server_shutdown = bridge_shutdown.clone();
    let assignment_shutdown = bridge_shutdown.clone();
    let artifact_replication_shutdown = bridge_shutdown.clone();
    let health_shutdown = bridge_shutdown.clone();
    let log_shutdown = bridge_shutdown.clone();
    let stats_shutdown = bridge_shutdown.clone();
    let host_telemetry_shutdown = bridge_shutdown.clone();
    let node_upgrade_shutdown = bridge_shutdown.clone();
    let node_registry_shutdown = bridge_shutdown.clone();
    let api_shutdown = bridge_shutdown.clone();
    let stats_metric_shutdown = bridge_shutdown.clone();
    let api_task = tokio::spawn(async move {
        api_server
            .serve(api_shutdown)
            .await
            .map_err(|error| role_error("serve operator API", error))
    });
    let node_registry_task = tokio::spawn(async move {
        node_registry_agent
            .run_registered(node_registration, node_registry_shutdown)
            .await
            .map_err(|error| role_error("run node registry agent", error))
    });
    let artifact_replication_task = tokio::spawn(async move {
        artifact_replication_agent
            .run(artifact_replication_shutdown)
            .await
            .map_err(|error| role_error("run artifact replication agent", error))
    });
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
        artifact_replication_task,
        node_registry_task,
        api_task,
        tokio::spawn(async move {
            stats_metric_sampler.run(stats_metric_shutdown).await;
            Ok(())
        }),
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
    if let Some(agent) = node_upgrade_agent {
        tasks.push(tokio::spawn(async move {
            agent
                .run(node_upgrade_shutdown)
                .await
                .map_err(|error| role_error("run node upgrade agent", error))
        }));
    }
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

async fn fail_after_registration<T>(
    runtimes: AgentStartupRuntimes,
    registration: NodeRegistration,
    error: RoleError,
) -> Result<T, RoleError> {
    let error = match registration.close().await {
        Ok(()) => error,
        Err(close_error) => RoleError::new(format!(
            "{}; failed to roll back node liveness: {close_error}",
            error.detail()
        )),
    };
    runtimes.fail(error).await
}
