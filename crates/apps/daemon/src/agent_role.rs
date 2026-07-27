use std::sync::Arc;

use node_agent::{FirewallBackend, MeshBackend, NodeRegistration, WorkloadBridgeBackend};

use crate::agent_api::{AgentApiInputs, bind_agent_api};
use crate::agent_lifecycle::{AgentRoleRuntime, AgentStartupRuntimes};
use crate::agent_network::prepare_agent_network;
use crate::agent_tasks::{AgentTaskInputs, spawn_agent_tasks};
use crate::artifact_replication::build_artifact_replication_agent;
use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::join_activation::activate_joined_member;
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
    let (store, store_runtime, joined_member) = match &factory.agent_store {
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
            let joined_member = match start_mode {
                cluster::StoreStartMode::Join(ticket) => Some((provider.clone(), ticket.clone())),
                cluster::StoreStartMode::Bootstrap | cluster::StoreStartMode::Restart => None,
            };
            (runtime.store(), Some(runtime), joined_member)
        }
        AgentStore::Remote(store) => (store.clone(), None, None),
    };
    let runtimes =
        AgentStartupRuntimes::new(store_runtime, log_store_runtime, metric_store_runtime);
    if let Some((provider, ticket)) = joined_member
        && let Err(error) = activate_joined_member(
            provider.as_ref(),
            &ticket,
            factory.monotonic_clock.as_ref(),
            factory.settings.join_activation,
        )
        .await
    {
        return runtimes
            .fail(role_error("activate joined store member", error))
            .await;
    }
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
    let api_server = match bind_agent_api(
        factory,
        plan,
        spec,
        AgentApiInputs {
            store: store.clone(),
            local_log_queries: runtimes.log_query_store(),
            local_traffic_queries: runtimes.traffic_query_store(),
            workload_metric_queries: runtimes.metric_query_store(),
            host_metric_queries: runtimes.host_metric_query_store(),
            controller_stats,
            backup_stats,
            stats_metric_queries,
        },
    )
    .await
    {
        Ok(server) => server,
        Err(error) => return runtimes.fail(error).await,
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

    let network_agents = match prepare_agent_network(
        factory,
        plan,
        spec,
        store.clone(),
        mesh_backend,
        firewall_backend,
        bridge_backend,
    )
    .await
    {
        Ok(agents) => agents,
        Err(error) => return runtimes.fail(error).await,
    };
    let mut assignment_agent = if spec.workload_enabled {
        match build_assignment_agent(
            factory,
            plan,
            spec,
            store.clone(),
            runtimes.log_store(),
            runtimes.otlp_envelope_store(),
        ) {
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
        tracing::warn!(
            node_id = %spec.node_id,
            error = %error,
            "initial runtime log snapshot failed; background collection will retry"
        );
    }
    if let Some(agent) = stats_agent.as_ref()
        && let Err(error) = agent.collect().await
    {
        tracing::warn!(
            node_id = %spec.node_id,
            error = %error,
            "initial workload stats snapshot failed; background collection will retry"
        );
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
    let (shutdown, tasks) = spawn_agent_tasks(AgentTaskInputs {
        api_server,
        node_registry_agent,
        node_registration,
        artifact_replication_agent,
        network_agents,
        stats_metric_sampler,
        assignment_agent,
        health_agent,
        log_agent,
        stats_agent,
        host_telemetry_agent,
        node_upgrade_agent,
        sink_workers,
        metric_sink_workers,
        host_metric_sink_workers,
        log_maintenance,
    })
    .into_parts();
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
