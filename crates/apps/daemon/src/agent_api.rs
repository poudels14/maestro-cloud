use std::sync::Arc;

use kernel_store::Store;
use logs::{
    BackupStatsProvider, ControllerStatsProvider, LogQueryStore, NodeLogQueryStore,
    NodeTrafficQueryStore, StatsMetricStore, TrafficQueryStore,
};
use metrics::{HostMetricQueryStore, WorkloadMetricQueryStore};

use crate::cluster_query_clients;
use crate::config_view::masked_cluster_config;
use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::{AgentStore, DaemonPlan, RoleError, RoleSpec};

pub(crate) struct AgentApiInputs {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) local_log_queries: Arc<dyn LogQueryStore>,
    pub(crate) local_traffic_queries: Arc<dyn TrafficQueryStore>,
    pub(crate) workload_metric_queries: Arc<dyn WorkloadMetricQueryStore>,
    pub(crate) host_metric_queries: Arc<dyn HostMetricQueryStore>,
    pub(crate) controller_stats: Arc<dyn ControllerStatsProvider>,
    pub(crate) backup_stats: Option<Arc<dyn BackupStatsProvider>>,
    pub(crate) stats_metric_queries: Arc<dyn StatsMetricStore>,
}

pub(crate) async fn bind_agent_api<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    inputs: AgentApiInputs,
) -> Result<Vec<server::BoundApiServer>, RoleError> {
    let AgentApiInputs {
        store,
        local_log_queries,
        local_traffic_queries,
        workload_metric_queries,
        host_metric_queries,
        controller_stats,
        backup_stats,
        stats_metric_queries,
    } = inputs;
    let cluster_query_client = Arc::new(cluster_query_clients::log_query_store(
        plan,
        &factory.api_settings,
        local_log_queries.clone(),
        local_traffic_queries.clone(),
    )?);
    let cluster_log_queries = cluster_query_client.clone() as Arc<dyn NodeLogQueryStore>;
    let cluster_traffic_queries = cluster_query_client as Arc<dyn NodeTrafficQueryStore>;
    let cluster_log_nodes = plan.cluster().nodes.keys().cloned().collect::<Vec<_>>();
    let cluster_traffic_nodes = cluster_log_nodes.clone();
    let cluster_metric_queries = Arc::new(cluster_query_clients::metric_query_store(
        plan,
        &factory.api_settings,
        workload_metric_queries.clone(),
        host_metric_queries.clone(),
    )?) as Arc<dyn server::NodeMetricQueryStore>;
    let cluster_metric_nodes = cluster_log_nodes.clone();
    let cluster_stats_queries = Arc::new(cluster_query_clients::stats_query_store(
        plan,
        &factory.api_settings,
        controller_stats.clone(),
        stats_metric_queries.clone(),
    )?) as Arc<dyn server::NodeStatsQueryStore>;
    let cluster_stats_nodes = cluster_log_nodes.clone();
    let exec_sessions = Arc::new(cluster_query_clients::exec_sessions(
        factory,
        plan,
        spec,
        store.clone(),
    )?) as Arc<dyn server::ClusterExecSessions>;
    let admission = match (&factory.admission, &factory.agent_store) {
        (Some(dependencies), AgentStore::Managed { provider, .. }) => Some(
            dependencies
                .coordinator(plan.cluster().clone(), provider.clone(), store.clone())
                .await
                .map_err(|error| role_error("construct cluster admission coordinator", error))?,
        ),
        (Some(_), AgentStore::Remote(_)) => {
            return Err(RoleError::new(
                "worker store clients cannot host cluster admission",
            ));
        }
        (None, _) => None,
    };
    let server = server::ApiServer::new(
        store,
        plan.cluster().cluster_id.clone(),
        factory.api_settings.clone(),
    )
    .map_err(|error| role_error("construct operator API", error))?;
    let server = server
        .with_cluster_config(masked_cluster_config(plan.cluster(), &spec.node_id))
        .with_artifact_archive_store(factory.artifact_archives.clone())
        .with_build_revision_resolver(factory.build_revisions.clone())
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
    let server = match &factory.launch_config_admin {
        Some(admin) => server.with_launch_config_admin(admin.clone()),
        None => server,
    };
    let server = server.with_exec_sessions(exec_sessions);
    let server = match admission {
        Some(coordinator) => server.with_admission_coordinator(coordinator),
        None => server,
    };
    let server = match &factory.agent_store {
        AgentStore::Managed { provider, .. } => server.with_store_provider(provider.clone()),
        AgentStore::Remote(_) => server,
    };
    let server = match &factory.webhook_backend {
        Some(backend) => server.with_webhook_backend(backend.clone()),
        None => server,
    };
    let additional = match &factory.admin_api_settings {
        Some(settings) => Some(
            server
                .bind_additional(settings.clone())
                .await
                .map_err(|error| role_error("bind bridge Admin API", error))?,
        ),
        None => None,
    };
    let primary = server
        .bind()
        .await
        .map_err(|error| role_error("bind operator API", error))?;
    Ok(additional.into_iter().chain([primary]).collect())
}
