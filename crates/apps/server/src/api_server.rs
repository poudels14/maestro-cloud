use std::sync::Arc;

use axum::Router;
use kernel_api::{ClusterId, MaskedClusterConfig, NodeId};
use kernel_controller::{RequestDeduplicator, SystemTimestampClock};
use kernel_store::Store;
use tokio::sync::Semaphore;

use crate::auth::AuthPolicy;
use crate::{
    AppState, BoundApiServer, ClusterExecSessions, LaunchConfigAdmin, NodeMetricQueryStore,
    NodeStatsQueryStore, ServerError, ServerSettings,
};

/// Validated API application that has not yet claimed its listener.
pub struct ApiServer {
    settings: ServerSettings,
    state: AppState,
    router: Router,
}

impl ApiServer {
    /// Composes all domain routers over one cluster store.
    pub fn new(
        store: Arc<dyn Store>,
        cluster_id: ClusterId,
        settings: ServerSettings,
    ) -> Result<Self, ServerError> {
        let settings = settings.validate()?;
        let state = AppState {
            requests: RequestDeduplicator::new(store.clone()),
            timestamp_clock: Arc::new(SystemTimestampClock),
            admission_coordinator: None,
            store_provider: None,
            store,
            cluster_id,
            cluster_config: None,
            launch_config_admin: None,
            artifact_archives: None,
            artifacts: None,
            firewall_settings: None,
            log_queries: None,
            cluster_log_nodes: Arc::from([]),
            cluster_log_queries: None,
            traffic_queries: None,
            cluster_traffic_nodes: Arc::from([]),
            cluster_traffic_queries: None,
            local_metric_node: None,
            workload_metric_queries: None,
            host_metric_queries: None,
            cluster_metric_nodes: Arc::from([]),
            cluster_metric_queries: None,
            controller_stats: None,
            backup_stats: None,
            stats_metrics: None,
            cluster_stats_nodes: Arc::from([]),
            cluster_stats_queries: None,
            uptime_clock: Arc::new(logs::SystemUptimeClock::new()),
            exec_sessions: None,
            exec_relays: Arc::new(Semaphore::new(8)),
            webhook_backend: None,
        };
        let router = crate::routes::router(
            state.clone(),
            auth_policy(&settings),
            settings.panel_directory.as_deref(),
        );
        Ok(Self {
            settings,
            state,
            router,
        })
    }

    /// Enables the secret-free configuration view for authenticated operators.
    pub fn with_cluster_config(mut self, config: MaskedClusterConfig) -> Self {
        self.state.cluster_config = Some(Arc::new(config));
        self.rebuild_router();
        self
    }

    /// Enables authenticated node-local launch-document updates.
    pub fn with_launch_config_admin(mut self, admin: Arc<dyn LaunchConfigAdmin>) -> Self {
        self.state.launch_config_admin = Some(admin);
        self.rebuild_router();
        self
    }

    /// Enables authenticated discovery and joining for configured cluster nodes.
    pub fn with_admission_coordinator(
        mut self,
        coordinator: Arc<cluster::AdmissionCoordinator>,
    ) -> Self {
        self.state.admission_coordinator = Some(coordinator);
        self.rebuild_router();
        self
    }

    /// Enables destructive membership changes from a store-owning control-plane node.
    pub fn with_store_provider(mut self, provider: Arc<dyn cluster::StoreProvider>) -> Self {
        self.state.store_provider = Some(provider);
        self.rebuild_router();
        self
    }

    /// Enables content-addressed build-context uploads through the configured archive store.
    pub fn with_artifact_archive_store(
        mut self,
        store: Arc<dyn build::ArtifactArchiveStore>,
    ) -> Self {
        self.state.artifact_archives = Some(store);
        self.rebuild_router();
        self
    }

    /// Enables authenticated node-to-node export of runtime-native artifact archives.
    pub fn with_artifact_store(mut self, store: Arc<dyn runtime::ArtifactStore>) -> Self {
        self.state.artifacts = Some(store);
        self.rebuild_router();
        self
    }

    /// Enables firewall dry-runs with the same static settings as the leader operator.
    pub fn with_firewall_settings(mut self, settings: firewall::FirewallSettings) -> Self {
        self.state.firewall_settings = Some(settings);
        self.rebuild_router();
        self
    }

    /// Enables node-local normalized-log reads and histograms.
    pub fn with_log_query_store(mut self, store: Arc<dyn logs::LogQueryStore>) -> Self {
        self.state.log_queries = Some(store);
        self.rebuild_router();
        self
    }

    /// Enables cluster-wide log fan-out over the declared node topology.
    pub fn with_cluster_log_query_store(
        mut self,
        mut node_ids: Vec<NodeId>,
        nodes: Arc<dyn logs::NodeLogQueryStore>,
    ) -> Self {
        node_ids.sort();
        node_ids.dedup();
        self.state.cluster_log_nodes = Arc::from(node_ids);
        self.state.cluster_log_queries =
            Some(Arc::new(logs::ClusterLogQueryCoordinator::new(nodes)));
        self.rebuild_router();
        self
    }

    /// Enables node-local and cluster-wide access-log traffic analytics.
    pub fn with_traffic_query_stores(
        mut self,
        local: Arc<dyn logs::TrafficQueryStore>,
        mut node_ids: Vec<NodeId>,
        nodes: Arc<dyn logs::NodeTrafficQueryStore>,
    ) -> Self {
        node_ids.sort();
        node_ids.dedup();
        self.state.traffic_queries = Some(local);
        self.state.cluster_traffic_nodes = Arc::from(node_ids);
        self.state.cluster_traffic_queries =
            Some(Arc::new(logs::ClusterTrafficQueryCoordinator::new(nodes)));
        self.rebuild_router();
        self
    }

    /// Enables node-local workload, host-resource, and disk metric reads.
    pub fn with_metric_query_stores(
        mut self,
        node_id: NodeId,
        workloads: Arc<dyn metrics::WorkloadMetricQueryStore>,
        hosts: Arc<dyn metrics::HostMetricQueryStore>,
    ) -> Self {
        self.state.local_metric_node = Some(node_id);
        self.state.workload_metric_queries = Some(workloads);
        self.state.host_metric_queries = Some(hosts);
        self.rebuild_router();
        self
    }

    /// Enables cluster-wide metric fanout over the declared node topology.
    pub fn with_cluster_metric_query_store(
        mut self,
        mut node_ids: Vec<NodeId>,
        queries: Arc<dyn NodeMetricQueryStore>,
    ) -> Self {
        node_ids.sort();
        node_ids.dedup();
        self.state.cluster_metric_nodes = Arc::from(node_ids);
        self.state.cluster_metric_queries = Some(queries);
        self.rebuild_router();
        self
    }

    /// Enables live controller snapshots and durable local and cluster operational history.
    pub fn with_stats_providers(
        mut self,
        controller: Arc<dyn logs::ControllerStatsProvider>,
        backup: Option<Arc<dyn logs::BackupStatsProvider>>,
        metrics: Arc<dyn logs::StatsMetricStore>,
        mut node_ids: Vec<NodeId>,
        cluster: Arc<dyn NodeStatsQueryStore>,
    ) -> Self {
        node_ids.sort();
        node_ids.dedup();
        self.state.controller_stats = Some(controller);
        self.state.backup_stats = backup;
        self.state.stats_metrics = Some(metrics);
        self.state.cluster_stats_nodes = Arc::from(node_ids);
        self.state.cluster_stats_queries = Some(cluster);
        self.rebuild_router();
        self
    }

    /// Replaces the production monotonic uptime clock for deterministic composition.
    pub fn with_uptime_clock(mut self, uptime_clock: Arc<dyn logs::UptimeClock>) -> Self {
        self.state.uptime_clock = uptime_clock;
        self.rebuild_router();
        self
    }

    /// Enables local and cross-node interactive exec session routing.
    pub fn with_exec_sessions(mut self, sessions: Arc<dyn ClusterExecSessions>) -> Self {
        self.state.exec_sessions = Some(sessions);
        self.rebuild_router();
        self
    }

    /// Enables webhook test commands through the same delivery seam as the leader operator.
    pub fn with_webhook_backend(
        mut self,
        backend: Arc<dyn webhook::WebhookDeliveryBackend>,
    ) -> Self {
        self.state.webhook_backend = Some(backend);
        self.rebuild_router();
        self
    }

    /// Returns a cloneable in-process router for composition and tests.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Claims an additional listener over the same application state with its own access policy.
    pub async fn bind_additional(
        &self,
        settings: ServerSettings,
    ) -> Result<BoundApiServer, ServerError> {
        let settings = settings.validate()?;
        let router = crate::routes::operator_router(
            self.state.clone(),
            auth_policy(&settings),
            settings.panel_directory.as_deref(),
        );
        BoundApiServer::bind(settings, router).await
    }

    /// Claims the configured listener before transferring runtime ownership.
    pub async fn bind(self) -> Result<BoundApiServer, ServerError> {
        BoundApiServer::bind(self.settings, self.router).await
    }

    fn rebuild_router(&mut self) {
        self.router = crate::routes::router(
            self.state.clone(),
            auth_policy(&self.settings),
            self.settings.panel_directory.as_deref(),
        );
    }
}

fn auth_policy(settings: &ServerSettings) -> AuthPolicy {
    AuthPolicy::new(
        settings.jwt_secret_key.clone(),
        settings.node_certificate_requirement(),
        settings.operator_proxy_cidrs.clone(),
        settings.bind_address.ip().is_loopback(),
    )
}
