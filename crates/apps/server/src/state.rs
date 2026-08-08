use std::sync::Arc;

use kernel_api::{ClusterId, MaskedClusterConfig, NodeId};
use kernel_controller::{RequestDeduplicator, TimestampClock};
use kernel_store::Store;
use tokio::sync::Semaphore;

use crate::{ClusterExecSessions, LaunchConfigAdmin, NodeMetricQueryStore, NodeStatsQueryStore};

#[derive(Debug, Clone, Copy)]
pub(crate) struct VerifiedNodeCertificate;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) cluster_id: ClusterId,
    pub(crate) cluster_config: Option<Arc<MaskedClusterConfig>>,
    pub(crate) launch_config_admin: Option<Arc<dyn LaunchConfigAdmin>>,
    pub(crate) requests: RequestDeduplicator,
    pub(crate) timestamp_clock: Arc<dyn TimestampClock>,
    pub(crate) admission_coordinator: Option<Arc<cluster::AdmissionCoordinator>>,
    pub(crate) store_provider: Option<Arc<dyn cluster::StoreProvider>>,
    pub(crate) artifact_archives: Option<Arc<dyn build::ArtifactArchiveStore>>,
    pub(crate) build_revisions: Option<Arc<dyn build::BuildRevisionResolver>>,
    pub(crate) artifacts: Option<Arc<dyn runtime::ArtifactStore>>,
    pub(crate) firewall_settings: Option<firewall::FirewallSettings>,
    pub(crate) log_queries: Option<Arc<dyn logs::LogQueryStore>>,
    pub(crate) cluster_log_nodes: Arc<[NodeId]>,
    pub(crate) cluster_log_queries: Option<Arc<logs::ClusterLogQueryCoordinator>>,
    pub(crate) traffic_queries: Option<Arc<dyn logs::TrafficQueryStore>>,
    pub(crate) cluster_traffic_nodes: Arc<[NodeId]>,
    pub(crate) cluster_traffic_queries: Option<Arc<logs::ClusterTrafficQueryCoordinator>>,
    pub(crate) local_metric_node: Option<NodeId>,
    pub(crate) workload_metric_queries: Option<Arc<dyn metrics::WorkloadMetricQueryStore>>,
    pub(crate) host_metric_queries: Option<Arc<dyn metrics::HostMetricQueryStore>>,
    pub(crate) cluster_metric_nodes: Arc<[NodeId]>,
    pub(crate) cluster_metric_queries: Option<Arc<dyn NodeMetricQueryStore>>,
    pub(crate) controller_stats: Option<Arc<dyn logs::ControllerStatsProvider>>,
    pub(crate) backup_stats: Option<Arc<dyn logs::BackupStatsProvider>>,
    pub(crate) stats_metrics: Option<Arc<dyn logs::StatsMetricStore>>,
    pub(crate) cluster_stats_nodes: Arc<[NodeId]>,
    pub(crate) cluster_stats_queries: Option<Arc<dyn NodeStatsQueryStore>>,
    pub(crate) uptime_clock: Arc<dyn logs::UptimeClock>,
    pub(crate) exec_sessions: Option<Arc<dyn ClusterExecSessions>>,
    pub(crate) exec_relays: Arc<Semaphore>,
    pub(crate) webhook_backend: Option<Arc<dyn webhook::WebhookDeliveryBackend>>,
}
