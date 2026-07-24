use std::time::Duration;

use logs::SinkWorkerSettings;
use metrics::MetricSinkWorkerSettings;

use crate::RoleError;
use crate::join_activation::JoinActivationSettings;

/// Time bounds for node resync, leadership, and graceful store shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonRoleSettings {
    pub(crate) bridge_resync_interval: Duration,
    pub(crate) mesh_resync_interval: Duration,
    pub(crate) dns_resync_interval: Duration,
    pub(crate) firewall_resync_interval: Duration,
    pub(crate) artifact_resync_interval: Duration,
    pub(crate) assignment_resync_interval: Duration,
    pub(crate) assignment_reconcile_timeout: Duration,
    pub(crate) health_poll_interval: Duration,
    pub(crate) stats_poll_interval: Duration,
    pub(crate) host_telemetry_poll_interval: Duration,
    pub(crate) node_liveness_ttl: Duration,
    pub(crate) node_liveness_keepalive_interval: Duration,
    pub(crate) upgrade_resync_interval: Duration,
    pub(crate) log_poll_interval: Duration,
    pub(crate) max_log_frames_per_workload: usize,
    pub(crate) sink_worker_settings: SinkWorkerSettings,
    pub(crate) metric_sink_worker_settings: MetricSinkWorkerSettings,
    pub(crate) workload_stop_timeout: Duration,
    pub(crate) restart_backoff_base: Duration,
    pub(crate) restart_backoff_max: Duration,
    pub(crate) leadership_ttl: Duration,
    pub(crate) leadership_keepalive_interval: Duration,
    pub(crate) campaign_retry_interval: Duration,
    pub(crate) store_shutdown_grace: Duration,
    pub(crate) join_activation: JoinActivationSettings,
}

impl DaemonRoleSettings {
    /// Creates bounded settings and rejects hot loops or expired leadership.
    pub fn new(
        bridge_resync_interval: Duration,
        mesh_resync_interval: Duration,
        dns_resync_interval: Duration,
        firewall_resync_interval: Duration,
        health_poll_interval: Duration,
        stats_poll_interval: Duration,
        log_poll_interval: Duration,
        max_log_frames_per_workload: usize,
        leadership_ttl: Duration,
        leadership_keepalive_interval: Duration,
        campaign_retry_interval: Duration,
        store_shutdown_grace: Duration,
    ) -> Result<Self, RoleError> {
        if bridge_resync_interval.is_zero()
            || mesh_resync_interval.is_zero()
            || dns_resync_interval.is_zero()
            || firewall_resync_interval.is_zero()
            || health_poll_interval.is_zero()
            || stats_poll_interval.is_zero()
            || log_poll_interval.is_zero()
            || max_log_frames_per_workload == 0
            || leadership_ttl.is_zero()
            || leadership_keepalive_interval.is_zero()
            || campaign_retry_interval.is_zero()
            || store_shutdown_grace.is_zero()
            || leadership_keepalive_interval >= leadership_ttl
        {
            return Err(RoleError::new(
                "daemon intervals and log frame bounds must be non-zero, and leadership keepalive must precede TTL",
            ));
        }
        Ok(Self {
            bridge_resync_interval,
            mesh_resync_interval,
            dns_resync_interval,
            firewall_resync_interval,
            artifact_resync_interval: Duration::from_secs(30),
            assignment_resync_interval: Duration::from_secs(30),
            assignment_reconcile_timeout: Duration::from_secs(20),
            health_poll_interval,
            stats_poll_interval,
            host_telemetry_poll_interval: Duration::from_secs(15),
            node_liveness_ttl: Duration::from_secs(15),
            node_liveness_keepalive_interval: Duration::from_secs(5),
            upgrade_resync_interval: Duration::from_secs(2),
            log_poll_interval,
            max_log_frames_per_workload,
            sink_worker_settings: SinkWorkerSettings::default(),
            metric_sink_worker_settings: MetricSinkWorkerSettings::default(),
            workload_stop_timeout: Duration::from_secs(10),
            restart_backoff_base: Duration::from_secs(5),
            restart_backoff_max: Duration::from_secs(60),
            leadership_ttl,
            leadership_keepalive_interval,
            campaign_retry_interval,
            store_shutdown_grace,
            join_activation: JoinActivationSettings::default(),
        })
    }

    /// Overrides bounded sink drain, retry, and poll settings for this daemon instance.
    pub fn with_sink_worker_settings(
        mut self,
        settings: SinkWorkerSettings,
    ) -> Result<Self, RoleError> {
        self.sink_worker_settings = settings
            .validate()
            .map_err(|error| RoleError::new(error.to_string()))?;
        Ok(self)
    }

    /// Overrides bounded metric sink drain, retry, and poll settings.
    pub fn with_metric_sink_worker_settings(
        mut self,
        settings: MetricSinkWorkerSettings,
    ) -> Result<Self, RoleError> {
        self.metric_sink_worker_settings = settings
            .validate()
            .map_err(|error| RoleError::new(error.to_string()))?;
        Ok(self)
    }
}

impl Default for DaemonRoleSettings {
    fn default() -> Self {
        Self {
            bridge_resync_interval: Duration::from_secs(30),
            mesh_resync_interval: Duration::from_secs(30),
            dns_resync_interval: Duration::from_secs(30),
            firewall_resync_interval: Duration::from_secs(30),
            artifact_resync_interval: Duration::from_secs(30),
            assignment_resync_interval: Duration::from_secs(30),
            assignment_reconcile_timeout: Duration::from_secs(20),
            health_poll_interval: Duration::from_secs(5),
            stats_poll_interval: Duration::from_secs(5),
            host_telemetry_poll_interval: Duration::from_secs(15),
            node_liveness_ttl: Duration::from_secs(15),
            node_liveness_keepalive_interval: Duration::from_secs(5),
            upgrade_resync_interval: Duration::from_secs(2),
            log_poll_interval: Duration::from_secs(1),
            max_log_frames_per_workload: 1_000,
            sink_worker_settings: SinkWorkerSettings::default(),
            metric_sink_worker_settings: MetricSinkWorkerSettings::default(),
            workload_stop_timeout: Duration::from_secs(10),
            restart_backoff_base: Duration::from_secs(5),
            restart_backoff_max: Duration::from_secs(60),
            leadership_ttl: Duration::from_secs(15),
            leadership_keepalive_interval: Duration::from_secs(5),
            campaign_retry_interval: Duration::from_secs(1),
            store_shutdown_grace: Duration::from_secs(10),
            join_activation: JoinActivationSettings::default(),
        }
    }
}
