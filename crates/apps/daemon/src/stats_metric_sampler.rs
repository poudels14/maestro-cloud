use std::sync::Arc;
use std::time::Duration;

use kernel_api::NodeId;
use kernel_store::Clock;
use logs::{
    BackupStatsProvider, BackupStatsSnapshot, ControllerStatsProvider, StatsMetricAppendReport,
    StatsMetricStore,
};
use node_agent::StatusClock;
use tokio::sync::watch;

/// Periodically persists live node health in the compatibility time-series format.
pub(crate) struct StatsMetricSampler {
    node_id: NodeId,
    store: Arc<dyn StatsMetricStore>,
    controller: Arc<dyn ControllerStatsProvider>,
    backup: Option<Arc<dyn BackupStatsProvider>>,
    monotonic_clock: Arc<dyn Clock>,
    timestamp_clock: Arc<dyn StatusClock>,
    interval: Duration,
}

impl StatsMetricSampler {
    pub(crate) fn new(
        node_id: NodeId,
        store: Arc<dyn StatsMetricStore>,
        controller: Arc<dyn ControllerStatsProvider>,
        backup: Option<Arc<dyn BackupStatsProvider>>,
        monotonic_clock: Arc<dyn Clock>,
        timestamp_clock: Arc<dyn StatusClock>,
        interval: Duration,
    ) -> Result<Self, StatsMetricSamplerError> {
        if interval.is_zero() {
            return Err(StatsMetricSamplerError::new(
                "stats metric sample interval must be non-zero",
            ));
        }
        Ok(Self {
            node_id,
            store,
            controller,
            backup,
            monotonic_clock,
            timestamp_clock,
            interval,
        })
    }

    /// Captures controller and backup state at one shared wall-clock timestamp.
    pub(crate) async fn collect_once(
        &self,
    ) -> Result<StatsMetricAppendReport, StatsMetricSamplerError> {
        let now = self.timestamp_clock.now().0;
        let controller = self
            .controller
            .controller_stats(now)
            .await
            .map_err(|error| StatsMetricSamplerError::new(error.to_string()))?;
        let backup = self
            .backup
            .as_ref()
            .map_or_else(
                || Ok(BackupStatsSnapshot::default()),
                |provider| provider.backup_stats(),
            )
            .map_err(|error| StatsMetricSamplerError::new(error.to_string()))?;
        let mut points = controller.metric_points();
        points.extend(backup.metric_points(now));
        for point in &mut points {
            point
                .labels
                .insert("node".to_owned(), self.node_id.as_str().to_owned());
        }
        self.store
            .append_stats_metrics(&points)
            .await
            .map_err(|error| StatsMetricSamplerError::new(error.to_string()))
    }

    /// Waits one interval after the startup sample, then continues until shutdown.
    pub(crate) async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let mut due = self.monotonic_clock.now().saturating_add(self.interval);
        loop {
            if *shutdown.borrow() {
                return;
            }
            tokio::select! {
                () = self.monotonic_clock.sleep_until(due) => {
                    if let Err(error) = self.collect_once().await {
                        eprintln!("operational stats sampling failed: {error}");
                    }
                    due = self.monotonic_clock.now().saturating_add(self.interval);
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}

/// Failure to construct or take one operational stats sample.
#[derive(Debug, thiserror::Error)]
#[error("operational stats sampler failed: {message}")]
pub(crate) struct StatsMetricSamplerError {
    message: String,
}

impl StatsMetricSamplerError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
