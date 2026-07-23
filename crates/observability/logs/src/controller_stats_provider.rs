use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    BackupStatsSnapshot, ControllerStatsSnapshot, LogSinkId, LogStatsStore, LogStatsStoreError,
    SinkRuntimeRegistry, SystemUptimeClock, UptimeClock, collect_controller_stats,
};

/// Live node-local controller observability snapshot provider.
#[async_trait]
pub trait ControllerStatsProvider: Send + Sync {
    /// Collects durable and process-local health at the supplied wall-clock time.
    async fn controller_stats(
        &self,
        reported_at_ms: i64,
    ) -> Result<ControllerStatsSnapshot, LogStatsStoreError>;
}

/// Joins one log store with the configured sink runtime registry and process lifetime.
pub struct LiveControllerStats {
    store: Arc<dyn LogStatsStore>,
    sink_ids: Arc<[LogSinkId]>,
    runtime: SinkRuntimeRegistry,
    version: String,
    uptime_clock: Arc<dyn UptimeClock>,
}

impl LiveControllerStats {
    /// Captures static process identity while retaining shared live health handles.
    pub fn new(
        store: Arc<dyn LogStatsStore>,
        mut sink_ids: Vec<LogSinkId>,
        runtime: SinkRuntimeRegistry,
        version: impl Into<String>,
    ) -> Self {
        sink_ids.sort();
        sink_ids.dedup();
        Self {
            store,
            sink_ids: Arc::from(sink_ids),
            runtime,
            version: version.into(),
            uptime_clock: Arc::new(SystemUptimeClock::new()),
        }
    }

    /// Replaces the production uptime clock for deterministic composition.
    pub fn with_uptime_clock(mut self, uptime_clock: Arc<dyn UptimeClock>) -> Self {
        self.uptime_clock = uptime_clock;
        self
    }
}

#[async_trait]
impl ControllerStatsProvider for LiveControllerStats {
    async fn controller_stats(
        &self,
        reported_at_ms: i64,
    ) -> Result<ControllerStatsSnapshot, LogStatsStoreError> {
        collect_controller_stats(
            self.store.as_ref(),
            &self.sink_ids,
            &self.runtime,
            reported_at_ms,
            self.version.clone(),
            self.uptime_clock
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        )
        .await
    }
}

/// Latest node-local persisted backup health provider.
pub trait BackupStatsProvider: Send + Sync {
    /// Returns the newest health state committed by the maintenance worker.
    fn backup_stats(&self) -> Result<BackupStatsSnapshot, BackupStatsProviderError>;
}

/// Backup health could not be read from process-local state.
#[derive(Debug, thiserror::Error)]
#[error("backup stats are unavailable: {message}")]
pub struct BackupStatsProviderError {
    /// Safe provider diagnostic.
    pub message: String,
}
