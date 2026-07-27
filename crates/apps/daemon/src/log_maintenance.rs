use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Days, Utc};
use kernel_controller::TimestampClock;
use kernel_store::Clock;
use logs::{BackupStatsProvider, BackupStatsProviderError, BackupStatsSnapshot, LogSinkId};
use logstore::{BackupObjectStore, DuckLogStore, LogBackupSettings, backup_log_partitions};
use tokio::sync::watch;

/// Intervals for node-local rollover and remote backup maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogMaintenanceSettings {
    rollover_interval: Duration,
    backup_interval: Duration,
}

impl LogMaintenanceSettings {
    /// Rejects zero intervals that would create a busy loop.
    pub fn new(
        rollover_interval: Duration,
        backup_interval: Duration,
    ) -> Result<Self, LogMaintenanceError> {
        if rollover_interval.is_zero() || backup_interval.is_zero() {
            return Err(invalid("log maintenance intervals must be non-zero"));
        }
        Ok(Self {
            rollover_interval,
            backup_interval,
        })
    }
}

impl Default for LogMaintenanceSettings {
    fn default() -> Self {
        Self {
            rollover_interval: Duration::from_secs(60 * 60),
            backup_interval: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Validated remote target and optional local retention policy.
pub struct LogBackupTarget {
    object_store: Arc<dyn BackupObjectStore>,
    settings: LogBackupSettings,
    retention_days: Option<u64>,
}

impl LogBackupTarget {
    /// Binds an object-store adapter to its namespace and non-zero retention window.
    pub fn new(
        object_store: Arc<dyn BackupObjectStore>,
        settings: LogBackupSettings,
        retention_days: Option<u64>,
    ) -> Result<Self, LogMaintenanceError> {
        if retention_days == Some(0) {
            return Err(invalid("log backup retention days must be at least one"));
        }
        Ok(Self {
            object_store,
            settings,
            retention_days,
        })
    }
}

/// Node-local owner for hourly rollover, daily backup, retention, and backup health.
pub struct LogMaintenanceWorker {
    store: Arc<DuckLogStore>,
    sink_ids: Arc<[LogSinkId]>,
    backup: Option<LogBackupTarget>,
    settings: LogMaintenanceSettings,
    monotonic_clock: Arc<dyn Clock>,
    timestamp_clock: Arc<dyn TimestampClock>,
    stats: Arc<RwLock<BackupStatsSnapshot>>,
}

impl LogMaintenanceWorker {
    /// Restores durable health and records current backup enablement before startup.
    pub async fn new(
        store: Arc<DuckLogStore>,
        mut sink_ids: Vec<LogSinkId>,
        backup: Option<LogBackupTarget>,
        settings: LogMaintenanceSettings,
        monotonic_clock: Arc<dyn Clock>,
        timestamp_clock: Arc<dyn TimestampClock>,
    ) -> Result<Self, LogMaintenanceError> {
        sink_ids.sort();
        sink_ids.dedup();
        let now = timestamp_clock.now();
        let mut stats = store
            .load_backup_stats()
            .await
            .map_err(store_error("load backup health"))?
            .unwrap_or_default();
        stats.configured = backup.is_some();
        store
            .save_backup_stats(&stats, now)
            .await
            .map_err(store_error("persist backup enablement"))?;
        Ok(Self {
            store,
            sink_ids: Arc::from(sink_ids),
            backup,
            settings,
            monotonic_clock,
            timestamp_clock,
            stats: Arc::new(RwLock::new(stats)),
        })
    }

    /// Returns the latest health committed by this worker.
    pub fn stats_snapshot(&self) -> Result<BackupStatsSnapshot, LogMaintenanceError> {
        self.stats
            .read()
            .map(|stats| stats.clone())
            .map_err(|_| state_error("backup stats lock was poisoned"))
    }

    pub(crate) fn stats_handle(&self) -> LogMaintenanceStatsHandle {
        LogMaintenanceStatsHandle {
            stats: self.stats.clone(),
        }
    }

    /// Runs immediate maintenance, then waits on injected monotonic deadlines until shutdown.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let mut rollover_due = self.monotonic_clock.now();
        let mut backup_due = self.backup.as_ref().map(|_| self.monotonic_clock.now());
        loop {
            if *shutdown.borrow() {
                return;
            }
            let now = self.monotonic_clock.now();
            if now >= rollover_due {
                if let Err(error) = self.rollover_once().await {
                    eprintln!("log rollover maintenance failed: {error}");
                }
                rollover_due = self
                    .monotonic_clock
                    .now()
                    .saturating_add(self.settings.rollover_interval);
            }
            if backup_due.is_some_and(|deadline| now >= deadline) {
                if let Err(error) = self.backup_once().await {
                    eprintln!("log backup maintenance failed: {error}");
                }
                backup_due = Some(
                    self.monotonic_clock
                        .now()
                        .saturating_add(self.settings.backup_interval),
                );
            }
            let deadline = backup_due.map_or(rollover_due, |backup| backup.min(rollover_due));
            tokio::select! {
                () = self.monotonic_clock.sleep_until(deadline) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    pub(crate) async fn rollover_once(&self) -> Result<(), LogMaintenanceError> {
        self.store
            .rollover_before(self.timestamp_clock.now(), &self.sink_ids)
            .await
            .map(|_| ())
            .map_err(store_error("roll over complete log hours"))
    }

    pub(crate) async fn backup_once(&self) -> Result<(), LogMaintenanceError> {
        let target = self
            .backup
            .as_ref()
            .ok_or_else(|| invalid("log backup is not configured"))?;
        let attempted_at = self.timestamp_clock.now();
        let mut stats = self.stats_snapshot()?;
        stats.configured = true;
        stats.last_attempt_at_ms = Some(attempted_at.0);
        match backup_log_partitions(
            self.store.as_ref(),
            target.object_store.as_ref(),
            &target.settings,
            attempted_at,
        )
        .await
        {
            Ok(report) => apply_backup_report(&mut stats, report, attempted_at.0),
            Err(error) => record_error(&mut stats, attempted_at.0, error.to_string()),
        }
        if let Some(days) = target.retention_days
            && let Err(error) = self.prune(days, attempted_at.0).await
        {
            record_error(&mut stats, attempted_at.0, error.to_string());
        }
        self.store
            .save_backup_stats(&stats, attempted_at)
            .await
            .map_err(store_error("persist backup health"))?;
        *self
            .stats
            .write()
            .map_err(|_| state_error("backup stats lock was poisoned"))? = stats;
        Ok(())
    }

    async fn prune(&self, retention_days: u64, now_ms: i64) -> Result<(), LogMaintenanceError> {
        let today = DateTime::<Utc>::from_timestamp_millis(now_ms)
            .ok_or_else(|| invalid("UTC retention timestamp is outside the supported range"))?
            .date_naive();
        let cutoff = today
            .checked_sub_days(Days::new(retention_days))
            .ok_or_else(|| invalid("UTC retention cutoff is outside the supported range"))?;
        self.store
            .prune_backed_up_before(cutoff)
            .await
            .map(|_| ())
            .map_err(store_error("prune retained log partitions"))
    }
}

/// Cloneable read handle retained by the API after the maintenance worker starts.
pub(crate) struct LogMaintenanceStatsHandle {
    stats: Arc<RwLock<BackupStatsSnapshot>>,
}

impl BackupStatsProvider for LogMaintenanceStatsHandle {
    fn backup_stats(&self) -> Result<BackupStatsSnapshot, BackupStatsProviderError> {
        self.stats
            .read()
            .map(|stats| stats.clone())
            .map_err(|_| BackupStatsProviderError {
                message: "backup stats lock was poisoned".to_owned(),
            })
    }
}

fn apply_backup_report(
    stats: &mut BackupStatsSnapshot,
    report: logstore::LogBackupRunReport,
    attempted_at_ms: i64,
) {
    stats.uploaded_bytes_last_run = report.uploaded_bytes;
    stats.completed_partitions_last_run = report.completed_partitions;
    stats.failed_partitions_last_run = report.failed_partitions;
    stats.pending_partitions = report.pending_partitions;
    stats.pending_bytes = report.pending_bytes;
    stats.oldest_pending_date = report.oldest_pending_date;
    if report.failed_partitions == 0 {
        stats.last_success_at_ms = Some(attempted_at_ms);
        stats.last_error = None;
    } else {
        record_error(
            stats,
            attempted_at_ms,
            report.latest_error.unwrap_or_else(|| {
                format!("{} log backup partitions failed", report.failed_partitions)
            }),
        );
    }
}

fn record_error(stats: &mut BackupStatsSnapshot, at_ms: i64, message: String) {
    stats.last_error_at_ms = Some(at_ms);
    stats.last_error = Some(message.chars().take(500).collect());
}

/// Invalid worker configuration or failed node-local maintenance.
#[derive(Debug, thiserror::Error)]
pub enum LogMaintenanceError {
    /// Static worker settings are invalid.
    #[error("invalid log maintenance configuration: {message}")]
    InvalidConfiguration { message: String },
    /// Durable log storage or backup could not complete.
    #[error("log maintenance storage failure: {message}")]
    Storage { message: String },
    /// Process-local health state could not be accessed.
    #[error("log maintenance state failure: {message}")]
    State { message: String },
}

fn invalid(message: impl Into<String>) -> LogMaintenanceError {
    LogMaintenanceError::InvalidConfiguration {
        message: message.into(),
    }
}

fn store_error<Error: std::fmt::Display>(
    action: &'static str,
) -> impl FnOnce(Error) -> LogMaintenanceError {
    move |error| LogMaintenanceError::Storage {
        message: format!("failed to {action}: {error}"),
    }
}

fn state_error(message: impl Into<String>) -> LogMaintenanceError {
    LogMaintenanceError::State {
        message: message.into(),
    }
}
