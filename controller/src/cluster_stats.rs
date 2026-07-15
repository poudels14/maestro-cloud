use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::logs::LogStore;
use crate::signal::ShutdownEvent;

const REPORT_INTERVAL: Duration = Duration::from_secs(10);
const REPORT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Default)]
struct SinkRuntimeState {
    last_success_at_ms: Option<i64>,
    last_error_at_ms: Option<i64>,
    last_error: Option<String>,
    consecutive_failures: u64,
    last_cursor_advance_at_ms: Option<i64>,
    filtered_entries: u64,
}

#[derive(Clone, Default)]
pub struct SinkRuntimeRegistry {
    inner: Arc<RwLock<HashMap<String, SinkRuntimeState>>>,
}

impl SinkRuntimeRegistry {
    pub fn record_success(&self, sink_id: &str) {
        let now = now_ms();
        let mut states = self.inner.write().unwrap_or_else(|err| err.into_inner());
        let state = states.entry(sink_id.to_string()).or_default();
        state.last_success_at_ms = Some(now);
        state.last_cursor_advance_at_ms = Some(now);
        state.consecutive_failures = 0;
    }

    pub fn record_failure(&self, sink_id: &str, error: &str) {
        let mut states = self.inner.write().unwrap_or_else(|err| err.into_inner());
        let state = states.entry(sink_id.to_string()).or_default();
        state.last_error_at_ms = Some(now_ms());
        state.last_error = Some(error.chars().take(500).collect());
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
    }

    pub fn record_recovered(&self, sink_id: &str) {
        let mut states = self.inner.write().unwrap_or_else(|err| err.into_inner());
        states
            .entry(sink_id.to_string())
            .or_default()
            .consecutive_failures = 0;
    }

    pub fn record_filtered(&self, sink_id: &str, count: u64) {
        let mut states = self.inner.write().unwrap_or_else(|err| err.into_inner());
        let state = states.entry(sink_id.to_string()).or_default();
        state.filtered_entries = state.filtered_entries.saturating_add(count);
    }

    fn snapshot(&self, sink_id: &str) -> SinkRuntimeState {
        self.inner
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .get(sink_id)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerStatsSnapshot {
    pub reported_at_ms: i64,
    pub version: String,
    pub uptime_ms: u64,
    pub spool: SpoolStatsSnapshot,
    pub sinks: Vec<SinkStatsSnapshot>,
    pub dead_letters: DeadLetterStatsSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpoolStatsSnapshot {
    pub row_count: u64,
    pub high_watermark: i64,
    pub oldest_entry_at_ms: Option<i64>,
    pub database_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkStatsSnapshot {
    pub id: String,
    pub cursor: i64,
    pub pending_entries: u64,
    pub oldest_pending_at_ms: Option<i64>,
    pub last_success_at_ms: Option<i64>,
    pub last_error_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub consecutive_failures: u64,
    pub last_cursor_advance_at_ms: Option<i64>,
    #[serde(default)]
    pub filtered_entries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetterStatsSnapshot {
    pub count: u64,
    pub capacity: u64,
    pub payload_bytes: u64,
    pub latest_at_ms: Option<i64>,
    pub latest_status: Option<u16>,
    pub latest_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BackupStatsSnapshot {
    pub configured: bool,
    pub last_attempt_at_ms: Option<i64>,
    pub last_success_at_ms: Option<i64>,
    pub last_error_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub pending_partitions: u64,
    pub pending_bytes: u64,
    pub oldest_pending_date: Option<String>,
    pub uploaded_bytes_last_run: u64,
    pub completed_partitions_last_run: u64,
    pub failed_partitions_last_run: u64,
}

pub type SharedControllerStats = Arc<RwLock<Option<ControllerStatsSnapshot>>>;
pub type SharedBackupStats = Arc<RwLock<BackupStatsSnapshot>>;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStatsResponse {
    pub generated_at_ms: i64,
    pub probe: ProbeStatsSnapshot,
    pub controller: Option<ControllerStatsSnapshot>,
    pub controller_heartbeat_age_ms: Option<u64>,
    pub backup: BackupStatsSnapshot,
    pub warnings: Vec<StatsWarning>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeStatsSnapshot {
    pub version: String,
    pub uptime_ms: u64,
    pub storage_mode: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsWarning {
    pub code: String,
    pub severity: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatsMetricPoint {
    pub ts: i64,
    pub name: String,
    pub value: f64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

impl ControllerStatsSnapshot {
    pub fn metric_points(&self) -> Vec<StatsMetricPoint> {
        let mut points = vec![
            metric_point(
                self.reported_at_ms,
                "controller.uptime_seconds",
                self.uptime_ms as f64 / 1_000.0,
            ),
            metric_point(
                self.reported_at_ms,
                "logs.spool.rows",
                self.spool.row_count as f64,
            ),
            metric_point(
                self.reported_at_ms,
                "logs.spool.bytes",
                self.spool.database_bytes as f64,
            ),
            metric_point(
                self.reported_at_ms,
                "logs.dead_letters.count",
                self.dead_letters.count as f64,
            ),
            metric_point(
                self.reported_at_ms,
                "logs.dead_letters.bytes",
                self.dead_letters.payload_bytes as f64,
            ),
        ];
        if let Some(oldest) = self.spool.oldest_entry_at_ms {
            points.push(metric_point(
                self.reported_at_ms,
                "logs.spool.oldest_age_seconds",
                self.reported_at_ms.saturating_sub(oldest).max(0) as f64 / 1_000.0,
            ));
        }
        for sink in &self.sinks {
            let labels = BTreeMap::from([("sink".to_string(), sink.id.clone())]);
            points.push(metric_point_with_labels(
                self.reported_at_ms,
                "logs.sink.pending_entries",
                sink.pending_entries as f64,
                labels.clone(),
            ));
            points.push(metric_point_with_labels(
                self.reported_at_ms,
                "logs.sink.consecutive_failures",
                sink.consecutive_failures as f64,
                labels.clone(),
            ));
            points.push(metric_point_with_labels(
                self.reported_at_ms,
                "logs.sink.filtered_entries",
                sink.filtered_entries as f64,
                labels.clone(),
            ));
            if let Some(oldest) = sink.oldest_pending_at_ms {
                points.push(metric_point_with_labels(
                    self.reported_at_ms,
                    "logs.sink.oldest_pending_age_seconds",
                    self.reported_at_ms.saturating_sub(oldest).max(0) as f64 / 1_000.0,
                    labels,
                ));
            }
        }
        points
    }
}

impl BackupStatsSnapshot {
    pub fn metric_points(&self, ts: i64) -> Vec<StatsMetricPoint> {
        vec![
            metric_point(
                ts,
                "logs.backup.pending_partitions",
                self.pending_partitions as f64,
            ),
            metric_point(ts, "logs.backup.pending_bytes", self.pending_bytes as f64),
            metric_point(
                ts,
                "logs.backup.failed_partitions_last_run",
                self.failed_partitions_last_run as f64,
            ),
        ]
    }
}

fn metric_point(ts: i64, name: &str, value: f64) -> StatsMetricPoint {
    metric_point_with_labels(ts, name, value, BTreeMap::new())
}

fn metric_point_with_labels(
    ts: i64,
    name: &str,
    value: f64,
    labels: BTreeMap<String, String>,
) -> StatsMetricPoint {
    StatsMetricPoint {
        ts,
        name: name.to_string(),
        value,
        labels,
    }
}

pub struct ClusterStatsReporter {
    endpoint: String,
    store: Arc<LogStore>,
    sink_runtime: SinkRuntimeRegistry,
    started_at: Instant,
    client: reqwest::Client,
    ingestion_token: Option<String>,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
}

impl ClusterStatsReporter {
    pub fn new(
        endpoint: String,
        store: Arc<LogStore>,
        sink_runtime: SinkRuntimeRegistry,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
        ingestion_token: Option<String>,
    ) -> Self {
        Self {
            endpoint,
            store,
            sink_runtime,
            started_at: Instant::now(),
            client: reqwest::Client::builder()
                .timeout(REPORT_TIMEOUT)
                .build()
                .expect("failed to build cluster stats client"),
            ingestion_token,
            signal_rx,
        }
    }

    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(REPORT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = self.report().await {
                        eprintln!("[maestro]: cluster stats report failed: {err}");
                    }
                }
                signal = self.signal_rx.recv() => {
                    match signal {
                        Ok(ShutdownEvent::Graceful)
                            | Ok(ShutdownEvent::Force)
                            | Ok(ShutdownEvent::Restart)
                        | Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
            }
        }
    }

    async fn report(&self) -> Result<()> {
        let stats = self.store.stats_snapshot().await?;
        let sinks = stats
            .sinks
            .into_iter()
            .map(|sink| {
                let runtime = self.sink_runtime.snapshot(&sink.sink_id);
                SinkStatsSnapshot {
                    id: sink.sink_id,
                    cursor: sink.cursor,
                    pending_entries: sink.pending_entries,
                    oldest_pending_at_ms: sink.oldest_pending_at_ms,
                    last_success_at_ms: runtime.last_success_at_ms,
                    last_error_at_ms: runtime.last_error_at_ms,
                    last_error: runtime.last_error,
                    consecutive_failures: runtime.consecutive_failures,
                    last_cursor_advance_at_ms: runtime.last_cursor_advance_at_ms,
                    filtered_entries: runtime.filtered_entries,
                }
            })
            .collect();
        let snapshot = ControllerStatsSnapshot {
            reported_at_ms: now_ms(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_ms: self
                .started_at
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            spool: SpoolStatsSnapshot {
                row_count: stats.row_count,
                high_watermark: stats.high_watermark,
                oldest_entry_at_ms: stats.oldest_entry_at_ms,
                database_bytes: stats.database_bytes,
            },
            sinks,
            dead_letters: DeadLetterStatsSnapshot {
                count: stats.dead_letters.count,
                capacity: u64::try_from(crate::logs::store::MAX_SINK_DEAD_LETTERS)
                    .unwrap_or_default(),
                payload_bytes: stats.dead_letters.payload_bytes,
                latest_at_ms: stats.dead_letters.latest_at_ms,
                latest_status: stats.dead_letters.latest_status,
                latest_error: stats.dead_letters.latest_error,
            },
        };
        let payload = crate::metrics::TypedMetricBatch::ControllerStats(snapshot);
        let mut request = self.client.post(&self.endpoint).json(&payload);
        if let Some(token) = &self.ingestion_token {
            request = request.header("X-Maestro-Ingestion-Token", token);
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "probe returned {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            ));
        }
        Ok(())
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_snapshot_emits_named_metrics_with_sink_labels() {
        let snapshot = ControllerStatsSnapshot {
            reported_at_ms: 10_000,
            version: "1.0.0".to_string(),
            uptime_ms: 5_000,
            spool: SpoolStatsSnapshot {
                row_count: 7,
                high_watermark: 9,
                oldest_entry_at_ms: Some(4_000),
                database_bytes: 11,
            },
            sinks: vec![SinkStatsSnapshot {
                id: "datadog".to_string(),
                cursor: 2,
                pending_entries: 5,
                oldest_pending_at_ms: Some(5_000),
                last_success_at_ms: None,
                last_error_at_ms: None,
                last_error: None,
                consecutive_failures: 1,
                last_cursor_advance_at_ms: None,
                filtered_entries: 4,
            }],
            dead_letters: DeadLetterStatsSnapshot {
                count: 3,
                capacity: 100_000,
                payload_bytes: 13,
                latest_at_ms: None,
                latest_status: None,
                latest_error: None,
            },
        };

        let points = snapshot.metric_points();
        let pending = points
            .iter()
            .find(|point| point.name == "logs.sink.pending_entries")
            .expect("sink pending metric");
        assert_eq!(pending.value, 5.0);
        assert_eq!(
            pending.labels.get("sink").map(String::as_str),
            Some("datadog")
        );
        assert!(
            points.iter().any(|point| {
                point.name == "logs.spool.oldest_age_seconds" && point.value == 6.0
            })
        );
        assert!(
            points
                .iter()
                .any(|point| { point.name == "logs.dead_letters.count" && point.value == 3.0 })
        );
    }
}
