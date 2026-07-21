use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    LogSinkId, LogSpoolStats, LogStatsStore, LogStatsStoreError, MAX_RETAINED_DEAD_LETTERS,
    SinkRuntimeRegistry,
};

/// API-compatible controller observability snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerStatsSnapshot {
    /// Wall-clock generation time.
    pub reported_at_ms: i64,
    /// Controller software version.
    pub version: String,
    /// Process uptime at generation time.
    pub uptime_ms: u64,
    /// Durable normalized-log spool health.
    pub spool: SpoolStatsSnapshot,
    /// Durable and process-local health for configured sinks.
    pub sinks: Vec<SinkStatsSnapshot>,
    /// Aggregate poison-payload quarantine health.
    pub dead_letters: DeadLetterStatsSnapshot,
}

/// API-compatible normalized-log spool health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpoolStatsSnapshot {
    /// Retained hot-tier records.
    pub row_count: u64,
    /// Highest assigned store-local sequence.
    pub high_watermark: i64,
    /// Event time of the first retained record.
    pub oldest_entry_at_ms: Option<i64>,
    /// Hot-tier database and journal bytes.
    pub database_bytes: u64,
}

/// API-compatible durable and runtime health for one log sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkStatsSnapshot {
    /// Stable sink namespace.
    pub id: String,
    /// Last fully committed sequence, or zero before initial progress.
    pub cursor: i64,
    /// Retained records after the cursor.
    pub pending_entries: u64,
    /// Event time of the first pending record.
    pub oldest_pending_at_ms: Option<i64>,
    /// Most recent successful durable delivery time.
    pub last_success_at_ms: Option<i64>,
    /// Most recent failed drain time.
    pub last_error_at_ms: Option<i64>,
    /// Bounded latest delivery error.
    pub last_error: Option<String>,
    /// Failures since the latest successful or healthy idle drain.
    pub consecutive_failures: u64,
    /// Most recent durable cursor-advance time.
    pub last_cursor_advance_at_ms: Option<i64>,
    /// Entries removed by sink-local filters in this process lifetime.
    #[serde(default)]
    pub filtered_entries: u64,
}

/// API-compatible aggregate dead-letter health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetterStatsSnapshot {
    /// Retained poison payloads.
    pub count: u64,
    /// Configured node-local retention ceiling.
    pub capacity: u64,
    /// Retained poison payload bytes.
    pub payload_bytes: u64,
    /// Latest retained dead-letter time.
    pub latest_at_ms: Option<i64>,
    /// Latest retained destination status.
    pub latest_status: Option<u16>,
    /// Latest retained rejection reason.
    pub latest_error: Option<String>,
}

/// API-compatible persisted backup health.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BackupStatsSnapshot {
    /// Whether object-store backup is configured.
    pub configured: bool,
    /// Latest backup attempt time.
    pub last_attempt_at_ms: Option<i64>,
    /// Latest completely successful backup time.
    pub last_success_at_ms: Option<i64>,
    /// Latest backup failure time.
    pub last_error_at_ms: Option<i64>,
    /// Bounded latest backup failure.
    pub last_error: Option<String>,
    /// Completed local partitions awaiting commit-marker upload.
    pub pending_partitions: u64,
    /// Bytes contained by pending partitions.
    pub pending_bytes: u64,
    /// Oldest pending hive partition date.
    pub oldest_pending_date: Option<String>,
    /// Payload bytes uploaded by the latest run.
    pub uploaded_bytes_last_run: u64,
    /// Partitions committed by the latest run.
    pub completed_partitions_last_run: u64,
    /// Partitions failed by the latest run.
    pub failed_partitions_last_run: u64,
}

/// Complete API-compatible cluster operational response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStatsResponse {
    /// Response generation time.
    pub generated_at_ms: i64,
    /// Probe/API process health.
    pub probe: ProbeStatsSnapshot,
    /// Latest controller report when one has arrived.
    pub controller: Option<ControllerStatsSnapshot>,
    /// Age of the latest controller report.
    pub controller_heartbeat_age_ms: Option<u64>,
    /// Latest durable backup health.
    pub backup: BackupStatsSnapshot,
    /// Derived actionable diagnostics.
    pub warnings: Vec<StatsWarning>,
}

/// API-compatible probe process health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeStatsSnapshot {
    /// Probe/API software version.
    pub version: String,
    /// Probe/API process uptime.
    pub uptime_ms: u64,
}

/// One API-compatible actionable operational diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsWarning {
    /// Stable machine-readable warning code.
    pub code: String,
    /// Established `warning` or `error` severity namespace.
    pub severity: String,
    /// Human-readable diagnostic.
    pub message: String,
}

/// One API-compatible named operational time-series point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsMetricPoint {
    /// Wall-clock sample time.
    pub ts: i64,
    /// Stable dotted metric name.
    pub name: String,
    /// Numeric sample value.
    pub value: f64,
    /// Stable metric dimensions.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// Queries durable state and joins it with process-local sink health for the stats API.
pub async fn collect_controller_stats(
    store: &dyn LogStatsStore,
    sink_ids: &[LogSinkId],
    runtime: &SinkRuntimeRegistry,
    reported_at_ms: i64,
    version: impl Into<String>,
    uptime_ms: u64,
) -> Result<ControllerStatsSnapshot, LogStatsStoreError> {
    let spool = store.stats_snapshot(sink_ids).await?;
    Ok(project_controller_stats(
        spool,
        runtime,
        reported_at_ms,
        version.into(),
        uptime_ms,
    ))
}

fn project_controller_stats(
    spool: LogSpoolStats,
    runtime: &SinkRuntimeRegistry,
    reported_at_ms: i64,
    version: String,
    uptime_ms: u64,
) -> ControllerStatsSnapshot {
    let sinks = spool
        .sinks
        .iter()
        .map(|sink| {
            let health = runtime.snapshot(&sink.sink_id);
            SinkStatsSnapshot {
                id: sink.sink_id.as_str().to_owned(),
                cursor: sink
                    .cursor
                    .map_or(0, |cursor| i64::try_from(cursor.0).unwrap_or(i64::MAX)),
                pending_entries: sink.pending_entries,
                oldest_pending_at_ms: sink.oldest_pending_at_ms,
                last_success_at_ms: health.last_success_at_ms,
                last_error_at_ms: health.last_error_at_ms,
                last_error: health.last_error,
                consecutive_failures: health.consecutive_failures,
                last_cursor_advance_at_ms: health.last_cursor_advance_at_ms,
                filtered_entries: health.filtered_entries,
            }
        })
        .collect();
    ControllerStatsSnapshot {
        reported_at_ms,
        version,
        uptime_ms,
        spool: SpoolStatsSnapshot {
            row_count: spool.row_count,
            high_watermark: i64::try_from(spool.high_watermark.0).unwrap_or(i64::MAX),
            oldest_entry_at_ms: spool.oldest_entry_at_ms,
            database_bytes: spool.database_bytes,
        },
        sinks,
        dead_letters: DeadLetterStatsSnapshot {
            count: spool.dead_letters.count,
            capacity: MAX_RETAINED_DEAD_LETTERS,
            payload_bytes: spool.dead_letters.payload_bytes,
            latest_at_ms: spool.dead_letters.latest_at_ms,
            latest_status: spool.dead_letters.latest_status,
            latest_error: spool.dead_letters.latest_error,
        },
    }
}

impl ControllerStatsSnapshot {
    /// Projects the established controller, spool, sink, and dead-letter metric names.
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
            let labels = BTreeMap::from([("sink".to_owned(), sink.id.clone())]);
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
    /// Projects the established pending and latest-run failure metric names.
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
        name: name.to_owned(),
        value,
        labels,
    }
}
