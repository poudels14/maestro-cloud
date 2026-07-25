use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, Timestamp};

use crate::{
    AgentStatsSnapshot, BackupStatsSnapshot, ClusterStatsResponse, ControllerStatsProvider,
    DeadLetterStore, InMemoryLogStore, IngestLogEntry, LiveControllerStats, LogBody,
    LogDeliveryStore, LogOrigin, LogProducer, LogRecordId, LogSinkId, LogStore, LogStream,
    OriginCursor, SinkDeadLetter, SinkRuntimeClock, SinkRuntimeRegistry, StatsWarning, UptimeClock,
    collect_controller_stats, derive_stats_warnings,
};

#[tokio::test]
async fn controller_stats_join_durable_backlog_with_runtime_health_and_wire_shape()
-> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryLogStore::new();
    store.append(&[entry(1, 1_000)?, entry(2, 2_000)?]).await?;
    let sink_id = LogSinkId::new("datadog")?;
    store
        .commit_sink_cursor(&sink_id, crate::LogSequence(1))
        .await?;
    store
        .record(&SinkDeadLetter {
            sink_id: sink_id.clone(),
            source_sequence: crate::LogSequence(1),
            status_code: Some(413),
            reason: "too large".to_owned(),
            payload: b"payload".to_vec(),
            recorded_at: Timestamp(3_500),
        })
        .await?;

    let clock = Arc::new(ManualClock::new(3_000));
    let runtime = SinkRuntimeRegistry::new(clock.clone());
    runtime.record_failure(&sink_id, "down");
    clock.set(4_000);
    runtime.record_success(&sink_id, 2);
    let controller = collect_controller_stats(
        &store,
        std::slice::from_ref(&sink_id),
        &runtime,
        10_000,
        "2.0.0",
        5_000,
    )
    .await?;

    assert_eq!(
        serde_json::to_value(&controller)?,
        serde_json::json!({
            "reportedAtMs": 10_000,
            "version": "2.0.0",
            "uptimeMs": 5_000,
            "spool": {
                "rowCount": 2,
                "highWatermark": 2,
                "oldestEntryAtMs": 1_000,
                "databaseBytes": 0,
            },
            "sinks": [{
                "id": "datadog",
                "cursor": 1,
                "pendingEntries": 1,
                "oldestPendingAtMs": 2_000,
                "lastSuccessAtMs": 4_000,
                "lastErrorAtMs": 3_000,
                "lastError": "down",
                "consecutiveFailures": 0,
                "lastCursorAdvanceAtMs": 4_000,
                "filteredEntries": 2,
            }],
            "deadLetters": {
                "count": 1,
                "capacity": 100_000,
                "payloadBytes": 7,
                "latestAtMs": 3_500,
                "latestStatus": 413,
                "latestError": "too large",
            },
        })
    );

    let points = controller.metric_points();
    assert!(
        points
            .iter()
            .any(|point| { point.name == "logs.spool.oldest_age_seconds" && point.value == 9.0 })
    );
    assert!(points.iter().any(|point| {
        point.name == "logs.sink.pending_entries"
            && point.value == 1.0
            && point.labels.get("sink").map(String::as_str) == Some("datadog")
    }));
    Ok(())
}

#[tokio::test]
async fn live_controller_stats_uses_the_injected_monotonic_uptime()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = LiveControllerStats::new(
        Arc::new(InMemoryLogStore::new()),
        Vec::new(),
        SinkRuntimeRegistry::default(),
        "2.0.0",
    )
    .with_uptime_clock(Arc::new(FixedUptimeClock(Duration::from_millis(12_345))));

    let stats = provider.controller_stats(50_000).await?;

    assert_eq!(stats.reported_at_ms, 50_000);
    assert_eq!(stats.uptime_ms, 12_345);
    Ok(())
}

#[test]
fn cluster_and_backup_stats_preserve_defaults_metrics_and_wire_shape()
-> Result<(), Box<dyn std::error::Error>> {
    let backup: BackupStatsSnapshot = serde_json::from_value(serde_json::json!({
        "configured": true,
        "pendingPartitions": 3,
        "pendingBytes": 5,
        "failedPartitionsLastRun": 2,
    }))?;
    assert_eq!(backup.pending_partitions, 3);
    assert_eq!(backup.uploaded_bytes_last_run, 0);
    assert_eq!(
        backup
            .metric_points(10)
            .iter()
            .map(|point| (point.name.as_str(), point.value))
            .collect::<Vec<_>>(),
        vec![
            ("logs.backup.pending_partitions", 3.0),
            ("logs.backup.pending_bytes", 5.0),
            ("logs.backup.failed_partitions_last_run", 2.0),
        ]
    );

    let response = ClusterStatsResponse {
        generated_at_ms: 10,
        agent: AgentStatsSnapshot {
            version: "2.0.0".to_owned(),
            uptime_ms: 20,
        },
        controller: None,
        controller_heartbeat_age_ms: None,
        backup,
        warnings: vec![StatsWarning {
            code: "controller-heartbeat-missing".to_owned(),
            severity: "error".to_owned(),
            message: "No stats report has been received from the controller".to_owned(),
        }],
    };
    assert_eq!(
        serde_json::to_value(response)?,
        serde_json::json!({
            "generatedAtMs": 10,
            "agent": { "version": "2.0.0", "uptimeMs": 20 },
            "controller": null,
            "controllerHeartbeatAgeMs": null,
            "backup": {
                "configured": true,
                "lastAttemptAtMs": null,
                "lastSuccessAtMs": null,
                "lastErrorAtMs": null,
                "lastError": null,
                "pendingPartitions": 3,
                "pendingBytes": 5,
                "oldestPendingDate": null,
                "uploadedBytesLastRun": 0,
                "completedPartitionsLastRun": 0,
                "failedPartitionsLastRun": 2,
            },
            "warnings": [{
                "code": "controller-heartbeat-missing",
                "severity": "error",
                "message": "No stats report has been received from the controller",
            }],
        })
    );
    Ok(())
}

#[tokio::test]
async fn warnings_surface_sink_dead_letter_backup_and_version_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryLogStore::new();
    let sink_id = LogSinkId::new("datadog")?;
    let runtime = SinkRuntimeRegistry::default();
    runtime.record_failure(&sink_id, "down");
    store
        .record(&SinkDeadLetter {
            sink_id: sink_id.clone(),
            source_sequence: crate::LogSequence(1),
            status_code: Some(413),
            reason: "too large".to_owned(),
            payload: b"payload".to_vec(),
            recorded_at: Timestamp(3_500),
        })
        .await?;
    let controller =
        collect_controller_stats(&store, &[sink_id], &runtime, 10_000, "old-version", 5_000)
            .await?;
    let backup = BackupStatsSnapshot {
        configured: true,
        last_success_at_ms: Some(8_000),
        last_error_at_ms: Some(9_000),
        ..BackupStatsSnapshot::default()
    };
    let warnings =
        derive_stats_warnings(Some(&controller), &backup, Some(0), 10_000, "new-version");
    let codes = warnings
        .iter()
        .map(|warning| warning.code.as_str())
        .collect::<Vec<_>>();
    assert!(codes.contains(&"component-version-mismatch"));
    assert!(codes.contains(&"sink-datadog-failing"));
    assert!(codes.contains(&"datadog-dead-letters"));
    assert!(codes.contains(&"log-backup-failing"));
    Ok(())
}

fn entry(index: u64, event_at: i64) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let cluster_id = ClusterId::new("cluster-1")?;
    let node_id = NodeId::new("node-1")?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("daemon".to_owned()),
            cursor: OriginCursor::new(format!("cursor-{index}")),
        },
        observed_at: Timestamp(event_at),
        event_at: Timestamp(event_at),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id,
            node_id: Some(node_id),
            component: "daemon".to_owned(),
        },
        body: LogBody::Text(format!("record-{index}")),
        attributes: BTreeMap::new(),
    })
}

struct ManualClock(AtomicI64);

struct FixedUptimeClock(Duration);

impl UptimeClock for FixedUptimeClock {
    fn elapsed(&self) -> Duration {
        self.0
    }
}

impl ManualClock {
    fn new(now: i64) -> Self {
        Self(AtomicI64::new(now))
    }

    fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl SinkRuntimeClock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}
