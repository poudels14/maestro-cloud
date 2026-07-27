use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use logs::{
    DeadLetterStore, IngestLogEntry, LogBody, LogDeliveryStore, LogOrigin, LogProducer,
    LogRecordId, LogSequence, LogSinkId, LogStatsStore, LogStore, LogStream, OriginCursor,
    OtlpEnvelope, OtlpEnvelopeStore, OtlpSignal, SinkDeadLetter, StatsMetricPoint,
    StatsMetricQuery, StatsMetricStore,
};
use runtime::WorkloadMetadata;

use crate::{DuckLogStoreRuntime, DuckStoreError, DuckStoreSettings};

#[tokio::test]
async fn duck_store_rejects_ambiguous_schema_history() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE schema_version (version BIGINT NOT NULL);
         INSERT INTO schema_version VALUES (1), (2);",
    )?;
    drop(connection);

    let result = DuckLogStoreRuntime::open(DuckStoreSettings::new(path, 8)?).await;
    assert!(matches!(result, Err(DuckStoreError::Initialize { .. })));
    Ok(())
}

#[tokio::test]
async fn duck_store_passes_shared_conformance_and_closes_cleanly()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    logs::conformance::check_log_store(runtime.store().as_ref()).await?;
    let delivery_entries = runtime.store().read_after(None, 8).await?;
    logs::conformance::check_log_delivery_store(runtime.store().as_ref(), &delivery_entries)
        .await?;
    logs::conformance::check_dead_letter_store(runtime.store().as_ref()).await?;
    logs::conformance::check_otlp_envelope_store(runtime.store().as_ref()).await?;
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_store_passes_operational_stats_conformance() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    let store = runtime.store();
    logs::conformance::check_log_stats_store(
        store.as_ref(),
        store.as_ref(),
        store.as_ref(),
        store.as_ref(),
    )
    .await?;
    assert!(store.stats_snapshot(&[]).await?.database_bytes > 0);
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_store_passes_operational_metric_history_conformance()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    logs::conformance::check_stats_metric_store(runtime.store().as_ref()).await?;
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_store_replays_persisted_entries_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let entry = entry()?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    assert_eq!(
        runtime
            .store()
            .append(std::slice::from_ref(&entry))
            .await?
            .committed,
        1
    );
    runtime.shutdown().await?;

    let restarted = DuckLogStoreRuntime::open(settings).await?;
    assert_eq!(
        restarted
            .store()
            .append(std::slice::from_ref(&entry))
            .await?
            .deduplicated,
        1
    );
    restarted.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_store_replays_operational_metric_history_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let point = StatsMetricPoint {
        ts: 1_700_000_000_000,
        name: "logs.spool.bytes".to_owned(),
        value: 42.0,
        labels: BTreeMap::from([("node".to_owned(), "node-1".to_owned())]),
    };
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    assert_eq!(
        runtime
            .store()
            .append_stats_metrics(std::slice::from_ref(&point))
            .await?
            .committed,
        1
    );
    runtime.shutdown().await?;

    let restarted = DuckLogStoreRuntime::open(settings).await?;
    assert_eq!(
        restarted
            .store()
            .query_stats_metrics(&StatsMetricQuery::new(None, point.ts, point.ts, 8)?)
            .await?,
        vec![point.clone()]
    );
    assert_eq!(
        restarted
            .store()
            .append_stats_metrics(&[point])
            .await?
            .deduplicated,
        1
    );
    restarted.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_store_replays_lossless_otlp_envelopes_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let envelope = otlp_envelope(Timestamp(100))?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    assert_eq!(
        runtime
            .store()
            .append_otlp_envelopes(std::slice::from_ref(&envelope))
            .await?
            .committed,
        1
    );
    runtime.shutdown().await?;

    let restarted = DuckLogStoreRuntime::open(settings).await?;
    let mut replay = envelope;
    replay.observed_at = Timestamp(200);
    assert_eq!(
        restarted
            .store()
            .append_otlp_envelopes(&[replay])
            .await?
            .deduplicated,
        1
    );
    restarted.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_store_persists_ordered_delivery_and_monotonic_sink_cursors()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = runtime.store();
    store.append(&[entry_at(1)?, entry_at(2)?]).await?;

    let first = store.read_after(None, 1).await?;
    assert_eq!(first.len(), 1);
    assert_eq!(
        first.first().expect("one delivery row").sequence,
        LogSequence(1)
    );
    let remaining = store.read_after(Some(LogSequence(1)), 8).await?;
    assert_eq!(
        remaining
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![LogSequence(2)]
    );

    let sink_id = LogSinkId::new("datadog")?;
    assert_eq!(store.load_sink_cursor(&sink_id).await?, None);
    assert!(
        store
            .commit_sink_cursor(&sink_id, LogSequence(99))
            .await
            .is_err()
    );
    store.commit_sink_cursor(&sink_id, LogSequence(2)).await?;
    assert!(
        store
            .commit_sink_cursor(&sink_id, LogSequence(1))
            .await
            .is_err()
    );
    runtime.shutdown().await?;

    let restarted = DuckLogStoreRuntime::open(settings).await?;
    let store = restarted.store();
    assert_eq!(
        store.load_sink_cursor(&sink_id).await?,
        Some(LogSequence(2))
    );
    store.append(&[entry_at(3)?]).await?;
    assert_eq!(
        store
            .read_after(Some(LogSequence(2)), 8)
            .await?
            .first()
            .expect("one appended row")
            .sequence,
        LogSequence(3)
    );
    restarted.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_store_persists_idempotent_dead_letters_and_explicit_purges()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = runtime.store();
    let first = dead_letter(1, b"first")?;
    let second = dead_letter(2, b"second")?;
    store.record(&first).await?;
    store.record(&first).await?;
    store.record(&second).await?;

    assert_eq!(store.stats(&first.sink_id).await?.count, 2);
    assert_eq!(store.stats(&first.sink_id).await?.payload_bytes, 11);
    assert_eq!(
        store.list(&first.sink_id, None, 1).await?,
        vec![first.clone()]
    );
    let mut collision = first.clone();
    collision.payload = b"different".to_vec();
    assert!(store.record(&collision).await.is_err());
    runtime.shutdown().await?;

    let restarted = DuckLogStoreRuntime::open(settings).await?;
    let store = restarted.store();
    assert_eq!(store.stats(&first.sink_id).await?.count, 2);
    assert_eq!(store.purge(&first.sink_id, Some(LogSequence(1))).await?, 1);
    assert_eq!(store.purge(&first.sink_id, None).await?, 1);
    restarted.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_store_migrates_v1_rows_to_deterministic_delivery_sequences()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE schema_version (version BIGINT NOT NULL);
         INSERT INTO schema_version VALUES (1);
         CREATE TABLE normalized_logs (
             node_id VARCHAR NOT NULL,
             producer_type VARCHAR NOT NULL,
             producer_id VARCHAR NOT NULL,
             cursor VARCHAR NOT NULL,
             event_at_ms BIGINT NOT NULL,
             entry_json VARCHAR NOT NULL,
             PRIMARY KEY (node_id, producer_type, producer_id, cursor)
         );",
    )?;
    let existing = entry_at(1)?;
    connection.execute(
        "INSERT INTO normalized_logs VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        duckdb::params![
            existing.id.node_id.as_str(),
            "system",
            "daemon",
            existing.id.cursor.as_str(),
            existing.event_at.0,
            serde_json::to_string(&existing)?
        ],
    )?;
    drop(connection);

    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(path.clone(), 8)?).await?;
    let store = runtime.store();
    assert_eq!(
        store
            .read_after(None, 8)
            .await?
            .first()
            .expect("one migrated row")
            .sequence,
        LogSequence(1)
    );
    store.append(&[entry_at(2)?]).await?;
    assert_eq!(
        store
            .read_after(Some(LogSequence(1)), 8)
            .await?
            .first()
            .expect("one post-migration row")
            .sequence,
        LogSequence(2)
    );
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(path)?;
    assert_eq!(
        connection.query_row("SELECT version FROM schema_version", [], |row| {
            row.get::<_, i64>(0)
        })?,
        7
    );
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM query_logs", [], |row| {
            row.get::<_, i64>(0)
        })?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn duck_store_migrates_v2_rows_into_compact_hot_query_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE schema_version (version BIGINT NOT NULL);
         INSERT INTO schema_version VALUES (2);
         CREATE TABLE normalized_logs (
             sequence BIGINT PRIMARY KEY,
             node_id VARCHAR NOT NULL,
             producer_type VARCHAR NOT NULL,
             producer_id VARCHAR NOT NULL,
             cursor VARCHAR NOT NULL,
             event_at_ms BIGINT NOT NULL,
             entry_json VARCHAR NOT NULL,
             UNIQUE (node_id, producer_type, producer_id, cursor)
         );
         CREATE TABLE sink_cursors (
             sink_id VARCHAR PRIMARY KEY,
             last_sequence BIGINT NOT NULL
         );
         CREATE TABLE sink_dead_letters (
             sink_id VARCHAR NOT NULL,
             source_sequence BIGINT NOT NULL,
             status_code INTEGER,
             reason VARCHAR NOT NULL,
             payload BLOB NOT NULL,
             recorded_at_ms BIGINT NOT NULL,
             PRIMARY KEY (sink_id, source_sequence)
         );",
    )?;
    let existing = entry_at(1)?;
    connection.execute(
        "INSERT INTO normalized_logs VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        duckdb::params![
            1_i64,
            existing.id.node_id.as_str(),
            "system",
            "daemon",
            existing.id.cursor.as_str(),
            existing.event_at.0,
            serde_json::to_string(&existing)?
        ],
    )?;
    drop(connection);

    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(path.clone(), 8)?).await?;
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(path)?;
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM normalized_logs", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM query_logs", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );
    assert_eq!(
        connection.query_row("SELECT version FROM schema_version", [], |row| {
            row.get::<_, i64>(0)
        })?,
        7
    );
    assert_eq!(
        connection.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('query_logs')",
            [],
            |row| row.get::<_, i64>(0)
        )?,
        2
    );
    assert_eq!(
        connection.query_row(
            "SELECT last_sequence FROM log_sequence WHERE singleton = TRUE",
            [],
            |row| row.get::<_, i64>(0)
        )?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn duck_store_migrates_v4_to_operational_metric_history()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE schema_version (version BIGINT NOT NULL);
         INSERT INTO schema_version VALUES (4);",
    )?;
    seed_v4_log_schema(&connection)?;
    drop(connection);

    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(path.clone(), 8)?).await?;
    let point = StatsMetricPoint {
        ts: 10,
        name: "controller.uptime_seconds".to_owned(),
        value: 1.0,
        labels: BTreeMap::new(),
    };
    runtime.store().append_stats_metrics(&[point]).await?;
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(path)?;
    assert_eq!(
        connection.query_row("SELECT version FROM schema_version", [], |row| {
            row.get::<_, i64>(0)
        })?,
        7
    );
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM stats_metrics", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn duck_store_migrates_v5_to_lossless_otlp_spooling() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE schema_version (version BIGINT NOT NULL);
         INSERT INTO schema_version VALUES (5);",
    )?;
    seed_v4_log_schema(&connection)?;
    connection.execute_batch(
        "CREATE TABLE stats_metrics (
             ts BIGINT NOT NULL,
             name VARCHAR NOT NULL,
             value DOUBLE NOT NULL,
             labels_json VARCHAR NOT NULL,
             PRIMARY KEY (ts, name, labels_json)
         );
         CREATE INDEX stats_metrics_name_ts
             ON stats_metrics(name, ts, labels_json);",
    )?;
    drop(connection);

    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(path.clone(), 8)?).await?;
    runtime
        .store()
        .append_otlp_envelopes(&[otlp_envelope(Timestamp(100))?])
        .await?;
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(path)?;
    assert_eq!(
        connection.query_row("SELECT version FROM schema_version", [], |row| {
            row.get::<_, i64>(0)
        })?,
        7
    );
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM otlp_envelopes", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );
    Ok(())
}

fn seed_v4_log_schema(connection: &duckdb::Connection) -> duckdb::Result<()> {
    connection.execute_batch(
        "CREATE TABLE normalized_logs (
             sequence BIGINT PRIMARY KEY,
             node_id VARCHAR NOT NULL,
             producer_type VARCHAR NOT NULL,
             producer_id VARCHAR NOT NULL,
             cursor VARCHAR NOT NULL,
             event_at_ms BIGINT NOT NULL,
             entry_json VARCHAR NOT NULL,
             UNIQUE (node_id, producer_type, producer_id, cursor)
         );
         CREATE TABLE sink_cursors (
             sink_id VARCHAR PRIMARY KEY,
             last_sequence BIGINT NOT NULL
         );
         CREATE TABLE sink_dead_letters (
             sink_id VARCHAR NOT NULL,
             source_sequence BIGINT NOT NULL,
             status_code INTEGER,
             reason VARCHAR NOT NULL,
             payload BLOB NOT NULL,
             recorded_at_ms BIGINT NOT NULL,
             PRIMARY KEY (sink_id, source_sequence)
         );
         CREATE TABLE query_logs (
             sequence BIGINT PRIMARY KEY,
             event_at_ms BIGINT NOT NULL,
             entry_json VARCHAR NOT NULL
         );
         CREATE INDEX query_logs_event_sequence
             ON query_logs(event_at_ms, sequence);
         CREATE TABLE log_partitions (
             partition_key VARCHAR NOT NULL,
             state VARCHAR NOT NULL,
             row_count BIGINT NOT NULL,
             sequence_low BIGINT NOT NULL,
             sequence_high BIGINT NOT NULL,
             sha256 VARCHAR NOT NULL,
             size_bytes BIGINT NOT NULL,
             updated_at_ms BIGINT NOT NULL,
             PRIMARY KEY (partition_key, sequence_low)
         );
         CREATE TABLE backup_stats (
             singleton BOOLEAN PRIMARY KEY CHECK (singleton = TRUE),
             value_json VARCHAR NOT NULL,
             updated_at_ms BIGINT NOT NULL
         );",
    )
}

fn entry() -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    entry_at(1)
}

fn otlp_envelope(observed_at: Timestamp) -> Result<OtlpEnvelope, Box<dyn std::error::Error>> {
    OtlpEnvelope::new(
        OtlpSignal::Metrics,
        WorkloadMetadata {
            cluster_id: ClusterId::new("cluster-1")?,
            node_id: NodeId::new("node-1")?,
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("deployment-1")?,
            assignment_id: AssignmentId::new("assignment-1")?,
            workload_id: WorkloadId::new("workload-1")?,
            labels: BTreeMap::new(),
        },
        observed_at,
        vec![1, 2, 3],
    )
    .map_err(Into::into)
}

fn entry_at(index: u64) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let cluster_id = ClusterId::new("cluster-1")?;
    let node_id = NodeId::new("node-1")?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("daemon".to_owned()),
            cursor: OriginCursor::new(format!("cursor-{index}")),
        },
        observed_at: Timestamp(i64::try_from(index).unwrap_or(i64::MAX)),
        event_at: Timestamp(i64::try_from(index).unwrap_or(i64::MAX)),
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

fn dead_letter(sequence: u64, payload: &[u8]) -> Result<SinkDeadLetter, logs::LogSinkIdError> {
    Ok(SinkDeadLetter {
        sink_id: LogSinkId::new("datadog")?,
        source_sequence: LogSequence(sequence),
        status_code: Some(400),
        reason: "rejected".to_owned(),
        payload: payload.to_vec(),
        recorded_at: Timestamp(100),
    })
}
