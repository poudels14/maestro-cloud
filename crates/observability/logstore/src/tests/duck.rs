use std::collections::BTreeMap;

use kernel_api::{ClusterId, NodeId, Timestamp};
use logs::{
    DeadLetterStore, IngestLogEntry, LogBody, LogDeliveryStore, LogOrigin, LogProducer,
    LogRecordId, LogSequence, LogSinkId, LogStore, LogStream, OriginCursor, SinkDeadLetter,
};

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
        2
    );
    Ok(())
}

fn entry() -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    entry_at(1)
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
