use std::collections::BTreeMap;

use kernel_api::{ClusterId, NodeId, Timestamp};
use logs::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore, LogStream, OriginCursor,
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

fn entry() -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let cluster_id = ClusterId::new("cluster-1")?;
    let node_id = NodeId::new("node-1")?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("daemon".to_owned()),
            cursor: OriginCursor::new("cursor-1"),
        },
        observed_at: Timestamp(1),
        event_at: Timestamp(1),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id,
            node_id: Some(node_id),
            component: "daemon".to_owned(),
        },
        body: LogBody::Text("started".to_owned()),
        attributes: BTreeMap::new(),
    })
}
