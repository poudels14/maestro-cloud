use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use kernel_api::{ClusterId, NodeId, Timestamp};
use logs::{
    IngestLogEntry, LogBody, LogDeliveryStore, LogOrigin, LogProducer, LogRecordId, LogStore,
    LogStream, OriginCursor,
};

use crate::{
    BackupObjectReceipt, BackupObjectStore, BackupObjectStoreError, BackupObjectUpload,
    DuckLogStoreRuntime, DuckStoreSettings, LogBackupSettings, LogRetentionReport,
    backup_log_partitions,
};

const TEN_FIFTEEN: i64 = 1_784_628_900_000;
const TEN_FORTY_FIVE: i64 = 1_784_630_700_000;
const ELEVEN_THIRTY: i64 = 1_784_633_400_000;

#[tokio::test]
async fn retention_prunes_only_complete_backed_up_partitions_and_keeps_delivery_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = runtime.store();
    store.append(&[entry(1, TEN_FIFTEEN)?]).await?;
    store.rollover_before(Timestamp(ELEVEN_THIRTY)).await?;
    backup_log_partitions(
        store.as_ref(),
        &AcceptingObjectStore,
        &LogBackupSettings::new("production", NodeId::new("node-one")?, "kms")?,
        Timestamp(ELEVEN_THIRTY),
    )
    .await?;
    let directory = partition_directory(store.cold_root());
    let expected_bytes = std::fs::metadata(directory.join("part-1-1.parquet"))?.len();

    assert_eq!(
        store
            .prune_backed_up_before(NaiveDate::from_ymd_opt(2026, 7, 21).ok_or("date")?)
            .await?,
        LogRetentionReport::default()
    );
    assert!(directory.is_dir());
    assert_eq!(
        store
            .prune_backed_up_before(NaiveDate::from_ymd_opt(2026, 7, 22).ok_or("date")?)
            .await?,
        LogRetentionReport {
            partitions: 1,
            objects: 1,
            bytes: expected_bytes,
        }
    );
    assert!(!directory.exists());
    assert_eq!(store.read_after(None, 8).await?.len(), 1);
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(settings.path)?;
    assert_eq!(row_count(&connection, "log_partitions")?, 0);
    assert_eq!(row_count(&connection, "normalized_logs")?, 1);
    Ok(())
}

#[tokio::test]
async fn late_export_prevents_pruning_an_older_backed_up_part()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = runtime.store();
    store.append(&[entry(1, TEN_FIFTEEN)?]).await?;
    store.rollover_before(Timestamp(ELEVEN_THIRTY)).await?;
    backup_log_partitions(
        store.as_ref(),
        &AcceptingObjectStore,
        &LogBackupSettings::new("production", NodeId::new("node-one")?, "kms")?,
        Timestamp(ELEVEN_THIRTY),
    )
    .await?;
    store.append(&[entry(2, TEN_FORTY_FIVE)?]).await?;
    store.rollover_before(Timestamp(ELEVEN_THIRTY)).await?;

    assert_eq!(
        store
            .prune_backed_up_before(NaiveDate::from_ymd_opt(2026, 7, 22).ok_or("date")?)
            .await?,
        LogRetentionReport::default()
    );
    assert!(partition_directory(store.cold_root()).is_dir());
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(settings.path)?;
    assert_eq!(row_count(&connection, "log_partitions")?, 2);
    assert_eq!(
        connection.query_row(
            "SELECT COUNT(*) FROM log_partitions WHERE state = 'backed_up'",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        1
    );
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

fn partition_directory(cold_root: &std::path::Path) -> std::path::PathBuf {
    cold_root.join("logs/date=2026-07-21/hour=10")
}

fn row_count(connection: &duckdb::Connection, table: &str) -> duckdb::Result<i64> {
    connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
}

struct AcceptingObjectStore;

#[async_trait]
impl BackupObjectStore for AcceptingObjectStore {
    async fn upload(
        &self,
        object: &BackupObjectUpload,
    ) -> Result<BackupObjectReceipt, BackupObjectStoreError> {
        Ok(BackupObjectReceipt {
            size_bytes: object.size_bytes,
            sha256: object.sha256.clone(),
            kms_key_id: object.kms_key_id.clone(),
        })
    }
}
