use std::collections::BTreeMap;

use kernel_api::{ClusterId, NodeId, Timestamp};
use logs::{
    IngestLogEntry, LogBody, LogDeliveryStore, LogOrigin, LogProducer, LogRecordId, LogStore,
    LogStream, OriginCursor,
};

use crate::{ColdPartitionManifest, DuckLogStoreRuntime, DuckStoreSettings, LogRolloverReport};

const TEN_FIFTEEN: i64 = 1_784_628_900_000;
const TEN_FORTY_FIVE: i64 = 1_784_630_700_000;
const TEN_FIFTY: i64 = 1_784_631_000_000;
const ELEVEN_FIVE: i64 = 1_784_631_900_000;
const ELEVEN_THIRTY: i64 = 1_784_633_400_000;
const NOON: i64 = 1_784_635_200_000;

#[tokio::test]
async fn hourly_rollover_writes_verified_manifests_without_consuming_delivery_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = runtime.store();
    store
        .append(&[
            entry(1, TEN_FIFTEEN)?,
            entry(2, TEN_FORTY_FIVE)?,
            entry(3, ELEVEN_FIVE)?,
        ])
        .await?;

    assert_eq!(
        store.rollover_before(Timestamp(ELEVEN_THIRTY)).await?,
        LogRolloverReport {
            partitions: 1,
            rows: 2,
            bytes: std::fs::metadata(parquet_path(store.cold_root(), 1, 2))?.len(),
        }
    );
    assert_eq!(
        store.rollover_before(Timestamp(ELEVEN_THIRTY)).await?,
        LogRolloverReport::default()
    );
    assert_eq!(store.read_after(None, 8).await?.len(), 3);
    let cold_root = store.cold_root().to_path_buf();
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(&settings.path)?;
    assert_eq!(row_count(&connection, "normalized_logs")?, 3);
    assert_eq!(row_count(&connection, "query_logs")?, 1);
    assert_eq!(
        connection.query_row(
            "SELECT state FROM log_partitions WHERE partition_key = '2026-07-21/10'",
            [],
            |row| row.get::<_, String>(0),
        )?,
        "exported"
    );
    let parquet = parquet_path(&cold_root, 1, 2);
    assert_eq!(
        connection.query_row(
            "SELECT DISTINCT compression FROM parquet_metadata(?1)",
            duckdb::params![parquet.to_string_lossy().as_ref()],
            |row| row.get::<_, String>(0),
        )?,
        "ZSTD"
    );
    drop(connection);

    let manifest = read_manifest(&cold_root)?;
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.partition_key, "2026-07-21/10");
    assert_eq!(manifest.updated_at_ms, 1_784_631_600_000);
    assert_eq!(manifest.parts.len(), 1);
    let first = manifest.parts.first().ok_or("manifest part missing")?;
    assert_eq!(first.file, "part-1-2.parquet");
    assert_eq!(first.row_count, 2);
    assert_eq!((first.sequence_low, first.sequence_high), (1, 2));
    assert_eq!(first.sha256.len(), 64);
    assert_eq!(first.size_bytes, std::fs::metadata(&parquet)?.len());

    let restarted = DuckLogStoreRuntime::open(settings).await?;
    let store = restarted.store();
    store.append(&[entry(4, TEN_FIFTY)?]).await?;
    assert_eq!(
        store.rollover_before(Timestamp(NOON)).await?,
        LogRolloverReport {
            partitions: 2,
            rows: 2,
            bytes: std::fs::metadata(parquet_path(store.cold_root(), 4, 4))?.len()
                + std::fs::metadata(
                    store
                        .cold_root()
                        .join("logs/date=2026-07-21/hour=11/part-3-3.parquet"),
                )?
                .len(),
        }
    );
    let manifest = read_manifest(store.cold_root())?;
    assert_eq!(manifest.parts.len(), 2);
    assert_eq!(manifest.parts.get(1).map(|part| part.sequence_low), Some(4));
    assert_eq!(store.read_after(None, 8).await?.len(), 4);
    restarted.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn rollover_failure_keeps_hot_and_delivery_rows_available()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = runtime.store();
    store.append(&[entry(1, TEN_FIFTEEN)?]).await?;
    let parquet = parquet_path(store.cold_root(), 1, 1);
    std::fs::create_dir_all(parquet.parent().ok_or("partition parent missing")?)?;
    std::fs::write(&parquet, b"not parquet")?;

    assert!(store.rollover_before(Timestamp(NOON)).await.is_err());
    assert_eq!(store.read_after(None, 8).await?.len(), 1);
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(settings.path)?;
    assert_eq!(row_count(&connection, "normalized_logs")?, 1);
    assert_eq!(row_count(&connection, "query_logs")?, 1);
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

fn parquet_path(cold_root: &std::path::Path, low: u64, high: u64) -> std::path::PathBuf {
    partition_directory(cold_root).join(format!("part-{low}-{high}.parquet"))
}

fn read_manifest(
    cold_root: &std::path::Path,
) -> Result<ColdPartitionManifest, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&std::fs::read(
        partition_directory(cold_root).join("manifest.json"),
    )?)?)
}

fn row_count(connection: &duckdb::Connection, table: &str) -> duckdb::Result<i64> {
    connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
}
