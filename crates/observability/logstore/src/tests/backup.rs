use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp};
use logs::{
    BackupStatsSnapshot, IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore,
    LogStream, OriginCursor,
};

use crate::{
    BackupObjectBody, BackupObjectReceipt, BackupObjectStore, BackupObjectStoreError,
    BackupObjectUpload, ColdPartitionManifest, DuckLogStoreRuntime, DuckStoreSettings,
    LogBackupSettings, backup_log_partitions,
};

const TEN_FIFTEEN: i64 = 1_784_628_900_000;
const TEN_FORTY_FIVE: i64 = 1_784_630_700_000;
const ELEVEN_THIRTY: i64 = 1_784_633_400_000;

#[tokio::test]
async fn backup_uploads_verified_node_objects_with_the_manifest_last()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = runtime.store();
    store.append(&[entry(1, TEN_FIFTEEN)?]).await?;
    store.rollover_before(Timestamp(ELEVEN_THIRTY), &[]).await?;
    store.append(&[entry(2, TEN_FORTY_FIVE)?]).await?;
    store.rollover_before(Timestamp(ELEVEN_THIRTY), &[]).await?;
    let object_store = RecordingObjectStore::new(false);
    let backup_settings =
        LogBackupSettings::new("clusters/production", NodeId::new("node-one")?, "kms-key")?;

    let report = backup_log_partitions(
        store.as_ref(),
        &object_store,
        &backup_settings,
        Timestamp(ELEVEN_THIRTY),
    )
    .await?;

    assert_eq!(report.attempted_partitions, 1);
    assert_eq!(report.completed_partitions, 1);
    assert_eq!(report.failed_partitions, 0);
    assert_eq!(report.uploaded_objects, 3);
    assert!(report.uploaded_bytes > 0);
    assert_eq!(report.pending_partitions, 0);
    let uploads = object_store.uploads()?;
    assert_eq!(
        uploads
            .iter()
            .map(|upload| upload.key.as_str())
            .collect::<Vec<_>>(),
        vec![
            "clusters/production/logs/date=2026-07-21/hour=10/node-one-part-1-1.parquet",
            "clusters/production/logs/date=2026-07-21/hour=10/node-one-part-2-2.parquet",
            "clusters/production/logs/date=2026-07-21/hour=10/node-one-manifest.json",
        ]
    );
    assert!(uploads.iter().take(2).all(|upload| !upload.commit_marker));
    let manifest_upload = uploads.last().ok_or("manifest upload missing")?;
    assert!(manifest_upload.commit_marker);
    assert!(uploads.iter().all(|upload| {
        upload.kms_key_id == "kms-key" && upload.sha256.len() == 64 && upload.size_bytes > 0
    }));
    let BackupObjectBody::Bytes(manifest_body) = &manifest_upload.body else {
        return Err("remote manifest was not uploaded from rewritten bytes".into());
    };
    let manifest = serde_json::from_slice::<ColdPartitionManifest>(manifest_body)?;
    assert_eq!(
        manifest
            .parts
            .iter()
            .map(|part| part.file.as_str())
            .collect::<Vec<_>>(),
        vec!["node-one-part-1-1.parquet", "node-one-part-2-2.parquet"]
    );
    assert_eq!(
        backup_log_partitions(
            store.as_ref(),
            &object_store,
            &backup_settings,
            Timestamp(ELEVEN_THIRTY),
        )
        .await?
        .attempted_partitions,
        0
    );
    runtime.shutdown().await?;

    let connection = duckdb::Connection::open(settings.path)?;
    assert_eq!(
        connection.query_row(
            "SELECT COUNT(*) FROM log_partitions WHERE state = 'backed_up'",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn manifest_upload_failure_keeps_the_partition_pending_for_full_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    let store = runtime.store();
    store.append(&[entry(1, TEN_FIFTEEN)?]).await?;
    store.rollover_before(Timestamp(ELEVEN_THIRTY), &[]).await?;
    let object_store = RecordingObjectStore::new(true);
    let settings = LogBackupSettings::new("production", NodeId::new("node-one")?, "kms")?;

    let failed = backup_log_partitions(
        store.as_ref(),
        &object_store,
        &settings,
        Timestamp(ELEVEN_THIRTY),
    )
    .await?;
    assert_eq!(failed.failed_partitions, 1);
    assert_eq!(failed.completed_partitions, 0);
    assert_eq!(failed.pending_partitions, 1);
    assert!(failed.pending_bytes > 0);
    assert_eq!(failed.oldest_pending_date.as_deref(), Some("2026-07-21"));
    assert!(failed.latest_error.is_some());

    let recovered = backup_log_partitions(
        store.as_ref(),
        &object_store,
        &settings,
        Timestamp(ELEVEN_THIRTY),
    )
    .await?;
    assert_eq!(recovered.completed_partitions, 1);
    assert_eq!(recovered.pending_partitions, 0);
    let uploads = object_store.uploads()?;
    assert_eq!(uploads.len(), 4);
    assert!(uploads.get(1).is_some_and(|upload| upload.commit_marker));
    assert!(uploads.get(3).is_some_and(|upload| upload.commit_marker));
    runtime.shutdown().await?;
    Ok(())
}

#[test]
fn backup_settings_reject_ambiguous_prefixes_and_kms_keys()
-> Result<(), kernel_api::InvalidIdentifier> {
    let node = NodeId::new("node-one")?;
    assert!(LogBackupSettings::new("../escape", node.clone(), "kms").is_err());
    assert!(LogBackupSettings::new("production", node, "").is_err());
    Ok(())
}

#[tokio::test]
async fn backup_stats_survive_restart_and_replace_the_singleton_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("logs.duckdb"), 8)?;
    let runtime = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = runtime.store();
    assert_eq!(store.load_backup_stats().await?, None);
    let stats = BackupStatsSnapshot {
        configured: true,
        last_attempt_at_ms: Some(100),
        pending_partitions: 3,
        pending_bytes: 4_096,
        oldest_pending_date: Some("2026-07-20".to_owned()),
        ..BackupStatsSnapshot::default()
    };
    store.save_backup_stats(&stats, Timestamp(100)).await?;
    runtime.shutdown().await?;

    let restarted = DuckLogStoreRuntime::open(settings.clone()).await?;
    let store = restarted.store();
    assert_eq!(store.load_backup_stats().await?, Some(stats.clone()));
    let replaced = BackupStatsSnapshot {
        last_success_at_ms: Some(200),
        pending_partitions: 0,
        pending_bytes: 0,
        ..stats
    };
    store.save_backup_stats(&replaced, Timestamp(200)).await?;
    assert_eq!(store.load_backup_stats().await?, Some(replaced));
    restarted.shutdown().await?;

    let connection = duckdb::Connection::open(settings.path)?;
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM backup_stats", [], |row| {
            row.get::<_, i64>(0)
        })?,
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

struct RecordingObjectStore {
    fail_manifest_once: AtomicBool,
    uploads: Mutex<Vec<BackupObjectUpload>>,
}

impl RecordingObjectStore {
    fn new(fail_manifest_once: bool) -> Self {
        Self {
            fail_manifest_once: AtomicBool::new(fail_manifest_once),
            uploads: Mutex::new(Vec::new()),
        }
    }

    fn uploads(&self) -> std::io::Result<Vec<BackupObjectUpload>> {
        self.uploads
            .lock()
            .map(|uploads| uploads.clone())
            .map_err(|_| std::io::Error::other("recording object-store lock poisoned"))
    }
}

#[async_trait]
impl BackupObjectStore for RecordingObjectStore {
    async fn upload(
        &self,
        object: &BackupObjectUpload,
    ) -> Result<BackupObjectReceipt, BackupObjectStoreError> {
        self.uploads
            .lock()
            .map_err(|_| BackupObjectStoreError::Unavailable {
                message: "recording object-store lock poisoned".to_owned(),
            })?
            .push(object.clone());
        if object.commit_marker && self.fail_manifest_once.swap(false, Ordering::SeqCst) {
            return Err(BackupObjectStoreError::Unavailable {
                message: "manifest unavailable".to_owned(),
            });
        }
        Ok(BackupObjectReceipt {
            size_bytes: object.size_bytes,
            sha256: object.sha256.clone(),
            kms_key_id: object.kms_key_id.clone(),
        })
    }
}
