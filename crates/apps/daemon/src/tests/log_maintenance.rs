use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp};
use kernel_controller::TimestampClock;
use kernel_store::TokioClock;
use logs::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore, LogStream, OriginCursor,
};
use logstore::{
    BackupObjectReceipt, BackupObjectStore, BackupObjectStoreError, BackupObjectUpload,
    DuckLogStoreRuntime, DuckStoreSettings, LogBackupSettings,
};

use crate::{LogBackupTarget, LogMaintenanceSettings, LogMaintenanceWorker};

const JULY_NINETEENTH_TEN: i64 = 1_784_457_000_000;
const JULY_TWENTY_FIRST_NOON: i64 = 1_784_635_200_000;

#[tokio::test]
async fn maintenance_rolls_over_backs_up_then_prunes_and_persists_success()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    let store = runtime.store();
    store.append(&[entry(JULY_NINETEENTH_TEN)?]).await?;
    let object_store = std::sync::Arc::new(RecordingObjectStore::default());
    let worker = LogMaintenanceWorker::new(
        store.clone(),
        Some(LogBackupTarget::new(
            object_store.clone(),
            LogBackupSettings::new("production", NodeId::new("node-one")?, "kms")?,
            Some(1),
        )?),
        LogMaintenanceSettings::default(),
        std::sync::Arc::new(TokioClock::new()),
        std::sync::Arc::new(FixedTimestampClock(JULY_TWENTY_FIRST_NOON)),
    )
    .await?;

    worker.rollover_once().await?;
    worker.backup_once().await?;

    let stats = worker.stats_snapshot()?;
    assert!(stats.configured);
    assert_eq!(stats.last_attempt_at_ms, Some(JULY_TWENTY_FIRST_NOON));
    assert_eq!(stats.last_success_at_ms, Some(JULY_TWENTY_FIRST_NOON));
    assert_eq!(stats.completed_partitions_last_run, 1);
    assert_eq!(stats.pending_partitions, 0);
    assert!(stats.last_error.is_none());
    assert!(
        !store
            .cold_root()
            .join("logs/date=2026-07-19/hour=10")
            .exists()
    );
    let uploads = object_store.uploads()?;
    assert_eq!(uploads.len(), 2);
    assert!(uploads.last().is_some_and(|upload| upload.commit_marker));
    assert_eq!(store.load_backup_stats().await?, Some(stats));
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_manifest_is_persisted_as_pending_health_and_recovers_next_run()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    let store = runtime.store();
    store.append(&[entry(JULY_NINETEENTH_TEN)?]).await?;
    store
        .rollover_before(Timestamp(JULY_TWENTY_FIRST_NOON))
        .await?;
    let object_store = std::sync::Arc::new(RecordingObjectStore::failing_manifest_once());
    let worker = LogMaintenanceWorker::new(
        store.clone(),
        Some(LogBackupTarget::new(
            object_store,
            LogBackupSettings::new("production", NodeId::new("node-one")?, "kms")?,
            None,
        )?),
        LogMaintenanceSettings::default(),
        std::sync::Arc::new(TokioClock::new()),
        std::sync::Arc::new(FixedTimestampClock(JULY_TWENTY_FIRST_NOON)),
    )
    .await?;

    worker.backup_once().await?;
    let failed = worker.stats_snapshot()?;
    assert_eq!(failed.failed_partitions_last_run, 1);
    assert_eq!(failed.pending_partitions, 1);
    assert!(failed.pending_bytes > 0);
    assert!(failed.last_error.is_some());
    worker.backup_once().await?;
    let recovered = worker.stats_snapshot()?;
    assert_eq!(recovered.failed_partitions_last_run, 0);
    assert_eq!(recovered.pending_partitions, 0);
    assert!(recovered.last_error.is_none());
    runtime.shutdown().await?;
    Ok(())
}

#[test]
fn maintenance_settings_reject_hot_loops() {
    assert!(LogMaintenanceSettings::new(Duration::ZERO, Duration::from_secs(1)).is_err());
    assert!(LogMaintenanceSettings::new(Duration::from_secs(1), Duration::ZERO).is_err());
}

fn entry(event_at: i64) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let cluster_id = ClusterId::new("cluster-1")?;
    let node_id = NodeId::new("node-1")?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("daemon".to_owned()),
            cursor: OriginCursor::new("cursor-1"),
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
        body: LogBody::Text("record".to_owned()),
        attributes: BTreeMap::new(),
    })
}

#[derive(Clone, Copy)]
struct FixedTimestampClock(i64);

impl TimestampClock for FixedTimestampClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0)
    }
}

#[derive(Default)]
struct RecordingObjectStore {
    uploads: Mutex<Vec<BackupObjectUpload>>,
    fail_manifest: Mutex<bool>,
}

impl RecordingObjectStore {
    fn failing_manifest_once() -> Self {
        Self {
            uploads: Mutex::new(Vec::new()),
            fail_manifest: Mutex::new(true),
        }
    }

    fn uploads(&self) -> std::io::Result<Vec<BackupObjectUpload>> {
        self.uploads
            .lock()
            .map(|uploads| uploads.clone())
            .map_err(|_| std::io::Error::other("upload lock poisoned"))
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
                message: "upload lock poisoned".to_owned(),
            })?
            .push(object.clone());
        let mut fail_manifest =
            self.fail_manifest
                .lock()
                .map_err(|_| BackupObjectStoreError::Unavailable {
                    message: "failure lock poisoned".to_owned(),
                })?;
        if object.commit_marker && *fail_manifest {
            *fail_manifest = false;
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
