use std::io::Read;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use kernel_api::{NodeId, Timestamp};
use sha2::{Digest, Sha256};

use crate::log_backup_schema::PendingLogBackupPartition;
use crate::{ColdPartitionManifest, DuckLogStore};

const MAX_BACKUP_TEXT_BYTES: usize = 4_096;

/// Validated remote namespace and KMS identity for node-local log backups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogBackupSettings {
    prefix: String,
    node_id: NodeId,
    kms_key_id: String,
}

impl LogBackupSettings {
    /// Validates keys that remain unambiguous across nodes and object stores.
    pub fn new(
        prefix: impl Into<String>,
        node_id: NodeId,
        kms_key_id: impl Into<String>,
    ) -> Result<Self, LogBackupError> {
        let prefix = prefix.into().trim_matches('/').to_owned();
        let kms_key_id = kms_key_id.into();
        if prefix.is_empty()
            || prefix.len() > MAX_BACKUP_TEXT_BYTES
            || prefix.split('/').any(|part| {
                part.is_empty()
                    || matches!(part, "." | "..")
                    || !part.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
            })
        {
            return Err(rejected("backup prefix is not a safe object-key path"));
        }
        if kms_key_id.is_empty()
            || kms_key_id.len() > MAX_BACKUP_TEXT_BYTES
            || kms_key_id.chars().any(char::is_control)
        {
            return Err(rejected("backup KMS key identity is invalid"));
        }
        Ok(Self {
            prefix,
            node_id,
            kms_key_id,
        })
    }
}

/// Immutable upload body suitable for streaming production adapters and exact fakes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupObjectBody {
    /// Existing immutable local Parquet object.
    File(PathBuf),
    /// Small rewritten commit manifest.
    Bytes(Vec<u8>),
}

/// One checksum- and encryption-bound object-store request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupObjectUpload {
    /// Complete destination key within the configured bucket.
    pub key: String,
    /// Immutable payload source.
    pub body: BackupObjectBody,
    /// Exact payload byte length.
    pub size_bytes: u64,
    /// Lowercase hexadecimal whole-object SHA-256.
    pub sha256: String,
    /// Required server-side encryption key identity.
    pub kms_key_id: String,
    /// Whether this object commits visibility of the partition.
    pub commit_marker: bool,
}

/// Verified remote metadata returned after an object becomes readable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupObjectReceipt {
    /// Remotely observed payload length.
    pub size_bytes: u64,
    /// Remotely observed whole-object SHA-256.
    pub sha256: String,
    /// Remotely confirmed encryption key identity.
    pub kms_key_id: String,
}

/// Object-store side effect isolated from backup ordering and durable commits.
#[async_trait]
pub trait BackupObjectStore: Send + Sync {
    /// Uploads, verifies, and returns authoritative metadata for one object.
    async fn upload(
        &self,
        object: &BackupObjectUpload,
    ) -> Result<BackupObjectReceipt, BackupObjectStoreError>;
}

/// A remote object could not be durably and verifiably uploaded.
#[derive(Debug, thiserror::Error)]
pub enum BackupObjectStoreError {
    /// Upload content is permanently invalid for this adapter.
    #[error("backup object store rejected upload: {message}")]
    Rejected { message: String },
    /// Remote storage is temporarily unavailable.
    #[error("backup object store is unavailable: {message}")]
    Unavailable { message: String },
}

/// Object backup or its local durable commit could not be completed safely.
#[derive(Debug, thiserror::Error)]
pub enum LogBackupError {
    /// Configuration or persisted backup state was invalid.
    #[error("log backup rejected operation: {message}")]
    Rejected { message: String },
    /// Local durable state or immutable files were unavailable.
    #[error("log backup is unavailable: {message}")]
    Unavailable { message: String },
}

/// Observable result from one bounded pass across current pending partitions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogBackupRunReport {
    /// Pending partitions inspected at the start of the pass.
    pub attempted_partitions: u64,
    /// Partitions whose remote manifests committed and local state advanced.
    pub completed_partitions: u64,
    /// Partitions left pending after an upload or verification failure.
    pub failed_partitions: u64,
    /// Successfully uploaded objects, including commit manifests.
    pub uploaded_objects: u64,
    /// Successfully uploaded payload bytes.
    pub uploaded_bytes: u64,
    /// Partitions still pending after the pass.
    pub pending_partitions: u64,
    /// Local object bytes still pending after the pass.
    pub pending_bytes: u64,
    /// Oldest pending UTC partition date.
    pub oldest_pending_date: Option<String>,
    /// Bounded latest per-partition error from this pass.
    pub latest_error: Option<String>,
}

/// Uploads immutable objects in order and commits each manifest strictly last.
pub async fn backup_log_partitions(
    store: &DuckLogStore,
    object_store: &dyn BackupObjectStore,
    settings: &LogBackupSettings,
    attempted_at: Timestamp,
) -> Result<LogBackupRunReport, LogBackupError> {
    let partitions = store.pending_backup_partitions().await?;
    let mut report = LogBackupRunReport {
        attempted_partitions: u64::try_from(partitions.len()).unwrap_or(u64::MAX),
        ..LogBackupRunReport::default()
    };
    for partition in partitions {
        match upload_partition(store, object_store, settings, &partition).await {
            Ok((objects, bytes)) => {
                store
                    .mark_partition_backed_up(&partition, attempted_at)
                    .await?;
                report.completed_partitions = report.completed_partitions.saturating_add(1);
                report.uploaded_objects = report.uploaded_objects.saturating_add(objects);
                report.uploaded_bytes = report.uploaded_bytes.saturating_add(bytes);
            }
            Err(error) => {
                report.failed_partitions = report.failed_partitions.saturating_add(1);
                report.latest_error = Some(error.to_string().chars().take(500).collect());
            }
        }
    }
    summarize_pending(store, &mut report).await?;
    Ok(report)
}

async fn summarize_pending(
    store: &DuckLogStore,
    report: &mut LogBackupRunReport,
) -> Result<(), LogBackupError> {
    let pending = store.pending_backup_partitions().await?;
    report.pending_partitions = u64::try_from(pending.len()).unwrap_or(u64::MAX);
    for partition in pending {
        if let Some(date) = partition.partition_key.split('/').next()
            && report
                .oldest_pending_date
                .as_deref()
                .is_none_or(|oldest| date < oldest)
        {
            report.oldest_pending_date = Some(date.to_owned());
        }
        for file in partition.files {
            report.pending_bytes = report.pending_bytes.saturating_add(
                file.metadata()
                    .map_err(unavailable("read pending object metadata"))?
                    .len(),
            );
        }
    }
    Ok(())
}

async fn upload_partition(
    store: &DuckLogStore,
    object_store: &dyn BackupObjectStore,
    settings: &LogBackupSettings,
    partition: &PendingLogBackupPartition,
) -> Result<(u64, u64), LogBackupError> {
    let mut objects = 0_u64;
    let mut bytes = 0_u64;
    for path in &partition.files {
        let commit_marker = path.file_name().is_some_and(|name| name == "manifest.json");
        let body = if commit_marker {
            BackupObjectBody::Bytes(remote_manifest(path, &settings.node_id, partition)?)
        } else {
            BackupObjectBody::File(path.clone())
        };
        let (size_bytes, sha256) = digest_body(&body)?;
        let upload = BackupObjectUpload {
            key: object_key(store.cold_root(), path, settings)?,
            body,
            size_bytes,
            sha256,
            kms_key_id: settings.kms_key_id.clone(),
            commit_marker,
        };
        let receipt = object_store
            .upload(&upload)
            .await
            .map_err(|error| unavailable_message(error.to_string()))?;
        if receipt.size_bytes != upload.size_bytes
            || receipt.sha256 != upload.sha256
            || receipt.kms_key_id != upload.kms_key_id
        {
            return Err(rejected("object-store verification metadata mismatched"));
        }
        objects = objects.saturating_add(1);
        bytes = bytes.saturating_add(size_bytes);
    }
    Ok((objects, bytes))
}

fn object_key(
    cold_root: &Path,
    path: &Path,
    settings: &LogBackupSettings,
) -> Result<String, LogBackupError> {
    let relative = path
        .strip_prefix(cold_root)
        .map_err(|_| rejected("backup object is outside the cold-tier root"))?;
    let file_name = relative
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| rejected("backup object has no UTF-8 filename"))?;
    let relative = relative.with_file_name(format!("{}-{file_name}", settings.node_id));
    let relative = relative
        .to_str()
        .ok_or_else(|| rejected("backup object key is not UTF-8"))?;
    Ok(format!("{}/{relative}", settings.prefix))
}

fn remote_manifest(
    path: &Path,
    node_id: &NodeId,
    partition: &PendingLogBackupPartition,
) -> Result<Vec<u8>, LogBackupError> {
    let mut manifest = serde_json::from_slice::<ColdPartitionManifest>(
        &std::fs::read(path).map_err(unavailable("read partition manifest"))?,
    )
    .map_err(|error| unavailable_message(format!("decode partition manifest: {error}")))?;
    let included = partition
        .files
        .iter()
        .filter(|file| {
            file.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .filter_map(|file| file.file_name().and_then(|name| name.to_str()))
        .collect::<std::collections::BTreeSet<_>>();
    manifest
        .parts
        .retain(|part| included.contains(part.file.as_str()));
    if manifest.parts.len() != included.len() {
        return Err(rejected(
            "partition manifest does not describe every committed object",
        ));
    }
    for part in &mut manifest.parts {
        part.file = format!("{node_id}-{}", part.file);
    }
    serde_json::to_vec_pretty(&manifest)
        .map_err(|error| unavailable_message(format!("encode remote manifest: {error}")))
}

fn digest_body(body: &BackupObjectBody) -> Result<(u64, String), LogBackupError> {
    match body {
        BackupObjectBody::File(path) => digest_file(path),
        BackupObjectBody::Bytes(bytes) => Ok((
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            format!("{:x}", Sha256::digest(bytes)),
        )),
    }
}

fn digest_file(path: &Path) -> Result<(u64, String), LogBackupError> {
    let mut file = std::fs::File::open(path).map_err(unavailable("open backup object"))?;
    let size = file
        .metadata()
        .map_err(unavailable("read backup object metadata"))?
        .len();
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(unavailable("hash backup object"))?;
        if read == 0 {
            break;
        }
        let bytes = buffer
            .get(..read)
            .ok_or_else(|| rejected("backup hash read exceeded its buffer"))?;
        digest.update(bytes);
    }
    Ok((size, format!("{:x}", digest.finalize())))
}

fn rejected(message: impl Into<String>) -> LogBackupError {
    LogBackupError::Rejected {
        message: message.into(),
    }
}

fn unavailable<Error: std::fmt::Display>(
    action: &'static str,
) -> impl FnOnce(Error) -> LogBackupError {
    move |error| unavailable_message(format!("failed to {action}: {error}"))
}

fn unavailable_message(message: impl Into<String>) -> LogBackupError {
    LogBackupError::Unavailable {
        message: message.into(),
    }
}
