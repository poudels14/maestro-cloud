use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumMode, ServerSideEncryption};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::store::{LogStore, PendingPartition};

const BACKUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MANIFEST: &str = "manifest.json";
const SHA256_METADATA: &str = "sha256";
const DAY_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackupConfig {
    pub bucket: String,
    pub kms_key_id: String,
    pub region: Option<String>,
    pub prefix: String,
    pub node: String,
    pub retention_days: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupStats {
    pub configured: bool,
    pub last_attempt_at_ms: Option<i64>,
    pub last_success_at_ms: Option<i64>,
    pub last_error_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub pending_partitions: usize,
    pub completed_partitions_last_run: usize,
    pub failed_partitions_last_run: usize,
    pub uploaded_bytes_last_run: u64,
}

struct Upload {
    key: String,
    bytes: Vec<u8>,
}

pub async fn run_backup(
    config: BackupConfig,
    store: Arc<LogStore>,
    stats: Arc<Mutex<BackupStats>>,
) -> crate::error::Result<()> {
    let client = s3_client(&config).await;
    let mut ticker = tokio::time::interval(BACKUP_INTERVAL);
    loop {
        ticker.tick().await;
        let pending = store.partitions_pending_backup().unwrap_or_default();
        lock(&stats).last_attempt_at_ms = Some(now_ms());
        let mut completed = 0;
        let mut failures = Vec::new();
        let mut uploaded = 0u64;
        for partition in &pending {
            match backup_partition(&client, &config, &store, partition).await {
                Ok(bytes) => {
                    completed += 1;
                    uploaded += bytes;
                }
                Err(error) => {
                    tracing::warn!(partition = %partition.key, %error, "log backup failed");
                    failures.push(error);
                }
            }
        }
        if let Some(days) = config.retention_days {
            let cutoff =
                chrono::DateTime::from_timestamp_millis(now_ms() - i64::from(days) * DAY_MS)
                    .map(|cutoff| cutoff.date_naive().to_string())
                    .unwrap_or_default();
            if let Err(error) = store.prune_backed_up_before(&cutoff) {
                tracing::warn!(%error, "failed to prune backed up partitions");
            }
        }
        let mut current = lock(&stats);
        current.pending_partitions = store
            .partitions_pending_backup()
            .map(|left| left.len())
            .unwrap_or_default();
        current.completed_partitions_last_run = completed;
        current.failed_partitions_last_run = failures.len();
        current.uploaded_bytes_last_run = uploaded;
        match failures.first() {
            Some(error) => {
                current.last_error_at_ms = Some(now_ms());
                current.last_error = Some(error.clone());
            }
            None => current.last_success_at_ms = Some(now_ms()),
        }
    }
}

async fn backup_partition(
    client: &Client,
    config: &BackupConfig,
    store: &LogStore,
    partition: &PendingPartition,
) -> Result<u64, String> {
    let manifest_path = partition.directory.join(MANIFEST);
    let manifest: Value = serde_json::from_slice(
        &tokio::fs::read(&manifest_path)
            .await
            .map_err(|error| format!("missing manifest: {error}"))?,
    )
    .map_err(|error| error.to_string())?;
    let (uploads, rewritten, seq_los) = plan_uploads(config, partition, &manifest).await?;
    let mut total = 0u64;
    for upload in uploads.iter().chain([&Upload {
        key: object_key(config, &partition.key, MANIFEST),
        bytes: serde_json::to_vec_pretty(&rewritten).map_err(|error| error.to_string())?,
    }]) {
        put_verified(client, config, upload).await?;
        total += upload.bytes.len() as u64;
    }
    store
        .mark_backed_up(&partition.key, &seq_los)
        .map_err(|error| error.to_string())?;
    Ok(total)
}

async fn plan_uploads(
    config: &BackupConfig,
    partition: &PendingPartition,
    manifest: &Value,
) -> Result<(Vec<Upload>, Value, Vec<i64>), String> {
    let mut rewritten = manifest.clone();
    let mut uploads = Vec::new();
    let mut seq_los = Vec::new();
    let parts = rewritten["parts"]
        .as_array_mut()
        .ok_or("manifest has no parts")?;
    for part in parts.iter_mut() {
        let file = part["file"]
            .as_str()
            .ok_or("manifest part has no file")?
            .to_string();
        let bytes = tokio::fs::read(partition.directory.join(&file))
            .await
            .map_err(|error| format!("failed to read {file}: {error}"))?;
        let node_file = format!("{}-{file}", config.node);
        uploads.push(Upload {
            key: object_key(config, &partition.key, &node_file),
            bytes,
        });
        seq_los.extend(part["seq_lo"].as_i64());
        part["file"] = Value::String(node_file);
    }
    Ok((uploads, rewritten, seq_los))
}

pub struct S3Archive {
    client: Client,
    config: BackupConfig,
}

impl S3Archive {
    pub async fn connect(config: BackupConfig) -> Self {
        Self {
            client: s3_client(&config).await,
            config,
        }
    }

    pub async fn put(&self, path: &str, bytes: Vec<u8>) -> Result<(), String> {
        let upload = Upload {
            key: object_key(&self.config, "", path),
            bytes,
        };
        put_verified(&self.client, &self.config, &upload).await
    }
}

async fn s3_client(config: &BackupConfig) -> Client {
    let shared = match &config.region {
        Some(region) => {
            aws_config::defaults(BehaviorVersion::latest())
                .region(aws_config::Region::new(region.clone()))
                .load()
                .await
        }
        None => aws_config::load_defaults(BehaviorVersion::latest()).await,
    };
    Client::new(&shared)
}

async fn put_verified(
    client: &Client,
    config: &BackupConfig,
    upload: &Upload,
) -> Result<(), String> {
    let digest = Sha256::digest(&upload.bytes);
    let checksum = BASE64.encode(digest);
    let hex_digest = hex::encode(digest);
    client
        .put_object()
        .bucket(&config.bucket)
        .key(&upload.key)
        .body(ByteStream::from(upload.bytes.clone()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&config.kms_key_id)
        .checksum_sha256(&checksum)
        .metadata(SHA256_METADATA, &hex_digest)
        .send()
        .await
        .map_err(|error| {
            format!(
                "upload {}: {}",
                upload.key,
                aws_sdk_s3::error::DisplayErrorContext(error)
            )
        })?;
    let head = client
        .head_object()
        .bucket(&config.bucket)
        .key(&upload.key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .map_err(|error| {
            format!(
                "verify {}: {}",
                upload.key,
                aws_sdk_s3::error::DisplayErrorContext(error)
            )
        })?;
    let verified = head.content_length() == Some(upload.bytes.len() as i64)
        && head
            .checksum_sha256()
            .is_none_or(|stored| stored.starts_with(&checksum))
        && head.server_side_encryption() == Some(&ServerSideEncryption::AwsKms)
        && head.ssekms_key_id().is_some()
        && head
            .metadata()
            .and_then(|metadata| metadata.get(SHA256_METADATA))
            .is_some_and(|stored| *stored == hex_digest);
    match verified {
        true => Ok(()),
        false => Err(format!("verification of {} failed", upload.key)),
    }
}

fn object_key(config: &BackupConfig, partition_key: &str, file: &str) -> String {
    [config.prefix.trim_matches('/'), partition_key, file]
        .into_iter()
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

pub fn new_backup_stats(configured: bool) -> Arc<Mutex<BackupStats>> {
    Arc::new(Mutex::new(BackupStats {
        configured,
        ..BackupStats::default()
    }))
}

fn lock(stats: &Mutex<BackupStats>) -> std::sync::MutexGuard<'_, BackupStats> {
    stats
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> BackupConfig {
        BackupConfig {
            bucket: "logs".to_string(),
            kms_key_id: "key".to_string(),
            region: None,
            prefix: "/prod/".to_string(),
            node: "node1".to_string(),
            retention_days: None,
        }
    }

    #[test]
    fn builds_object_keys_under_the_prefix() {
        assert_eq!(
            object_key(
                &config(),
                "service_id=api/deployment_id=d1/date=2026-10-13",
                "node1-part-1-9.parquet"
            ),
            "prod/service_id=api/deployment_id=d1/date=2026-10-13/node1-part-1-9.parquet"
        );
    }

    #[tokio::test]
    async fn prefixes_part_files_with_the_node_in_the_uploaded_manifest() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("part-1-9.parquet"), b"parquet").unwrap();
        let manifest = serde_json::json!({
            "version": 1,
            "partition_key": "date=2026-10-13",
            "parts": [{ "file": "part-1-9.parquet", "seq_lo": 1, "seq_hi": 9 }]
        });
        let partition = PendingPartition {
            key: "date=2026-10-13".to_string(),
            directory: directory.path().to_path_buf(),
        };
        let (uploads, rewritten, seq_los) = plan_uploads(&config(), &partition, &manifest)
            .await
            .unwrap();
        assert_eq!(
            uploads[0].key,
            "prod/date=2026-10-13/node1-part-1-9.parquet"
        );
        assert_eq!(uploads[0].bytes, b"parquet");
        assert_eq!(rewritten["parts"][0]["file"], "node1-part-1-9.parquet");
        assert_eq!(seq_los, [1]);
    }
}
