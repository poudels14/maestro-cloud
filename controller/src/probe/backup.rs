use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumMode, ServerSideEncryption};

use crate::logs::{BackupPartition, DuckLogStore};

mod digest;
mod multipart;

use digest::digest_file;
use multipart::upload_file_multipart;

#[cfg(test)]
use digest::{
    DEFAULT_MULTIPART_PART_BYTES, MAX_MULTIPART_PART_BYTES, MAX_MULTIPART_PARTS,
    MULTIPART_THRESHOLD_BYTES, composite_sha256, digest_file_with_part_size, multipart_part_size,
};

const BACKUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BackupRunStats {
    attempted_partitions: usize,
    completed_partitions: usize,
    failed_partitions: usize,
    uploaded_objects: usize,
    multipart_objects: usize,
    uploaded_parts: usize,
    uploaded_bytes: u64,
    duration_ms: u64,
}

#[derive(Default)]
struct PartitionUploadStats {
    objects: usize,
    multipart_objects: usize,
    parts: usize,
    bytes: u64,
}

pub struct BackupConfig {
    bucket: String,
    prefix: String,
    kms_key_id: String,
    region: Option<String>,
    parts_root: PathBuf,
}

impl BackupConfig {
    pub fn from_config(
        data_root: &Path,
        cluster_name: &str,
        config: &crate::config::LogBackupConfig,
    ) -> Result<Self> {
        let bucket = config.bucket.trim();
        let kms_key_id = config.kms_key_id.trim();
        if bucket.is_empty() {
            bail!("log-backup.bucket cannot be empty");
        }
        if kms_key_id.is_empty() {
            bail!("log-backup.kms-key-id cannot be empty");
        }
        if config.retention_days == Some(0) {
            bail!("log-backup.retention-days must be at least 1");
        }
        let prefix = config
            .prefix
            .as_deref()
            .map(str::trim)
            .filter(|prefix| !prefix.is_empty())
            .unwrap_or(cluster_name.trim())
            .trim_matches('/')
            .to_string();
        Ok(Self {
            bucket: bucket.to_string(),
            prefix,
            kms_key_id: kms_key_id.to_string(),
            region: config
                .region
                .as_deref()
                .map(str::trim)
                .filter(|region| !region.is_empty())
                .map(ToString::to_string),
            parts_root: data_root.join("parts"),
        })
    }
}

pub async fn run(store: Arc<DuckLogStore>, config: BackupConfig) {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = config.region.clone() {
        loader = loader.region(aws_sdk_s3::config::Region::new(region));
    }
    let sdk_config = loader.load().await;
    let client = aws_sdk_s3::Client::new(&sdk_config);
    let mut interval = tokio::time::interval(BACKUP_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        match backup_once(&client, &store, &config).await {
            Ok(stats) if stats.attempted_partitions > 0 => eprintln!(
                "[maestro]: log_backup {}",
                serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string())
            ),
            Ok(_) => {}
            Err(err) => eprintln!("log partition backup failed: {err:#}"),
        }
    }
}

async fn backup_once(
    client: &aws_sdk_s3::Client,
    store: &DuckLogStore,
    config: &BackupConfig,
) -> Result<BackupRunStats> {
    let started = Instant::now();
    let partitions = store.pending_backups().await?;
    let mut stats = BackupRunStats {
        attempted_partitions: partitions.len(),
        ..BackupRunStats::default()
    };
    for partition in partitions {
        let uploaded = match upload_partition(client, config, &partition).await {
            Ok(uploaded) => uploaded,
            Err(err) => {
                stats.failed_partitions += 1;
                eprintln!(
                    "[maestro]: log_backup_partition_failed tier={} partition={} error={err:#}",
                    partition.tier, partition.partition_key
                );
                continue;
            }
        };
        store
            .mark_backed_up(
                &partition.tier,
                &partition.partition_key,
                &partition.seq_los,
            )
            .await?;
        stats.completed_partitions += 1;
        stats.uploaded_objects += uploaded.objects;
        stats.multipart_objects += uploaded.multipart_objects;
        stats.uploaded_parts += uploaded.parts;
        stats.uploaded_bytes += uploaded.bytes;
    }
    stats.duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    Ok(stats)
}

async fn upload_partition(
    client: &aws_sdk_s3::Client,
    config: &BackupConfig,
    partition: &BackupPartition,
) -> Result<PartitionUploadStats> {
    if partition.files.is_empty() {
        bail!("partition has no backup objects");
    }
    let mut stats = PartitionUploadStats::default();
    for path in &partition.files {
        let relative = path.strip_prefix(&config.parts_root).with_context(|| {
            format!(
                "{} is outside {}",
                path.display(),
                config.parts_root.display()
            )
        })?;
        let relative = relative
            .to_str()
            .ok_or_else(|| anyhow!("backup path is not UTF-8: {}", path.display()))?;
        let key = if config.prefix.is_empty() {
            relative.to_string()
        } else {
            format!("{}/{relative}", config.prefix)
        };
        let object = upload_file(client, config, path, &key).await?;
        stats.objects += 1;
        stats.multipart_objects += usize::from(object.multipart);
        stats.parts += object.parts;
        stats.bytes += object.bytes;
    }
    Ok(stats)
}

struct ObjectUploadStats {
    multipart: bool,
    parts: usize,
    bytes: u64,
}

async fn upload_file(
    client: &aws_sdk_s3::Client,
    config: &BackupConfig,
    path: &Path,
    key: &str,
) -> Result<ObjectUploadStats> {
    let path_for_hash = path.to_path_buf();
    let digest = tokio::task::spawn_blocking(move || digest_file(&path_for_hash)).await??;
    let stored_checksum = if digest.parts.is_empty() {
        let body = ByteStream::from_path(path)
            .await
            .with_context(|| format!("open backup object {}", path.display()))?;
        let uploaded = client
            .put_object()
            .bucket(&config.bucket)
            .key(key)
            .body(body)
            .content_length(i64::try_from(digest.size)?)
            .checksum_sha256(&digest.whole_base64)
            .metadata("sha256", &digest.whole_hex)
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(&config.kms_key_id)
            .send()
            .await
            .with_context(|| format!("upload s3://{}/{key}", config.bucket))?;
        if uploaded.checksum_sha256() != Some(digest.whole_base64.as_str()) {
            bail!("S3 upload checksum mismatch for {key}");
        }
        digest.whole_base64.clone()
    } else {
        upload_file_multipart(client, config, path, key, &digest).await?
    };

    let head = client
        .head_object()
        .bucket(&config.bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .with_context(|| format!("verify s3://{}/{key}", config.bucket))?;
    if head.content_length() != Some(i64::try_from(digest.size)?)
        || head.checksum_sha256() != Some(stored_checksum.as_str())
        || head.server_side_encryption() != Some(&ServerSideEncryption::AwsKms)
        || head.ssekms_key_id().is_none()
        || head
            .metadata()
            .and_then(|metadata| metadata.get("sha256"))
            .map(String::as_str)
            != Some(digest.whole_hex.as_str())
    {
        bail!("S3 verification failed for {key}");
    }
    Ok(ObjectUploadStats {
        multipart: !digest.parts.is_empty(),
        parts: digest.parts.len().max(1),
        bytes: digest.size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use sha2::{Digest, Sha256};

    #[test]
    fn builds_backup_settings_from_cluster_config() {
        let settings = crate::config::LogBackupConfig {
            bucket: " maestro-logs ".into(),
            kms_key_id: " kms-key ".into(),
            region: Some(" us-west-2 ".into()),
            prefix: None,
            retention_days: Some(30),
        };
        let config = BackupConfig::from_config(Path::new("/data"), "production", &settings)
            .expect("backup config");
        assert_eq!(config.bucket, "maestro-logs");
        assert_eq!(config.kms_key_id, "kms-key");
        assert_eq!(config.region.as_deref(), Some("us-west-2"));
        assert_eq!(config.prefix, "production");
        assert_eq!(config.parts_root, Path::new("/data/parts"));
    }

    #[test]
    fn hashes_backup_objects_for_s3_and_manifests() {
        let path = std::env::temp_dir().join(format!(
            "maestro-backup-hash-{}-{}",
            std::process::id(),
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::write(&path, b"maestro").expect("write fixture");
        let digest = digest_file(&path).expect("hash");
        assert_eq!(digest.size, 7);
        assert_eq!(
            digest.whole_base64,
            "Iz4c3tYazoEdepMJR7BqyxjNodx7Z+IVlB4YMP3sO3w="
        );
        assert_eq!(
            digest.whole_hex,
            "233e1cded61ace811d7a930947b06acb18cda1dc7b67e215941e1830fdec3b7c"
        );
        assert!(digest.parts.is_empty());
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn multipart_part_plan_stays_within_s3_limits() {
        assert_eq!(
            multipart_part_size(MULTIPART_THRESHOLD_BYTES).expect("default part size"),
            DEFAULT_MULTIPART_PART_BYTES
        );
        let five_tebibytes = 5 * 1024_u64.pow(4);
        let part_size = multipart_part_size(five_tebibytes).expect("maximum S3 object");
        assert!(part_size <= MAX_MULTIPART_PART_BYTES);
        assert!(five_tebibytes.div_ceil(part_size) <= MAX_MULTIPART_PARTS);
    }

    #[test]
    fn computes_part_and_composite_sha256_checksums() {
        let path = std::env::temp_dir().join(format!(
            "maestro-backup-parts-{}-{}",
            std::process::id(),
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::write(&path, b"abcdefghij").expect("write fixture");
        let digest = digest_file_with_part_size(&path, Some(4)).expect("part checksums");
        let checksums = digest
            .parts
            .iter()
            .map(|part| part.checksum_base64.clone())
            .collect::<Vec<_>>();
        let expected = [b"abcd".as_slice(), b"efgh".as_slice(), b"ij".as_slice()]
            .map(|part| base64::engine::general_purpose::STANDARD.encode(Sha256::digest(part)));
        assert_eq!(checksums, expected);
        assert_eq!(digest.parts[0].offset, 0);
        assert_eq!(digest.parts[0].length, 4);
        assert_eq!(digest.parts[1].offset, 4);
        assert_eq!(digest.parts[1].length, 4);
        assert_eq!(digest.parts[2].offset, 8);
        assert_eq!(digest.parts[2].length, 2);
        let composite = composite_sha256(&checksums).expect("composite checksum");
        assert!(composite.ends_with("-3"));
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    #[ignore = "requires a real S3 bucket and KMS key"]
    async fn uploads_and_verifies_multipart_object_against_s3() {
        let bucket = std::env::var("MAESTRO_TEST_S3_BUCKET")
            .expect("set MAESTRO_TEST_S3_BUCKET for the ignored S3 integration test");
        let kms_key_id = std::env::var("MAESTRO_TEST_S3_KMS_KEY_ID")
            .expect("set MAESTRO_TEST_S3_KMS_KEY_ID for the ignored S3 integration test");
        let region = std::env::var("MAESTRO_TEST_S3_REGION").ok();
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = region.clone() {
            loader = loader.region(aws_sdk_s3::config::Region::new(region));
        }
        let client = aws_sdk_s3::Client::new(&loader.load().await);
        let path = std::env::temp_dir().join(format!(
            "maestro-s3-integration-{}-{}",
            std::process::id(),
            crate::utils::nanoid::unique_id(8)
        ));
        let file = std::fs::File::create(&path).expect("create sparse multipart fixture");
        file.set_len(MULTIPART_THRESHOLD_BYTES)
            .expect("size multipart fixture");
        drop(file);
        let key = format!(
            "maestro-integration-tests/multipart-{}",
            crate::utils::nanoid::unique_id(16)
        );
        let config = BackupConfig {
            bucket,
            prefix: "maestro-integration-tests".into(),
            kms_key_id,
            region,
            parts_root: std::env::temp_dir(),
        };

        let uploaded = upload_file(&client, &config, &path, &key).await;
        let cleanup = client
            .delete_object()
            .bucket(&config.bucket)
            .key(&key)
            .send()
            .await;
        std::fs::remove_file(path).ok();

        let uploaded = uploaded.expect("multipart upload and verification");
        assert!(uploaded.multipart);
        assert!(uploaded.parts >= 2);
        cleanup.expect("delete S3 integration object");
    }
}
