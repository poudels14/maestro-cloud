use std::sync::Arc;

use crate::{
    DaemonLaunchError, LogBackupTarget, LogMaintenanceSettings, LogMaintenanceWorker,
    S3BackupObjectStore,
};
use cluster::LogBackupLaunchConfig;
use kernel_api::NodeId;
use kernel_controller::TimestampClock;
use kernel_store::Clock;
use logstore::{DuckLogStore, LogBackupSettings};

const MAX_BUCKET_BYTES: usize = 255;
const MAX_REGION_BYTES: usize = 64;
const MAX_KMS_KEY_BYTES: usize = 4_096;

pub(crate) fn validate_log_backup(
    config: &LogBackupLaunchConfig,
    cluster_name: &str,
    node_id: &NodeId,
) -> Result<(), DaemonLaunchError> {
    validate_bounded_text(&config.bucket, MAX_BUCKET_BYTES, "S3 backup bucket")?;
    validate_bounded_text(
        &config.kms_key_id,
        MAX_KMS_KEY_BYTES,
        "S3 backup KMS key identity",
    )?;
    if let Some(region) = &config.region {
        validate_bounded_text(region, MAX_REGION_BYTES, "S3 backup region")?;
        if !region
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(invalid("S3 backup region contains unsupported characters"));
        }
    }
    if config.retention_days == Some(0) {
        return Err(invalid("log backup retention days must be at least one"));
    }
    backup_settings(config, cluster_name, node_id)?;
    Ok(())
}

fn backup_settings(
    config: &LogBackupLaunchConfig,
    cluster_name: &str,
    node_id: &NodeId,
) -> Result<LogBackupSettings, DaemonLaunchError> {
    let prefix = config
        .prefix
        .as_deref()
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty())
        .unwrap_or(cluster_name);
    LogBackupSettings::new(prefix, node_id.clone(), config.kms_key_id.clone()).map_err(Into::into)
}

pub(crate) async fn configure_log_maintenance(
    config: Option<&LogBackupLaunchConfig>,
    cluster_name: &str,
    node_id: &NodeId,
    store: Arc<DuckLogStore>,
    monotonic_clock: Arc<dyn Clock>,
    timestamp_clock: Arc<dyn TimestampClock>,
) -> Result<LogMaintenanceWorker, DaemonLaunchError> {
    let backup = match config {
        Some(config) => {
            validate_log_backup(config, cluster_name, node_id)?;
            let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
            if let Some(region) = &config.region {
                loader = loader.region(aws_sdk_s3::config::Region::new(region.clone()));
            }
            let sdk_config = loader.load().await;
            let object_store = Arc::new(S3BackupObjectStore::new(
                aws_sdk_s3::Client::new(&sdk_config),
                config.bucket.clone(),
            )?);
            Some(LogBackupTarget::new(
                object_store,
                backup_settings(config, cluster_name, node_id)?,
                config.retention_days,
            )?)
        }
        None => None,
    };
    LogMaintenanceWorker::new(
        store,
        backup,
        LogMaintenanceSettings::default(),
        monotonic_clock,
        timestamp_clock,
    )
    .await
    .map_err(Into::into)
}

fn validate_bounded_text(
    value: &str,
    maximum: usize,
    field: &str,
) -> Result<(), DaemonLaunchError> {
    if value.is_empty()
        || value.len() > maximum
        || value.trim() != value
        || value.chars().any(char::is_whitespace)
        || value.chars().any(char::is_control)
    {
        return Err(invalid(format!("{field} is invalid")));
    }
    Ok(())
}

fn invalid(detail: impl Into<String>) -> DaemonLaunchError {
    DaemonLaunchError::InvalidConfiguration {
        detail: detail.into(),
    }
}
