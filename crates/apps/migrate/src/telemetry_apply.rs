use std::path::{Path, PathBuf};

use kernel_api::{ClusterId, NodeId, Timestamp};
use logs::StatsMetricStore;
use logstore::{DuckLogStoreRuntime, DuckMetricStoreRuntime, DuckStoreSettings};
use metrics::{HostMetricStore, MetricStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::telemetry_destination::{
    DestinationPaths, existing_destination, inspect_destination, prepare_destination,
    read_completion, resume_log_high_watermark, write_completion,
};
use crate::telemetry_projection::{ProjectionBatch, TelemetryProjection};
use crate::{LegacyTelemetryCounts, LegacyTelemetryPlan};

const MARKER_SCHEMA_VERSION: u32 = 1;

/// Binds one destination agent directory to exactly one reviewed source plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyTelemetryDestination {
    /// Marker wire version.
    pub schema_version: u32,
    /// Destination cluster identity.
    pub cluster_id: ClusterId,
    /// Destination node identity.
    pub node_id: NodeId,
    /// Reviewed source inventory digest.
    pub source_sha256: String,
}

impl LegacyTelemetryDestination {
    fn new(plan: &LegacyTelemetryPlan) -> Self {
        Self {
            schema_version: MARKER_SCHEMA_VERSION,
            cluster_id: plan.cluster_id.clone(),
            node_id: plan.node_id.clone(),
            source_sha256: plan.source_sha256.clone(),
        }
    }
}

/// Exact logical destination proof produced before the completion marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyTelemetryVerification {
    /// Imported normalized logs.
    pub logs: LegacyTelemetryStreamVerification,
    /// Imported per-container resource samples.
    pub workload_metrics: LegacyTelemetryStreamVerification,
    /// Imported node resource samples.
    pub host_metrics: LegacyTelemetryStreamVerification,
    /// Imported controller and backup operational samples.
    pub operational_metrics: LegacyTelemetryStreamVerification,
    /// Imported persisted backup health singleton.
    pub backup_stats: LegacyTelemetryStreamVerification,
}

/// Count and ordered content digest for one converted stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyTelemetryStreamVerification {
    /// Records in the destination stream.
    pub records: u64,
    /// SHA-256 over length-framed canonical JSON records in insertion order.
    pub sha256: String,
}

/// Whether apply wrote records or verified an existing completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LegacyTelemetryApplyOutcome {
    /// This invocation completed a new or resumed import.
    Applied,
    /// An exact completion marker and destination already existed.
    AlreadyComplete,
}

/// Machine-readable result of one node-local telemetry apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyTelemetryApplyReport {
    /// Report wire version.
    pub schema_version: u32,
    /// Reviewed destination binding.
    pub destination: LegacyTelemetryDestination,
    /// Apply replay outcome.
    pub outcome: LegacyTelemetryApplyOutcome,
    /// Exact independently readable destination proof.
    pub verification: LegacyTelemetryVerification,
    /// Reviewed source inventory, including converted, regenerated, and retired rows.
    pub source_counts: LegacyTelemetryCounts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CompletionMarker {
    destination: LegacyTelemetryDestination,
    verification: LegacyTelemetryVerification,
}

/// Applies one reviewed node-local telemetry plan with resumable exact replays.
pub async fn apply_legacy_telemetry(
    plan: &LegacyTelemetryPlan,
    legacy_data_directory: &Path,
    destination_data_directory: &Path,
) -> Result<LegacyTelemetryApplyReport, LegacyTelemetryMigrationError> {
    let source = verify_source(plan, legacy_data_directory).await?;
    let destination = LegacyTelemetryDestination::new(plan);
    let paths = prepare_destination(destination_data_directory, &destination).await?;
    if let Some(completion) = read_completion(&paths).await? {
        let verification = verify_destination(plan, &source, &paths).await?;
        if completion
            != (CompletionMarker {
                destination: destination.clone(),
                verification: verification.clone(),
            })
        {
            return Err(LegacyTelemetryMigrationError::CompletionMismatch);
        }
        return Ok(report(
            plan,
            destination,
            LegacyTelemetryApplyOutcome::AlreadyComplete,
            verification,
        ));
    }

    import_projection(plan, &source, &paths).await?;
    verify_source(plan, legacy_data_directory).await?;
    let actual = verify_destination(plan, &source, &paths).await?;
    write_completion(
        &paths,
        &CompletionMarker {
            destination: destination.clone(),
            verification: actual.clone(),
        },
    )
    .await?;
    Ok(report(
        plan,
        destination,
        LegacyTelemetryApplyOutcome::Applied,
        actual,
    ))
}

/// Reprojects the reviewed source and proves a completed destination exactly.
pub async fn verify_legacy_telemetry(
    plan: &LegacyTelemetryPlan,
    legacy_data_directory: &Path,
    destination_data_directory: &Path,
) -> Result<LegacyTelemetryVerification, LegacyTelemetryMigrationError> {
    let source = verify_source(plan, legacy_data_directory).await?;
    let destination = LegacyTelemetryDestination::new(plan);
    let paths = existing_destination(destination_data_directory, &destination).await?;
    let completion = read_completion(&paths)
        .await?
        .ok_or(LegacyTelemetryMigrationError::MissingCompletion)?;
    let verification = verify_destination(plan, &source, &paths).await?;
    if completion
        != (CompletionMarker {
            destination,
            verification: verification.clone(),
        })
    {
        return Err(LegacyTelemetryMigrationError::CompletionMismatch);
    }
    Ok(verification)
}

fn report(
    plan: &LegacyTelemetryPlan,
    destination: LegacyTelemetryDestination,
    outcome: LegacyTelemetryApplyOutcome,
    verification: LegacyTelemetryVerification,
) -> LegacyTelemetryApplyReport {
    LegacyTelemetryApplyReport {
        schema_version: MARKER_SCHEMA_VERSION,
        destination,
        outcome,
        verification,
        source_counts: plan.counts.clone(),
    }
}

async fn verify_destination(
    plan: &LegacyTelemetryPlan,
    source: &Path,
    paths: &DestinationPaths,
) -> Result<LegacyTelemetryVerification, LegacyTelemetryMigrationError> {
    let expected = digest_projection(plan, source).await?;
    let actual = inspect_destination(paths).await?;
    if actual == expected {
        Ok(actual)
    } else {
        Err(LegacyTelemetryMigrationError::DestinationMismatch {
            expected: Box::new(expected),
            actual: Box::new(actual),
        })
    }
}

async fn verify_source(
    plan: &LegacyTelemetryPlan,
    source: &Path,
) -> Result<PathBuf, LegacyTelemetryMigrationError> {
    let plan = plan.clone();
    let source = source.to_path_buf();
    tokio::task::spawn_blocking(move || plan.verify_source(&source))
        .await
        .map_err(|error| LegacyTelemetryMigrationError::Worker(error.to_string()))?
        .map_err(Into::into)
}

async fn import_projection(
    plan: &LegacyTelemetryPlan,
    source: &Path,
    paths: &DestinationPaths,
) -> Result<(), LegacyTelemetryMigrationError> {
    let resume_logs = resume_log_high_watermark(paths).await?;
    let log_runtime =
        DuckLogStoreRuntime::open(DuckStoreSettings::new(paths.logs.clone(), 32)?).await?;
    let log_store = log_runtime.store();
    let metric_runtime = match DuckMetricStoreRuntime::open(DuckStoreSettings::new(
        paths.metrics.clone(),
        32,
    )?)
    .await
    {
        Ok(runtime) => runtime,
        Err(error) => {
            log_runtime.shutdown().await?;
            return Err(error.into());
        }
    };
    let metric_store = metric_runtime.store();
    let mut projection =
        TelemetryProjection::spawn(plan.clone(), source.to_path_buf(), resume_logs);
    let mut migration_error = None;
    while let Some(batch) = projection.next().await {
        let result = match batch {
            Ok(ProjectionBatch::Logs(entries)) => log_store
                .append_migration(&entries)
                .await
                .map(|_| ())
                .map_err(append),
            Ok(ProjectionBatch::WorkloadMetrics(points)) => metric_store
                .append(&points)
                .await
                .map(|_| ())
                .map_err(append),
            Ok(ProjectionBatch::HostMetrics(points)) => metric_store
                .append_host_metrics(&points)
                .await
                .map(|_| ())
                .map_err(append),
            Ok(ProjectionBatch::OperationalMetrics(points)) => log_store
                .append_stats_metrics(&points)
                .await
                .map(|_| ())
                .map_err(append),
            Ok(ProjectionBatch::BackupStats(stats)) => match stats {
                Some(stats) => log_store
                    .save_backup_stats(&stats, backup_updated_at(&stats))
                    .await
                    .map_err(append),
                None => Ok(()),
            },
            Err(error) => Err(error.into()),
        };
        if let Err(error) = result {
            migration_error = Some(error);
            projection.cancel();
            break;
        }
    }
    let projection_result = projection.finish().await;
    let metric_shutdown = metric_runtime.shutdown().await;
    let log_shutdown = log_runtime.shutdown().await;
    if let Some(error) = migration_error {
        return Err(error);
    }
    projection_result?;
    metric_shutdown?;
    log_shutdown?;
    Ok(())
}

async fn digest_projection(
    plan: &LegacyTelemetryPlan,
    source: &Path,
) -> Result<LegacyTelemetryVerification, LegacyTelemetryMigrationError> {
    let mut projection = TelemetryProjection::spawn(plan.clone(), source.to_path_buf(), 0);
    let mut digest = VerificationAccumulator::default();
    while let Some(batch) = projection.next().await {
        match batch? {
            ProjectionBatch::Logs(entries) => digest.logs.update_all(&entries)?,
            ProjectionBatch::WorkloadMetrics(points) => {
                digest.workload_metrics.update_all(&points)?;
            }
            ProjectionBatch::HostMetrics(points) => digest.host_metrics.update_all(&points)?,
            ProjectionBatch::OperationalMetrics(points) => {
                digest.operational_metrics.update_all(&points)?;
            }
            ProjectionBatch::BackupStats(stats) => {
                digest.backup_stats.update_option(stats.as_ref())?;
            }
        }
    }
    projection.finish().await?;
    Ok(digest.finish())
}

fn backup_updated_at(stats: &logs::BackupStatsSnapshot) -> Timestamp {
    Timestamp(
        stats
            .last_attempt_at_ms
            .or(stats.last_success_at_ms)
            .or(stats.last_error_at_ms)
            .unwrap_or(0),
    )
}

fn append(error: impl std::fmt::Display) -> LegacyTelemetryMigrationError {
    LegacyTelemetryMigrationError::Append(error.to_string())
}

#[derive(Default)]
struct VerificationAccumulator {
    logs: StreamAccumulator,
    workload_metrics: StreamAccumulator,
    host_metrics: StreamAccumulator,
    operational_metrics: StreamAccumulator,
    backup_stats: StreamAccumulator,
}

impl VerificationAccumulator {
    fn finish(self) -> LegacyTelemetryVerification {
        LegacyTelemetryVerification {
            logs: self.logs.finish(),
            workload_metrics: self.workload_metrics.finish(),
            host_metrics: self.host_metrics.finish(),
            operational_metrics: self.operational_metrics.finish(),
            backup_stats: self.backup_stats.finish(),
        }
    }
}

pub(crate) struct StreamAccumulator {
    records: u64,
    digest: Sha256,
}

impl Default for StreamAccumulator {
    fn default() -> Self {
        Self {
            records: 0,
            digest: Sha256::new(),
        }
    }
}

impl StreamAccumulator {
    fn update_option<Value: Serialize>(
        &mut self,
        value: Option<&Value>,
    ) -> Result<(), LegacyTelemetryMigrationError> {
        if let Some(value) = value {
            self.update_json(&serde_json::to_vec(value)?)?;
        }
        Ok(())
    }

    pub(crate) fn update_all<Value: Serialize>(
        &mut self,
        values: &[Value],
    ) -> Result<(), LegacyTelemetryMigrationError> {
        for value in values {
            self.update_json(&serde_json::to_vec(value)?)?;
        }
        Ok(())
    }

    pub(crate) fn update_json(
        &mut self,
        encoded: &[u8],
    ) -> Result<(), LegacyTelemetryMigrationError> {
        let length = u64::try_from(encoded.len())
            .map_err(|_| LegacyTelemetryMigrationError::RecordTooLarge)?;
        self.digest.update(length.to_be_bytes());
        self.digest.update(encoded);
        self.records = self.records.saturating_add(1);
        Ok(())
    }

    pub(crate) fn finish(self) -> LegacyTelemetryStreamVerification {
        LegacyTelemetryStreamVerification {
            records: self.records,
            sha256: hex::encode(self.digest.finalize()),
        }
    }
}

/// Node-local observability conversion could not complete safely.
#[derive(Debug, thiserror::Error)]
pub enum LegacyTelemetryMigrationError {
    #[error(transparent)]
    Plan(#[from] crate::LegacyTelemetryPlanError),
    #[error(transparent)]
    Store(#[from] logstore::DuckStoreError),
    #[error("legacy telemetry projection failed: {0}")]
    Projection(String),
    #[error("could not encode or decode telemetry migration JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("telemetry migration worker failed: {0}")]
    Worker(String),
    #[error("telemetry destination path must be absolute without traversal: {}", path.display())]
    InvalidDestination { path: PathBuf },
    #[error(
        "telemetry destination `{}` must be canonical; use `{}`",
        path.display(),
        canonical.display()
    )]
    NonCanonicalDestination { path: PathBuf, canonical: PathBuf },
    #[error("telemetry destination entry `{}` is a symlink or special file", path.display())]
    UnsafeDestination { path: PathBuf },
    #[error("telemetry destination `{}` contains stores without a migration intent", path.display())]
    UnownedDestination { path: PathBuf },
    #[error("telemetry migration intent does not match the reviewed source")]
    IntentMismatch,
    #[error("telemetry migration completion marker is missing")]
    MissingCompletion,
    #[error("telemetry migration completion marker does not match destination verification")]
    CompletionMismatch,
    #[error("telemetry destination contains non-migration state")]
    UnexpectedDestinationState,
    #[error("telemetry hot query tier does not match the normalized delivery tier")]
    QueryTierMismatch,
    #[error("telemetry append failed: {0}")]
    Append(String),
    #[error("telemetry record is too large to hash")]
    RecordTooLarge,
    #[error("telemetry destination does not match the projected source")]
    DestinationMismatch {
        expected: Box<LegacyTelemetryVerification>,
        actual: Box<LegacyTelemetryVerification>,
    },
    #[error("telemetry marker `{}` is not a bounded regular file", path.display())]
    InvalidMarker { path: PathBuf },
    #[error("telemetry marker `{}` must have owner-only permissions", path.display())]
    PublicMarker { path: PathBuf },
    #[error("telemetry destination database failed: {0}")]
    Database(String),
    #[error("could not {action} telemetry path `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl From<crate::telemetry_conversion::TelemetryProjectionError> for LegacyTelemetryMigrationError {
    fn from(error: crate::telemetry_conversion::TelemetryProjectionError) -> Self {
        Self::Projection(error.to_string())
    }
}
