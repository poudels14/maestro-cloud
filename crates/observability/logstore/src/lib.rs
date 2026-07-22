//! Durable DuckDB hot-tier storage for normalized Maestro observability data.
//!
//! This crate implements log and metric storage contracts. It must not depend on operators,
//! cluster provisioning, or application composition.

mod delivery_schema;
mod duck;
mod duck_query_compiler;
mod duck_worker;
mod error;
mod host_metric_delivery_schema;
mod host_metric_duck;
mod host_metric_schema;
mod log_archive;
mod log_backup;
mod log_backup_schema;
mod log_backup_stats_schema;
mod log_query_schema;
mod log_retention;
mod metric_delivery_schema;
mod metric_duck;
mod metric_schema;
mod query_duck;
mod schema;
mod settings;
mod workload_metric_schema;

pub use duck::{DuckLogStore, DuckLogStoreRuntime};
pub use error::{DuckStoreError, LogArchiveError, LogRetentionError};
pub use log_archive::{ColdPartitionManifest, ColdPartitionManifestPart, LogRolloverReport};
pub use log_backup::{
    BackupObjectBody, BackupObjectReceipt, BackupObjectStore, BackupObjectStoreError,
    BackupObjectUpload, LogBackupError, LogBackupRunReport, LogBackupSettings,
    backup_log_partitions,
};
pub use log_retention::LogRetentionReport;
pub use metric_duck::{DuckMetricStore, DuckMetricStoreRuntime};
pub use settings::DuckStoreSettings;

#[cfg(test)]
mod tests;
