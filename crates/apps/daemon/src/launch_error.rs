use std::path::PathBuf;

pub(crate) fn invalid(detail: impl Into<String>) -> DaemonLaunchError {
    DaemonLaunchError::InvalidConfiguration {
        detail: detail.into(),
    }
}

/// Why a protected launch document or production daemon start failed.
#[derive(Debug, thiserror::Error)]
pub enum DaemonLaunchError {
    /// Topology preflight rejected authoritative cluster settings.
    #[error(transparent)]
    InvalidTopology(#[from] cluster::ClusterPreflightError),
    /// Launch-specific mode, node, or path selection was invalid.
    #[error("invalid daemon launch configuration: {detail}")]
    InvalidConfiguration { detail: String },
    /// A secret-bearing launch document was accessible by other users.
    #[error("daemon launch document `{}` has insecure permissions {mode:#o}", path.display())]
    InsecurePermissions { path: PathBuf, mode: u32 },
    /// Launch document filesystem access failed.
    #[error("failed to {action} daemon launch document `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A launch document did not match its strict JSON schema.
    #[error("invalid daemon launch document `{}`: {source}", path.display())]
    InvalidDocument {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    /// AWS Secrets Manager did not return a usable operator signing key.
    #[error("failed to resolve operator JWT secret `{source_uri}`: {message}")]
    OperatorSecret { source_uri: String, message: String },
    /// Datadog log delivery configuration was unsafe or incomplete.
    #[error(transparent)]
    DatadogSettings(#[from] logs::DatadogLogSinkSettingsError),
    /// Datadog metric delivery configuration was unsafe or incomplete.
    #[error(transparent)]
    DatadogMetricSettings(#[from] metrics::DatadogMetricSinkSettingsError),
    /// Log backup namespace or KMS settings were invalid.
    #[error(transparent)]
    LogBackupSettings(#[from] logstore::LogBackupError),
    /// Production S3 backup adapter settings were invalid.
    #[error(transparent)]
    S3Backup(#[from] crate::S3BackupObjectStoreError),
    /// Scheduled log rollover, backup, or retention could not be initialized.
    #[error(transparent)]
    LogMaintenance(#[from] crate::LogMaintenanceError),
    /// The bounded production sink HTTP adapter could not be constructed.
    #[error(transparent)]
    HttpTransport(#[from] logs::ReqwestHttpTransportError),
    /// The bounded production metric sink HTTP adapter could not be constructed.
    #[error(transparent)]
    MetricHttpTransport(#[from] metrics::ReqwestMetricHttpTransportError),
    /// Provider configuration or store lifecycle failed.
    #[error(transparent)]
    StoreProvider(#[from] cluster::StoreProviderError),
    /// A worker could not connect to any declared control-plane store endpoint.
    #[error("worker store connection failed: {detail}")]
    RemoteStore { detail: String },
    /// The node-local WireGuard identity could not be loaded safely.
    #[error(transparent)]
    MeshIdentity(#[from] node_agent::MeshIdentityError),
    /// The production workload health probe adapter could not be constructed.
    #[error(transparent)]
    HealthProbe(#[from] node_agent::HealthProbeError),
    /// The native workload runtime could not be configured or reached.
    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),
    /// The node-local normalized log store could not be opened or initialized.
    #[error(transparent)]
    LogStore(#[from] logstore::DuckStoreError),
    /// A second observability store failed and the first could not be rolled back cleanly.
    #[error(
        "observability store startup failed: {startup}; prior store rollback failed: {rollback}"
    )]
    ObservabilityStoreRollback {
        /// Metric-store initialization failure.
        startup: String,
        /// Log-store shutdown failure observed during rollback.
        rollback: String,
    },
    /// A generated process identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// Static operator views could not be constructed from cluster settings.
    #[error(transparent)]
    OperatorSettings(#[from] crate::OperatorSuiteError),
    /// Build source roots or Git integration settings were invalid.
    #[error(transparent)]
    BuildSource(#[from] build::BuildSourceError),
    /// Pull-request preview launch settings were invalid.
    #[error(transparent)]
    Preview(#[from] crate::PreviewLaunchError),
    /// NixOS staging and reboot launch settings were invalid.
    #[error(transparent)]
    NixosUpgrade(#[from] crate::NixosUpgradeLaunchError),
    /// API listener or authentication policy was unsafe.
    #[error(transparent)]
    ApiSettings(#[from] server::ServerSettingsError),
    /// Role planning, startup, or rollback failed.
    #[error(transparent)]
    Daemon(#[from] crate::DaemonError),
}
