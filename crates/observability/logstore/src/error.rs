use std::path::PathBuf;

/// Failure to create, own, or stop a DuckDB log-store worker.
#[derive(Debug, thiserror::Error)]
pub enum DuckStoreError {
    /// Static store settings cannot form a safe worker.
    #[error("invalid DuckDB store configuration: {message}")]
    InvalidConfiguration {
        /// Stable validation detail.
        message: String,
    },
    /// The dedicated writer thread could not be created.
    #[error("failed to spawn DuckDB store worker for `{}`: {source}", path.display())]
    Spawn {
        /// Database path assigned to the worker.
        path: PathBuf,
        /// Host thread creation error.
        #[source]
        source: std::io::Error,
    },
    /// Database open or versioned schema initialization failed.
    #[error("failed to initialize DuckDB store `{}`: {message}", path.display())]
    Initialize {
        /// Database path that failed initialization.
        path: PathBuf,
        /// Database or filesystem detail.
        message: String,
    },
    /// The writer stopped before accepting a lifecycle command.
    #[error("DuckDB store worker stopped before {action}")]
    WorkerStopped {
        /// Operation interrupted by worker exit.
        action: &'static str,
    },
    /// The operating system thread terminated with a panic.
    #[error("DuckDB store worker panicked")]
    WorkerPanicked,
}

/// Hourly hot-to-cold log archival could not complete safely.
#[derive(Debug, thiserror::Error)]
pub enum LogArchiveError {
    /// The cutoff or persisted archive state was invalid.
    #[error("log archive rejected operation: {message}")]
    Rejected {
        /// Stable validation detail.
        message: String,
    },
    /// DuckDB or the cold-tier filesystem was unavailable.
    #[error("log archive is unavailable: {message}")]
    Unavailable {
        /// Safe storage diagnostic.
        message: String,
    },
}

/// Fully backed-up cold log partitions could not be pruned safely.
#[derive(Debug, thiserror::Error)]
pub enum LogRetentionError {
    /// The cutoff or persisted partition state was invalid.
    #[error("log retention rejected operation: {message}")]
    Rejected {
        /// Stable validation detail.
        message: String,
    },
    /// DuckDB or the cold-tier filesystem was unavailable.
    #[error("log retention is unavailable: {message}")]
    Unavailable {
        /// Safe storage diagnostic.
        message: String,
    },
}
