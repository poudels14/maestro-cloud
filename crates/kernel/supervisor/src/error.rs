use std::path::PathBuf;

use crate::ProcessHandle;

/// Matchable detached-process supervision failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SupervisorError {
    /// Process specification violates a portable safety invariant.
    #[error("invalid process specification: {message}")]
    InvalidSpec {
        /// Stable explanation safe to surface in workload status.
        message: String,
    },
    /// Filesystem or process creation failed.
    #[error("process {operation} failed for `{path}`: {message}")]
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Executable, log, or procfs path involved.
        path: PathBuf,
        /// OS detail safe to log.
        message: String,
    },
    /// A persisted PID now belongs to a different operating-system process.
    #[error(
        "process {pid} start time changed from {expected_start_time_ticks} to {actual_start_time_ticks}"
    )]
    IdentityMismatch {
        /// Reused process identifier.
        pid: u32,
        /// Start time persisted in the handle.
        expected_start_time_ticks: u64,
        /// Start time currently reported by procfs.
        actual_start_time_ticks: u64,
    },
    /// Waiting is only available while this supervisor owns the original child handle.
    #[error("process {handle:?} is running but is not an owned child")]
    NotOwned {
        /// Adopted process that cannot be reaped by this instance.
        handle: ProcessHandle,
    },
    /// Process-group signal delivery failed.
    #[error("failed to send {signal} to process group {pid}: {message}")]
    Signal {
        /// Process-group leader identity.
        pid: u32,
        /// Requested signal name.
        signal: &'static str,
        /// OS detail safe to log.
        message: String,
    },
    /// Internal state lock was poisoned by a prior panic.
    #[error("process supervisor state is unavailable")]
    StateUnavailable,
    /// A blocking operation task could not be joined.
    #[error("process supervisor task failed: {message}")]
    Task {
        /// Join failure safe to log.
        message: String,
    },
}
