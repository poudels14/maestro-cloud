use std::path::PathBuf;

use kernel_api::WorkloadId;

use crate::RuntimeCapability;

/// Matchable failure returned by workload lifecycle and stream operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeError {
    /// The desired workload specification cannot be represented by the backend.
    #[error("invalid workload specification: {message}")]
    InvalidSpec {
        /// Stable explanation safe to surface in assignment status.
        message: String,
    },
    /// No backend object corresponds to the requested workload.
    #[error("workload `{workload_id}` does not exist")]
    NotFound {
        /// Missing Maestro workload identity.
        workload_id: WorkloadId,
    },
    /// Existing backend state conflicts with the requested transition.
    #[error("workload `{workload_id}` is in a conflicting state: {message}")]
    Conflict {
        /// Conflicting Maestro workload identity.
        workload_id: WorkloadId,
        /// Stable explanation safe to surface in assignment status.
        message: String,
    },
    /// The caller invoked an operation excluded by the backend's capabilities.
    #[error("runtime capability `{capability:?}` is not supported")]
    Unsupported {
        /// Missing backend capability.
        capability: RuntimeCapability,
    },
    /// The backend is temporarily unavailable and reconciliation should retry.
    #[error("runtime backend is unavailable: {message}")]
    Unavailable {
        /// Backend detail safe to log.
        message: String,
    },
    /// The backend rejected an operation that retrying cannot repair.
    #[error("runtime backend rejected the operation: {message}")]
    Rejected {
        /// Backend detail safe to surface in assignment status.
        message: String,
    },
    /// An event, log, or exec stream failed and should be re-established.
    #[error("runtime stream failed: {message}")]
    Stream {
        /// Backend detail safe to log.
        message: String,
    },
}

/// Why a backend-provided cgroup path was unsafe to consume.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CgroupPathError {
    /// The cgroup path must be rooted so the agent never resolves it relative to its cwd.
    #[error("cgroup path `{path}` is not absolute")]
    Relative {
        /// Rejected backend path.
        path: PathBuf,
    },
}
