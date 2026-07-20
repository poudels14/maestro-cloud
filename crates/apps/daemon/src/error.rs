use crate::DaemonRole;

/// Adapter failure at one daemon role boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{detail}")]
pub struct RoleError {
    detail: String,
}

impl RoleError {
    /// Creates a role error without exposing adapter-specific types.
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    /// Returns operator-facing role detail.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// One role and the failure observed while stopping it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleFailure {
    /// Role that failed.
    pub role: DaemonRole,
    /// Adapter detail returned by that role.
    pub error: RoleError,
}

/// Why daemon planning or owned role lifecycle failed.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// The declared topology was unsafe before role selection.
    #[error("cluster preflight failed: {0}")]
    InvalidTopology(#[from] cluster::ClusterPreflightError),
    /// The selected local node was absent from the declared topology.
    #[error("node `{node_id}` is absent from the cluster topology")]
    UnknownNode { node_id: kernel_api::NodeId },
    /// An empty data root cannot provide deterministic role persistence.
    #[error("daemon data directory cannot be empty")]
    EmptyDataDirectory,
    /// A role failed after zero or more earlier roles had started.
    #[error("failed to start {role}: {error}; rollback failures: {rollback_failures:?}")]
    Startup {
        role: DaemonRole,
        error: RoleError,
        rollback_failures: Vec<RoleFailure>,
    },
    /// Every role was asked to stop, but one or more returned an error.
    #[error("daemon role shutdown failed: {failures:?}")]
    Shutdown { failures: Vec<RoleFailure> },
}
