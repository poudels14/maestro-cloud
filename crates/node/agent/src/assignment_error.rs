use kernel_store::StoreError;
use runtime::{NetworkProviderError, RuntimeError};

#[cfg(unix)]
use crate::NodeApiMountError;
use crate::secret_mount::SecretMountError;

/// Why node-local assignment reconciliation could not complete its snapshot.
#[derive(Debug, thiserror::Error)]
pub enum AssignmentAgentError {
    /// A configured resource kind or observed identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// An invalid deadline would create an unbounded loop, hot loop, or inverted backoff.
    #[error("assignment deadlines must be positive and restart backoff must not decrease")]
    ZeroDeadline,
    /// Store access or watch setup failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Runtime cleanup failed and will be retried by a later resync.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// Network cleanup failed and will be retried by a later resync.
    #[error(transparent)]
    Network(#[from] NetworkProviderError),
    /// Secret materialization or zeroizing cleanup failed.
    #[error(transparent)]
    Secret(#[from] SecretMountError),
    /// Node API credential, listener, or cleanup management failed.
    #[cfg(unix)]
    #[error(transparent)]
    NodeApi(#[from] NodeApiMountError),
    /// The assignment disappeared while its observed status was being committed.
    #[error("assignment `{assignment_id}` disappeared before status update")]
    AssignmentDisappeared { assignment_id: String },
    /// A scheduler mutation moved an assignment away from this node during reconciliation.
    #[error("assignment `{assignment_id}` moved to another node during status update")]
    AssignmentMoved { assignment_id: String },
    /// A stored assignment could not be decoded for its conditional status write.
    #[error("malformed Assignment resource at `{key}`: {message}")]
    MalformedAssignment { key: String, message: String },
    /// A status-bearing assignment could not be encoded.
    #[error("failed to serialize Assignment resource: {message}")]
    SerializeResource { message: String },
    /// Repeated concurrent status writes exhausted the bounded retry budget.
    #[error("store contention prevented status update for assignment `{assignment_id}`")]
    Contention { assignment_id: String },
}
