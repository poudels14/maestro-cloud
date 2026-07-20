/// Matchable failure from controller-kernel infrastructure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControllerError {
    /// The persistence backend could not complete an operation.
    #[error(transparent)]
    Store(#[from] kernel_store::StoreError),
    /// A formerly valid leader attempted a mutation after losing its fence.
    #[error("leadership fence no longer matches the active leader key")]
    LeadershipLost,
    /// A controller-kernel invariant was violated.
    #[error("controller runtime violated its contract: {message}")]
    Contract {
        /// Invariant violation detail.
        message: String,
    },
    /// Persisted leader identity could not be decoded safely.
    #[error("persisted leader identity is malformed: {message}")]
    MalformedLeader {
        /// Serde decoding detail.
        message: String,
    },
    /// A persisted request claim could not be decoded safely.
    #[error("persisted request claim is malformed: {message}")]
    MalformedRequestClaim {
        /// Serde decoding detail.
        message: String,
    },
    /// One request identity was reused for different mutation content.
    #[error("request identity was already claimed with a different fingerprint")]
    RequestCollision,
}
