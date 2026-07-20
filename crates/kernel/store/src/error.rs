use crate::{SessionId, WatchCursor};

/// A backend-neutral persistence failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// A resume cursor is no longer retained by the backend.
    #[error("watch cursor {cursor:?} expired and requires a full relist")]
    CursorExpired {
        /// Cursor rejected by the backend.
        cursor: WatchCursor,
    },
    /// A session expired or was explicitly closed.
    #[error("store session {session_id:?} is no longer active")]
    SessionExpired {
        /// Session rejected by the backend.
        session_id: SessionId,
    },
    /// The backend cannot currently serve a linearizable operation.
    #[error("store backend is unavailable: {message}")]
    Unavailable {
        /// Backend detail suitable for an operator-facing condition.
        message: String,
    },
    /// A backend returned state that violates the store contract.
    #[error("store backend violated its contract: {message}")]
    Contract {
        /// Invariant violation detail.
        message: String,
    },
}
