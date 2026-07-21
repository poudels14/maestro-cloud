use async_trait::async_trait;
use std::sync::Arc;

use crate::IngestLogEntry;

/// Outcome of one atomic idempotent append batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogAppendReport {
    /// New producer identities committed by this append.
    pub committed: usize,
    /// Exact producer replays already present in the store.
    pub deduplicated: usize,
}

/// Durable normalized-log boundary shared by local stores and forwarding sinks.
#[async_trait]
pub trait LogStore: Send + Sync {
    /// Atomically appends a batch, deduplicating exact `LogRecordId` replays.
    ///
    /// Cancellation may leave the entire batch committed. Retrying the same entries is safe and
    /// reports them as deduplicated; reusing an identity with different content is rejected.
    async fn append(&self, entries: &[IngestLogEntry]) -> Result<LogAppendReport, LogStoreError>;
}

/// Explicit lifetime owner for a log store and any workers behind it.
#[async_trait]
pub trait LogStoreRuntime: Send {
    /// Returns the shared append boundary while retaining lifecycle ownership.
    fn store(&self) -> Arc<dyn LogStore>;

    /// Drains accepted writes and releases the store's owned resources.
    async fn shutdown(self: Box<Self>) -> Result<(), LogStoreRuntimeError>;
}

/// A normalized batch could not cross the durable log boundary.
#[derive(Debug, thiserror::Error)]
pub enum LogStoreError {
    /// Content or replay identity permanently violates the store contract.
    #[error("log store rejected entry: {message}")]
    Rejected {
        /// Stable rejection detail.
        message: String,
    },
    /// The store is temporarily unable to accept a valid batch.
    #[error("log store is unavailable: {message}")]
    Unavailable {
        /// Safe availability detail.
        message: String,
    },
}

/// Failure to stop an owned log-store runtime cleanly.
#[derive(Debug, thiserror::Error)]
#[error("log-store runtime shutdown failed: {message}")]
pub struct LogStoreRuntimeError {
    /// Stable backend-neutral shutdown detail.
    pub message: String,
}
