use async_trait::async_trait;

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
