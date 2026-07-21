use async_trait::async_trait;

use crate::{LogSequence, LogSinkId};

/// Maximum dead-letter rows retained by one node-local store.
pub const MAX_RETAINED_DEAD_LETTERS: u64 = 100_000;

/// Durable log spool, delivery cursor, and quarantine health at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSpoolStats {
    /// Committed normalized records currently retained in the hot tier.
    pub row_count: u64,
    /// Highest store-local sequence ever assigned, or zero for an empty store.
    pub high_watermark: LogSequence,
    /// Event time of the first retained sequence.
    pub oldest_entry_at_ms: Option<i64>,
    /// On-disk bytes attributable to the hot-tier database and journal.
    pub database_bytes: u64,
    /// Delivery progress for each requested configured sink.
    pub sinks: Vec<LogSinkCursorStats>,
    /// Aggregate retained poison-payload state across every sink.
    pub dead_letters: SinkDeadLetterSnapshot,
}

/// Durable backlog for one independently checkpointed sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSinkCursorStats {
    /// Stable sink namespace.
    pub sink_id: LogSinkId,
    /// Last fully committed delivery sequence; absent before first progress.
    pub cursor: Option<LogSequence>,
    /// Retained records strictly after the cursor.
    pub pending_entries: u64,
    /// Event time of the first pending sequence.
    pub oldest_pending_at_ms: Option<i64>,
}

/// Aggregate durable dead-letter state exposed by cluster diagnostics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkDeadLetterSnapshot {
    /// Total retained poison payloads across all sinks.
    pub count: u64,
    /// Total retained raw payload bytes.
    pub payload_bytes: u64,
    /// Wall-clock time of the latest retained dead letter.
    pub latest_at_ms: Option<i64>,
    /// Destination status attached to the latest retained dead letter.
    pub latest_status: Option<u16>,
    /// Bounded rejection reason attached to the latest retained dead letter.
    pub latest_error: Option<String>,
}

/// Read-only operational statistics boundary for a node-local normalized log store.
#[async_trait]
pub trait LogStatsStore: Send + Sync {
    /// Returns one consistent snapshot for the requested configured sink identifiers.
    async fn stats_snapshot(
        &self,
        sink_ids: &[LogSinkId],
    ) -> Result<LogSpoolStats, LogStatsStoreError>;
}

/// Durable log operational statistics could not be read.
#[derive(Debug, thiserror::Error)]
pub enum LogStatsStoreError {
    /// The query contains an invalid bound or duplicate-incompatible identity.
    #[error("log stats store rejected query: {message}")]
    Rejected {
        /// Stable validation detail.
        message: String,
    },
    /// The backing store cannot currently return a valid snapshot.
    #[error("log stats store is unavailable: {message}")]
    Unavailable {
        /// Safe backend diagnostic.
        message: String,
    },
}
