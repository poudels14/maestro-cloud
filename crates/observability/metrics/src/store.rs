use async_trait::async_trait;

use crate::WorkloadMetricPoint;

/// Outcome of one atomic idempotent metric append batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricAppendReport {
    /// New sample identities committed by this append.
    pub committed: usize,
    /// Exact sample replays already present in the store.
    pub deduplicated: usize,
}

/// Durable normalized-metric boundary shared by local stores and forwarding sinks.
#[async_trait]
pub trait MetricStore: Send + Sync {
    /// Atomically appends a batch, deduplicating exact sample-identity replays.
    ///
    /// Cancellation may leave the entire batch committed. Retrying identical points is safe;
    /// reusing an identity with different counters or ownership is rejected.
    async fn append(
        &self,
        points: &[WorkloadMetricPoint],
    ) -> Result<MetricAppendReport, MetricStoreError>;
}

/// A normalized metric batch could not cross the durable storage boundary.
#[derive(Debug, thiserror::Error)]
pub enum MetricStoreError {
    /// Content or replay identity permanently violates the store contract.
    #[error("metric store rejected point: {message}")]
    Rejected {
        /// Stable rejection detail.
        message: String,
    },
    /// The store is temporarily unable to accept a valid batch.
    #[error("metric store is unavailable: {message}")]
    Unavailable {
        /// Safe availability detail.
        message: String,
    },
}
