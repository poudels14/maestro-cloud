use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::WorkloadMetricPoint;

/// Store-assigned total ordering used only for downstream delivery progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MetricSequence(pub u64);

/// One normalized point paired with its stable rate baseline and delivery ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SequencedMetricPoint {
    /// Monotonic store-local sequence.
    pub sequence: MetricSequence,
    /// Normalized point delivered to the sink.
    pub point: WorkloadMetricPoint,
    /// Point that immediately preceded this workload in append order.
    pub previous: Option<WorkloadMetricPoint>,
}

/// Validated stable identity for one independently checkpointed metric sink.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MetricSinkId(String);

impl MetricSinkId {
    /// Validates a non-empty bounded identifier safe for storage and diagnostics.
    pub fn new(value: impl Into<String>) -> Result<Self, MetricSinkIdError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(MetricSinkIdError);
        }
        Ok(Self(value))
    }

    /// Returns the validated identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A metric sink identifier was empty, oversized, or contained unsafe characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "metric sink identifiers must be 1-128 ASCII letters, digits, dots, dashes, or underscores"
)]
pub struct MetricSinkIdError;

/// External destination for ordered normalized metric batches.
#[async_trait]
pub trait MetricSink: Send + Sync {
    /// Stable cursor namespace for this sink.
    fn id(&self) -> &MetricSinkId;

    /// Handles the complete batch or returns an error without claiming progress.
    ///
    /// Cancellation may leave the destination updated while the local cursor remains unchanged,
    /// so implementations must produce the same payload for an exact replay.
    async fn send(&self, points: &[SequencedMetricPoint]) -> Result<(), MetricSinkError>;
}

/// Read and cursor boundary required by independently checkpointed metric sink workers.
#[async_trait]
pub trait MetricDeliveryStore: Send + Sync {
    /// Reads the next store-ordered points strictly after `cursor`.
    async fn read_after(
        &self,
        cursor: Option<MetricSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedMetricPoint>, MetricDeliveryStoreError>;

    /// Loads this sink's last completely committed sequence.
    async fn load_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
    ) -> Result<Option<MetricSequence>, MetricDeliveryStoreError>;

    /// Atomically advances a cursor; regressions and unknown sequences must be rejected.
    async fn commit_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
        sequence: MetricSequence,
    ) -> Result<(), MetricDeliveryStoreError>;
}

/// A metric sink could not completely handle a batch.
#[derive(Debug, thiserror::Error)]
pub enum MetricSinkError {
    /// The batch permanently violates the destination contract.
    #[error("metric sink rejected batch: {message}")]
    Rejected {
        /// Safe destination diagnostic.
        message: String,
    },
    /// The destination is temporarily unavailable.
    #[error("metric sink is unavailable: {message}")]
    Unavailable {
        /// Safe destination diagnostic.
        message: String,
    },
}

/// Durable metric read or cursor state could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum MetricDeliveryStoreError {
    /// The requested cursor transition or read bound is invalid.
    #[error("metric delivery store rejected operation: {message}")]
    Rejected {
        /// Stable validation detail.
        message: String,
    },
    /// Durable delivery state is temporarily unavailable.
    #[error("metric delivery store is unavailable: {message}")]
    Unavailable {
        /// Safe backend diagnostic.
        message: String,
    },
}
