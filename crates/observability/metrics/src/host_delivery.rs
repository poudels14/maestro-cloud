use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{HostMetricPoint, MetricSinkError, MetricSinkId};

/// Store-assigned total ordering for host-metric delivery progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HostMetricSequence(pub u64);

/// One normalized host point paired with its stable resource-rate baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SequencedHostMetricPoint {
    /// Monotonic store-local host-delivery sequence.
    pub sequence: HostMetricSequence,
    /// Normalized host resource and/or disk point.
    pub point: HostMetricPoint,
    /// Prior resource-complete point for the same cluster and node.
    pub previous_resources: Option<HostMetricPoint>,
}

/// External destination for ordered normalized host-metric batches.
#[async_trait]
pub trait HostMetricSink: Send + Sync {
    /// Stable cursor namespace for this destination.
    fn id(&self) -> &MetricSinkId;

    /// Handles the complete batch or returns without claiming durable progress.
    async fn send_host_metrics(
        &self,
        points: &[SequencedHostMetricPoint],
    ) -> Result<(), MetricSinkError>;
}

/// Ordered host reads and independently checkpointed destination cursors.
#[async_trait]
pub trait HostMetricDeliveryStore: Send + Sync {
    /// Reads host points strictly after `cursor` in store-assigned order.
    async fn read_host_metrics_after(
        &self,
        cursor: Option<HostMetricSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedHostMetricPoint>, HostMetricDeliveryStoreError>;

    /// Loads one destination's last completely committed host sequence.
    async fn load_host_metric_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
    ) -> Result<Option<HostMetricSequence>, HostMetricDeliveryStoreError>;

    /// Atomically advances a host cursor; regressions and unknown sequences are rejected.
    async fn commit_host_metric_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
        sequence: HostMetricSequence,
    ) -> Result<(), HostMetricDeliveryStoreError>;
}

/// Durable host-metric delivery state could not be read or advanced.
#[derive(Debug, thiserror::Error)]
pub enum HostMetricDeliveryStoreError {
    /// The cursor transition or read bound is invalid.
    #[error("host metric delivery store rejected operation: {message}")]
    Rejected {
        /// Stable validation detail.
        message: String,
    },
    /// Durable host delivery state is temporarily unavailable.
    #[error("host metric delivery store is unavailable: {message}")]
    Unavailable {
        /// Safe backend diagnostic.
        message: String,
    },
}
