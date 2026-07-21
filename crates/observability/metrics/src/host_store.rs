use async_trait::async_trait;

use crate::{HostMetricPoint, MetricAppendReport, MetricStoreError};

/// Durable normalized host-metric append boundary.
#[async_trait]
pub trait HostMetricStore: Send + Sync {
    /// Atomically appends samples, deduplicating exact replay identities.
    ///
    /// Cancellation may leave the entire batch committed. Retrying identical points is safe;
    /// reusing an identity with different resource or disk content is rejected.
    async fn append_host_metrics(
        &self,
        points: &[HostMetricPoint],
    ) -> Result<MetricAppendReport, MetricStoreError>;
}
