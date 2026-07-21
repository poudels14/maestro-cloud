use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp};

use crate::HostMetricPoint;

const MAX_HOST_QUERY_POINTS: usize = 10_000;

/// Host sample component required by a history or latest-per-node query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostMetricComponent {
    /// Samples containing either resources or disks.
    Any,
    /// Samples containing aggregate CPU, memory, and network values.
    Resources,
    /// Samples containing a complete disk inventory.
    Disks,
}

impl HostMetricComponent {
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn matches(self, point: &HostMetricPoint) -> bool {
        match self {
            Self::Any => true,
            Self::Resources => point.resources.is_some(),
            Self::Disks => point.disks.is_some(),
        }
    }
}

/// Bounded inclusive host-history query, ordered by node and sample time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostMetricQuery {
    cluster_id: ClusterId,
    node_id: Option<NodeId>,
    from: Timestamp,
    to: Timestamp,
    component: HostMetricComponent,
    limit: usize,
}

impl HostMetricQuery {
    /// Validates one cluster-wide or node-local history query.
    pub fn new(
        cluster_id: ClusterId,
        node_id: Option<NodeId>,
        from: Timestamp,
        to: Timestamp,
        component: HostMetricComponent,
        limit: usize,
    ) -> Result<Self, HostMetricQueryError> {
        validate_bounds(from, to, limit)?;
        Ok(Self {
            cluster_id,
            node_id,
            from,
            to,
            component,
            limit,
        })
    }

    /// Cluster whose host samples may be returned.
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Optional node restriction.
    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    /// Inclusive lower wall-clock bound.
    pub fn from(&self) -> Timestamp {
        self.from
    }

    /// Inclusive upper wall-clock bound.
    pub fn to(&self) -> Timestamp {
        self.to
    }

    /// Required component for every result.
    pub fn component(&self) -> HostMetricComponent {
        self.component
    }

    /// Maximum number of results.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Bounded query for the latest matching sample on each cluster node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestHostMetricQuery {
    cluster_id: ClusterId,
    component: HostMetricComponent,
    limit: usize,
}

impl LatestHostMetricQuery {
    /// Validates a latest-per-node query.
    pub fn new(
        cluster_id: ClusterId,
        component: HostMetricComponent,
        limit: usize,
    ) -> Result<Self, HostMetricQueryError> {
        validate_limit(limit)?;
        Ok(Self {
            cluster_id,
            component,
            limit,
        })
    }

    /// Cluster whose nodes may be returned.
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Required component for every result.
    pub fn component(&self) -> HostMetricComponent {
        self.component
    }

    /// Maximum number of nodes returned.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Read-only query boundary over durable host telemetry.
#[async_trait]
pub trait HostMetricQueryStore: Send + Sync {
    /// Reads an inclusive bounded history in node-id then timestamp order.
    async fn query_host_metrics(
        &self,
        query: &HostMetricQuery,
    ) -> Result<Vec<HostMetricPoint>, HostMetricQueryStoreError>;

    /// Reads at most one latest matching sample per node in node-id order.
    async fn latest_host_metrics(
        &self,
        query: &LatestHostMetricQuery,
    ) -> Result<Vec<HostMetricPoint>, HostMetricQueryStoreError>;
}

fn validate_bounds(
    from: Timestamp,
    to: Timestamp,
    limit: usize,
) -> Result<(), HostMetricQueryError> {
    if from.0 > to.0 {
        return Err(HostMetricQueryError::InvertedTimeRange);
    }
    validate_limit(limit)
}

fn validate_limit(limit: usize) -> Result<(), HostMetricQueryError> {
    if !(1..=MAX_HOST_QUERY_POINTS).contains(&limit) {
        return Err(HostMetricQueryError::InvalidLimit {
            maximum: MAX_HOST_QUERY_POINTS,
        });
    }
    Ok(())
}

/// Invalid bounds for a host-metric read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HostMetricQueryError {
    /// The lower time bound was after the upper bound.
    #[error("host metric query start must not be after its end")]
    InvertedTimeRange,
    /// The query was empty or could exhaust memory with an unbounded result.
    #[error("host metric query limit must be between 1 and {maximum}")]
    InvalidLimit {
        /// Largest supported result bound.
        maximum: usize,
    },
}

/// A valid host-metric query could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum HostMetricQueryStoreError {
    /// Durable data could not be read or decoded safely.
    #[error("host metric query store is unavailable: {message}")]
    Unavailable {
        /// Safe backend-neutral diagnostic.
        message: String,
    },
}
