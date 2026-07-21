use async_trait::async_trait;
use kernel_api::{ClusterId, DeploymentId, NodeId, ServiceId, Timestamp};
use serde::{Deserialize, Serialize};

use crate::WorkloadMetricPoint;

const MAX_WORKLOAD_QUERY_POINTS: usize = 10_000;

/// One workload metric sample paired with its immediately preceding rate baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadMetricHistoryPoint {
    /// Sample inside the requested time range.
    pub point: WorkloadMetricPoint,
    /// Previous sample for the same node and workload, even when it predates the range.
    pub previous: Option<WorkloadMetricPoint>,
}

/// Bounded inclusive query over normalized workload metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadMetricQuery {
    cluster_id: ClusterId,
    node_id: Option<NodeId>,
    service_id: Option<ServiceId>,
    deployment_id: Option<DeploymentId>,
    from: Timestamp,
    to: Timestamp,
    limit: usize,
}

impl WorkloadMetricQuery {
    /// Validates required ownership, time, and result bounds.
    pub fn new(
        cluster_id: ClusterId,
        from: Timestamp,
        to: Timestamp,
        limit: usize,
    ) -> Result<Self, WorkloadMetricQueryError> {
        if from.0 > to.0 {
            return Err(WorkloadMetricQueryError::InvertedTimeRange);
        }
        if !(1..=MAX_WORKLOAD_QUERY_POINTS).contains(&limit) {
            return Err(WorkloadMetricQueryError::InvalidLimit {
                maximum: MAX_WORKLOAD_QUERY_POINTS,
            });
        }
        Ok(Self {
            cluster_id,
            node_id: None,
            service_id: None,
            deployment_id: None,
            from,
            to,
            limit,
        })
    }

    /// Restricts results to one collecting node.
    pub fn with_node(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Restricts results to one service.
    pub fn with_service(mut self, service_id: ServiceId) -> Self {
        self.service_id = Some(service_id);
        self
    }

    /// Restricts results to one deployment.
    pub fn with_deployment(mut self, deployment_id: DeploymentId) -> Self {
        self.deployment_id = Some(deployment_id);
        self
    }

    /// Cluster whose samples may be returned.
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Optional collecting-node restriction.
    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    /// Optional service restriction.
    pub fn service_id(&self) -> Option<&ServiceId> {
        self.service_id.as_ref()
    }

    /// Optional deployment restriction.
    pub fn deployment_id(&self) -> Option<&DeploymentId> {
        self.deployment_id.as_ref()
    }

    /// Inclusive lower wall-clock bound.
    pub fn from(&self) -> Timestamp {
        self.from
    }

    /// Inclusive upper wall-clock bound.
    pub fn to(&self) -> Timestamp {
        self.to
    }

    /// Maximum number of current samples returned.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Read-only query boundary over durable normalized workload metrics.
#[async_trait]
pub trait WorkloadMetricQueryStore: Send + Sync {
    /// Reads matching samples in node, workload, and timestamp order with rate baselines.
    async fn query_workload_metrics(
        &self,
        query: &WorkloadMetricQuery,
    ) -> Result<Vec<WorkloadMetricHistoryPoint>, WorkloadMetricQueryStoreError>;
}

/// Invalid bounds for a workload-metric read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WorkloadMetricQueryError {
    /// The lower time bound was after the upper bound.
    #[error("workload metric query start must not be after its end")]
    InvertedTimeRange,
    /// The query was empty or could exhaust memory with an unbounded result.
    #[error("workload metric query limit must be between 1 and {maximum}")]
    InvalidLimit {
        /// Largest supported result bound.
        maximum: usize,
    },
}

/// A valid workload-metric query could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadMetricQueryStoreError {
    /// Durable data could not be read or decoded safely.
    #[error("workload metric query store is unavailable: {message}")]
    Unavailable {
        /// Safe backend-neutral diagnostic.
        message: String,
    },
}
