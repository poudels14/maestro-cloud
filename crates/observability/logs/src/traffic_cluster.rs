use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::try_join_all;
use kernel_api::NodeId;

use crate::{
    IngressTrafficBreakdown, IngressTrafficQuery, ServiceTrafficQuery, TrafficMetricPoint,
    TrafficQueryError, merge_ingress_traffic, merge_service_traffic,
};

/// Transport-neutral access to one selected node's traffic analytics.
#[async_trait]
pub trait NodeTrafficQueryStore: Send + Sync {
    /// Queries one node's ranked ingress traffic.
    async fn query_node_ingress_traffic(
        &self,
        node_id: &NodeId,
        query: &IngressTrafficQuery,
    ) -> Result<IngressTrafficBreakdown, TrafficQueryError>;

    /// Queries one node's service traffic intervals.
    async fn query_node_service_traffic(
        &self,
        node_id: &NodeId,
        query: &ServiceTrafficQuery,
    ) -> Result<Vec<TrafficMetricPoint>, TrafficQueryError>;
}

/// Concurrent traffic fanout with deterministic cluster-wide aggregation.
pub struct ClusterTrafficQueryCoordinator {
    nodes: Arc<dyn NodeTrafficQueryStore>,
}

impl ClusterTrafficQueryCoordinator {
    /// Wraps a node query transport without taking ownership of its lifecycle.
    pub fn new(nodes: Arc<dyn NodeTrafficQueryStore>) -> Self {
        Self { nodes }
    }

    /// Queries distinct nodes and re-ranks values after saturating merge.
    pub async fn query_ingress_traffic(
        &self,
        node_ids: &[NodeId],
        query: &IngressTrafficQuery,
    ) -> Result<IngressTrafficBreakdown, TrafficQueryError> {
        let results = try_join_all(distinct_nodes(node_ids).into_iter().map(
            |node_id| async move {
                self.nodes
                    .query_node_ingress_traffic(&node_id, query)
                    .await
                    .map_err(|error| node_error(error, &node_id))
            },
        ))
        .await?;
        Ok(merge_ingress_traffic(results, query.limit()))
    }

    /// Queries distinct nodes and merges matching five-second intervals.
    pub async fn query_service_traffic(
        &self,
        node_ids: &[NodeId],
        query: &ServiceTrafficQuery,
    ) -> Result<Vec<TrafficMetricPoint>, TrafficQueryError> {
        let results = try_join_all(distinct_nodes(node_ids).into_iter().map(
            |node_id| async move {
                self.nodes
                    .query_node_service_traffic(&node_id, query)
                    .await
                    .map_err(|error| node_error(error, &node_id))
            },
        ))
        .await?;
        Ok(merge_service_traffic(
            results.into_iter().flatten(),
            query.limit(),
        ))
    }
}

fn distinct_nodes(node_ids: &[NodeId]) -> Vec<NodeId> {
    node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn node_error(error: TrafficQueryError, node_id: &NodeId) -> TrafficQueryError {
    match error {
        TrafficQueryError::Rejected { message } => TrafficQueryError::Rejected {
            message: format!("node `{node_id}` rejected traffic query: {message}"),
        },
        TrafficQueryError::Unavailable { message } => TrafficQueryError::Unavailable {
            message: format!("node `{node_id}` traffic query is unavailable: {message}"),
        },
    }
}
