use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{NodeId, SecretValue};
use logs::{
    ControllerStatsProvider, ControllerStatsSnapshot, LogStatsStoreError, StatsMetricPoint,
    StatsMetricQuery, StatsMetricStore, StatsMetricStoreError,
};

use crate::TlsIdentity;
use crate::node_http_client::{NodeHttpClient, NodeHttpClientError, NodeHttpRequestError};

/// Node-selected controller health reads used by the cluster stats API.
#[async_trait]
pub trait NodeStatsQueryStore: Send + Sync {
    /// Reads one node's current controller, spool, sink, and dead-letter health.
    async fn query_node_stats(
        &self,
        node_id: &NodeId,
        reported_at_ms: i64,
    ) -> Result<ControllerStatsSnapshot, NodeStatsQueryError>;

    /// Reads one node's durable controller and backup metric history.
    async fn query_node_stats_metrics(
        &self,
        node_id: &NodeId,
        query: &StatsMetricQuery,
    ) -> Result<Vec<StatsMetricPoint>, NodeStatsQueryError>;
}

/// Mutual-TLS node stats proxy with direct local-provider bypass.
pub struct HttpNodeStatsQueryStore {
    local: Arc<dyn ControllerStatsProvider>,
    local_metrics: Arc<dyn StatsMetricStore>,
    transport: NodeHttpClient,
}

impl HttpNodeStatsQueryStore {
    /// Builds one bounded client over a validated static topology.
    pub fn new(
        local_node_id: NodeId,
        endpoints: BTreeMap<NodeId, SocketAddr>,
        trust_root_pem: &str,
        identity: &TlsIdentity,
        jwt_secret_key: &SecretValue,
        local: Arc<dyn ControllerStatsProvider>,
        local_metrics: Arc<dyn StatsMetricStore>,
    ) -> Result<Self, NodeHttpClientError> {
        Ok(Self {
            local,
            local_metrics,
            transport: NodeHttpClient::new(
                local_node_id,
                endpoints,
                trust_root_pem,
                identity,
                jwt_secret_key,
            )?,
        })
    }
}

#[async_trait]
impl NodeStatsQueryStore for HttpNodeStatsQueryStore {
    async fn query_node_stats(
        &self,
        node_id: &NodeId,
        reported_at_ms: i64,
    ) -> Result<ControllerStatsSnapshot, NodeStatsQueryError> {
        if node_id == self.transport.local_node_id() {
            return self
                .local
                .controller_stats(reported_at_ms)
                .await
                .map_err(store_error);
        }
        self.transport
            .get_json(node_id, "/api/node/stats", &[])
            .await
            .map_err(request_error)
    }

    async fn query_node_stats_metrics(
        &self,
        node_id: &NodeId,
        query: &StatsMetricQuery,
    ) -> Result<Vec<StatsMetricPoint>, NodeStatsQueryError> {
        if node_id == self.transport.local_node_id() {
            return self
                .local_metrics
                .query_stats_metrics(query)
                .await
                .map_err(metric_store_error);
        }
        let mut parameters = vec![
            ("from".to_owned(), query.from().to_string()),
            ("to".to_owned(), query.to().to_string()),
            ("limit".to_owned(), query.limit().to_string()),
        ];
        if let Some(name) = query.name() {
            parameters.push(("name".to_owned(), name.to_owned()));
        }
        self.transport
            .get_json(node_id, "/api/node/metrics/stats", &parameters)
            .await
            .map_err(request_error)
    }
}

fn request_error(error: NodeHttpRequestError) -> NodeStatsQueryError {
    match error {
        NodeHttpRequestError::Rejected { message } => NodeStatsQueryError::Rejected { message },
        NodeHttpRequestError::Unavailable { message } => {
            NodeStatsQueryError::Unavailable { message }
        }
    }
}

fn store_error(error: LogStatsStoreError) -> NodeStatsQueryError {
    match error {
        LogStatsStoreError::Rejected { message } => NodeStatsQueryError::Rejected { message },
        LogStatsStoreError::Unavailable { message } => NodeStatsQueryError::Unavailable { message },
    }
}

fn metric_store_error(error: StatsMetricStoreError) -> NodeStatsQueryError {
    match error {
        StatsMetricStoreError::Rejected { message } => NodeStatsQueryError::Rejected { message },
        StatsMetricStoreError::Unavailable { message } => {
            NodeStatsQueryError::Unavailable { message }
        }
    }
}

/// A node rejected an operational stats query or could not serve it safely.
#[derive(Debug, thiserror::Error)]
pub enum NodeStatsQueryError {
    /// The peer rejected a generated bounded query.
    #[error("node stats query was rejected: {message}")]
    Rejected { message: String },
    /// The node or its durable log store could not produce a snapshot.
    #[error("node stats query is unavailable: {message}")]
    Unavailable { message: String },
}
