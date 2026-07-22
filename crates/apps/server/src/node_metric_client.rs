use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, SecretValue};
use metrics::{
    DiskInfo, HostMetricHistoryPoint, HostMetricQuery, HostMetricQueryStore,
    HostMetricQueryStoreError, LatestHostMetricQuery, WorkloadMetricHistoryPoint,
    WorkloadMetricQuery, WorkloadMetricQueryStore, WorkloadMetricQueryStoreError,
    project_latest_disks,
};

use crate::TlsIdentity;
use crate::node_http_client::{NodeHttpClient, NodeHttpClientError, NodeHttpRequestError};

/// Node-selected metric reads used by cluster-wide API fanout.
#[async_trait]
pub trait NodeMetricQueryStore: Send + Sync {
    /// Reads one node's bounded workload history.
    async fn query_node_workloads(
        &self,
        node_id: &NodeId,
        query: &WorkloadMetricQuery,
    ) -> Result<Vec<WorkloadMetricHistoryPoint>, NodeMetricQueryError>;

    /// Reads one node's bounded host-resource history.
    async fn query_node_host(
        &self,
        node_id: &NodeId,
        query: &HostMetricQuery,
    ) -> Result<Vec<HostMetricHistoryPoint>, NodeMetricQueryError>;

    /// Reads one node's latest complete disk inventory.
    async fn query_node_disks(
        &self,
        node_id: &NodeId,
        cluster_id: &ClusterId,
    ) -> Result<Vec<DiskInfo>, NodeMetricQueryError>;
}

/// Mutual-TLS node metric proxy with direct local-store bypass.
pub struct HttpNodeMetricQueryStore {
    workloads: Arc<dyn WorkloadMetricQueryStore>,
    hosts: Arc<dyn HostMetricQueryStore>,
    transport: NodeHttpClient,
}

impl HttpNodeMetricQueryStore {
    /// Builds one bounded client over a validated static topology.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node_id: NodeId,
        endpoints: BTreeMap<NodeId, SocketAddr>,
        trust_root_pem: &str,
        identity: &TlsIdentity,
        jwt_secret_key: &SecretValue,
        workloads: Arc<dyn WorkloadMetricQueryStore>,
        hosts: Arc<dyn HostMetricQueryStore>,
    ) -> Result<Self, NodeHttpClientError> {
        Ok(Self {
            workloads,
            hosts,
            transport: NodeHttpClient::new(
                local_node_id,
                endpoints,
                trust_root_pem,
                identity,
                jwt_secret_key,
            )?,
        })
    }

    async fn local_workloads(
        &self,
        node_id: &NodeId,
        query: &WorkloadMetricQuery,
    ) -> Result<Vec<WorkloadMetricHistoryPoint>, NodeMetricQueryError> {
        let query = scoped_workload_query(query, node_id.clone())?;
        self.workloads
            .query_workload_metrics(&query)
            .await
            .map_err(workload_store_error)
    }

    async fn local_host(
        &self,
        node_id: &NodeId,
        query: &HostMetricQuery,
    ) -> Result<Vec<HostMetricHistoryPoint>, NodeMetricQueryError> {
        let query = HostMetricQuery::new(
            query.cluster_id().clone(),
            Some(node_id.clone()),
            query.from(),
            query.to(),
            query.component(),
            query.limit(),
        )
        .map_err(|error| NodeMetricQueryError::Rejected {
            message: error.to_string(),
        })?;
        self.hosts
            .query_host_metrics(&query)
            .await
            .map_err(host_store_error)
    }

    async fn local_disks(
        &self,
        node_id: &NodeId,
        cluster_id: &ClusterId,
    ) -> Result<Vec<DiskInfo>, NodeMetricQueryError> {
        let query = LatestHostMetricQuery::new(
            cluster_id.clone(),
            metrics::HostMetricComponent::Disks,
            10_000,
        )
        .map_err(|error| NodeMetricQueryError::Rejected {
            message: error.to_string(),
        })?;
        let points = self
            .hosts
            .latest_host_metrics(&query)
            .await
            .map_err(host_store_error)?;
        Ok(project_latest_disks(&points)
            .remove(node_id)
            .unwrap_or_default())
    }
}

#[async_trait]
impl NodeMetricQueryStore for HttpNodeMetricQueryStore {
    async fn query_node_workloads(
        &self,
        node_id: &NodeId,
        query: &WorkloadMetricQuery,
    ) -> Result<Vec<WorkloadMetricHistoryPoint>, NodeMetricQueryError> {
        if node_id == self.transport.local_node_id() {
            return self.local_workloads(node_id, query).await;
        }
        self.transport
            .get_json(
                node_id,
                "/api/node/metrics/workloads",
                &workload_parameters(query),
            )
            .await
            .map_err(request_error)
    }

    async fn query_node_host(
        &self,
        node_id: &NodeId,
        query: &HostMetricQuery,
    ) -> Result<Vec<HostMetricHistoryPoint>, NodeMetricQueryError> {
        if node_id == self.transport.local_node_id() {
            return self.local_host(node_id, query).await;
        }
        self.transport
            .get_json(node_id, "/api/node/metrics/host", &host_parameters(query))
            .await
            .map_err(request_error)
    }

    async fn query_node_disks(
        &self,
        node_id: &NodeId,
        cluster_id: &ClusterId,
    ) -> Result<Vec<DiskInfo>, NodeMetricQueryError> {
        if node_id == self.transport.local_node_id() {
            return self.local_disks(node_id, cluster_id).await;
        }
        self.transport
            .get_json(node_id, "/api/node/disks", &[])
            .await
            .map_err(request_error)
    }
}

fn scoped_workload_query(
    query: &WorkloadMetricQuery,
    node_id: NodeId,
) -> Result<WorkloadMetricQuery, NodeMetricQueryError> {
    let mut scoped = WorkloadMetricQuery::new(
        query.cluster_id().clone(),
        query.from(),
        query.to(),
        query.limit(),
    )
    .map_err(|error| NodeMetricQueryError::Rejected {
        message: error.to_string(),
    })?
    .with_node(node_id);
    if let Some(service_id) = query.service_id() {
        scoped = scoped.with_service(service_id.clone());
    }
    if let Some(deployment_id) = query.deployment_id() {
        scoped = scoped.with_deployment(deployment_id.clone());
    }
    Ok(scoped)
}

fn workload_parameters(query: &WorkloadMetricQuery) -> Vec<(String, String)> {
    let mut parameters = vec![
        ("from".to_owned(), query.from().0.to_string()),
        ("to".to_owned(), query.to().0.to_string()),
        ("limit".to_owned(), query.limit().to_string()),
    ];
    if let Some(service_id) = query.service_id() {
        parameters.push(("serviceId".to_owned(), service_id.as_str().to_owned()));
    }
    if let Some(deployment_id) = query.deployment_id() {
        parameters.push(("deploymentId".to_owned(), deployment_id.as_str().to_owned()));
    }
    parameters
}

fn host_parameters(query: &HostMetricQuery) -> Vec<(String, String)> {
    vec![
        ("from".to_owned(), query.from().0.to_string()),
        ("to".to_owned(), query.to().0.to_string()),
        ("limit".to_owned(), query.limit().to_string()),
        (
            "component".to_owned(),
            match query.component() {
                metrics::HostMetricComponent::Any => "any",
                metrics::HostMetricComponent::Resources => "resources",
                metrics::HostMetricComponent::Disks => "disks",
            }
            .to_owned(),
        ),
    ]
}

fn request_error(error: NodeHttpRequestError) -> NodeMetricQueryError {
    match error {
        NodeHttpRequestError::Rejected { message } => NodeMetricQueryError::Rejected { message },
        NodeHttpRequestError::Unavailable { message } => {
            NodeMetricQueryError::Unavailable { message }
        }
    }
}

fn workload_store_error(error: WorkloadMetricQueryStoreError) -> NodeMetricQueryError {
    NodeMetricQueryError::Unavailable {
        message: error.to_string(),
    }
}

fn host_store_error(error: HostMetricQueryStoreError) -> NodeMetricQueryError {
    NodeMetricQueryError::Unavailable {
        message: error.to_string(),
    }
}

/// A node rejected a metric query or could not serve it safely.
#[derive(Debug, thiserror::Error)]
pub enum NodeMetricQueryError {
    /// Query parameters violated the node's bounded read contract.
    #[error("node metric query was rejected: {message}")]
    Rejected { message: String },
    /// The node or its durable metric store could not complete the read.
    #[error("node metric query is unavailable: {message}")]
    Unavailable { message: String },
}
