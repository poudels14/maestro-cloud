use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use kernel_api::{ClusterId, NodeId, SecretValue, Timestamp};
use metrics::conformance::{host_metric_point, metric_point};
use metrics::{
    DiskInfo, HostMetricComponent, HostMetricHistoryPoint, HostMetricQuery, HostMetricQueryStore,
    HostMetricStore, InMemoryHostMetricStore, InMemoryMetricStore, InMemoryMetricStoreRuntime,
    LatestHostMetricQuery, MetricStore, ResourceMetricPoint, WorkloadMetricHistoryPoint,
    WorkloadMetricQuery, WorkloadMetricQueryStore, project_latest_disks,
};

use super::{decode, request, seeded_store, token};
use crate::{
    ApiServer, HttpNodeMetricQueryStore, NodeMetricQueryError, NodeMetricQueryStore,
    ServerSettings, TlsIdentity,
};

#[tokio::test]
async fn node_metric_routes_project_history_and_latest_disks()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let runtime = InMemoryMetricStoreRuntime::new();
    let workloads = runtime.store_handle();
    let hosts = runtime.host_store_handle();
    let mut first = host_metric_point("node-1", 1_000, 256)?;
    first.id.cluster_id = cluster_id.clone();
    let mut second = host_metric_point("node-1", 2_000, 512)?;
    second.id.cluster_id = cluster_id.clone();
    second
        .resources
        .as_mut()
        .ok_or("resources missing")?
        .cpu_total_ticks = 200;
    second
        .resources
        .as_mut()
        .ok_or("resources missing")?
        .cpu_idle_ticks = 50;
    second
        .disks
        .as_mut()
        .and_then(|disks| disks.first_mut())
        .ok_or("disks missing")?
        .available_bytes = 3_000;
    hosts.append_host_metrics(&[first, second]).await?;

    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_metric_query_stores(NodeId::new("node-1")?, workloads, hosts);

    let response = request(&server, "/api/metrics/node?from=0&to=3000", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let points: Vec<ResourceMetricPoint> = decode(response).await?;
    assert_eq!(points.len(), 2);
    let first = points.first().ok_or("first resource point missing")?;
    let second = points.get(1).ok_or("second resource point missing")?;
    assert_eq!(first.source, "node");
    assert_eq!(first.cpu_percent, 0.0);
    assert_eq!(second.cpu_percent, 90.0);

    let response = request(&server, "/api/disks", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let disks: Vec<DiskInfo> = decode(response).await?;
    assert_eq!(disks.len(), 1);
    let disk = disks.first().ok_or("latest disk missing")?;
    assert_eq!(disk.mount_point, "/");
    assert_eq!(disk.available_bytes, 3_000);
    Ok(())
}

#[tokio::test]
async fn internal_metric_routes_are_node_scoped_and_force_local_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = "metric-route-test-secret-that-is-long-enough";
    let (store, cluster_id) = seeded_store().await?;
    let runtime = InMemoryMetricStoreRuntime::new();
    let workloads = runtime.store_handle();
    let hosts = runtime.host_store_handle();
    let mut workload = metric_point("workload-1", 1_000, 100)?;
    workload.metadata.cluster_id = cluster_id.clone();
    workload.metadata.node_id = NodeId::new("node-1")?;
    workload.id.node_id = NodeId::new("node-1")?;
    workloads.append(&[workload]).await?;
    let mut host = host_metric_point("node-1", 1_000, 256)?;
    host.id.cluster_id = cluster_id.clone();
    hosts.append_host_metrics(&[host]).await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, Some(SecretValue::new(secret))),
    )?
    .with_metric_query_stores(NodeId::new("node-1")?, workloads, hosts);
    let operator = token(secret, "operator")?;
    let node = token(secret, "node")?;

    let uri = "/api/node/metrics/workloads?from=0&to=2000&serviceId=api";
    assert_eq!(
        request(&server, uri, Some(&operator)).await?.status(),
        StatusCode::FORBIDDEN
    );
    let response = request(&server, uri, Some(&node)).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let history: Vec<metrics::WorkloadMetricHistoryPoint> = decode(response).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history
            .first()
            .ok_or("workload history missing")?
            .point
            .id
            .node_id
            .as_str(),
        "node-1"
    );

    let response = request(
        &server,
        "/api/node/metrics/host?from=0&to=2000&component=resources",
        Some(&node),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let history: Vec<HostMetricHistoryPoint> = decode(response).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history
            .first()
            .ok_or("host history missing")?
            .point
            .id
            .collected_at,
        Timestamp(1_000)
    );
    Ok(())
}

#[tokio::test]
async fn metric_routes_reject_invalid_ranges_and_missing_composition()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        request(&server, "/api/metrics/node?from=2&to=1", None)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&server, "/api/disks", None).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    Ok(())
}

#[tokio::test]
async fn cluster_metric_routes_merge_nodes_by_bucket_and_preserve_sources()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let workloads = Arc::new(InMemoryMetricStore::new());
    let hosts = Arc::new(InMemoryHostMetricStore::new());
    seed_workload_pair(&workloads, &cluster_id, "node-1", "workload-1", 5_100).await?;
    seed_workload_pair(&workloads, &cluster_id, "node-2", "workload-2", 5_600).await?;
    for (node, available) in [("node-1", 3_000), ("node-2", 2_000)] {
        let mut host = host_metric_point(node, 5_500, 256)?;
        host.id.cluster_id = cluster_id.clone();
        host.disks
            .as_mut()
            .and_then(|disks| disks.first_mut())
            .ok_or("disk fixture missing")?
            .available_bytes = available;
        hosts.append_host_metrics(&[host]).await?;
    }
    let nodes = vec![NodeId::new("node-1")?, NodeId::new("node-2")?];
    let queries = Arc::new(TestNodeMetricQueries { workloads, hosts });
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_cluster_metric_query_store(nodes, queries);

    let response = request(
        &server,
        "/api/metrics/cluster?from=5000&to=6000&bucketMs=5000",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let points: Vec<ResourceMetricPoint> = decode(response).await?;
    let point = points.first().ok_or("cluster point missing")?;
    assert_eq!(points.len(), 1);
    assert_eq!(point.ts, 5_000);
    assert_eq!(point.source, "cluster");
    assert_eq!(point.memory_bytes, 2_048);

    let response = request(
        &server,
        "/api/services/api/metrics?from=5000&to=6000&bucketMs=5000",
        None,
    )
    .await?;
    let points: Vec<ResourceMetricPoint> = decode(response).await?;
    assert_eq!(
        points.first().map(|point| point.source.as_str()),
        Some("service:api")
    );

    let response = request(
        &server,
        "/api/services/api/metrics/containers?from=5000&to=6000",
        None,
    )
    .await?;
    let points: Vec<ResourceMetricPoint> = decode(response).await?;
    assert_eq!(points.len(), 2);
    assert_eq!(points.first().map(|point| point.ts), Some(5_100));

    let response = request(&server, "/api/disks/nodes", None).await?;
    let disks: BTreeMap<NodeId, Vec<DiskInfo>> = decode(response).await?;
    assert_eq!(disks.len(), 2);
    assert_eq!(
        disks
            .get(&NodeId::new("node-2")?)
            .and_then(|disks| disks.first())
            .map(|disk| disk.available_bytes),
        Some(2_000)
    );
    assert_eq!(
        request(
            &server,
            "/api/metrics/cluster?from=5000&to=6000&bucketMs=0",
            None,
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            &server,
            "/api/metrics/cluster?from=5000&to=6000&limit=0",
            None,
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn node_metric_client_queries_a_peer_over_authenticated_mutual_tls()
-> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretValue::new("cluster-metric-test-secret-that-is-long-enough");
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let identity = TlsIdentity::new(
        certificate_pem.clone(),
        SecretValue::new(certified.signing_key.serialize_pem()),
    );
    let (store, cluster_id) = seeded_store().await?;
    let remote_runtime = InMemoryMetricStoreRuntime::new();
    let remote_workloads = remote_runtime.store_handle();
    let remote_hosts = remote_runtime.host_store_handle();
    let remote_node = NodeId::new("node-remote")?;
    seed_workload_pair(
        &remote_workloads,
        &cluster_id,
        remote_node.as_str(),
        "remote-workload",
        5_100,
    )
    .await?;
    let mut host = host_metric_point(remote_node.as_str(), 5_100, 256)?;
    host.id.cluster_id = cluster_id.clone();
    remote_hosts.append_host_metrics(&[host]).await?;
    let remote = ApiServer::new(
        store,
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:0".parse()?, Some(secret.clone()))
            .with_tls_identity(identity.clone())
            .with_cluster_trust_root(certificate_pem.clone()),
    )?
    .with_metric_query_stores(remote_node.clone(), remote_workloads, remote_hosts)
    .bind()
    .await?;
    let remote_address = remote.local_address();
    let (shutdown, shutdown_receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(remote.serve(shutdown_receiver));

    let anonymous = reqwest::Client::builder()
        .https_only(true)
        .add_root_certificate(reqwest::Certificate::from_pem(certificate_pem.as_bytes())?)
        .build()?;
    let anonymous_response = anonymous
        .get(format!("https://{remote_address}/api/node/disks"))
        .bearer_auth(token(secret.expose(), "node")?)
        .send()
        .await?;
    assert_eq!(anonymous_response.status(), StatusCode::FORBIDDEN);

    let local_node = NodeId::new("node-local")?;
    let local_runtime = InMemoryMetricStoreRuntime::new();
    let client = HttpNodeMetricQueryStore::new(
        local_node.clone(),
        BTreeMap::from([
            (local_node, "127.0.0.1:1".parse()?),
            (remote_node.clone(), remote_address),
        ]),
        &certificate_pem,
        &identity,
        &secret,
        local_runtime.store_handle(),
        local_runtime.host_store_handle(),
    )?;
    let query =
        WorkloadMetricQuery::new(cluster_id.clone(), Timestamp(5_000), Timestamp(6_000), 10)?;
    let history = client.query_node_workloads(&remote_node, &query).await?;
    let disks = client.query_node_disks(&remote_node, &cluster_id).await?;
    assert_eq!(history.len(), 1);
    assert_eq!(disks.len(), 1);
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

struct TestNodeMetricQueries {
    workloads: Arc<InMemoryMetricStore>,
    hosts: Arc<InMemoryHostMetricStore>,
}

#[async_trait]
impl NodeMetricQueryStore for TestNodeMetricQueries {
    async fn query_node_workloads(
        &self,
        node_id: &NodeId,
        query: &WorkloadMetricQuery,
    ) -> Result<Vec<WorkloadMetricHistoryPoint>, NodeMetricQueryError> {
        self.workloads
            .query_workload_metrics(&query.clone().with_node(node_id.clone()))
            .await
            .map_err(unavailable)
    }

    async fn query_node_host(
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
        .map_err(unavailable)?;
        self.hosts
            .query_host_metrics(&query)
            .await
            .map_err(unavailable)
    }

    async fn query_node_disks(
        &self,
        node_id: &NodeId,
        cluster_id: &ClusterId,
    ) -> Result<Vec<DiskInfo>, NodeMetricQueryError> {
        let query =
            LatestHostMetricQuery::new(cluster_id.clone(), HostMetricComponent::Disks, 10_000)
                .map_err(unavailable)?;
        let points = self
            .hosts
            .latest_host_metrics(&query)
            .await
            .map_err(unavailable)?;
        Ok(project_latest_disks(&points)
            .remove(node_id)
            .unwrap_or_default())
    }
}

async fn seed_workload_pair(
    store: &InMemoryMetricStore,
    cluster_id: &ClusterId,
    node: &str,
    workload: &str,
    timestamp: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new(node)?;
    let mut previous = metric_point(workload, timestamp.saturating_sub(1_000), 1_000_000)?;
    previous.metadata.cluster_id = cluster_id.clone();
    previous.metadata.node_id = node_id.clone();
    previous.id.node_id = node_id.clone();
    let mut current = metric_point(workload, timestamp, 1_500_000)?;
    current.metadata.cluster_id = cluster_id.clone();
    current.metadata.node_id = node_id.clone();
    current.id.node_id = node_id;
    store.append(&[previous, current]).await?;
    Ok(())
}

fn unavailable(error: impl std::fmt::Display) -> NodeMetricQueryError {
    NodeMetricQueryError::Unavailable {
        message: error.to_string(),
    }
}
