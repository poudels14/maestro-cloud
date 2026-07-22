use axum::http::StatusCode;
use kernel_api::{NodeId, SecretValue, Timestamp};
use metrics::conformance::{host_metric_point, metric_point};
use metrics::{
    DiskInfo, HostMetricHistoryPoint, HostMetricStore, InMemoryMetricStoreRuntime, MetricStore,
    ResourceMetricPoint,
};

use super::{decode, request, seeded_store, token};
use crate::{ApiServer, ServerSettings};

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
