use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use metrics::{
    HostMetricComponent, HostMetricQueryStore, HostMetricStore, LatestHostMetricQuery,
    MetricDeliveryStore, MetricRecordId, MetricSequence, MetricSinkId, MetricStore,
    WorkloadMetricPoint, WorkloadMetricQuery, WorkloadMetricQueryStore,
};
use runtime::WorkloadMetadata;

use crate::{DuckMetricStoreRuntime, DuckStoreError, DuckStoreSettings};

#[tokio::test]
async fn duck_metric_store_passes_shared_conformance_and_closes_cleanly()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckMetricStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("metrics.duckdb"),
        8,
    )?)
    .await?;
    metrics::conformance::check_metric_store(runtime.store().as_ref()).await?;
    metrics::conformance::check_workload_metric_query_store(
        runtime.store().as_ref(),
        runtime.store().as_ref(),
    )
    .await?;
    metrics::conformance::check_host_metric_store(runtime.store().as_ref()).await?;
    metrics::conformance::check_host_metric_query_store(
        runtime.store().as_ref(),
        runtime.store().as_ref(),
    )
    .await?;
    runtime.shutdown().await?;

    let delivery = DuckMetricStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("metric-delivery.duckdb"),
        8,
    )?)
    .await?;
    metrics::conformance::check_metric_delivery_store(
        delivery.store().as_ref(),
        delivery.store().as_ref(),
    )
    .await?;
    delivery.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_metric_store_replays_persisted_points_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("metrics.duckdb"), 8)?;
    let point = point()?;
    let mut next = point.clone();
    next.id.collected_at = Timestamp(2);
    next.cpu_usage_usec = 20;
    next.network_receive_bytes = Some(150);
    next.network_transmit_bytes = Some(275);
    let runtime = DuckMetricStoreRuntime::open(settings.clone()).await?;
    let host = metrics::conformance::host_metric_point("node-1", 1, 1_024)?;
    assert_eq!(
        runtime
            .store()
            .append(&[point.clone(), next.clone()])
            .await?
            .committed,
        2
    );
    assert_eq!(
        runtime
            .store()
            .append_host_metrics(std::slice::from_ref(&host))
            .await?
            .committed,
        1
    );
    runtime
        .store()
        .commit_sink_cursor(&MetricSinkId::new("datadog")?, MetricSequence(1))
        .await?;
    runtime.shutdown().await?;

    let restarted = DuckMetricStoreRuntime::open(settings).await?;
    assert_eq!(
        restarted
            .store()
            .append(std::slice::from_ref(&next))
            .await?
            .deduplicated,
        1
    );
    assert_eq!(
        restarted
            .store()
            .append_host_metrics(std::slice::from_ref(&host))
            .await?
            .deduplicated,
        1
    );
    assert_eq!(
        restarted
            .store()
            .load_sink_cursor(&MetricSinkId::new("datadog")?)
            .await?,
        Some(MetricSequence(1))
    );
    let pending = restarted
        .store()
        .read_after(Some(MetricSequence(1)), 8)
        .await?;
    let pending = pending
        .first()
        .ok_or("persisted metric delivery point missing")?;
    assert_eq!(pending.sequence, MetricSequence(2));
    assert_eq!(pending.previous.as_ref(), Some(&point));
    assert_eq!(&pending.point, &next);
    let history = restarted
        .store()
        .query_workload_metrics(&WorkloadMetricQuery::new(
            ClusterId::new("cluster-1")?,
            Timestamp(1),
            Timestamp(2),
            8,
        )?)
        .await?;
    assert_eq!(history.len(), 2);
    assert_eq!(
        history.get(1).and_then(|history| history.previous.as_ref()),
        Some(&point)
    );
    restarted.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_metric_store_migrates_v1_points_into_stable_delivery_order()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("metrics.duckdb");
    let first = point()?;
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE schema_version (version BIGINT NOT NULL);
         INSERT INTO schema_version VALUES (1);
         CREATE TABLE normalized_metrics (
             node_id VARCHAR NOT NULL,
             workload_id VARCHAR NOT NULL,
             collected_at_ms BIGINT NOT NULL,
             service_id VARCHAR NOT NULL,
             deployment_id VARCHAR NOT NULL,
             point_json VARCHAR NOT NULL,
             PRIMARY KEY (node_id, workload_id, collected_at_ms)
         );
         CREATE INDEX normalized_metrics_service_time
             ON normalized_metrics (service_id, collected_at_ms);
         CREATE INDEX normalized_metrics_deployment_time
             ON normalized_metrics (deployment_id, collected_at_ms);",
    )?;
    connection.execute(
        "INSERT INTO normalized_metrics VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        duckdb::params![
            first.id.node_id.as_str(),
            first.id.workload_id.as_str(),
            first.id.collected_at.0,
            first.metadata.service_id.as_str(),
            first.metadata.deployment_id.as_str(),
            serde_json::to_string(&first)?,
        ],
    )?;
    drop(connection);

    let runtime = DuckMetricStoreRuntime::open(DuckStoreSettings::new(path, 8)?).await?;
    let host = metrics::conformance::host_metric_point("node-1", 1, 1_024)?;
    assert_eq!(
        runtime
            .store()
            .append_host_metrics(&[host])
            .await?
            .committed,
        1
    );
    let migrated = runtime.store().read_after(None, 8).await?;

    assert_eq!(migrated.len(), 1);
    let migrated = migrated.first().ok_or("migrated metric point missing")?;
    assert_eq!(migrated.sequence, MetricSequence(1));
    assert_eq!(migrated.previous, None);
    assert_eq!(migrated.point, first);
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_metric_store_migrates_v3_host_component_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("metrics.duckdb");
    let mut host = metrics::conformance::host_metric_point("node-1", 5, 512)?;
    host.disks = None;
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE schema_version (version BIGINT NOT NULL);
         INSERT INTO schema_version VALUES (3);
         CREATE TABLE host_metrics (
             cluster_id VARCHAR NOT NULL,
             node_id VARCHAR NOT NULL,
             collected_at_ms BIGINT NOT NULL,
             point_json VARCHAR NOT NULL,
             PRIMARY KEY (cluster_id, node_id, collected_at_ms)
         );
         CREATE INDEX host_metrics_node_time
             ON host_metrics (cluster_id, node_id, collected_at_ms);
         CREATE TABLE normalized_metrics (
             sequence BIGINT NOT NULL UNIQUE,
             previous_sequence BIGINT,
             node_id VARCHAR NOT NULL,
             workload_id VARCHAR NOT NULL,
             collected_at_ms BIGINT NOT NULL,
             service_id VARCHAR NOT NULL,
             deployment_id VARCHAR NOT NULL,
             point_json VARCHAR NOT NULL,
             PRIMARY KEY (node_id, workload_id, collected_at_ms)
         );",
    )?;
    connection.execute(
        "INSERT INTO host_metrics VALUES (?1, ?2, ?3, ?4)",
        duckdb::params![
            host.id.cluster_id.as_str(),
            host.id.node_id.as_str(),
            host.id.collected_at.0,
            serde_json::to_string(&host)?,
        ],
    )?;
    drop(connection);

    let runtime = DuckMetricStoreRuntime::open(DuckStoreSettings::new(path, 8)?).await?;
    let resources = runtime
        .store()
        .latest_host_metrics(&LatestHostMetricQuery::new(
            host.id.cluster_id.clone(),
            HostMetricComponent::Resources,
            8,
        )?)
        .await?;
    let disks = runtime
        .store()
        .latest_host_metrics(&LatestHostMetricQuery::new(
            host.id.cluster_id.clone(),
            HostMetricComponent::Disks,
            8,
        )?)
        .await?;
    assert_eq!(resources, [host]);
    assert!(disks.is_empty());
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_metric_store_rejects_ambiguous_schema_history()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("metrics.duckdb");
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch(
        "CREATE TABLE schema_version (version BIGINT NOT NULL);
         INSERT INTO schema_version VALUES (1), (2);",
    )?;
    drop(connection);

    let result = DuckMetricStoreRuntime::open(DuckStoreSettings::new(path, 8)?).await;
    assert!(matches!(result, Err(DuckStoreError::Initialize { .. })));
    Ok(())
}

fn point() -> Result<WorkloadMetricPoint, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-1")?;
    let workload_id = WorkloadId::new("workload-1")?;
    Ok(WorkloadMetricPoint {
        id: MetricRecordId {
            node_id: node_id.clone(),
            workload_id: workload_id.clone(),
            collected_at: Timestamp(1),
        },
        metadata: WorkloadMetadata {
            cluster_id: ClusterId::new("cluster-1")?,
            node_id,
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("deployment-1")?,
            assignment_id: AssignmentId::new("assignment-1")?,
            workload_id,
            labels: BTreeMap::new(),
        },
        cpu_usage_usec: 10,
        cpu_user_usec: 7,
        cpu_system_usec: 3,
        cpu_periods: 2,
        cpu_throttled_periods: 1,
        cpu_throttled_usec: 4,
        memory_current_bytes: 1_024,
        memory_maximum_bytes: Some(2_048),
        memory_out_of_memory_kills: 0,
        memory_low_events: 0,
        memory_high_events: 0,
        memory_maximum_events: 0,
        memory_out_of_memory_events: 0,
        memory_out_of_memory_group_kills: 0,
        io_read_bytes: 10,
        io_write_bytes: 20,
        io_read_operations: 1,
        io_write_operations: 2,
        io_discarded_bytes: 0,
        io_discard_operations: 0,
        network_receive_bytes: Some(100),
        network_transmit_bytes: Some(200),
        processes_current: 1,
        processes_maximum: Some(32),
    })
}
