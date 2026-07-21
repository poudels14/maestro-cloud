use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use metrics::{MetricRecordId, MetricStore, WorkloadMetricPoint};
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
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duck_metric_store_replays_persisted_points_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let settings = DuckStoreSettings::new(temporary.path().join("metrics.duckdb"), 8)?;
    let point = point()?;
    let runtime = DuckMetricStoreRuntime::open(settings.clone()).await?;
    assert_eq!(
        runtime
            .store()
            .append(std::slice::from_ref(&point))
            .await?
            .committed,
        1
    );
    runtime.shutdown().await?;

    let restarted = DuckMetricStoreRuntime::open(settings).await?;
    assert_eq!(
        restarted
            .store()
            .append(std::slice::from_ref(&point))
            .await?
            .deduplicated,
        1
    );
    restarted.shutdown().await?;
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
        processes_current: 1,
        processes_maximum: Some(32),
    })
}
