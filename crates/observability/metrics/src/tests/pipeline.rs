use std::collections::BTreeMap;
use std::sync::Arc;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use node_agent::{
    CgroupCpuStats, CgroupIoStats, CgroupMemoryEvents, CgroupMemoryStats, CgroupProcessStats,
    CgroupStats, WorkloadStatsSample, WorkloadStatsSink,
};
use runtime::WorkloadMetadata;

use crate::{InMemoryMetricStore, WorkloadMetricPipeline};

#[tokio::test]
async fn workload_pipeline_normalizes_ownership_counters_and_exact_replays()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryMetricStore::new());
    let pipeline = WorkloadMetricPipeline::new(store.clone());
    let sample = sample()?;

    pipeline.ingest(std::slice::from_ref(&sample)).await?;
    pipeline.ingest(&[sample]).await?;

    let points = store.points()?;
    assert_eq!(points.len(), 1);
    let point = points.first().ok_or("normalized metric point missing")?;
    assert_eq!(point.metadata.service_id, ServiceId::new("api")?);
    assert_eq!(
        point.metadata.deployment_id,
        DeploymentId::new("deployment-1")?
    );
    assert_eq!(point.cpu_usage_usec, 10);
    assert_eq!(point.memory_current_bytes, 1_024);
    assert_eq!(point.io_write_bytes, 40);
    Ok(())
}

fn sample() -> Result<WorkloadStatsSample, kernel_api::InvalidIdentifier> {
    Ok(WorkloadStatsSample {
        metadata: WorkloadMetadata {
            cluster_id: ClusterId::new("cluster-1")?,
            node_id: NodeId::new("node-1")?,
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("deployment-1")?,
            assignment_id: AssignmentId::new("assignment-1")?,
            workload_id: WorkloadId::new("workload-1")?,
            labels: BTreeMap::new(),
        },
        collected_at: Timestamp(1_750_000_000_000),
        stats: CgroupStats {
            cpu: CgroupCpuStats {
                usage_usec: 10,
                user_usec: 7,
                system_usec: 3,
                periods: 2,
                throttled_periods: 1,
                throttled_usec: 4,
            },
            memory: CgroupMemoryStats {
                current_bytes: 1_024,
                maximum_bytes: Some(2_048),
                events: CgroupMemoryEvents {
                    low: 0,
                    high: 0,
                    maximum: 0,
                    out_of_memory: 0,
                    out_of_memory_kills: 1,
                    out_of_memory_group_kills: 0,
                },
            },
            io: CgroupIoStats {
                read_bytes: 30,
                write_bytes: 40,
                read_operations: 3,
                write_operations: 4,
                discarded_bytes: 0,
                discard_operations: 0,
            },
            processes: CgroupProcessStats {
                current: 2,
                maximum: Some(32),
            },
        },
    })
}
