use std::sync::Arc;

use kernel_api::{ClusterId, NodeId, Timestamp};
use node_agent::{
    HostCpuStats, HostDiskStats, HostMemoryStats, HostNetworkStats, HostResourceStats,
    HostTelemetrySample, HostTelemetrySink,
};

use crate::{HostMetricPipeline, InMemoryHostMetricStore};

#[tokio::test]
async fn host_pipeline_normalizes_partial_snapshots_and_exact_replays()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryHostMetricStore::new());
    let pipeline = HostMetricPipeline::new(store.clone());
    let complete = sample(true, true)?;
    let resources_only = sample(true, false)?;

    pipeline.ingest(&complete).await?;
    pipeline.ingest(&complete).await?;
    pipeline.ingest(&resources_only).await?;

    let points = store.points()?;
    assert_eq!(points.len(), 2);
    let first = points.first().ok_or("complete host point missing")?;
    let resources = first.resources.ok_or("host resources missing")?;
    assert_eq!(resources.cpu_total_ticks, 1_000);
    assert_eq!(resources.cpu_idle_ticks, 400);
    assert_eq!(resources.memory_used_bytes, 2_048);
    assert_eq!(resources.network_transmit_bytes, 200);
    let disk = first
        .disks
        .as_ref()
        .and_then(|disks| disks.first())
        .ok_or("host disk missing")?;
    assert_eq!(disk.name, "/dev/vda");
    assert_eq!(disk.mount_point, "/");
    let second = points.get(1).ok_or("partial host point missing")?;
    assert!(second.resources.is_some());
    assert!(second.disks.is_none());
    Ok(())
}

fn sample(
    resources: bool,
    disks: bool,
) -> Result<HostTelemetrySample, kernel_api::InvalidIdentifier> {
    Ok(HostTelemetrySample {
        cluster_id: ClusterId::new("cluster-1")?,
        node_id: NodeId::new("node-1")?,
        collected_at: Timestamp(if disks { 1 } else { 2 }),
        resources: resources.then_some(HostResourceStats {
            cpu: HostCpuStats {
                total_ticks: 1_000,
                idle_ticks: 400,
            },
            memory: HostMemoryStats {
                used_bytes: 2_048,
                total_bytes: 4_096,
            },
            network: HostNetworkStats {
                receive_bytes: 100,
                transmit_bytes: 200,
            },
        }),
        disks: disks.then(|| {
            vec![HostDiskStats {
                name: "/dev/vda".to_owned(),
                mount_point: "/".to_owned(),
                total_bytes: 10_000,
                available_bytes: 4_000,
                file_system: "ext4".to_owned(),
            }]
        }),
    })
}
