use kernel_api::{ClusterId, NodeId, ServiceId, Timestamp};

use crate::{
    HostDiskMetricPoint, HostMetricHistoryPoint, HostMetricPoint, HostMetricRecordId,
    HostResourceMetricPoint, ResourceMetricSource, WorkloadMetricHistoryPoint,
    aggregate_workload_resource_metrics, aggregate_workload_resource_metrics_by_bucket,
    project_host_resource_metrics, project_latest_disks, project_workload_resource_metrics,
};

#[test]
fn host_projection_preserves_wire_shape_and_uses_counter_deltas()
-> Result<(), Box<dyn std::error::Error>> {
    let previous = host_point(1_000, 100, 40);
    let current = host_point(2_000, 300, 100);
    let projected = project_host_resource_metrics(
        &[HostMetricHistoryPoint {
            point: current,
            previous: Some(previous),
        }],
        ResourceMetricSource::Node(NodeId::new("node-1")?),
    );
    let point = projected.first().ok_or("projected host point missing")?;
    assert_eq!(point.cpu_percent, 70.0);
    assert_eq!(point.memory_bytes, 600);
    assert_eq!(point.memory_limit_bytes, 1_000);
    assert_eq!(point.net_rx_bytes, 2_000);
    assert_eq!(
        serde_json::to_value(point)?,
        serde_json::json!({
            "ts": 2_000,
            "source": "node:node-1",
            "cpuPercent": 70.0,
            "memoryBytes": 600,
            "memoryLimitBytes": 1_000,
            "netRxBytes": 2_000,
            "netTxBytes": 3_000,
        })
    );

    let reset = project_host_resource_metrics(
        &[HostMetricHistoryPoint {
            point: host_point(3_000, 10, 5),
            previous: Some(host_point(2_000, 300, 100)),
        }],
        ResourceMetricSource::Node(NodeId::new("node-1")?),
    );
    assert_eq!(reset.first().map(|point| point.cpu_percent), Some(0.0));
    Ok(())
}

#[test]
fn workload_projection_aggregates_exact_sweeps_and_isolates_resets()
-> Result<(), Box<dyn std::error::Error>> {
    let first = workload_history("workload-1", 1_000, 1_000_000, 1_500_000)?;
    let second = workload_history("workload-2", 1_000, 2_000_000, 2_250_000)?;
    let aggregate = aggregate_workload_resource_metrics(
        &[first.clone(), second],
        ResourceMetricSource::Service(ServiceId::new("api")?),
    );
    let point = aggregate.first().ok_or("aggregate point missing")?;
    assert_eq!(point.source, "service:api");
    assert_eq!(point.cpu_percent, 75.0);
    assert_eq!(point.memory_bytes, 2_000);
    assert_eq!(point.memory_limit_bytes, 4_000);
    assert_eq!(point.net_rx_bytes, 400);
    assert_eq!(point.net_tx_bytes, 600);

    let containers = project_workload_resource_metrics(&[first]);
    assert_eq!(
        containers.first().map(|point| point.source.as_str()),
        Some("container:workload-1")
    );

    let reset = workload_history("workload-3", 2_000, 2_000_000, 1_000_000)?;
    let reset = project_workload_resource_metrics(&[reset]);
    assert_eq!(reset.first().map(|point| point.cpu_percent), Some(0.0));

    let mut unlimited = workload_history("workload-4", 1_000, 1_000_000, 1_250_000)?;
    unlimited.point.memory_maximum_bytes = None;
    let unlimited_aggregate =
        aggregate_workload_resource_metrics(&[unlimited], ResourceMetricSource::AllContainers);
    assert_eq!(
        unlimited_aggregate
            .first()
            .map(|point| point.memory_limit_bytes),
        Some(0)
    );
    Ok(())
}

#[test]
fn workload_bucket_projection_merges_unaligned_nodes_and_deduplicates_a_workload()
-> Result<(), Box<dyn std::error::Error>> {
    let early = workload_history("workload-1", 5_100, 1_000_000, 1_250_000)?;
    let mut replacement = workload_history("workload-1", 5_900, 1_250_000, 1_750_000)?;
    replacement.point.memory_current_bytes = 2_000;
    let mut peer = workload_history("workload-2", 5_600, 2_000_000, 2_250_000)?;
    peer.point.id.node_id = NodeId::new("node-2")?;
    peer.point.metadata.node_id = NodeId::new("node-2")?;
    peer.previous
        .as_mut()
        .ok_or("peer baseline missing")?
        .id
        .node_id = NodeId::new("node-2")?;

    let points = aggregate_workload_resource_metrics_by_bucket(
        &[early, replacement, peer],
        ResourceMetricSource::AllContainers,
        NonZeroU64::new(5_000).ok_or("invalid bucket")?,
    );
    let point = points.first().ok_or("bucket aggregate missing")?;
    assert_eq!(points.len(), 1);
    assert_eq!(point.ts, 5_000);
    assert_eq!(point.source, "containers");
    assert_eq!(point.cpu_percent, 75.0);
    assert_eq!(point.memory_bytes, 3_000);
    Ok(())
}

#[test]
fn latest_disk_projection_is_node_keyed_and_wire_compatible()
-> Result<(), Box<dyn std::error::Error>> {
    let point = host_point(2_000, 300, 100);
    let disks = project_latest_disks(&[point]);
    let disk = disks
        .get(&NodeId::new("node-1")?)
        .and_then(|disks| disks.first())
        .ok_or("projected disk missing")?;
    assert_eq!(
        serde_json::to_value(disk)?,
        serde_json::json!({
            "name": "/dev/vda",
            "mountPoint": "/",
            "totalBytes": 10_000,
            "availableBytes": 4_000,
            "fileSystem": "ext4",
        })
    );
    Ok(())
}

fn host_point(ts: i64, total_ticks: u64, idle_ticks: u64) -> HostMetricPoint {
    HostMetricPoint {
        id: HostMetricRecordId {
            cluster_id: ClusterId::new("cluster-1").expect("valid cluster fixture"),
            node_id: NodeId::new("node-1").expect("valid node fixture"),
            collected_at: Timestamp(ts),
        },
        resources: Some(HostResourceMetricPoint {
            cpu_total_ticks: total_ticks,
            cpu_idle_ticks: idle_ticks,
            memory_used_bytes: 600,
            memory_total_bytes: 1_000,
            network_receive_bytes: 2_000,
            network_transmit_bytes: 3_000,
        }),
        disks: Some(vec![HostDiskMetricPoint {
            name: "/dev/vda".to_owned(),
            mount_point: "/".to_owned(),
            total_bytes: 10_000,
            available_bytes: 4_000,
            file_system: "ext4".to_owned(),
        }]),
    }
}

fn workload_history(
    workload: &str,
    ts: i64,
    previous_usage: u64,
    current_usage: u64,
) -> Result<WorkloadMetricHistoryPoint, kernel_api::InvalidIdentifier> {
    let mut previous = crate::conformance::metric_point(workload, ts - 1_000, previous_usage)?;
    previous.memory_current_bytes = 1_000;
    previous.memory_maximum_bytes = Some(2_000);
    previous.network_receive_bytes = Some(100);
    previous.network_transmit_bytes = Some(200);
    let mut current = previous.clone();
    current.id.collected_at = Timestamp(ts);
    current.cpu_usage_usec = current_usage;
    current.memory_current_bytes = 1_000;
    current.network_receive_bytes = Some(200);
    current.network_transmit_bytes = Some(300);
    Ok(WorkloadMetricHistoryPoint {
        point: current,
        previous: Some(previous),
    })
}
use std::num::NonZeroU64;
