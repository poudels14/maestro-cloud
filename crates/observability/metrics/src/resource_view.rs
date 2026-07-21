use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{NodeId, ServiceId, WorkloadId};
use serde::{Deserialize, Serialize};

use crate::{
    HostMetricHistoryPoint, HostMetricPoint, HostResourceMetricPoint, WorkloadMetricHistoryPoint,
    WorkloadMetricPoint,
};

/// Stable source namespace preserved by the resource-metrics API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceMetricSource {
    /// One node's aggregate host resources.
    Node,
    /// Cluster-wide aggregate resources.
    Cluster,
    /// All selected workloads for one service.
    Service(ServiceId),
    /// One runtime workload.
    Workload(WorkloadId),
}

impl ResourceMetricSource {
    fn legacy_name(&self) -> String {
        match self {
            Self::Node => "node".to_owned(),
            Self::Cluster => "cluster".to_owned(),
            Self::Service(service_id) => format!("service:{service_id}"),
            Self::Workload(workload_id) => format!("container:{workload_id}"),
        }
    }
}

/// API-facing resource sample preserving the established camel-case wire shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMetricPoint {
    /// Wall-clock sample time in milliseconds.
    pub ts: i64,
    /// Stable legacy source namespace.
    pub source: String,
    /// CPU consumed during the preceding interval, as a percentage of one core.
    pub cpu_percent: f64,
    /// Current memory usage in bytes.
    pub memory_bytes: u64,
    /// Configured or physical memory ceiling in bytes; zero means unlimited.
    pub memory_limit_bytes: u64,
    /// Cumulative received bytes.
    pub net_rx_bytes: u64,
    /// Cumulative transmitted bytes.
    pub net_tx_bytes: u64,
}

/// API-facing disk inventory preserving the established camel-case wire shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiskInfo {
    /// Kernel mount source.
    pub name: String,
    /// Host-visible mount point.
    pub mount_point: String,
    /// Filesystem capacity in bytes.
    pub total_bytes: u64,
    /// Bytes available to an unprivileged process.
    pub available_bytes: u64,
    /// Kernel filesystem type.
    pub file_system: String,
}

/// Projects resource-complete host history into the established chart response shape.
pub fn project_host_resource_metrics(
    history: &[HostMetricHistoryPoint],
    source: ResourceMetricSource,
) -> Vec<ResourceMetricPoint> {
    let source = source.legacy_name();
    history
        .iter()
        .filter_map(|history| {
            let resources = history.point.resources.as_ref()?;
            Some(ResourceMetricPoint {
                ts: history.point.id.collected_at.0,
                source: source.clone(),
                cpu_percent: host_cpu_percent(
                    history
                        .previous
                        .as_ref()
                        .filter(|previous| {
                            previous.id.cluster_id == history.point.id.cluster_id
                                && previous.id.node_id == history.point.id.node_id
                                && previous.id.collected_at.0 < history.point.id.collected_at.0
                        })
                        .and_then(|previous| previous.resources.as_ref()),
                    resources,
                ),
                memory_bytes: resources.memory_used_bytes,
                memory_limit_bytes: resources.memory_total_bytes,
                net_rx_bytes: resources.network_receive_bytes,
                net_tx_bytes: resources.network_transmit_bytes,
            })
        })
        .collect()
}

/// Projects each workload history independently using its stable workload identity.
pub fn project_workload_resource_metrics(
    history: &[WorkloadMetricHistoryPoint],
) -> Vec<ResourceMetricPoint> {
    history
        .iter()
        .map(|history| {
            workload_point(
                history,
                &ResourceMetricSource::Workload(history.point.id.workload_id.clone()),
            )
        })
        .collect()
}

/// Aggregates selected workload samples at each exact collection timestamp.
pub fn aggregate_workload_resource_metrics(
    history: &[WorkloadMetricHistoryPoint],
    source: ResourceMetricSource,
) -> Vec<ResourceMetricPoint> {
    let mut aggregated = BTreeMap::<i64, ResourceMetricPoint>::new();
    let mut unlimited_memory = BTreeSet::new();
    for history in history {
        let point = workload_point(history, &source);
        let aggregate = aggregated.entry(point.ts).or_insert(ResourceMetricPoint {
            ts: point.ts,
            source: point.source.clone(),
            cpu_percent: 0.0,
            memory_bytes: 0,
            memory_limit_bytes: 0,
            net_rx_bytes: 0,
            net_tx_bytes: 0,
        });
        aggregate.cpu_percent += point.cpu_percent;
        aggregate.memory_bytes = aggregate.memory_bytes.saturating_add(point.memory_bytes);
        if point.memory_limit_bytes == 0 {
            unlimited_memory.insert(point.ts);
            aggregate.memory_limit_bytes = 0;
        } else if !unlimited_memory.contains(&point.ts) {
            aggregate.memory_limit_bytes = aggregate
                .memory_limit_bytes
                .saturating_add(point.memory_limit_bytes);
        }
        aggregate.net_rx_bytes = aggregate.net_rx_bytes.saturating_add(point.net_rx_bytes);
        aggregate.net_tx_bytes = aggregate.net_tx_bytes.saturating_add(point.net_tx_bytes);
    }
    aggregated.into_values().collect()
}

/// Projects one latest disk-complete sample per node into stable node-id order.
pub fn project_latest_disks(points: &[HostMetricPoint]) -> BTreeMap<NodeId, Vec<DiskInfo>> {
    points
        .iter()
        .filter_map(|point| {
            point.disks.as_ref().map(|disks| {
                (
                    point.id.node_id.clone(),
                    disks
                        .iter()
                        .map(|disk| DiskInfo {
                            name: disk.name.clone(),
                            mount_point: disk.mount_point.clone(),
                            total_bytes: disk.total_bytes,
                            available_bytes: disk.available_bytes,
                            file_system: disk.file_system.clone(),
                        })
                        .collect(),
                )
            })
        })
        .collect()
}

fn workload_point(
    history: &WorkloadMetricHistoryPoint,
    source: &ResourceMetricSource,
) -> ResourceMetricPoint {
    let point = &history.point;
    ResourceMetricPoint {
        ts: point.id.collected_at.0,
        source: source.legacy_name(),
        cpu_percent: workload_cpu_percent(history.previous.as_ref(), point),
        memory_bytes: point.memory_current_bytes,
        memory_limit_bytes: point.memory_maximum_bytes.unwrap_or(0),
        net_rx_bytes: point.network_receive_bytes.unwrap_or(0),
        net_tx_bytes: point.network_transmit_bytes.unwrap_or(0),
    }
}

fn host_cpu_percent(
    previous: Option<&HostResourceMetricPoint>,
    current: &HostResourceMetricPoint,
) -> f64 {
    let Some(previous) = previous else {
        return 0.0;
    };
    let Some(total_delta) = current
        .cpu_total_ticks
        .checked_sub(previous.cpu_total_ticks)
    else {
        return 0.0;
    };
    let Some(idle_delta) = current.cpu_idle_ticks.checked_sub(previous.cpu_idle_ticks) else {
        return 0.0;
    };
    if total_delta == 0 || idle_delta > total_delta {
        return 0.0;
    }
    total_delta.saturating_sub(idle_delta) as f64 / total_delta as f64 * 100.0
}

fn workload_cpu_percent(
    previous: Option<&WorkloadMetricPoint>,
    current: &WorkloadMetricPoint,
) -> f64 {
    let Some(previous) = previous else {
        return 0.0;
    };
    if previous.id.node_id != current.id.node_id
        || previous.id.workload_id != current.id.workload_id
    {
        return 0.0;
    }
    let Some(elapsed_ms) = current
        .id
        .collected_at
        .0
        .checked_sub(previous.id.collected_at.0)
        .and_then(|elapsed| u64::try_from(elapsed).ok())
    else {
        return 0.0;
    };
    let Some(elapsed_usec) = elapsed_ms.checked_mul(1_000) else {
        return 0.0;
    };
    let Some(usage_delta) = current.cpu_usage_usec.checked_sub(previous.cpu_usage_usec) else {
        return 0.0;
    };
    if elapsed_usec == 0 {
        return 0.0;
    }
    usage_delta as f64 / elapsed_usec as f64 * 100.0
}
