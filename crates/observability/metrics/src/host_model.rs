use kernel_api::{ClusterId, NodeId, Timestamp};
use serde::{Deserialize, Serialize};

/// Stable identity for one timestamped host telemetry sample.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostMetricRecordId {
    /// Cluster that owns the node sample.
    pub cluster_id: ClusterId,
    /// Node that collected the sample.
    pub node_id: NodeId,
    /// Wall-clock sample time used as its local replay identity.
    pub collected_at: Timestamp,
}

/// Backend-neutral aggregate host CPU, memory, and network values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostResourceMetricPoint {
    /// All aggregate processor ticks, including idle and I/O wait.
    pub cpu_total_ticks: u64,
    /// Aggregate idle and I/O wait ticks.
    pub cpu_idle_ticks: u64,
    /// Memory not currently available without swapping.
    pub memory_used_bytes: u64,
    /// Physical memory visible to the kernel.
    pub memory_total_bytes: u64,
    /// Bytes received across every host interface.
    pub network_receive_bytes: u64,
    /// Bytes transmitted across every host interface.
    pub network_transmit_bytes: u64,
}

/// Backend-neutral capacity and identity for one host mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostDiskMetricPoint {
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

/// One normalized partial-or-complete host telemetry sample.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostMetricPoint {
    /// Replay-safe cluster, node, and timestamp identity.
    pub id: HostMetricRecordId,
    /// Resource values, absent when that reader failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<HostResourceMetricPoint>,
    /// Complete disk inventory, absent when the mount-table reader failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disks: Option<Vec<HostDiskMetricPoint>>,
}
