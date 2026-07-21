use kernel_api::{ClusterId, NodeId, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MAX_DISKS_PER_HOST_POINT: usize = 4_096;
const MAX_DISK_IDENTITY_BYTES: usize = 4_096;

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

impl HostMetricPoint {
    /// Validates invariants required by every durable host metric backend.
    pub fn validate(&self) -> Result<(), HostMetricValidationError> {
        if self.resources.is_none() && self.disks.is_none() {
            return Err(HostMetricValidationError::Empty);
        }
        if self.resources.is_some_and(|resources| {
            resources.cpu_idle_ticks > resources.cpu_total_ticks
                || resources.memory_total_bytes == 0
                || resources.memory_used_bytes > resources.memory_total_bytes
        }) {
            return Err(HostMetricValidationError::InvalidResources);
        }
        let mut mounts = BTreeSet::new();
        if self.disks.as_ref().is_some_and(|disks| {
            disks.len() > MAX_DISKS_PER_HOST_POINT
                || disks.iter().any(|disk| {
                    disk.mount_point.is_empty()
                        || !disk.mount_point.starts_with('/')
                        || disk.mount_point.len() > MAX_DISK_IDENTITY_BYTES
                        || disk.name.is_empty()
                        || disk.name.len() > MAX_DISK_IDENTITY_BYTES
                        || disk.file_system.is_empty()
                        || disk.file_system.len() > MAX_DISK_IDENTITY_BYTES
                        || disk.total_bytes == 0
                        || disk.available_bytes > disk.total_bytes
                        || !mounts.insert(disk.mount_point.as_str())
                })
        }) {
            return Err(HostMetricValidationError::InvalidDisks);
        }
        Ok(())
    }
}

/// A normalized host sample violated durable storage invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HostMetricValidationError {
    /// Neither reader produced telemetry.
    #[error("host metric point has no collected telemetry")]
    Empty,
    /// A current resource value exceeded or lacked its corresponding total.
    #[error("host resource values exceed or lack their corresponding totals")]
    InvalidResources,
    /// Disk identity, capacity, uniqueness, or collection bounds were invalid.
    #[error("host disk inventory contains invalid values")]
    InvalidDisks,
}
