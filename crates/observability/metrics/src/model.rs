use kernel_api::{NodeId, Timestamp, WorkloadId};
use runtime::WorkloadMetadata;
use serde::{Deserialize, Serialize};

/// Stable identity for one timestamped workload sample.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricRecordId {
    /// Node that collected the sample.
    pub node_id: NodeId,
    /// Runtime workload sampled on that node.
    pub workload_id: WorkloadId,
    /// Wall-clock sample time used as its local sequence.
    pub collected_at: Timestamp,
}

/// One backend-neutral set of cumulative cgroup v2 workload counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadMetricPoint {
    /// Replay-safe identity for this workload sample.
    pub id: MetricRecordId,
    /// Durable service, deployment, assignment, and cluster ownership.
    pub metadata: WorkloadMetadata,
    /// Total CPU time consumed by the workload cgroup.
    pub cpu_usage_usec: u64,
    /// CPU time consumed in user mode.
    pub cpu_user_usec: u64,
    /// CPU time consumed in kernel mode.
    pub cpu_system_usec: u64,
    /// Configured CPU scheduling periods elapsed.
    pub cpu_periods: u64,
    /// Scheduling periods in which CPU was throttled.
    pub cpu_throttled_periods: u64,
    /// Total duration for which CPU execution was throttled.
    pub cpu_throttled_usec: u64,
    /// Current memory charged to the workload cgroup.
    pub memory_current_bytes: u64,
    /// Hard memory limit, absent when unlimited.
    pub memory_maximum_bytes: Option<u64>,
    /// Processes killed by the cgroup out-of-memory handler.
    pub memory_out_of_memory_kills: u64,
    /// Low-memory reclaim events.
    pub memory_low_events: u64,
    /// High-memory throttle events.
    pub memory_high_events: u64,
    /// Hard memory-limit events.
    pub memory_maximum_events: u64,
    /// Out-of-memory events observed by the cgroup.
    pub memory_out_of_memory_events: u64,
    /// Whole-cgroup out-of-memory kills, when supported by the kernel.
    pub memory_out_of_memory_group_kills: u64,
    /// Bytes read from block devices.
    pub io_read_bytes: u64,
    /// Bytes written to block devices.
    pub io_write_bytes: u64,
    /// Block-device read operations issued.
    pub io_read_operations: u64,
    /// Block-device write operations issued.
    pub io_write_operations: u64,
    /// Bytes discarded from block devices.
    pub io_discarded_bytes: u64,
    /// Block-device discard operations issued.
    pub io_discard_operations: u64,
    /// Bytes received by the workload since its owned interfaces were created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_receive_bytes: Option<u64>,
    /// Bytes transmitted by the workload since its owned interfaces were created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_transmit_bytes: Option<u64>,
    /// Current processes and threads charged to the workload.
    pub processes_current: u64,
    /// Maximum allowed processes, absent when unlimited.
    pub processes_maximum: Option<u64>,
}

impl WorkloadMetricPoint {
    /// Validates ownership and counter relationships required by durable backends.
    pub fn validate(&self) -> Result<(), WorkloadMetricValidationError> {
        if self.id.node_id != self.metadata.node_id
            || self.id.workload_id != self.metadata.workload_id
        {
            return Err(WorkloadMetricValidationError::Ownership);
        }
        if self.cpu_throttled_periods > self.cpu_periods {
            return Err(WorkloadMetricValidationError::Cpu);
        }
        if self.network_receive_bytes.is_some() != self.network_transmit_bytes.is_some() {
            return Err(WorkloadMetricValidationError::Network);
        }
        Ok(())
    }
}

/// A normalized workload sample violated durable storage invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WorkloadMetricValidationError {
    /// Record identity and durable workload ownership differed.
    #[error("workload metric identity does not match its metadata ownership")]
    Ownership,
    /// Throttled scheduling periods exceeded all observed scheduling periods.
    #[error("workload metric CPU counters are internally inconsistent")]
    Cpu,
    /// Only one direction of the optional network counter pair was present.
    #[error("workload metric network counters must be both present or both absent")]
    Network,
}
