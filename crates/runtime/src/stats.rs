use std::path::{Path, PathBuf};

use crate::CgroupPathError;

/// CPU accounting normalized across runtime-native and cgroup-v2 samples.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkloadCpuStats {
    /// Total CPU time consumed by the workload.
    pub usage_usec: u64,
    /// CPU time consumed in user mode.
    pub user_usec: u64,
    /// CPU time consumed in kernel mode.
    pub system_usec: u64,
    /// Configured CPU scheduling periods elapsed.
    pub periods: u64,
    /// Scheduling periods in which the workload was throttled.
    pub throttled_periods: u64,
    /// Total duration for which CPU execution was throttled.
    pub throttled_usec: u64,
}

/// Memory pressure counters normalized across supported runtime backends.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkloadMemoryEvents {
    /// Times the low boundary was reclaimed through.
    pub low: u64,
    /// Times workloads were throttled by the high boundary.
    pub high: u64,
    /// Times the hard memory limit was reached.
    pub maximum: u64,
    /// Out-of-memory events observed for the workload.
    pub out_of_memory: u64,
    /// Processes killed by the out-of-memory handler.
    pub out_of_memory_kills: u64,
    /// Whole-workload out-of-memory kills, when supported.
    pub out_of_memory_group_kills: u64,
}

/// Current memory use, limit, and pressure events for one workload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkloadMemoryStats {
    /// Current resident and cached memory charged to the workload.
    pub current_bytes: u64,
    /// Hard memory limit, or `None` when unlimited or not reported.
    pub maximum_bytes: Option<u64>,
    /// Memory pressure and out-of-memory counters.
    pub events: WorkloadMemoryEvents,
}

/// Block-device totals summed across every workload device.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkloadIoStats {
    /// Bytes read from block devices.
    pub read_bytes: u64,
    /// Bytes written to block devices.
    pub write_bytes: u64,
    /// Read operations issued.
    pub read_operations: u64,
    /// Write operations issued.
    pub write_operations: u64,
    /// Bytes discarded from block devices.
    pub discarded_bytes: u64,
    /// Discard operations issued.
    pub discard_operations: u64,
}

/// Process occupancy and configured limit for one workload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkloadProcessStats {
    /// Current processes and threads charged to the workload.
    pub current: u64,
    /// Maximum allowed processes, or `None` when unlimited or not reported.
    pub maximum: Option<u64>,
}

/// One backend-neutral workload resource sample.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkloadResourceStats {
    /// CPU accounting.
    pub cpu: WorkloadCpuStats,
    /// Memory accounting.
    pub memory: WorkloadMemoryStats,
    /// Block I/O accounting.
    pub io: WorkloadIoStats,
    /// Process accounting.
    pub processes: WorkloadProcessStats,
}

/// Cumulative network byte counters reported for one workload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkloadNetworkStats {
    /// Bytes received across owned workload interfaces.
    pub receive_bytes: u64,
    /// Bytes transmitted across owned workload interfaces.
    pub transmit_bytes: u64,
}

/// Runtime-native resource and optional network counters captured together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadStatsSnapshot {
    /// Backend-neutral CPU, memory, I/O, and process counters.
    pub resources: WorkloadResourceStats,
    /// Cumulative network counters when the runtime reports them.
    pub network: Option<WorkloadNetworkStats>,
}

/// Runtime-specific route by which the node agent obtains a workload sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadStatsReading {
    /// Read the returned exact cgroup-v2 path directly on the host.
    CgroupV2(CgroupPath),
    /// Use counters sampled through the runtime's native API.
    Snapshot(WorkloadStatsSnapshot),
}

impl WorkloadStatsReading {
    /// Returns the cgroup-v2 path when direct host sampling was requested.
    pub fn cgroup_path(&self) -> Option<&CgroupPath> {
        match self {
            Self::CgroupV2(path) => Some(path),
            Self::Snapshot(_) => None,
        }
    }
}

/// Validated absolute cgroup-v2 path returned by a runtime backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupPath(PathBuf);

impl CgroupPath {
    /// Validates that a backend path is absolute.
    pub fn new(path: PathBuf) -> Result<Self, CgroupPathError> {
        if path.is_absolute() {
            Ok(Self(path))
        } else {
            Err(CgroupPathError::Relative { path })
        }
    }

    /// Returns the absolute backend-reported cgroup path.
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}
