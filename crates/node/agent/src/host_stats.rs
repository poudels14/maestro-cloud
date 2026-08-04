use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use procfs_core::net::InterfaceDeviceStatus;
use procfs_core::prelude::{FromRead, FromReadSI};
use procfs_core::{ExplicitSystemInfo, KernelStats, Meminfo};

const MAX_HOST_STATS_FILE_BYTES: u64 = 1024 * 1024;
const PROC_STAT: &str = "stat";
const PROC_MEMINFO: &str = "meminfo";
const PROC_NET_DEV: &str = "net/dev";

/// Cumulative host CPU accounting from the aggregate Linux processor row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostCpuStats {
    /// All aggregate processor ticks, including idle and I/O wait time.
    pub total_ticks: u64,
    /// Aggregate idle and I/O wait ticks.
    pub idle_ticks: u64,
}

/// Current host memory accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostMemoryStats {
    /// Memory not currently available to new workloads without swapping.
    pub used_bytes: u64,
    /// Physical memory visible to the kernel.
    pub total_bytes: u64,
}

/// Cumulative network counters summed across host interfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostNetworkStats {
    /// Bytes received across every host interface.
    pub receive_bytes: u64,
    /// Bytes transmitted across every host interface.
    pub transmit_bytes: u64,
}

/// One backend-neutral host resource snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostResourceStats {
    /// Cumulative processor accounting used to derive utilization deltas.
    pub cpu: HostCpuStats,
    /// Current physical memory accounting.
    pub memory: HostMemoryStats,
    /// Cumulative host network accounting used to derive byte rates.
    pub network: HostNetworkStats,
}

/// Reads one finite host resource snapshot without retaining rate state.
#[async_trait]
pub trait HostStatsReader: Send + Sync + 'static {
    /// Reads aggregate CPU, memory, and network counters from the host.
    async fn read(&self) -> Result<HostResourceStats, HostStatsError>;
}

/// Bounded Linux `/proc` host resource reader.
#[derive(Debug, Clone)]
pub struct LinuxHostStatsReader {
    proc_root: PathBuf,
}

impl LinuxHostStatsReader {
    /// Constructs the production reader for the host proc filesystem.
    pub fn production() -> Self {
        Self {
            proc_root: PathBuf::from("/proc"),
        }
    }

    /// Constructs a reader for an explicitly mounted host proc filesystem.
    pub fn from_proc_root(proc_root: PathBuf) -> Self {
        Self { proc_root }
    }
}

impl Default for LinuxHostStatsReader {
    fn default() -> Self {
        Self::production()
    }
}

#[async_trait]
impl HostStatsReader for LinuxHostStatsReader {
    async fn read(&self) -> Result<HostResourceStats, HostStatsError> {
        let proc_root = self.proc_root.clone();
        tokio::task::spawn_blocking(move || read_host_stats(&proc_root))
            .await
            .map_err(|error| HostStatsError::Task {
                message: error.to_string(),
            })?
    }
}

fn read_host_stats(proc_root: &Path) -> Result<HostResourceStats, HostStatsError> {
    validate_proc_root(proc_root)?;
    let cpu = parse_cpu(&read_bounded_file(proc_root, PROC_STAT)?)?;
    let memory = parse_memory(&read_bounded_file(proc_root, PROC_MEMINFO)?)?;
    let network = parse_network(&read_bounded_file(proc_root, PROC_NET_DEV)?)?;
    Ok(HostResourceStats {
        cpu,
        memory,
        network,
    })
}

fn validate_proc_root(path: &Path) -> Result<(), HostStatsError> {
    let canonical =
        std::fs::canonicalize(path).map_err(|source| io_error("resolve", path, source))?;
    let metadata =
        std::fs::symlink_metadata(path).map_err(|source| io_error("inspect", path, source))?;
    if canonical == path && metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        Err(HostStatsError::UnsafePath {
            path: path.to_path_buf(),
        })
    }
}

fn read_bounded_file(root: &Path, name: &'static str) -> Result<String, HostStatsError> {
    let path = root.join(name);
    let metadata =
        std::fs::symlink_metadata(&path).map_err(|source| io_error("inspect", &path, source))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(HostStatsError::UnsafePath { path });
    }
    let file = File::open(&path).map_err(|source| io_error("open", &path, source))?;
    let mut contents = String::new();
    file.take(MAX_HOST_STATS_FILE_BYTES.saturating_add(1))
        .read_to_string(&mut contents)
        .map_err(|source| io_error("read", &path, source))?;
    if contents.len() as u64 > MAX_HOST_STATS_FILE_BYTES {
        Err(HostStatsError::FileTooLarge { path })
    } else {
        Ok(contents)
    }
}

fn parse_cpu(contents: &str) -> Result<HostCpuStats, HostStatsError> {
    if !contents.lines().any(|line| line.starts_with("cpu ")) {
        return Err(HostStatsError::MissingField {
            file: PROC_STAT,
            field: "cpu",
        });
    }
    let system_info = ExplicitSystemInfo {
        boot_time_secs: 0,
        ticks_per_second: 100,
        page_size: 4096,
        is_little_endian: cfg!(target_endian = "little"),
    };
    let stats = KernelStats::from_read(contents.as_bytes(), &system_info)
        .map_err(|error| invalid(PROC_STAT, &error.to_string()))?;
    let cpu = stats.total;
    let idle_ticks = cpu
        .idle
        .checked_add(cpu.iowait.unwrap_or_default())
        .ok_or(HostStatsError::CounterOverflow { file: PROC_STAT })?;
    let fields = [
        cpu.user,
        cpu.nice,
        cpu.system,
        cpu.idle,
        cpu.iowait.unwrap_or_default(),
        cpu.irq.unwrap_or_default(),
        cpu.softirq.unwrap_or_default(),
        cpu.steal.unwrap_or_default(),
    ];
    let total_ticks = fields.iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(*value)
            .ok_or(HostStatsError::CounterOverflow { file: PROC_STAT })
    })?;
    if idle_ticks > total_ticks {
        return Err(invalid(PROC_STAT, "idle ticks exceed total ticks"));
    }
    Ok(HostCpuStats {
        total_ticks,
        idle_ticks,
    })
}

fn parse_memory(contents: &str) -> Result<HostMemoryStats, HostStatsError> {
    let memory = Meminfo::from_read(contents.as_bytes())
        .map_err(|error| invalid(PROC_MEMINFO, &error.to_string()))?;
    let available_bytes = memory.mem_available.ok_or(HostStatsError::MissingField {
        file: PROC_MEMINFO,
        field: "MemAvailable",
    })?;
    let used_bytes = memory
        .mem_total
        .checked_sub(available_bytes)
        .ok_or_else(|| invalid(PROC_MEMINFO, "MemAvailable exceeds MemTotal"))?;
    Ok(HostMemoryStats {
        used_bytes,
        total_bytes: memory.mem_total,
    })
}

fn parse_network(contents: &str) -> Result<HostNetworkStats, HostStatsError> {
    let interfaces = InterfaceDeviceStatus::from_read(contents.as_bytes())
        .map_err(|error| invalid(PROC_NET_DEV, &error.to_string()))?;
    if interfaces.0.is_empty() {
        return Err(HostStatsError::MissingField {
            file: PROC_NET_DEV,
            field: "interface counters",
        });
    }
    let mut receive_bytes = 0_u64;
    let mut transmit_bytes = 0_u64;
    for interface in interfaces.0.values() {
        receive_bytes = receive_bytes
            .checked_add(interface.recv_bytes)
            .ok_or(HostStatsError::CounterOverflow { file: PROC_NET_DEV })?;
        transmit_bytes = transmit_bytes
            .checked_add(interface.sent_bytes)
            .ok_or(HostStatsError::CounterOverflow { file: PROC_NET_DEV })?;
    }
    Ok(HostNetworkStats {
        receive_bytes,
        transmit_bytes,
    })
}

fn invalid(file: &'static str, value: &str) -> HostStatsError {
    HostStatsError::InvalidValue {
        file,
        value: value.to_owned(),
    }
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> HostStatsError {
    HostStatsError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

/// Failure to safely read or parse one Linux host resource snapshot.
#[derive(Debug, thiserror::Error)]
pub enum HostStatsError {
    /// The proc root or one pseudo-file was replaced with an unsafe file type.
    #[error("host stats path `{}` is not a safe directory or regular file", path.display())]
    UnsafePath {
        /// Rejected path.
        path: PathBuf,
    },
    /// A proc pseudo-file exceeded the defensive read bound.
    #[error("host stats file `{}` exceeds the read limit", path.display())]
    FileTooLarge {
        /// Oversized path.
        path: PathBuf,
    },
    /// A required kernel value was absent.
    #[error("host stats file `{file}` is missing required field `{field}`")]
    MissingField {
        /// Kernel pseudo-file.
        file: &'static str,
        /// Required value.
        field: &'static str,
    },
    /// A kernel pseudo-file did not match its documented grammar.
    #[error("host stats file `{file}` contains invalid value `{value}`")]
    InvalidValue {
        /// Kernel pseudo-file.
        file: &'static str,
        /// Invalid line or scalar.
        value: String,
    },
    /// Summing or converting host counters exceeded `u64`.
    #[error("host stats counters from `{file}` overflowed")]
    CounterOverflow {
        /// Kernel pseudo-file.
        file: &'static str,
    },
    /// Blocking filesystem work could not complete.
    #[error("host stats filesystem task failed: {message}")]
    Task {
        /// Task failure detail.
        message: String,
    },
    /// A proc filesystem operation failed.
    #[error("failed to {operation} host stats path `{}`: {source}", path.display())]
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Proc path.
        path: PathBuf,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
}
