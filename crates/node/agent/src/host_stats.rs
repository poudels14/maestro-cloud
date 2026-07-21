use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

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
    let line = contents
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or(HostStatsError::MissingField {
            file: PROC_STAT,
            field: "cpu",
        })?;
    let values = line
        .split_ascii_whitespace()
        .skip(1)
        .map(|value| value.parse::<u64>().map_err(|_| invalid(PROC_STAT, line)))
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() < 4 {
        return Err(invalid(PROC_STAT, line));
    }
    let idle = value_at(&values, 3, PROC_STAT, line)?;
    let io_wait = values.get(4).copied().unwrap_or_default();
    let idle_ticks = idle
        .checked_add(io_wait)
        .ok_or(HostStatsError::CounterOverflow { file: PROC_STAT })?;
    let total_ticks = values.iter().take(8).try_fold(0_u64, |total, value| {
        total
            .checked_add(*value)
            .ok_or(HostStatsError::CounterOverflow { file: PROC_STAT })
    })?;
    if idle_ticks > total_ticks {
        return Err(invalid(PROC_STAT, line));
    }
    Ok(HostCpuStats {
        total_ticks,
        idle_ticks,
    })
}

fn parse_memory(contents: &str) -> Result<HostMemoryStats, HostStatsError> {
    let mut total_kibibytes = None;
    let mut available_kibibytes = None;
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let Some((name, raw_value)) = line.split_once(':') else {
            return Err(invalid(PROC_MEMINFO, line));
        };
        if name != "MemTotal" && name != "MemAvailable" {
            continue;
        }
        let mut fields = raw_value.split_ascii_whitespace();
        let value = fields
            .next()
            .ok_or_else(|| invalid(PROC_MEMINFO, line))?
            .parse::<u64>()
            .map_err(|_| invalid(PROC_MEMINFO, line))?;
        if fields.next() != Some("kB") || fields.next().is_some() {
            return Err(invalid(PROC_MEMINFO, line));
        }
        let target = if name == "MemTotal" {
            &mut total_kibibytes
        } else {
            &mut available_kibibytes
        };
        if target.replace(value).is_some() {
            return Err(invalid(PROC_MEMINFO, line));
        }
    }
    let total_kibibytes = required(total_kibibytes, PROC_MEMINFO, "MemTotal")?;
    let available_kibibytes = required(available_kibibytes, PROC_MEMINFO, "MemAvailable")?;
    let used_kibibytes = total_kibibytes
        .checked_sub(available_kibibytes)
        .ok_or_else(|| invalid(PROC_MEMINFO, "MemAvailable exceeds MemTotal"))?;
    Ok(HostMemoryStats {
        used_bytes: kibibytes_to_bytes(used_kibibytes)?,
        total_bytes: kibibytes_to_bytes(total_kibibytes)?,
    })
}

fn parse_network(contents: &str) -> Result<HostNetworkStats, HostStatsError> {
    let mut receive_bytes = 0_u64;
    let mut transmit_bytes = 0_u64;
    let mut interfaces = 0_usize;
    for line in contents
        .lines()
        .skip(2)
        .filter(|line| !line.trim().is_empty())
    {
        let (_interface, counters) = line
            .rsplit_once(':')
            .ok_or_else(|| invalid(PROC_NET_DEV, line))?;
        let values = counters
            .split_ascii_whitespace()
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| invalid(PROC_NET_DEV, line))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if values.len() < 16 {
            return Err(invalid(PROC_NET_DEV, line));
        }
        receive_bytes = receive_bytes
            .checked_add(value_at(&values, 0, PROC_NET_DEV, line)?)
            .ok_or(HostStatsError::CounterOverflow { file: PROC_NET_DEV })?;
        transmit_bytes = transmit_bytes
            .checked_add(value_at(&values, 8, PROC_NET_DEV, line)?)
            .ok_or(HostStatsError::CounterOverflow { file: PROC_NET_DEV })?;
        interfaces = interfaces.saturating_add(1);
    }
    if interfaces == 0 {
        return Err(HostStatsError::MissingField {
            file: PROC_NET_DEV,
            field: "interface counters",
        });
    }
    Ok(HostNetworkStats {
        receive_bytes,
        transmit_bytes,
    })
}

fn value_at(
    values: &[u64],
    index: usize,
    file: &'static str,
    line: &str,
) -> Result<u64, HostStatsError> {
    values
        .get(index)
        .copied()
        .ok_or_else(|| invalid(file, line))
}

fn required(
    value: Option<u64>,
    file: &'static str,
    field: &'static str,
) -> Result<u64, HostStatsError> {
    value.ok_or(HostStatsError::MissingField { file, field })
}

fn kibibytes_to_bytes(value: u64) -> Result<u64, HostStatsError> {
    value
        .checked_mul(1024)
        .ok_or(HostStatsError::CounterOverflow { file: PROC_MEMINFO })
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
