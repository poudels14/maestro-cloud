use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use runtime::CgroupPath;

const MAX_CGROUP_FILE_BYTES: u64 = 64 * 1024;

/// CPU accounting read directly from cgroup v2 `cpu.stat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupCpuStats {
    /// Total CPU time consumed by this cgroup.
    pub usage_usec: u64,
    /// CPU time consumed in user mode.
    pub user_usec: u64,
    /// CPU time consumed in kernel mode.
    pub system_usec: u64,
    /// Configured CPU scheduling periods elapsed.
    pub periods: u64,
    /// Scheduling periods in which the cgroup was throttled.
    pub throttled_periods: u64,
    /// Total duration for which CPU execution was throttled.
    pub throttled_usec: u64,
}

/// Memory pressure counters read from cgroup v2 `memory.events`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupMemoryEvents {
    /// Times the low boundary was reclaimed through.
    pub low: u64,
    /// Times processes were throttled by the high boundary.
    pub high: u64,
    /// Times the hard memory limit was reached.
    pub maximum: u64,
    /// Out-of-memory events observed for this cgroup.
    pub out_of_memory: u64,
    /// Processes killed by the cgroup OOM handler.
    pub out_of_memory_kills: u64,
    /// Whole-cgroup OOM kills, when supported by the kernel.
    pub out_of_memory_group_kills: u64,
}

/// Current memory use, limit, and pressure events for one workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupMemoryStats {
    /// Current resident and cached memory charged to the cgroup.
    pub current_bytes: u64,
    /// Hard memory limit, or `None` when the cgroup is unlimited.
    pub maximum_bytes: Option<u64>,
    /// Memory pressure and OOM counters.
    pub events: CgroupMemoryEvents,
}

/// Block-device totals summed across every device in cgroup v2 `io.stat`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CgroupIoStats {
    /// Bytes read from block devices.
    pub read_bytes: u64,
    /// Bytes written to block devices.
    pub write_bytes: u64,
    /// Read operations issued.
    pub read_operations: u64,
    /// Write operations issued.
    pub write_operations: u64,
    /// Discarded bytes.
    pub discarded_bytes: u64,
    /// Discard operations issued.
    pub discard_operations: u64,
}

/// Process occupancy and configured limit for one cgroup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupProcessStats {
    /// Current processes and threads charged to the cgroup.
    pub current: u64,
    /// Maximum allowed processes, or `None` when unlimited.
    pub maximum: Option<u64>,
}

/// One backend-neutral cgroup v2 workload sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupStats {
    /// CPU accounting.
    pub cpu: CgroupCpuStats,
    /// Memory accounting.
    pub memory: CgroupMemoryStats,
    /// Block I/O accounting.
    pub io: CgroupIoStats,
    /// Process accounting.
    pub processes: CgroupProcessStats,
}

/// Reads backend-neutral workload stats from a runtime-resolved cgroup.
#[async_trait]
pub trait CgroupStatsReader: Send + Sync + 'static {
    /// Reads one best-effort sample directly from the kernel pseudo-files.
    async fn read(&self, path: &CgroupPath) -> Result<CgroupStats, CgroupStatsError>;
}

/// Linux cgroup v2 filesystem reader.
#[derive(Debug, Clone, Copy, Default)]
pub struct CgroupV2StatsReader;

#[async_trait]
impl CgroupStatsReader for CgroupV2StatsReader {
    async fn read(&self, path: &CgroupPath) -> Result<CgroupStats, CgroupStatsError> {
        let path = path.as_path().to_path_buf();
        tokio::task::spawn_blocking(move || read_cgroup_stats(&path))
            .await
            .map_err(|error| CgroupStatsError::Task {
                message: error.to_string(),
            })?
    }
}

fn read_cgroup_stats(path: &Path) -> Result<CgroupStats, CgroupStatsError> {
    validate_directory(path)?;
    let cpu = parse_cpu(&read_bounded_file(path, "cpu.stat")?)?;
    let current_memory = parse_single_value(
        "memory.current",
        &read_bounded_file(path, "memory.current")?,
    )?;
    let maximum_memory = parse_limit("memory.max", &read_bounded_file(path, "memory.max")?)?;
    let memory_events = parse_memory_events(&read_bounded_file(path, "memory.events")?)?;
    let io = parse_io(&read_bounded_file(path, "io.stat")?)?;
    let current_processes =
        parse_single_value("pids.current", &read_bounded_file(path, "pids.current")?)?;
    let maximum_processes = parse_limit("pids.max", &read_bounded_file(path, "pids.max")?)?;
    Ok(CgroupStats {
        cpu,
        memory: CgroupMemoryStats {
            current_bytes: current_memory,
            maximum_bytes: maximum_memory,
            events: memory_events,
        },
        io,
        processes: CgroupProcessStats {
            current: current_processes,
            maximum: maximum_processes,
        },
    })
}

fn validate_directory(path: &Path) -> Result<(), CgroupStatsError> {
    let canonical =
        std::fs::canonicalize(path).map_err(|source| io_error("resolve cgroup", path, source))?;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io_error("inspect cgroup", path, source))?;
    if canonical == path && metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        Err(CgroupStatsError::UnsafePath {
            path: path.to_path_buf(),
        })
    }
}

fn read_bounded_file(path: &Path, name: &'static str) -> Result<String, CgroupStatsError> {
    let file_path = path.join(name);
    let metadata = std::fs::symlink_metadata(&file_path)
        .map_err(|source| io_error("inspect cgroup file", &file_path, source))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(CgroupStatsError::UnsafePath { path: file_path });
    }
    let file = File::open(&file_path)
        .map_err(|source| io_error("open cgroup file", &file_path, source))?;
    let mut contents = String::new();
    file.take(MAX_CGROUP_FILE_BYTES.saturating_add(1))
        .read_to_string(&mut contents)
        .map_err(|source| io_error("read cgroup file", &file_path, source))?;
    if contents.len() as u64 > MAX_CGROUP_FILE_BYTES {
        Err(CgroupStatsError::FileTooLarge { path: file_path })
    } else {
        Ok(contents)
    }
}

fn parse_cpu(contents: &str) -> Result<CgroupCpuStats, CgroupStatsError> {
    let values = parse_key_values("cpu.stat", contents)?;
    Ok(CgroupCpuStats {
        usage_usec: required(&values, "cpu.stat", "usage_usec")?,
        user_usec: required(&values, "cpu.stat", "user_usec")?,
        system_usec: required(&values, "cpu.stat", "system_usec")?,
        periods: required(&values, "cpu.stat", "nr_periods")?,
        throttled_periods: required(&values, "cpu.stat", "nr_throttled")?,
        throttled_usec: required(&values, "cpu.stat", "throttled_usec")?,
    })
}

fn parse_memory_events(contents: &str) -> Result<CgroupMemoryEvents, CgroupStatsError> {
    let values = parse_key_values("memory.events", contents)?;
    Ok(CgroupMemoryEvents {
        low: required(&values, "memory.events", "low")?,
        high: required(&values, "memory.events", "high")?,
        maximum: required(&values, "memory.events", "max")?,
        out_of_memory: required(&values, "memory.events", "oom")?,
        out_of_memory_kills: required(&values, "memory.events", "oom_kill")?,
        out_of_memory_group_kills: values.get("oom_group_kill").copied().unwrap_or_default(),
    })
}

fn parse_key_values(
    file: &'static str,
    contents: &str,
) -> Result<BTreeMap<String, u64>, CgroupStatsError> {
    let mut values = BTreeMap::new();
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let mut fields = line.split_ascii_whitespace();
        let key = fields.next().ok_or_else(|| invalid(file, line))?;
        let value = fields
            .next()
            .ok_or_else(|| invalid(file, line))?
            .parse::<u64>()
            .map_err(|_| invalid(file, line))?;
        if fields.next().is_some() || values.insert(key.to_owned(), value).is_some() {
            return Err(invalid(file, line));
        }
    }
    Ok(values)
}

fn parse_single_value(file: &'static str, contents: &str) -> Result<u64, CgroupStatsError> {
    let value = contents.trim();
    value.parse::<u64>().map_err(|_| invalid(file, value))
}

fn parse_limit(file: &'static str, contents: &str) -> Result<Option<u64>, CgroupStatsError> {
    let value = contents.trim();
    if value == "max" {
        Ok(None)
    } else {
        value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| invalid(file, value))
    }
}

fn parse_io(contents: &str) -> Result<CgroupIoStats, CgroupStatsError> {
    let mut totals = CgroupIoStats::default();
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let mut fields = line.split_ascii_whitespace();
        validate_device(fields.next().ok_or_else(|| invalid("io.stat", line))?, line)?;
        for field in fields {
            let (key, value) = field
                .split_once('=')
                .ok_or_else(|| invalid("io.stat", line))?;
            let value = value.parse::<u64>().map_err(|_| invalid("io.stat", line))?;
            match key {
                "rbytes" => add(&mut totals.read_bytes, value)?,
                "wbytes" => add(&mut totals.write_bytes, value)?,
                "rios" => add(&mut totals.read_operations, value)?,
                "wios" => add(&mut totals.write_operations, value)?,
                "dbytes" => add(&mut totals.discarded_bytes, value)?,
                "dios" => add(&mut totals.discard_operations, value)?,
                _ => {}
            }
        }
    }
    Ok(totals)
}

fn validate_device(device: &str, line: &str) -> Result<(), CgroupStatsError> {
    let (major, minor) = device
        .split_once(':')
        .ok_or_else(|| invalid("io.stat", line))?;
    major
        .parse::<u32>()
        .and_then(|_| minor.parse::<u32>())
        .map(|_| ())
        .map_err(|_| invalid("io.stat", line))
}

fn add(total: &mut u64, value: u64) -> Result<(), CgroupStatsError> {
    *total = total
        .checked_add(value)
        .ok_or(CgroupStatsError::CounterOverflow)?;
    Ok(())
}

fn required(
    values: &BTreeMap<String, u64>,
    file: &'static str,
    field: &'static str,
) -> Result<u64, CgroupStatsError> {
    values
        .get(field)
        .copied()
        .ok_or(CgroupStatsError::MissingField { file, field })
}

fn invalid(file: &'static str, value: &str) -> CgroupStatsError {
    CgroupStatsError::InvalidValue {
        file,
        value: value.to_owned(),
    }
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> CgroupStatsError {
    CgroupStatsError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

/// Failure to safely read or parse one cgroup v2 sample.
#[derive(Debug, thiserror::Error)]
pub enum CgroupStatsError {
    /// A cgroup directory or pseudo-file was replaced with an unsafe file type.
    #[error("cgroup stats path `{}` is not a safe directory or regular file", path.display())]
    UnsafePath {
        /// Rejected path.
        path: PathBuf,
    },
    /// A pseudo-file exceeded the defensive read bound.
    #[error("cgroup stats file `{}` exceeds the read limit", path.display())]
    FileTooLarge {
        /// Oversized path.
        path: PathBuf,
    },
    /// A required kernel counter was absent.
    #[error("cgroup stats file `{file}` is missing required field `{field}`")]
    MissingField {
        /// Kernel pseudo-file.
        file: &'static str,
        /// Required counter.
        field: &'static str,
    },
    /// A kernel pseudo-file did not match its documented v2 grammar.
    #[error("cgroup stats file `{file}` contains invalid value `{value}`")]
    InvalidValue {
        /// Kernel pseudo-file.
        file: &'static str,
        /// Invalid line or scalar.
        value: String,
    },
    /// Summing per-device I/O counters exceeded `u64`.
    #[error("cgroup I/O counters overflowed while summing devices")]
    CounterOverflow,
    /// Blocking filesystem work could not complete.
    #[error("cgroup stats filesystem task failed: {message}")]
    Task {
        /// Task failure detail.
        message: String,
    },
    /// A cgroup filesystem operation failed.
    #[error("failed to {operation} cgroup stats path `{}`: {source}", path.display())]
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Cgroup path.
        path: PathBuf,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
}
