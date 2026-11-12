use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};
use std::time::{Duration, Instant};

use duckdb::Connection;

use crate::error::{Result, TelemetryError};

const READER_CONNECTIONS: usize = 4;
const LOCK_WAIT_LOG_THRESHOLD: Duration = Duration::from_millis(100);
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const DUCKDB_MEMORY_SHARE_PERCENT: u64 = 60;

pub(super) struct Db {
    writer: Mutex<Connection>,
    maintenance: Mutex<Connection>,
    readers: Vec<Mutex<Connection>>,
    next_reader: AtomicUsize,
    parquet_visibility: RwLock<()>,
}

impl Db {
    pub(super) fn open(path: &Path, schema: &str) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        if let Some(limit) = duckdb_memory_limit() {
            tracing::info!(limit_mib = limit >> 20, "limiting DuckDB memory");
            connection.execute_batch(&format!("SET memory_limit = '{}MiB'", limit >> 20))?;
        }
        connection.execute_batch(schema)?;
        let maintenance = connection.try_clone()?;
        let readers = (0..READER_CONNECTIONS)
            .map(|_| connection.try_clone().map(Mutex::new))
            .collect::<duckdb::Result<Vec<_>>>()?;
        Ok(Self {
            writer: Mutex::new(connection),
            maintenance: Mutex::new(maintenance),
            readers,
            next_reader: AtomicUsize::new(0),
            parquet_visibility: RwLock::new(()),
        })
    }

    pub(super) fn reader(&self) -> Result<MutexGuard<'_, Connection>> {
        let start = self.next_reader.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        let available = (0..self.readers.len()).find_map(|offset| {
            match self.readers[(start + offset) % self.readers.len()].try_lock() {
                Ok(connection) => Some(Ok(connection)),
                Err(TryLockError::WouldBlock) => None,
                Err(TryLockError::Poisoned(_)) => Some(Err(poisoned("reader"))),
            }
        });
        available.unwrap_or_else(|| lock_with_wait_log(&self.readers[start], "reader"))
    }

    pub(super) fn writer(&self) -> Result<MutexGuard<'_, Connection>> {
        lock_with_wait_log(&self.writer, "writer")
    }

    pub(super) fn maintenance(&self) -> Result<MutexGuard<'_, Connection>> {
        lock_with_wait_log(&self.maintenance, "maintenance")
    }

    pub(super) fn read_parquet(&self) -> Result<RwLockReadGuard<'_, ()>> {
        self.parquet_visibility
            .read()
            .map_err(|_| poisoned("parquet visibility"))
    }

    pub(super) fn write_parquet(&self) -> Result<RwLockWriteGuard<'_, ()>> {
        self.parquet_visibility
            .write()
            .map_err(|_| poisoned("parquet visibility"))
    }
}

fn lock_with_wait_log<'a>(
    lock: &'a Mutex<Connection>,
    role: &str,
) -> Result<MutexGuard<'a, Connection>> {
    let started = Instant::now();
    let connection = lock.lock().map_err(|_| poisoned(role))?;
    let waited = started.elapsed();
    if waited >= LOCK_WAIT_LOG_THRESHOLD {
        tracing::warn!(
            role,
            wait_ms = waited.as_millis() as u64,
            "slow DuckDB connection wait"
        );
    }
    Ok(connection)
}

fn poisoned(role: &str) -> TelemetryError {
    TelemetryError::Storage(format!("DuckDB {role} lock poisoned"))
}

/// DuckDB defaults to 80% of the memory it can see, which leaves too little for the rest of
/// telemetry under the service's cgroup cap; a lower limit makes it spill to disk instead of
/// getting the whole process OOM-killed.
fn duckdb_memory_limit() -> Option<u64> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let cgroup = unified_cgroup(&membership)?;
    let memory_max = std::fs::read_to_string(
        Path::new(CGROUP_ROOT)
            .join(cgroup.trim_start_matches('/'))
            .join("memory.max"),
    )
    .ok()?;
    memory_cap(&memory_max).map(|cap| cap / 100 * DUCKDB_MEMORY_SHARE_PERCENT)
}

fn unified_cgroup(membership: &str) -> Option<&str> {
    membership.lines().find_map(|line| line.strip_prefix("0::"))
}

fn memory_cap(memory_max: &str) -> Option<u64> {
    memory_max.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_service_memory_cap_from_its_cgroup() {
        assert_eq!(
            unified_cgroup("0::/maestro.slice/maestro-system.slice/maestro-telemetry.service\n"),
            Some("/maestro.slice/maestro-system.slice/maestro-telemetry.service")
        );
        assert_eq!(unified_cgroup("1:name=systemd:/init.scope\n"), None);
        assert_eq!(memory_cap("1073741824\n"), Some(1 << 30));
        assert_eq!(memory_cap("max\n"), None);
        assert_eq!(
            memory_cap("1073741824").map(|cap| (cap / 100 * DUCKDB_MEMORY_SHARE_PERCENT) >> 20),
            Some(614)
        );
    }
}
