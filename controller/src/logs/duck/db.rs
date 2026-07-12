use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use duckdb::Connection;

const READER_CONNECTIONS: usize = 4;
const LOCK_WAIT_LOG_THRESHOLD: Duration = Duration::from_millis(100);

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
        let connection =
            Connection::open(path).with_context(|| format!("open DuckDB {}", path.display()))?;
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
        for offset in 0..self.readers.len() {
            match self.readers[(start + offset) % self.readers.len()].try_lock() {
                Ok(connection) => return Ok(connection),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Poisoned(_)) => bail!("DuckDB reader mutex poisoned"),
            }
        }
        lock_with_wait_metric(&self.readers[start], "reader")
    }

    pub(super) fn writer(&self) -> Result<MutexGuard<'_, Connection>> {
        lock_with_wait_metric(&self.writer, "writer")
    }

    pub(super) fn maintenance(&self) -> Result<MutexGuard<'_, Connection>> {
        lock_with_wait_metric(&self.maintenance, "maintenance")
    }

    pub(super) fn read_parquet(&self) -> Result<RwLockReadGuard<'_, ()>> {
        self.parquet_visibility
            .read()
            .map_err(|_| anyhow!("DuckDB parquet visibility lock poisoned"))
    }

    pub(super) fn write_parquet(&self) -> Result<RwLockWriteGuard<'_, ()>> {
        self.parquet_visibility
            .write()
            .map_err(|_| anyhow!("DuckDB parquet visibility lock poisoned"))
    }
}

fn lock_with_wait_metric<'a>(
    lock: &'a Mutex<Connection>,
    role: &str,
) -> Result<MutexGuard<'a, Connection>> {
    let started = Instant::now();
    let connection = lock
        .lock()
        .map_err(|_| anyhow!("DuckDB {role} mutex poisoned"))?;
    let waited = started.elapsed();
    if waited >= LOCK_WAIT_LOG_THRESHOLD {
        eprintln!(
            "[maestro]: duckdb_connection_wait role={role} wait_ms={}",
            waited.as_millis()
        );
    }
    Ok(connection)
}
