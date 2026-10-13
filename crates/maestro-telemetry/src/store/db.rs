use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};
use std::time::{Duration, Instant};

use duckdb::Connection;

use crate::error::{Result, TelemetryError};

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
        let connection = Connection::open(path)?;
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
