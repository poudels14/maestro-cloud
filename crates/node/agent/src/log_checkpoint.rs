use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use kernel_api::WorkloadId;
use runtime::LogCursor;
use tokio::sync::Mutex;

const DIRECTORY_MODE: u32 = 0o700;
const CURSOR_MODE: u32 = 0o600;
const CURSOR_SUFFIX: &str = ".cursor";
const MAX_CURSOR_BYTES: usize = 4 * 1024;
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Durable resume positions for runtime-native workload log streams.
#[async_trait]
pub trait LogCheckpointStore: Send + Sync + 'static {
    /// Loads the last cursor committed after successful sink delivery.
    async fn load(&self, workload_id: &WorkloadId)
    -> Result<Option<LogCursor>, LogCheckpointError>;

    /// Atomically commits a cursor after its frame reaches the sink.
    async fn commit(
        &self,
        workload_id: &WorkloadId,
        cursor: &LogCursor,
    ) -> Result<(), LogCheckpointError>;

    /// Removes checkpoints for runtime workloads no longer owned by this node.
    async fn cleanup_stale(
        &self,
        active_workloads: &BTreeSet<WorkloadId>,
    ) -> Result<usize, LogCheckpointError>;
}

/// Private local-filesystem checkpoint store for one node agent.
pub struct FileLogCheckpointStore {
    root: PathBuf,
    operation: Mutex<()>,
}

impl FileLogCheckpointStore {
    /// Validates the root without creating it.
    pub fn new(root: PathBuf) -> Result<Self, LogCheckpointError> {
        validate_root(&root)?;
        Ok(Self {
            root,
            operation: Mutex::new(()),
        })
    }
}

#[async_trait]
impl LogCheckpointStore for FileLogCheckpointStore {
    async fn load(
        &self,
        workload_id: &WorkloadId,
    ) -> Result<Option<LogCursor>, LogCheckpointError> {
        let _operation = self.operation.lock().await;
        let root = self.root.clone();
        let workload_id = workload_id.clone();
        tokio::task::spawn_blocking(move || load_cursor(&root, &workload_id))
            .await
            .map_err(task_error)?
    }

    async fn commit(
        &self,
        workload_id: &WorkloadId,
        cursor: &LogCursor,
    ) -> Result<(), LogCheckpointError> {
        let _operation = self.operation.lock().await;
        let root = self.root.clone();
        let workload_id = workload_id.clone();
        let cursor = cursor.clone();
        tokio::task::spawn_blocking(move || commit_cursor(&root, &workload_id, &cursor))
            .await
            .map_err(task_error)?
    }

    async fn cleanup_stale(
        &self,
        active_workloads: &BTreeSet<WorkloadId>,
    ) -> Result<usize, LogCheckpointError> {
        let _operation = self.operation.lock().await;
        let root = self.root.clone();
        let active_workloads = active_workloads.clone();
        tokio::task::spawn_blocking(move || cleanup_stale(&root, &active_workloads))
            .await
            .map_err(task_error)?
    }
}

fn load_cursor(
    root: &Path,
    workload_id: &WorkloadId,
) -> Result<Option<LogCursor>, LogCheckpointError> {
    if !private_directory_exists(root)? {
        return Ok(None);
    }
    let path = cursor_path(root, workload_id);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("inspect checkpoint", &path, source)),
    };
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(LogCheckpointError::UnsafePath { path });
    }
    let bytes = fs::read(&path).map_err(|source| io_error("read checkpoint", &path, source))?;
    if bytes.is_empty() || bytes.len() > MAX_CURSOR_BYTES {
        return Err(LogCheckpointError::InvalidCursor { path });
    }
    let cursor = String::from_utf8(bytes)
        .map_err(|_| LogCheckpointError::InvalidCursor { path: path.clone() })?;
    Ok(Some(LogCursor::new(cursor)))
}

fn commit_cursor(
    root: &Path,
    workload_id: &WorkloadId,
    cursor: &LogCursor,
) -> Result<(), LogCheckpointError> {
    let bytes = cursor.as_str().as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_CURSOR_BYTES {
        return Err(LogCheckpointError::InvalidCursor {
            path: cursor_path(root, workload_id),
        });
    }
    ensure_private_directory(root)?;
    let installed = cursor_path(root, workload_id);
    match fs::symlink_metadata(&installed) {
        Ok(metadata) if !metadata.file_type().is_file() || metadata.file_type().is_symlink() => {
            return Err(LogCheckpointError::UnsafePath { path: installed });
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => return Err(io_error("inspect checkpoint", &installed, source)),
    }
    let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = root.join(format!(
        ".{}.{process}.{sequence}.new",
        workload_id.as_str(),
        process = std::process::id()
    ));
    remove_staging_file(&temporary)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(CURSOR_MODE)
        .open(&temporary)
        .map_err(|source| io_error("create checkpoint", &temporary, source))?;
    if let Err(source) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _cleanup = fs::remove_file(&temporary);
        return Err(io_error("write checkpoint", &temporary, source));
    }
    drop(file);
    fs::rename(&temporary, &installed)
        .map_err(|source| io_error("install checkpoint", &installed, source))?;
    fs::set_permissions(&installed, fs::Permissions::from_mode(CURSOR_MODE))
        .map_err(|source| io_error("protect checkpoint", &installed, source))?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("sync checkpoint directory", root, source))
}

fn cleanup_stale(
    root: &Path,
    active_workloads: &BTreeSet<WorkloadId>,
) -> Result<usize, LogCheckpointError> {
    if !private_directory_exists(root)? {
        return Ok(0);
    }
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => return Err(io_error("list checkpoint root", root, source)),
    };
    let mut removed = 0_usize;
    for entry in entries {
        let entry = entry.map_err(|source| io_error("list checkpoint root", root, source))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(id) = name.strip_suffix(CURSOR_SUFFIX) else {
            continue;
        };
        let Ok(workload_id) = WorkloadId::new(id) else {
            continue;
        };
        if active_workloads.contains(&workload_id) {
            continue;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("inspect checkpoint", &path, source))?;
        if !file_type.is_file() || file_type.is_symlink() {
            return Err(LogCheckpointError::UnsafePath { path });
        }
        fs::remove_file(&path).map_err(|source| io_error("remove checkpoint", &path, source))?;
        removed = removed.saturating_add(1);
    }
    if removed > 0 {
        File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| io_error("sync checkpoint directory", root, source))?;
    }
    Ok(removed)
}

fn ensure_private_directory(path: &Path) -> Result<(), LogCheckpointError> {
    fs::create_dir_all(path).map_err(|source| io_error("create checkpoint root", path, source))?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| io_error("inspect checkpoint root", path, source))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(LogCheckpointError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    fs::set_permissions(path, fs::Permissions::from_mode(DIRECTORY_MODE))
        .map_err(|source| io_error("protect checkpoint root", path, source))
}

fn private_directory_exists(path: &Path) -> Result<bool, LogCheckpointError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(io_error("inspect checkpoint root", path, source)),
    };
    if !metadata.file_type().is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        Err(LogCheckpointError::UnsafePath {
            path: path.to_path_buf(),
        })
    } else {
        Ok(true)
    }
}

fn remove_staging_file(path: &Path) -> Result<(), LogCheckpointError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(io_error("inspect checkpoint staging file", path, source)),
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(LogCheckpointError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    fs::remove_file(path).map_err(|source| io_error("remove checkpoint staging file", path, source))
}

fn cursor_path(root: &Path, workload_id: &WorkloadId) -> PathBuf {
    root.join(format!("{}{CURSOR_SUFFIX}", workload_id.as_str()))
}

fn validate_root(root: &Path) -> Result<(), LogCheckpointError> {
    if !root.is_absolute()
        || root.parent().is_none()
        || root
            .components()
            .any(|component| component == Component::ParentDir)
    {
        Err(LogCheckpointError::InvalidRoot {
            path: root.to_path_buf(),
        })
    } else {
        Ok(())
    }
}

fn task_error(error: tokio::task::JoinError) -> LogCheckpointError {
    LogCheckpointError::Task {
        message: error.to_string(),
    }
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> LogCheckpointError {
    LogCheckpointError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

/// Failure to load, commit, or garbage-collect a runtime log cursor.
#[derive(Debug, thiserror::Error)]
pub enum LogCheckpointError {
    /// The checkpoint root was not an absolute, scoped path.
    #[error("log checkpoint root `{}` must be an absolute non-root path", path.display())]
    InvalidRoot {
        /// Rejected root.
        path: PathBuf,
    },
    /// A checkpoint path was replaced with an unsafe file type.
    #[error("log checkpoint path `{}` is not a private regular file or directory", path.display())]
    UnsafePath {
        /// Rejected path.
        path: PathBuf,
    },
    /// A cursor was empty, oversized, or not UTF-8.
    #[error("log checkpoint `{}` contains an invalid cursor", path.display())]
    InvalidCursor {
        /// Invalid checkpoint path.
        path: PathBuf,
    },
    /// Blocking filesystem work could not complete.
    #[error("log checkpoint filesystem task failed: {message}")]
    Task {
        /// Task failure detail.
        message: String,
    },
    /// A checkpoint filesystem operation failed.
    #[error("failed to {operation} log checkpoint `{}`: {source}", path.display())]
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Checkpoint path.
        path: PathBuf,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
}
