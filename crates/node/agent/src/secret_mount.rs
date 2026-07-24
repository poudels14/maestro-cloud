use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Weak};

use kernel_api::{SecretMountSpec, WorkloadId};
use runtime::WorkloadMount;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore};

pub(crate) use crate::secret_mount_files::{cleanup_directory, cleanup_stale, materialize};

pub(crate) struct SecretMountManager {
    root: PathBuf,
    files: Arc<dyn SecretMountFileSystem>,
    cleanup_gate: RwLock<()>,
    operation_gates: Mutex<BTreeMap<WorkloadId, Weak<Semaphore>>>,
}

impl SecretMountManager {
    pub(crate) fn new(root: PathBuf) -> Result<Self, SecretMountError> {
        Self::with_file_system(root, Arc::new(HostSecretMountFileSystem))
    }

    pub(crate) fn with_file_system(
        root: PathBuf,
        files: Arc<dyn SecretMountFileSystem>,
    ) -> Result<Self, SecretMountError> {
        if !root.is_absolute()
            || root.parent().is_none()
            || root
                .components()
                .any(|component| component == Component::ParentDir)
        {
            return Err(SecretMountError::InvalidRoot { path: root });
        }
        Ok(Self {
            root,
            files,
            cleanup_gate: RwLock::new(()),
            operation_gates: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) async fn materialize(
        &self,
        workload_id: &WorkloadId,
        spec: &SecretMountSpec,
    ) -> Result<WorkloadMount, SecretMountError> {
        let _cleanup = self.cleanup_gate.read().await;
        let _operation = self.operation(workload_id).await?;
        let files = self.files.clone();
        let root = self.root.clone();
        let workload_id = workload_id.clone();
        let spec = spec.clone();
        tokio::task::spawn_blocking(move || files.materialize(&root, &workload_id, &spec))
            .await
            .map_err(task_error)?
    }

    pub(crate) async fn cleanup(&self, workload_id: &WorkloadId) -> Result<(), SecretMountError> {
        let _cleanup = self.cleanup_gate.read().await;
        let _operation = self.operation(workload_id).await?;
        let files = self.files.clone();
        let directory = self.root.join(workload_id.as_str());
        tokio::task::spawn_blocking(move || files.cleanup(&directory))
            .await
            .map_err(task_error)?
    }

    pub(crate) async fn cleanup_stale(
        &self,
        active_workloads: &BTreeSet<String>,
    ) -> Result<usize, SecretMountError> {
        let _cleanup = self.cleanup_gate.write().await;
        let files = self.files.clone();
        let root = self.root.clone();
        let active_workloads = active_workloads.clone();
        tokio::task::spawn_blocking(move || files.cleanup_stale(&root, &active_workloads))
            .await
            .map_err(task_error)?
    }

    async fn operation(
        &self,
        workload_id: &WorkloadId,
    ) -> Result<OwnedSemaphorePermit, SecretMountError> {
        let gate = {
            let mut gates = self.operation_gates.lock().await;
            gates.retain(|_, gate| gate.strong_count() > 0);
            match gates.get(workload_id).and_then(Weak::upgrade) {
                Some(gate) => gate,
                None => {
                    let gate = Arc::new(Semaphore::new(1));
                    gates.insert(workload_id.clone(), Arc::downgrade(&gate));
                    gate
                }
            }
        };
        gate.acquire_owned()
            .await
            .map_err(|error| SecretMountError::Task {
                message: format!("secret mount operation gate closed unexpectedly: {error}"),
            })
    }
}

pub(crate) trait SecretMountFileSystem: Send + Sync {
    fn materialize(
        &self,
        root: &Path,
        workload_id: &WorkloadId,
        spec: &SecretMountSpec,
    ) -> Result<WorkloadMount, SecretMountError>;

    fn cleanup(&self, directory: &Path) -> Result<(), SecretMountError>;

    fn cleanup_stale(
        &self,
        root: &Path,
        active_workloads: &BTreeSet<String>,
    ) -> Result<usize, SecretMountError>;
}

struct HostSecretMountFileSystem;

impl SecretMountFileSystem for HostSecretMountFileSystem {
    fn materialize(
        &self,
        root: &Path,
        workload_id: &WorkloadId,
        spec: &SecretMountSpec,
    ) -> Result<WorkloadMount, SecretMountError> {
        materialize(root, workload_id, spec)
    }

    fn cleanup(&self, directory: &Path) -> Result<(), SecretMountError> {
        cleanup_directory(directory)
    }

    fn cleanup_stale(
        &self,
        root: &Path,
        active_workloads: &BTreeSet<String>,
    ) -> Result<usize, SecretMountError> {
        cleanup_stale(root, active_workloads)
    }
}

fn task_error(error: tokio::task::JoinError) -> SecretMountError {
    SecretMountError::Task {
        message: error.to_string(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecretMountError {
    #[error("secret root `{}` must be an absolute non-root path without parent traversal", path.display())]
    InvalidRoot { path: PathBuf },
    #[error("secret mount target `{target}` must be an absolute non-root path")]
    InvalidTarget { target: String },
    #[error("secret key `{name}` is not a valid environment-style name")]
    InvalidKey { name: String },
    #[error("secret file name `{name}` must contain exactly one normal path component")]
    InvalidFileName { name: String },
    #[error("secret path `{}` is not a private regular file or directory", path.display())]
    UnsafePath { path: PathBuf },
    #[error("secret content for workload `{workload_id}` changed without a new assignment")]
    ContentConflict { workload_id: String },
    #[error("failed to encode secret mount content: {message}")]
    Encode { message: String },
    #[error("secret filesystem task failed: {message}")]
    Task { message: String },
    #[error("failed to {operation} at `{}`: {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
