use std::fs::{self, OpenOptions};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use kernel_api::WorkloadId;

use crate::RuntimeError;

pub(crate) struct ContainerdIoPaths {
    pub(crate) directory: PathBuf,
    pub(crate) stdout: PathBuf,
    pub(crate) stderr: PathBuf,
}

pub(crate) fn task_paths(root: &Path, workload_id: &WorkloadId) -> ContainerdIoPaths {
    let directory = root.join("workloads").join(workload_id.as_str());
    ContainerdIoPaths {
        stdout: directory.join("stdout.log"),
        stderr: directory.join("stderr.log"),
        directory,
    }
}

pub(crate) async fn prepare_task_files(
    root: &Path,
    workload_id: &WorkloadId,
) -> Result<ContainerdIoPaths, RuntimeError> {
    let paths = task_paths(root, workload_id);
    let directory = paths.directory.clone();
    let stdout = paths.stdout.clone();
    let stderr = paths.stderr.clone();
    tokio::task::spawn_blocking(move || {
        fs::create_dir_all(&directory).map_err(|error| io_error("create", &directory, error))?;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error("protect", &directory, error))?;
        create_log(&stdout)?;
        create_log(&stderr)?;
        Ok::<(), RuntimeError>(())
    })
    .await
    .map_err(|error| RuntimeError::Unavailable {
        message: format!("containerd IO preparation task failed: {error}"),
    })??;
    Ok(paths)
}

pub(crate) fn path_text(path: &Path) -> Result<String, RuntimeError> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| RuntimeError::InvalidSpec {
            message: format!("containerd IO path `{}` is not UTF-8", path.display()),
        })
}

fn create_log(path: &Path) -> Result<(), RuntimeError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| io_error("open", path, error))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|error| io_error("protect", path, error))
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::Unavailable {
        message: format!(
            "failed to {operation} containerd IO path `{}`: {error}",
            path.display()
        ),
    }
}
