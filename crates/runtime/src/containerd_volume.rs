use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use kernel_api::ClusterId;
use nix::unistd::{Gid, Uid, chown};

use crate::managed_volume::managed_volume_key;
use crate::{MountSource, RuntimeError, WorkloadConfiguration, WorkloadUser};

const VOLUME_DIRECTORY: &str = "volumes";

pub(crate) fn managed_volume_path(
    state_root: &Path,
    cluster_id: &ClusterId,
    name: &str,
) -> Result<PathBuf, RuntimeError> {
    Ok(state_root
        .join(VOLUME_DIRECTORY)
        .join(managed_volume_key(cluster_id, name)?))
}

pub(crate) async fn prepare_managed_volumes(
    state_root: &Path,
    configuration: &WorkloadConfiguration,
) -> Result<(), RuntimeError> {
    let paths = configuration
        .mounts
        .iter()
        .filter_map(|mount| match &mount.source {
            MountSource::ManagedVolume(name) => Some(managed_volume_path(
                state_root,
                &configuration.metadata.cluster_id,
                name,
            )),
            MountSource::HostPath(_) => None,
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if paths.is_empty() {
        return Ok(());
    }
    let owner = configuration.user;
    let root = state_root.join(VOLUME_DIRECTORY);
    tokio::task::spawn_blocking(move || prepare_directories(&root, &paths, owner))
        .await
        .map_err(|error| RuntimeError::Unavailable {
            message: format!("containerd volume preparation task failed: {error}"),
        })?
}

fn prepare_directories(
    root: &Path,
    paths: &BTreeSet<PathBuf>,
    owner: Option<WorkloadUser>,
) -> Result<(), RuntimeError> {
    fs::create_dir_all(root).map_err(|error| io_error("create", root, error))?;
    require_directory(root, "root")?;
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))
        .map_err(|error| io_error("protect", root, error))?;
    for path in paths {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error("create", path, error)),
        }
        require_directory(path, "data")?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error("protect", path, error))?;
        if let Some(owner) = owner {
            chown(
                path,
                Some(Uid::from_raw(owner.user_id)),
                Some(Gid::from_raw(owner.group_id)),
            )
            .map_err(|error| {
                io_error(
                    "set owner on",
                    path,
                    std::io::Error::from_raw_os_error(error as i32),
                )
            })?;
        }
    }
    fs::File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync", root, error))
}

fn require_directory(path: &Path, purpose: &str) -> Result<(), RuntimeError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| io_error("inspect", path, error))?;
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        Err(RuntimeError::Unavailable {
            message: format!(
                "containerd managed-volume {purpose} `{}` is not a directory",
                path.display()
            ),
        })
    }
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::Unavailable {
        message: format!(
            "failed to {operation} containerd managed-volume path `{}`: {error}",
            path.display()
        ),
    }
}
