use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::net::IpAddr;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use kernel_api::{ClusterId, WorkloadId};

use crate::RuntimeError;
use crate::containerd_io::task_paths;

const RESOLVER_FILE: &str = "resolv.conf";

pub(crate) fn resolver_path(root: &Path, workload_id: &WorkloadId) -> PathBuf {
    task_paths(root, workload_id).directory.join(RESOLVER_FILE)
}

pub(crate) async fn prepare_resolver_file(
    root: &Path,
    workload_id: &WorkloadId,
    dns_server: IpAddr,
    cluster_id: &ClusterId,
) -> Result<PathBuf, RuntimeError> {
    let path = resolver_path(root, workload_id);
    let prepared = path.clone();
    let search_domain = format!("{}.maestro.internal", cluster_id.as_str());
    tokio::task::spawn_blocking(move || write_resolver(&prepared, dns_server, &search_domain))
        .await
        .map_err(|error| RuntimeError::Unavailable {
            message: format!("containerd resolver preparation task failed: {error}"),
        })??;
    Ok(path)
}

fn write_resolver(
    path: &Path,
    dns_server: IpAddr,
    search_domain: &str,
) -> Result<(), RuntimeError> {
    let directory = path.parent().ok_or_else(|| RuntimeError::InvalidSpec {
        message: format!(
            "containerd resolver path `{}` has no parent",
            path.display()
        ),
    })?;
    fs::create_dir_all(directory).map_err(|error| io_error("create", directory, error))?;
    let metadata =
        fs::symlink_metadata(directory).map_err(|error| io_error("inspect", directory, error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(RuntimeError::Unavailable {
            message: format!(
                "containerd resolver directory `{}` is not a private directory",
                directory.display()
            ),
        });
    }
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
        .map_err(|error| io_error("protect", directory, error))?;
    let temporary = directory.join(format!(".{RESOLVER_FILE}.{}.new", std::process::id()));
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("remove stale", &temporary, error)),
    }
    let result = install_resolver(&temporary, path, dns_server, search_domain);
    if result.is_err() {
        let _cleanup = fs::remove_file(&temporary);
    }
    result
}

fn install_resolver(
    temporary: &Path,
    destination: &Path,
    dns_server: IpAddr,
    search_domain: &str,
) -> Result<(), RuntimeError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o644)
        .open(temporary)
        .map_err(|error| io_error("create", temporary, error))?;
    writeln!(file, "search {search_domain}")
        .and_then(|()| writeln!(file, "nameserver {dns_server}"))
        .and_then(|()| file.sync_all())
        .map_err(|error| io_error("write", temporary, error))?;
    drop(file);
    fs::rename(temporary, destination).map_err(|error| io_error("install", destination, error))?;
    fs::set_permissions(destination, fs::Permissions::from_mode(0o644))
        .map_err(|error| io_error("protect", destination, error))?;
    let directory = destination
        .parent()
        .ok_or_else(|| RuntimeError::InvalidSpec {
            message: format!(
                "containerd resolver path `{}` has no parent",
                destination.display()
            ),
        })?;
    std::fs::File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync", directory, error))
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::Unavailable {
        message: format!(
            "failed to {operation} containerd resolver path `{}`: {error}",
            path.display()
        ),
    }
}
