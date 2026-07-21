use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use kernel_api::{SecretMountSpec, WorkloadId};
use runtime::{MountAccess, MountSource, WorkloadMount};
use zeroize::{Zeroize, Zeroizing};

const DIRECTORY_MODE: u32 = 0o700;
const SECRET_MODE: u32 = 0o600;
const SECRET_FILE: &str = "secrets.env";

#[derive(Clone)]
pub(crate) struct SecretMountManager {
    root: PathBuf,
}

impl SecretMountManager {
    pub(crate) fn new(root: PathBuf) -> Result<Self, SecretMountError> {
        if !root.is_absolute()
            || root.parent().is_none()
            || root
                .components()
                .any(|component| component == Component::ParentDir)
        {
            return Err(SecretMountError::InvalidRoot { path: root });
        }
        Ok(Self { root })
    }

    pub(crate) async fn materialize(
        &self,
        workload_id: &WorkloadId,
        spec: &SecretMountSpec,
    ) -> Result<WorkloadMount, SecretMountError> {
        let root = self.root.clone();
        let workload_id = workload_id.clone();
        let spec = spec.clone();
        tokio::task::spawn_blocking(move || materialize(&root, &workload_id, &spec))
            .await
            .map_err(task_error)?
    }

    pub(crate) async fn cleanup(&self, workload_id: &WorkloadId) -> Result<(), SecretMountError> {
        let directory = self.root.join(workload_id.as_str());
        tokio::task::spawn_blocking(move || cleanup_directory(&directory))
            .await
            .map_err(task_error)?
    }

    pub(crate) async fn cleanup_stale(
        &self,
        active_workloads: &BTreeSet<String>,
    ) -> Result<usize, SecretMountError> {
        let root = self.root.clone();
        let active_workloads = active_workloads.clone();
        tokio::task::spawn_blocking(move || cleanup_stale(&root, &active_workloads))
            .await
            .map_err(task_error)?
    }
}

fn materialize(
    root: &Path,
    workload_id: &WorkloadId,
    spec: &SecretMountSpec,
) -> Result<WorkloadMount, SecretMountError> {
    let target = PathBuf::from(&spec.mount_path);
    if !target.is_absolute() || target.parent().is_none() || target == Path::new("/") {
        return Err(SecretMountError::InvalidTarget {
            target: spec.mount_path.clone(),
        });
    }
    let mut content = Zeroizing::new(String::new());
    for (name, value) in &spec.items {
        if !valid_secret_name(name) {
            return Err(SecretMountError::InvalidKey { name: name.clone() });
        }
        let encoded = Zeroizing::new(serde_json::to_string(value.expose()).map_err(|error| {
            SecretMountError::Encode {
                message: error.to_string(),
            }
        })?);
        writeln!(&mut *content, "{name}={}", encoded.as_str()).map_err(|error| {
            SecretMountError::Encode {
                message: error.to_string(),
            }
        })?;
    }

    ensure_private_directory(root)?;
    let directory = root.join(workload_id.as_str());
    ensure_private_directory(&directory)?;
    let secret_path = directory.join(SECRET_FILE);
    if secret_path.exists() {
        ensure_existing_content(&secret_path, content.as_bytes(), workload_id)?;
    } else {
        install_secret(&directory, &secret_path, content.as_bytes(), workload_id)?;
    }
    Ok(WorkloadMount {
        source: MountSource::HostPath(secret_path),
        target,
        access: MountAccess::ReadOnly,
    })
}

fn ensure_private_directory(path: &Path) -> Result<(), SecretMountError> {
    fs::create_dir_all(path).map_err(|source| io_error("create directory", path, source))?;
    let metadata =
        fs::symlink_metadata(path).map_err(|source| io_error("inspect directory", path, source))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(SecretMountError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    fs::set_permissions(path, fs::Permissions::from_mode(DIRECTORY_MODE))
        .map_err(|source| io_error("protect directory", path, source))
}

fn install_secret(
    directory: &Path,
    secret_path: &Path,
    content: &[u8],
    workload_id: &WorkloadId,
) -> Result<(), SecretMountError> {
    let temporary = directory.join(format!(".{SECRET_FILE}.{}.new", std::process::id()));
    cleanup_temporary(&temporary, secret_path)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(SECRET_MODE)
        .open(&temporary)
        .map_err(|source| io_error("create secret", &temporary, source))?;
    if let Err(source) = file.write_all(content).and_then(|()| file.sync_all()) {
        drop(file);
        let _cleanup = zeroize_and_remove(&temporary);
        return Err(io_error("write secret", &temporary, source));
    }
    drop(file);
    match fs::hard_link(&temporary, secret_path) {
        Ok(()) => {
            fs::remove_file(&temporary)
                .map_err(|source| io_error("remove secret staging link", &temporary, source))?;
            File::open(directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|source| io_error("sync secret directory", directory, source))?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            cleanup_temporary(&temporary, secret_path)?;
            ensure_existing_content(secret_path, content, workload_id)
        }
        Err(source) => {
            let _cleanup = cleanup_temporary(&temporary, secret_path);
            Err(io_error("install secret", secret_path, source))
        }
    }
}

fn ensure_existing_content(
    path: &Path,
    expected: &[u8],
    workload_id: &WorkloadId,
) -> Result<(), SecretMountError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| io_error("inspect secret", path, source))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(SecretMountError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    let mut existing = Zeroizing::new(
        fs::read(path).map_err(|source| io_error("read existing secret", path, source))?,
    );
    let matches = existing.as_slice() == expected;
    existing.zeroize();
    if !matches {
        return Err(SecretMountError::ContentConflict {
            workload_id: workload_id.to_string(),
        });
    }
    fs::set_permissions(path, fs::Permissions::from_mode(SECRET_MODE))
        .map_err(|source| io_error("protect secret", path, source))
}

fn cleanup_stale(root: &Path, active: &BTreeSet<String>) -> Result<usize, SecretMountError> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => return Err(io_error("list secret root", root, source)),
    };
    let mut cleaned = 0_usize;
    for entry in entries {
        let entry = entry.map_err(|source| io_error("list secret root", root, source))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry
            .file_type()
            .map_err(|source| io_error("inspect secret entry", &entry.path(), source))?
            .is_dir()
            && WorkloadId::new(&name).is_ok()
            && !active.contains(&name)
        {
            cleanup_directory(&entry.path())?;
            cleaned = cleaned.saturating_add(1);
        }
    }
    Ok(cleaned)
}

fn cleanup_directory(directory: &Path) -> Result<(), SecretMountError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(io_error("list workload secrets", directory, source)),
    };
    for entry in entries {
        let entry = entry.map_err(|source| io_error("list workload secrets", directory, source))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("inspect workload secret", &path, source))?;
        if file_type.is_file() {
            zeroize_and_remove(&path)?;
        } else if file_type.is_symlink() {
            fs::remove_file(&path)
                .map_err(|source| io_error("remove unsafe link", &path, source))?;
        }
    }
    fs::remove_dir(directory)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(error)
            }
        })
        .map_err(|source| io_error("remove workload secret directory", directory, source))
}

fn cleanup_temporary(temporary: &Path, installed: &Path) -> Result<(), SecretMountError> {
    let temporary_metadata = match fs::symlink_metadata(temporary) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(io_error("inspect secret staging file", temporary, source)),
    };
    let installed_metadata = fs::symlink_metadata(installed).ok();
    let aliases_installed = installed_metadata.is_some_and(|installed_metadata| {
        installed_metadata.dev() == temporary_metadata.dev()
            && installed_metadata.ino() == temporary_metadata.ino()
    });
    if aliases_installed {
        fs::remove_file(temporary)
            .map_err(|source| io_error("remove secret staging link", temporary, source))
    } else {
        zeroize_and_remove(temporary)
    }
}

fn zeroize_and_remove(path: &Path) -> Result<(), SecretMountError> {
    let length = fs::metadata(path)
        .map_err(|source| io_error("inspect secret for cleanup", path, source))?
        .len();
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| io_error("open secret for cleanup", path, source))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| io_error("seek secret for cleanup", path, source))?;
    let zeros = [0_u8; 4096];
    let mut remaining = length;
    while remaining > 0 {
        let count = usize::try_from(remaining.min(zeros.len() as u64)).unwrap_or(zeros.len());
        let chunk = zeros.get(..count).unwrap_or(&zeros);
        file.write_all(chunk)
            .map_err(|source| io_error("zero secret", path, source))?;
        remaining = remaining.saturating_sub(count as u64);
    }
    file.sync_all()
        .map_err(|source| io_error("sync zeroed secret", path, source))?;
    drop(file);
    fs::remove_file(path).map_err(|source| io_error("remove zeroed secret", path, source))
}

fn valid_secret_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

fn task_error(error: tokio::task::JoinError) -> SecretMountError {
    SecretMountError::Task {
        message: error.to_string(),
    }
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> SecretMountError {
    SecretMountError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecretMountError {
    #[error("secret root `{}` must be an absolute non-root path without parent traversal", path.display())]
    InvalidRoot { path: PathBuf },
    #[error("secret mount target `{target}` must be an absolute file path")]
    InvalidTarget { target: String },
    #[error("secret key `{name}` is not a valid environment-style name")]
    InvalidKey { name: String },
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
