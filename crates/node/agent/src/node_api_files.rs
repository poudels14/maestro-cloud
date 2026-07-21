use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kernel_api::WorkloadId;
use node_fabric::{
    WORKLOAD_NODE_DIRECTORY, WORKLOAD_NODE_SOCKET_FILE, WORKLOAD_NODE_TOKEN_FILE, WorkloadToken,
};
use runtime::{MountAccess, MountSource, WorkloadMount};
use zeroize::{Zeroize, Zeroizing};

use crate::NodeApiSocketOwner;

const ROOT_MODE: u32 = 0o700;
const WORKLOAD_DIRECTORY_MODE: u32 = 0o711;
const TOKEN_MODE: u32 = 0o400;
const TOKEN_LENGTH: usize = 32;

pub(crate) struct PreparedNodeApiFiles {
    pub(crate) directory: PathBuf,
    pub(crate) socket_path: PathBuf,
    pub(crate) token: WorkloadToken,
}

impl PreparedNodeApiFiles {
    pub(crate) fn workload_mount(&self) -> WorkloadMount {
        WorkloadMount {
            source: MountSource::HostPath(self.directory.clone()),
            target: PathBuf::from(WORKLOAD_NODE_DIRECTORY),
            access: MountAccess::ReadOnly,
        }
    }
}

pub(crate) fn prepare_node_api_files(
    root: &Path,
    workload_id: &WorkloadId,
    owner: NodeApiSocketOwner,
) -> Result<PreparedNodeApiFiles, NodeApiMountError> {
    validate_root(root)?;
    ensure_directory(root, ROOT_MODE)?;
    let directory = root.join(workload_id.as_str());
    ensure_directory(&directory, WORKLOAD_DIRECTORY_MODE)?;
    let token_path = directory.join(WORKLOAD_NODE_TOKEN_FILE);
    let token = if token_path.exists() {
        read_token(&token_path, owner)?
    } else {
        install_token(&directory, &token_path, owner)?
    };
    Ok(PreparedNodeApiFiles {
        socket_path: directory.join(WORKLOAD_NODE_SOCKET_FILE),
        directory,
        token,
    })
}

pub(crate) fn remove_stale_socket(socket_path: &Path) -> Result<(), NodeApiMountError> {
    let metadata = match fs::symlink_metadata(socket_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(io_error("inspect stale socket", socket_path, source)),
    };
    if !metadata.file_type().is_socket() {
        return Err(NodeApiMountError::UnsafePath {
            path: socket_path.to_path_buf(),
        });
    }
    fs::remove_file(socket_path)
        .map_err(|source| io_error("remove stale socket", socket_path, source))
}

pub(crate) fn cleanup_node_api_directory(directory: &Path) -> Result<(), NodeApiMountError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(io_error("list node API directory", directory, source)),
    };
    for entry in entries {
        let entry =
            entry.map_err(|source| io_error("list node API directory", directory, source))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("inspect node API entry", &path, source))?;
        if file_type.is_file() {
            zeroize_and_remove(&path)?;
        } else if file_type.is_socket() || file_type.is_symlink() {
            fs::remove_file(&path)
                .map_err(|source| io_error("remove node API entry", &path, source))?;
        } else {
            return Err(NodeApiMountError::UnsafePath { path });
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
        .map_err(|source| io_error("remove node API directory", directory, source))
}

pub(crate) fn list_workload_directories(root: &Path) -> Result<Vec<WorkloadId>, NodeApiMountError> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(io_error("list node API root", root, source)),
    };
    let mut workloads = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| io_error("list node API root", root, source))?;
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("inspect node API root entry", &entry.path(), source))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if file_type.is_dir()
            && let Ok(workload_id) = WorkloadId::new(name)
        {
            workloads.push(workload_id);
        }
    }
    workloads.sort();
    Ok(workloads)
}

pub(crate) fn validate_root(root: &Path) -> Result<(), NodeApiMountError> {
    if !root.is_absolute()
        || root.parent().is_none()
        || root
            .components()
            .any(|component| component == Component::ParentDir)
    {
        Err(NodeApiMountError::InvalidRoot {
            path: root.to_path_buf(),
        })
    } else {
        Ok(())
    }
}

fn ensure_directory(path: &Path, mode: u32) -> Result<(), NodeApiMountError> {
    fs::create_dir_all(path).map_err(|source| io_error("create directory", path, source))?;
    let metadata =
        fs::symlink_metadata(path).map_err(|source| io_error("inspect directory", path, source))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(NodeApiMountError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .map_err(|source| io_error("protect directory", path, source))
}

fn install_token(
    directory: &Path,
    token_path: &Path,
    owner: NodeApiSocketOwner,
) -> Result<WorkloadToken, NodeApiMountError> {
    let mut token_bytes = Zeroizing::new([0_u8; TOKEN_LENGTH]);
    getrandom::fill(token_bytes.as_mut()).map_err(|error| NodeApiMountError::Random {
        message: error.to_string(),
    })?;
    let encoded = Zeroizing::new(URL_SAFE_NO_PAD.encode(token_bytes.as_slice()));
    let temporary = directory.join(format!(
        ".{WORKLOAD_NODE_TOKEN_FILE}.{}.new",
        std::process::id()
    ));
    if temporary.exists() {
        zeroize_and_remove(&temporary)?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|source| io_error("create token", &temporary, source))?;
    if let Err(source) = file
        .write_all(encoded.as_bytes())
        .and_then(|()| file.sync_all())
    {
        drop(file);
        let _cleanup = zeroize_and_remove(&temporary);
        return Err(io_error("write token", &temporary, source));
    }
    drop(file);
    std::os::unix::fs::chown(&temporary, Some(owner.user_id), Some(owner.group_id))
        .map_err(|source| io_error("set token owner", &temporary, source))?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(TOKEN_MODE))
        .map_err(|source| io_error("protect token", &temporary, source))?;
    match fs::hard_link(&temporary, token_path) {
        Ok(()) => {
            fs::remove_file(&temporary)
                .map_err(|source| io_error("remove token staging link", &temporary, source))?;
            File::open(directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|source| io_error("sync node API directory", directory, source))?;
            let mut copied = [0_u8; TOKEN_LENGTH];
            copied.copy_from_slice(token_bytes.as_slice());
            let token = WorkloadToken::from_bytes(copied);
            copied.zeroize();
            Ok(token)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            zeroize_and_remove(&temporary)?;
            read_token(token_path, owner)
        }
        Err(source) => {
            let _cleanup = zeroize_and_remove(&temporary);
            Err(io_error("install token", token_path, source))
        }
    }
}

fn read_token(
    token_path: &Path,
    owner: NodeApiSocketOwner,
) -> Result<WorkloadToken, NodeApiMountError> {
    let metadata = fs::symlink_metadata(token_path)
        .map_err(|source| io_error("inspect token", token_path, source))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(NodeApiMountError::UnsafePath {
            path: token_path.to_path_buf(),
        });
    }
    if metadata.uid() != owner.user_id || metadata.gid() != owner.group_id {
        return Err(NodeApiMountError::OwnerConflict {
            path: token_path.to_path_buf(),
            expected_user_id: owner.user_id,
            expected_group_id: owner.group_id,
            actual_user_id: metadata.uid(),
            actual_group_id: metadata.gid(),
        });
    }
    fs::set_permissions(token_path, fs::Permissions::from_mode(TOKEN_MODE))
        .map_err(|source| io_error("protect token", token_path, source))?;
    let encoded = Zeroizing::new(
        fs::read(token_path).map_err(|source| io_error("read token", token_path, source))?,
    );
    let decoded = Zeroizing::new(URL_SAFE_NO_PAD.decode(encoded.as_slice()).map_err(|_| {
        NodeApiMountError::InvalidCredential {
            path: token_path.to_path_buf(),
        }
    })?);
    if decoded.len() != TOKEN_LENGTH {
        return Err(NodeApiMountError::InvalidCredential {
            path: token_path.to_path_buf(),
        });
    }
    let mut bytes = [0_u8; TOKEN_LENGTH];
    bytes.copy_from_slice(decoded.as_slice());
    let token = WorkloadToken::from_bytes(bytes);
    bytes.zeroize();
    Ok(token)
}

fn zeroize_and_remove(path: &Path) -> Result<(), NodeApiMountError> {
    let length = fs::metadata(path)
        .map_err(|source| io_error("inspect credential for cleanup", path, source))?
        .len();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|source| io_error("make credential writable", path, source))?;
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| io_error("open credential for cleanup", path, source))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| io_error("seek credential for cleanup", path, source))?;
    let zeros = [0_u8; 4096];
    let mut remaining = length;
    while remaining > 0 {
        let count = usize::try_from(remaining.min(zeros.len() as u64)).unwrap_or(zeros.len());
        let chunk = zeros.get(..count).unwrap_or(&zeros);
        file.write_all(chunk)
            .map_err(|source| io_error("zero credential", path, source))?;
        remaining = remaining.saturating_sub(count as u64);
    }
    file.sync_all()
        .map_err(|source| io_error("sync credential", path, source))?;
    drop(file);
    fs::remove_file(path).map_err(|source| io_error("remove credential", path, source))
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> NodeApiMountError {
    NodeApiMountError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NodeApiMountError {
    #[error("node API root `{}` must be an absolute non-root path without parent traversal", path.display())]
    InvalidRoot { path: PathBuf },
    #[error("node API access is enabled but no agent services are configured")]
    ServicesUnavailable,
    #[error("node API path `{}` is not a safe file, directory, or socket", path.display())]
    UnsafePath { path: PathBuf },
    #[error("node API token at `{}` is malformed", path.display())]
    InvalidCredential { path: PathBuf },
    #[error("node API binding for workload `{workload_id}` changed without a new assignment")]
    BindingConflict { workload_id: WorkloadId },
    #[error(
        "node API token `{}` belongs to {actual_user_id}:{actual_group_id}, expected {expected_user_id}:{expected_group_id}",
        path.display()
    )]
    OwnerConflict {
        path: PathBuf,
        expected_user_id: u32,
        expected_group_id: u32,
        actual_user_id: u32,
        actual_group_id: u32,
    },
    #[error("operating-system randomness failed while minting a node API token: {message}")]
    Random { message: String },
    #[error("node API filesystem or server task failed: {message}")]
    Task { message: String },
    #[error(transparent)]
    Server(#[from] crate::NodeApiServerError),
    #[error("failed to {operation} at `{}`: {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
