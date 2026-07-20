use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use kernel_api::WorkloadId;
use serde::{Deserialize, Serialize};
use supervisor::{ProcessHandle, SupervisorError};

use crate::{RuntimeError, WorkloadMetadata};

const MANIFEST_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ManifestState {
    Created,
    Starting,
    Running,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProcessManifest {
    version: u8,
    pub(crate) metadata: WorkloadMetadata,
    pub(crate) fingerprint: String,
    pub(crate) process: Option<ProcessHandle>,
    pub(crate) state: ManifestState,
    pub(crate) operation_generation: u64,
    pub(crate) stdout_path: PathBuf,
    pub(crate) stderr_path: PathBuf,
}

impl ProcessManifest {
    pub(crate) fn created(
        metadata: WorkloadMetadata,
        fingerprint: String,
        stdout_path: PathBuf,
        stderr_path: PathBuf,
    ) -> Self {
        Self {
            version: MANIFEST_VERSION,
            metadata,
            fingerprint,
            process: None,
            state: ManifestState::Created,
            operation_generation: 0,
            stdout_path,
            stderr_path,
        }
    }
}

pub(crate) fn commit_process_start(
    root: &Path,
    workload_id: &WorkloadId,
    operation_generation: u64,
    process: ProcessHandle,
) -> Result<(), ManifestError> {
    let mut manifest = load_manifest(root, workload_id)?;
    if manifest.state != ManifestState::Starting
        || manifest.operation_generation != operation_generation
    {
        return Err(ManifestError::message(
            "commit start",
            &manifest_path(root, workload_id),
            "start intent was superseded before process identity commit",
        ));
    }
    manifest.process = Some(process);
    manifest.state = ManifestState::Running;
    write_manifest(root, &manifest)
}

pub(crate) fn load_manifest(
    root: &Path,
    workload_id: &WorkloadId,
) -> Result<ProcessManifest, ManifestError> {
    load_optional_manifest(root, workload_id)?.ok_or_else(|| {
        ManifestError::message(
            "read",
            &manifest_path(root, workload_id),
            "manifest does not exist",
        )
    })
}

pub(crate) fn load_optional_manifest(
    root: &Path,
    workload_id: &WorkloadId,
) -> Result<Option<ProcessManifest>, ManifestError> {
    let path = manifest_path(root, workload_id);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ManifestError::io("read", &path, error)),
    };
    let manifest: ProcessManifest = serde_json::from_slice(&bytes)
        .map_err(|error| ManifestError::message("decode", &path, error))?;
    validate_manifest(workload_id, &manifest, &path)?;
    Ok(Some(manifest))
}

pub(crate) fn load_manifests(root: &Path) -> Result<Vec<ProcessManifest>, ManifestError> {
    let workloads = root.join("workloads");
    let entries = match fs::read_dir(&workloads) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(ManifestError::io("list", &workloads, error)),
    };
    let mut manifests = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| ManifestError::io("list", &workloads, error))?;
        if !entry
            .file_type()
            .map_err(|error| ManifestError::io("inspect", &entry.path(), error))?
            .is_dir()
        {
            continue;
        }
        let workload_id = WorkloadId::new(entry.file_name().to_string_lossy().into_owned())
            .map_err(|error| ManifestError::message("validate identity", &entry.path(), error))?;
        manifests.push(load_manifest(root, &workload_id)?);
    }
    manifests.sort_by(|left, right| left.metadata.workload_id.cmp(&right.metadata.workload_id));
    Ok(manifests)
}

pub(crate) fn write_manifest(root: &Path, manifest: &ProcessManifest) -> Result<(), ManifestError> {
    let directory = workload_directory(root, &manifest.metadata.workload_id);
    fs::create_dir_all(&directory)
        .map_err(|error| ManifestError::io("create directory", &directory, error))?;
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
        .map_err(|error| ManifestError::io("protect directory", &directory, error))?;
    let path = directory.join("manifest.json");
    let temporary = directory.join("manifest.json.tmp");
    let bytes = serde_json::to_vec(manifest)
        .map_err(|error| ManifestError::message("encode", &path, error))?;
    let mut file = private_file(&temporary)?;
    file.write_all(&bytes)
        .map_err(|error| ManifestError::io("write", &temporary, error))?;
    file.sync_all()
        .map_err(|error| ManifestError::io("sync", &temporary, error))?;
    fs::rename(&temporary, &path).map_err(|error| ManifestError::io("commit", &path, error))?;
    Ok(())
}

pub(crate) fn remove_manifest(root: &Path, workload_id: &WorkloadId) -> Result<(), ManifestError> {
    let directory = workload_directory(root, workload_id);
    match fs::remove_dir_all(&directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ManifestError::io("remove", &directory, error)),
    }
}

pub(crate) fn workload_directory(root: &Path, workload_id: &WorkloadId) -> PathBuf {
    root.join("workloads").join(workload_id.as_str())
}

#[derive(Debug)]
pub(crate) struct ManifestError {
    operation: &'static str,
    path: PathBuf,
    message: String,
}

impl ManifestError {
    fn io(operation: &'static str, path: &Path, error: std::io::Error) -> Self {
        Self {
            operation,
            path: path.to_path_buf(),
            message: error.to_string(),
        }
    }

    fn message(operation: &'static str, path: &Path, error: impl std::fmt::Display) -> Self {
        Self {
            operation,
            path: path.to_path_buf(),
            message: error.to_string(),
        }
    }

    pub(crate) fn into_runtime(self) -> RuntimeError {
        RuntimeError::Rejected {
            message: format!(
                "process manifest {} failed for `{}`: {}",
                self.operation,
                self.path.display(),
                self.message
            ),
        }
    }

    pub(crate) fn into_supervisor(self) -> SupervisorError {
        SupervisorError::Io {
            operation: self.operation,
            path: self.path,
            message: self.message,
        }
    }
}

fn manifest_path(root: &Path, workload_id: &WorkloadId) -> PathBuf {
    workload_directory(root, workload_id).join("manifest.json")
}

fn validate_manifest(
    workload_id: &WorkloadId,
    manifest: &ProcessManifest,
    path: &Path,
) -> Result<(), ManifestError> {
    if manifest.version != MANIFEST_VERSION {
        Err(ManifestError::message(
            "validate version",
            path,
            format!("unsupported version {}", manifest.version),
        ))
    } else if &manifest.metadata.workload_id != workload_id {
        Err(ManifestError::message(
            "validate identity",
            path,
            "directory and manifest workload identities differ",
        ))
    } else {
        Ok(())
    }
}

fn private_file(path: &Path) -> Result<File, ManifestError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| ManifestError::io("open", path, error))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| ManifestError::io("protect", path, error))?;
    Ok(file)
}
