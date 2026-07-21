use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::BuildSourceError;

pub(crate) enum WorkspaceState {
    Missing,
    Checkout,
    Incomplete,
}

pub(crate) fn validate_root(label: &str, path: &Path) -> Result<(), BuildSourceError> {
    let valid_components = path
        .components()
        .all(|component| matches!(component, Component::RootDir | Component::Normal(_)));
    if !path.is_absolute() || !valid_components || path.parent().is_none() {
        Err(BuildSourceError::rejected(format!(
            "{label} root must be a normalized absolute non-root path"
        )))
    } else {
        Ok(())
    }
}

pub(crate) async fn ensure_root(path: &Path) -> Result<PathBuf, BuildSourceError> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|error| io_unavailable("create build source root", path, error))?;
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|error| io_unavailable("inspect build source root", path, error))?;
    if !metadata.file_type().is_dir() {
        return Err(BuildSourceError::rejected(format!(
            "build source root `{}` must be a directory",
            path.display()
        )));
    }
    let canonical = tokio::fs::canonicalize(path)
        .await
        .map_err(|error| io_unavailable("canonicalize build source root", path, error))?;
    if canonical != path {
        return Err(BuildSourceError::rejected(format!(
            "build source root `{}` traverses a symbolic link",
            path.display()
        )));
    }
    Ok(canonical)
}

pub(crate) async fn validate_workspace(path: &Path) -> Result<WorkspaceState, BuildSourceError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() => {
            let git = path.join(".git");
            match tokio::fs::symlink_metadata(&git).await {
                Ok(metadata) if metadata.file_type().is_dir() => Ok(WorkspaceState::Checkout),
                Ok(_) => Err(BuildSourceError::rejected(format!(
                    "Git metadata `{}` must be a directory",
                    git.display()
                ))),
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(WorkspaceState::Incomplete),
                Err(error) => Err(io_unavailable("inspect Git metadata", &git, error)),
            }
        }
        Ok(_) => Err(BuildSourceError::rejected(format!(
            "build workspace `{}` must be a directory",
            path.display()
        ))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(WorkspaceState::Missing),
        Err(error) => Err(io_unavailable("inspect build workspace", path, error)),
    }
}

pub(crate) async fn validate_git_directory(workspace: &Path) -> Result<(), BuildSourceError> {
    let git = workspace.join(".git");
    let metadata = tokio::fs::symlink_metadata(&git)
        .await
        .map_err(|error| match error.kind() {
            ErrorKind::NotFound => BuildSourceError::rejected(format!(
                "build workspace `{}` is not a Git checkout",
                workspace.display()
            )),
            _ => io_unavailable("inspect Git metadata", &git, error),
        })?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(BuildSourceError::rejected(format!(
            "Git metadata `{}` must be a directory",
            git.display()
        )))
    }
}

pub(crate) async fn hash_file(path: &Path) -> Result<String, BuildSourceError> {
    Ok(format!(
        "sha256:{}",
        hex::encode(hash_file_digest(path).await?)
    ))
}

pub(crate) async fn hash_file_digest(path: &Path) -> Result<[u8; 32], BuildSourceError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| io_unavailable("open build archive", path, error))?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1_024];
    loop {
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|error| io_unavailable("read build archive", path, error))?;
        if count == 0 {
            break;
        }
        hash.update(buffer.get(..count).ok_or_else(|| {
            BuildSourceError::rejected("archive reader returned an invalid byte count")
        })?);
    }
    Ok(hash.finalize().into())
}

pub(crate) fn path_text(path: &Path) -> Result<&str, BuildSourceError> {
    path.to_str().ok_or_else(|| {
        BuildSourceError::rejected(format!(
            "build source path `{}` is not valid UTF-8",
            path.display()
        ))
    })
}

pub(crate) fn io_unavailable(
    operation: &str,
    path: &Path,
    error: std::io::Error,
) -> BuildSourceError {
    BuildSourceError::unavailable(format!("{operation} `{}`: {error}", path.display()))
}
