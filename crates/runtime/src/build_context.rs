use std::ffi::OsStr;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use flate2::read::GzDecoder;

use crate::{ArtifactSource, ArtifactStoreError};

/// A validated local build context and its relative build-definition path.
#[derive(Debug)]
pub struct BuildContext {
    /// Canonical context directory that can be handed to a build backend.
    pub root: PathBuf,
    /// Normalized UTF-8 definition path relative to `root`.
    pub definition: String,
}

/// Validates or safely extracts an artifact source for a named build backend.
pub async fn prepare_context(
    source: &ArtifactSource,
    workspace: &Path,
    max_bytes: u64,
    max_entries: usize,
    backend: &'static str,
) -> Result<BuildContext, ArtifactStoreError> {
    match source {
        ArtifactSource::Directory { root, definition } => {
            validate_directory(root, definition, backend).await
        }
        ArtifactSource::Archive { path, definition } => {
            validate_archive(path, backend).await?;
            let root = workspace.join("context");
            tokio::fs::create_dir(&root).await.map_err(|error| {
                rejected_io(
                    &format!("create extracted {backend} build context"),
                    &root,
                    error,
                )
            })?;
            let archive = path.clone();
            let destination = root.clone();
            tokio::task::spawn_blocking(move || {
                extract_archive(&archive, &destination, max_bytes, max_entries, backend)
            })
            .await
            .map_err(|error| ArtifactStoreError::Unavailable {
                message: format!("{backend} archive extraction stopped unexpectedly: {error}"),
            })??;
            validate_directory(&root, definition, backend).await
        }
    }
}

fn definition_text(definition: &Path, backend: &str) -> Result<String, ArtifactStoreError> {
    if definition.as_os_str().is_empty() {
        return Ok("Dockerfile".to_owned());
    }
    if definition.is_absolute()
        || !definition
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        || definition
            .components()
            .any(|component| component.as_os_str() == OsStr::new(".git"))
    {
        return Err(rejected(format!(
            "{backend} definition must be a normalized relative path outside `.git`"
        )));
    }
    definition
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| rejected(format!("{backend} definition must be valid UTF-8")))
}

async fn validate_directory(
    root: &Path,
    definition: &Path,
    backend: &str,
) -> Result<BuildContext, ArtifactStoreError> {
    validate_absolute(root, "build context", backend)?;
    let metadata = tokio::fs::symlink_metadata(root)
        .await
        .map_err(|error| rejected_io(&format!("inspect {backend} context"), root, error))?;
    if !metadata.file_type().is_dir() {
        return Err(rejected(format!(
            "{backend} context `{}` must be a directory",
            root.display()
        )));
    }
    let canonical = tokio::fs::canonicalize(root)
        .await
        .map_err(|error| rejected_io(&format!("resolve {backend} context"), root, error))?;
    if canonical != root {
        return Err(rejected(format!(
            "{backend} context `{}` traverses a symbolic link or non-normal path",
            root.display()
        )));
    }
    let definition = definition_text(definition, backend)?;
    let definition_path = root.join(&definition);
    let resolved = tokio::fs::canonicalize(&definition_path)
        .await
        .map_err(|error| {
            rejected_io(
                &format!("resolve {backend} definition"),
                &definition_path,
                error,
            )
        })?;
    let metadata = tokio::fs::metadata(&resolved)
        .await
        .map_err(|error| rejected_io(&format!("inspect {backend} definition"), &resolved, error))?;
    if !resolved.starts_with(root) || !metadata.is_file() {
        return Err(rejected(format!(
            "{backend} definition `{definition}` must resolve to a file inside the context"
        )));
    }
    Ok(BuildContext {
        root: canonical,
        definition,
    })
}

async fn validate_archive(path: &Path, backend: &str) -> Result<(), ArtifactStoreError> {
    validate_absolute(path, "build archive", backend)?;
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|error| rejected_io(&format!("inspect {backend} archive"), path, error))?;
    if !metadata.file_type().is_file() {
        return Err(rejected(format!(
            "{backend} archive `{}` must be a regular file",
            path.display()
        )));
    }
    let canonical = tokio::fs::canonicalize(path)
        .await
        .map_err(|error| rejected_io(&format!("resolve {backend} archive"), path, error))?;
    if canonical != path {
        return Err(rejected(format!(
            "{backend} archive `{}` traverses a symbolic link or non-normal path",
            path.display()
        )));
    }
    Ok(())
}

fn extract_archive(
    archive_path: &Path,
    destination: &Path,
    max_bytes: u64,
    max_entries: usize,
    backend: &str,
) -> Result<(), ArtifactStoreError> {
    let file = std::fs::File::open(archive_path)
        .map_err(|error| rejected_io(&format!("open {backend} archive"), archive_path, error))?;
    let mut reader = BufReader::new(file);
    let mut magic = [0_u8; 2];
    let read = reader
        .read(&mut magic)
        .map_err(|error| rejected_io(&format!("read {backend} archive"), archive_path, error))?;
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|error| rejected_io(&format!("rewind {backend} archive"), archive_path, error))?;
    let input: Box<dyn Read> = if read == magic.len() && magic == [0x1f, 0x8b] {
        Box::new(GzDecoder::new(reader))
    } else {
        Box::new(reader)
    };
    let mut archive = tar::Archive::new(input);
    let entries = archive
        .entries()
        .map_err(|error| rejected_archive(archive_path, error, backend))?;
    let mut expanded_bytes = 0_u64;
    for (index, entry) in entries.enumerate() {
        if index >= max_entries {
            return Err(rejected(format!(
                "{backend} archive exceeds the {max_entries} entry limit"
            )));
        }
        let mut entry = entry.map_err(|error| rejected_archive(archive_path, error, backend))?;
        let path = entry
            .path()
            .map_err(|error| rejected_archive(archive_path, error, backend))?
            .into_owned();
        validate_entry_path(&path, backend)?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() {
            let target = entry
                .link_name()
                .map_err(|error| rejected_archive(archive_path, error, backend))?
                .ok_or_else(|| {
                    rejected(format!(
                        "{backend} archive link `{}` has no target",
                        path.display()
                    ))
                })?;
            validate_link_target(&path, &target, backend)?;
        } else if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(rejected(format!(
                "{backend} archive entry `{}` must be a file, directory, or safe symbolic link",
                path.display()
            )));
        }
        expanded_bytes = expanded_bytes
            .checked_add(entry.size())
            .filter(|bytes| *bytes <= max_bytes)
            .ok_or_else(|| {
                rejected(format!(
                    "{backend} archive exceeds the {max_bytes} byte expanded-size limit"
                ))
            })?;
        entry.set_mask(0o7000);
        let unpacked = entry
            .unpack_in(destination)
            .map_err(|error| rejected_archive(archive_path, error, backend))?;
        if !unpacked {
            return Err(rejected(format!(
                "{backend} archive entry `{}` escapes the context",
                path.display()
            )));
        }
    }
    Ok(())
}

fn validate_entry_path(path: &Path, backend: &str) -> Result<(), ArtifactStoreError> {
    if path.as_os_str().is_empty()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
    {
        Err(rejected(format!(
            "{backend} archive entry `{}` is not a normalized relative path",
            path.display()
        )))
    } else {
        Ok(())
    }
}

fn validate_link_target(
    path: &Path,
    target: &Path,
    backend: &str,
) -> Result<(), ArtifactStoreError> {
    let parent_depth = path
        .parent()
        .map_or(0, |parent| parent.components().count());
    let mut depth = parent_depth;
    for component in target.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir if depth > 0 => depth -= 1,
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(rejected(format!(
                    "{backend} archive link `{}` escapes the context",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn validate_absolute(path: &Path, kind: &str, backend: &str) -> Result<(), ArtifactStoreError> {
    if !path.is_absolute() || path.to_str().is_none() {
        Err(rejected(format!(
            "{backend} {kind} `{}` must be an absolute UTF-8 path",
            path.display()
        )))
    } else {
        Ok(())
    }
}

fn rejected_archive(path: &Path, error: std::io::Error, backend: &str) -> ArtifactStoreError {
    rejected(format!(
        "read {backend} archive `{}`: {error}",
        path.display()
    ))
}

fn rejected_io(operation: &str, path: &Path, error: std::io::Error) -> ArtifactStoreError {
    rejected(format!("{operation} `{}`: {error}", path.display()))
}

fn rejected(message: impl Into<String>) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: message.into(),
    }
}
