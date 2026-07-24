use std::ffi::OsStr;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use flate2::read::GzDecoder;
use runtime::{ArtifactSource, ArtifactStoreError};

pub(crate) struct DepotBuildContext {
    pub(crate) root: PathBuf,
    pub(crate) definition: String,
}

pub(crate) async fn prepare_context(
    source: &ArtifactSource,
    workspace: &Path,
    max_bytes: u64,
    max_entries: usize,
) -> Result<DepotBuildContext, ArtifactStoreError> {
    match source {
        ArtifactSource::Directory { root, definition } => {
            validate_directory(root, definition).await
        }
        ArtifactSource::Archive { path, definition } => {
            validate_archive(path).await?;
            let root = workspace.join("context");
            tokio::fs::create_dir(&root).await.map_err(|error| {
                rejected_io("create extracted Depot build context", &root, error)
            })?;
            let archive = path.clone();
            let destination = root.clone();
            tokio::task::spawn_blocking(move || {
                extract_archive(&archive, &destination, max_bytes, max_entries)
            })
            .await
            .map_err(|error| ArtifactStoreError::Unavailable {
                message: format!("Depot archive extraction stopped unexpectedly: {error}"),
            })??;
            validate_directory(&root, definition).await
        }
    }
}

fn definition_text(definition: &Path) -> Result<String, ArtifactStoreError> {
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
        return Err(rejected(
            "Depot definition must be a normalized relative path outside `.git`",
        ));
    }
    definition
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| rejected("Depot definition must be valid UTF-8"))
}

async fn validate_directory(
    root: &Path,
    definition: &Path,
) -> Result<DepotBuildContext, ArtifactStoreError> {
    validate_absolute(root, "build context")?;
    let metadata = tokio::fs::symlink_metadata(root)
        .await
        .map_err(|error| rejected_io("inspect Depot context", root, error))?;
    if !metadata.file_type().is_dir() {
        return Err(rejected(format!(
            "Depot context `{}` must be a directory",
            root.display()
        )));
    }
    let canonical = tokio::fs::canonicalize(root)
        .await
        .map_err(|error| rejected_io("resolve Depot context", root, error))?;
    if canonical != root {
        return Err(rejected(format!(
            "Depot context `{}` traverses a symbolic link or non-normal path",
            root.display()
        )));
    }
    let definition = definition_text(definition)?;
    let definition_path = root.join(&definition);
    let resolved = tokio::fs::canonicalize(&definition_path)
        .await
        .map_err(|error| rejected_io("resolve Depot definition", &definition_path, error))?;
    let metadata = tokio::fs::metadata(&resolved)
        .await
        .map_err(|error| rejected_io("inspect Depot definition", &resolved, error))?;
    if !resolved.starts_with(root) || !metadata.is_file() {
        return Err(rejected(format!(
            "Depot definition `{definition}` must resolve to a file inside the context"
        )));
    }
    Ok(DepotBuildContext {
        root: canonical,
        definition,
    })
}

async fn validate_archive(path: &Path) -> Result<(), ArtifactStoreError> {
    validate_absolute(path, "build archive")?;
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|error| rejected_io("inspect Depot archive", path, error))?;
    if !metadata.file_type().is_file() {
        return Err(rejected(format!(
            "Depot archive `{}` must be a regular file",
            path.display()
        )));
    }
    let canonical = tokio::fs::canonicalize(path)
        .await
        .map_err(|error| rejected_io("resolve Depot archive", path, error))?;
    if canonical != path {
        return Err(rejected(format!(
            "Depot archive `{}` traverses a symbolic link or non-normal path",
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
) -> Result<(), ArtifactStoreError> {
    let file = std::fs::File::open(archive_path)
        .map_err(|error| rejected_io("open Depot archive", archive_path, error))?;
    let mut reader = BufReader::new(file);
    let mut magic = [0_u8; 2];
    let read = reader
        .read(&mut magic)
        .map_err(|error| rejected_io("read Depot archive", archive_path, error))?;
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|error| rejected_io("rewind Depot archive", archive_path, error))?;
    let input: Box<dyn Read> = if read == magic.len() && magic == [0x1f, 0x8b] {
        Box::new(GzDecoder::new(reader))
    } else {
        Box::new(reader)
    };
    let mut archive = tar::Archive::new(input);
    let entries = archive
        .entries()
        .map_err(|error| rejected_archive(archive_path, error))?;
    let mut expanded_bytes = 0_u64;
    for (index, entry) in entries.enumerate() {
        if index >= max_entries {
            return Err(rejected(format!(
                "Depot archive exceeds the {max_entries} entry limit"
            )));
        }
        let mut entry = entry.map_err(|error| rejected_archive(archive_path, error))?;
        let path = entry
            .path()
            .map_err(|error| rejected_archive(archive_path, error))?
            .into_owned();
        validate_entry_path(&path)?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() {
            let target = entry
                .link_name()
                .map_err(|error| rejected_archive(archive_path, error))?
                .ok_or_else(|| {
                    rejected(format!(
                        "Depot archive link `{}` has no target",
                        path.display()
                    ))
                })?;
            validate_link_target(&path, &target)?;
        } else if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(rejected(format!(
                "Depot archive entry `{}` must be a file, directory, or safe symbolic link",
                path.display()
            )));
        }
        expanded_bytes = expanded_bytes
            .checked_add(entry.size())
            .filter(|bytes| *bytes <= max_bytes)
            .ok_or_else(|| {
                rejected(format!(
                    "Depot archive exceeds the {max_bytes} byte expanded-size limit"
                ))
            })?;
        entry.set_mask(0o7000);
        let unpacked = entry
            .unpack_in(destination)
            .map_err(|error| rejected_archive(archive_path, error))?;
        if !unpacked {
            return Err(rejected(format!(
                "Depot archive entry `{}` escapes the context",
                path.display()
            )));
        }
    }
    Ok(())
}

fn validate_entry_path(path: &Path) -> Result<(), ArtifactStoreError> {
    if path.as_os_str().is_empty()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
    {
        Err(rejected(format!(
            "Depot archive entry `{}` is not a normalized relative path",
            path.display()
        )))
    } else {
        Ok(())
    }
}

fn validate_link_target(path: &Path, target: &Path) -> Result<(), ArtifactStoreError> {
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
                    "Depot archive link `{}` escapes the context",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn validate_absolute(path: &Path, kind: &str) -> Result<(), ArtifactStoreError> {
    if !path.is_absolute() || path.to_str().is_none() {
        Err(rejected(format!(
            "Depot {kind} `{}` must be an absolute UTF-8 path",
            path.display()
        )))
    } else {
        Ok(())
    }
}

fn rejected_archive(path: &Path, error: std::io::Error) -> ArtifactStoreError {
    rejected(format!("read Depot archive `{}`: {error}", path.display()))
}

fn rejected_io(operation: &str, path: &Path, error: std::io::Error) -> ArtifactStoreError {
    rejected(format!("{operation} `{}`: {error}", path.display()))
}

fn rejected(message: impl Into<String>) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: message.into(),
    }
}
