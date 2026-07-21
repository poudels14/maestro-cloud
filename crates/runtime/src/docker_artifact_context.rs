use std::io::Write;
use std::path::{Component, Path, PathBuf};

use ignore::WalkBuilder;
use tokio::task::JoinHandle;
use tokio_util::io::{ReaderStream, SyncIoBridge};

use crate::ArtifactStoreError;

const CONTEXT_STREAM_CAPACITY: usize = 64 * 1_024;

pub(crate) fn stream_directory_archive(
    root: PathBuf,
    definition: PathBuf,
) -> (
    ReaderStream<tokio::io::DuplexStream>,
    JoinHandle<Result<(), ArtifactStoreError>>,
) {
    let (reader, writer) = tokio::io::duplex(CONTEXT_STREAM_CAPACITY);
    let task = tokio::task::spawn_blocking(move || {
        write_directory_archive(&root, &definition, SyncIoBridge::new(writer))
    });
    (ReaderStream::new(reader), task)
}

pub(crate) fn file_stream(file: tokio::fs::File) -> ReaderStream<tokio::fs::File> {
    ReaderStream::new(file)
}

pub(crate) async fn finish_archive_task(
    task: JoinHandle<Result<(), ArtifactStoreError>>,
) -> Result<(), ArtifactStoreError> {
    task.await
        .map_err(|error| ArtifactStoreError::Unavailable {
            message: format!("Docker build-context task stopped unexpectedly: {error}"),
        })?
}

pub(crate) fn write_directory_archive(
    root: &Path,
    definition: &Path,
    writer: impl Write,
) -> Result<(), ArtifactStoreError> {
    let absolute =
        std::fs::canonicalize(root).map_err(|error| rejected_io("resolve", root, error))?;
    let metadata =
        std::fs::metadata(&absolute).map_err(|error| rejected_io("inspect", &absolute, error))?;
    if !metadata.is_dir() {
        return Err(rejected(format!(
            "Docker build context `{}` must be a directory",
            root.display()
        )));
    }

    let mut walker = WalkBuilder::new(&absolute);
    walker
        .hidden(false)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(false)
        .add_custom_ignore_filename(".dockerignore");

    let mut archive = tar::Builder::new(writer);
    archive.follow_symlinks(false);
    let mut definition_archived = false;
    for result in walker.build() {
        let entry =
            result.map_err(|error| rejected(format!("walk Docker build context: {error}")))?;
        let path = entry.path();
        if path == absolute {
            continue;
        }
        let relative = path.strip_prefix(&absolute).map_err(|error| {
            rejected(format!(
                "derive Docker build-context path for `{}`: {error}",
                path.display()
            ))
        })?;
        if relative
            .components()
            .any(|component| component.as_os_str() == ".git")
        {
            continue;
        }
        append_entry(&mut archive, relative, path, entry.file_type())?;
        definition_archived |= relative == definition;
    }
    if !definition_archived {
        let path = absolute.join(definition);
        let file_type = std::fs::symlink_metadata(&path)
            .map_err(|error| rejected_io("inspect build definition", &path, error))?
            .file_type();
        append_entry(&mut archive, definition, &path, Some(file_type))?;
    }
    archive
        .finish()
        .map_err(|error| rejected_io("finish Docker build context", &absolute, error))
}

fn append_entry(
    archive: &mut tar::Builder<impl Write>,
    relative: &Path,
    path: &Path,
    file_type: Option<std::fs::FileType>,
) -> Result<(), ArtifactStoreError> {
    let file_type = file_type.ok_or_else(|| {
        rejected(format!(
            "Docker build-context entry `{}` has no file type",
            path.display()
        ))
    })?;
    if file_type.is_dir() {
        archive
            .append_dir(relative, path)
            .map_err(|error| rejected_io("archive directory", path, error))
    } else if file_type.is_file() {
        let mut file = std::fs::File::open(path)
            .map_err(|error| rejected_io("open context file", path, error))?;
        archive
            .append_file(relative, &mut file)
            .map_err(|error| rejected_io("archive context file", path, error))
    } else if file_type.is_symlink() {
        append_link(archive, relative, path)
    } else {
        Err(rejected(format!(
            "Docker build-context entry `{}` is not a file, directory, or symbolic link",
            path.display()
        )))
    }
}

fn append_link(
    archive: &mut tar::Builder<impl Write>,
    relative: &Path,
    path: &Path,
) -> Result<(), ArtifactStoreError> {
    let target =
        std::fs::read_link(path).map_err(|error| rejected_io("read context link", path, error))?;
    validate_link_target(relative, &target)?;
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_size(0);
    header.set_mode(0o777);
    archive
        .append_link(&mut header, relative, target)
        .map_err(|error| rejected_io("archive context link", path, error))
}

fn validate_link_target(relative: &Path, target: &Path) -> Result<(), ArtifactStoreError> {
    let parent_depth = relative
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
                    "Docker build-context link `{}` escapes the context",
                    relative.display()
                )));
            }
        }
    }
    Ok(())
}

fn rejected_io(operation: &str, path: &Path, error: std::io::Error) -> ArtifactStoreError {
    rejected(format!("{operation} `{}`: {error}", path.display()))
}

fn rejected(message: impl Into<String>) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: message.into(),
    }
}
