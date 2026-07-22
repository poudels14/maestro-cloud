use std::io::Write;
use std::path::{Component, Path};

use flate2::{Compression, GzBuilder};
use ignore::WalkBuilder;
use kernel_api::MAXIMUM_ARTIFACT_ARCHIVE_BYTES;

use crate::CliError;

pub(crate) fn pack_context(context: &Path) -> Result<Vec<u8>, CliError> {
    let metadata = std::fs::symlink_metadata(context).map_err(|source| {
        CliError::io(
            format!("failed to inspect build context `{}`", context.display()),
            source,
        )
    })?;
    if !metadata.file_type().is_dir() {
        return Err(CliError::invalid_input(format!(
            "build context `{}` must be a directory, not a link or file",
            context.display()
        )));
    }
    let root = std::fs::canonicalize(context).map_err(|source| {
        CliError::io(
            format!("failed to resolve build context `{}`", context.display()),
            source,
        )
    })?;
    let writer = LimitedWriter::default();
    let encoder = GzBuilder::new()
        .mtime(0)
        .operating_system(255)
        .write(writer, Compression::default());
    let mut archive = tar::Builder::new(encoder);
    archive.follow_symlinks(false);
    let mut walker = WalkBuilder::new(&root);
    walker
        .hidden(false)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(true)
        .add_custom_ignore_filename(".dockerignore")
        .sort_by_file_path(|left, right| left.cmp(right));
    for entry in walker.build() {
        let entry = entry.map_err(|error| {
            CliError::invalid_input(format!("failed to walk build context: {error}"))
        })?;
        let path = entry.path();
        if path == root {
            continue;
        }
        let relative = path.strip_prefix(&root).map_err(|_| {
            CliError::invalid_input(format!(
                "build context entry `{}` escaped its root",
                path.display()
            ))
        })?;
        if relative
            .components()
            .any(|component| component == Component::Normal(".git".as_ref()))
        {
            continue;
        }
        append_entry(&mut archive, path, relative)?;
    }
    let encoder = archive.into_inner().map_err(archive_error)?;
    encoder
        .finish()
        .map(LimitedWriter::into_bytes)
        .map_err(archive_error)
}

fn append_entry(
    archive: &mut tar::Builder<flate2::write::GzEncoder<LimitedWriter>>,
    path: &Path,
    relative: &Path,
) -> Result<(), CliError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| {
        CliError::io(
            format!("failed to inspect context entry `{}`", path.display()),
            source,
        )
    })?;
    if metadata.file_type().is_dir() {
        let mut header = header(tar::EntryType::Directory, 0, 0o755);
        archive
            .append_data(&mut header, relative, &mut std::io::empty())
            .map_err(archive_error)
    } else if metadata.file_type().is_file() {
        let mut file = std::fs::File::open(path).map_err(|source| {
            CliError::io(
                format!("failed to open context entry `{}`", path.display()),
                source,
            )
        })?;
        let size = file
            .metadata()
            .map_err(|source| {
                CliError::io(
                    format!("failed to inspect open context entry `{}`", path.display()),
                    source,
                )
            })?
            .len();
        let mut header = header(tar::EntryType::Regular, size, file_mode(&metadata));
        archive
            .append_data(&mut header, relative, &mut file)
            .map_err(archive_error)
    } else {
        Err(CliError::invalid_input(format!(
            "build context entry `{}` must be a regular file or directory",
            path.display()
        )))
    }
}

fn header(entry_type: tar::EntryType, size: u64, mode: u32) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(entry_type);
    header.set_size(size);
    header.set_mode(mode);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    header
}

fn file_mode(metadata: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o111 == 0 {
            0o644
        } else {
            0o755
        }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0o644
    }
}

fn archive_error(source: std::io::Error) -> CliError {
    if source.kind() == std::io::ErrorKind::FileTooLarge {
        CliError::invalid_input(format!(
            "compressed build context exceeds {MAXIMUM_ARTIFACT_ARCHIVE_BYTES} bytes"
        ))
    } else {
        CliError::io("failed to package build context", source)
    }
}

#[derive(Default)]
struct LimitedWriter {
    bytes: Vec<u8>,
}

impl LimitedWriter {
    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for LimitedWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(buffer.len()) > MAXIMUM_ARTIFACT_ARCHIVE_BYTES {
            return Err(std::io::Error::from(std::io::ErrorKind::FileTooLarge));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
