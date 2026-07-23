use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
use zeroize::Zeroizing;

const MAXIMUM_SECRET_BYTES: usize = 64 * 1024;
const MAXIMUM_PEM_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy)]
enum InputPermissions {
    PublicAllowed,
    OwnerOnly,
}

pub(crate) fn read_snapshot(path: &Path) -> Result<Vec<u8>, CutoverFileError> {
    read_bounded(
        path,
        migrate::LegacySnapshot::maximum_artifact_bytes(),
        InputPermissions::OwnerOnly,
    )
}

pub(crate) fn read_telemetry_plan(path: &Path) -> Result<Vec<u8>, CutoverFileError> {
    read_bounded(
        path,
        migrate::LegacyTelemetryPlan::maximum_artifact_bytes(),
        InputPermissions::OwnerOnly,
    )
}

pub(crate) fn read_master_secret(path: &Path) -> Result<Zeroizing<String>, CutoverFileError> {
    let bytes = Zeroizing::new(read_bounded(
        path,
        MAXIMUM_SECRET_BYTES,
        InputPermissions::OwnerOnly,
    )?);
    let secret = String::from_utf8(bytes.to_vec()).map_err(|_| CutoverFileError::InvalidSecret)?;
    if secret.chars().count() < 32 || secret.contains('\0') {
        return Err(CutoverFileError::InvalidSecret);
    }
    Ok(Zeroizing::new(secret))
}

pub(crate) fn read_public_pem(path: &Path) -> Result<Vec<u8>, CutoverFileError> {
    read_bounded(path, MAXIMUM_PEM_BYTES, InputPermissions::PublicAllowed)
}

pub(crate) fn read_private_pem(path: &Path) -> Result<Vec<u8>, CutoverFileError> {
    read_bounded(path, MAXIMUM_PEM_BYTES, InputPermissions::OwnerOnly)
}

pub(crate) fn write_new_private(path: &Path, value: &[u8]) -> Result<(), CutoverFileError> {
    validate_absolute(path)?;
    let parent = path.parent().ok_or_else(|| CutoverFileError::InvalidPath {
        path: path.to_path_buf(),
    })?;
    let mut temporary =
        NamedTempFile::new_in(parent).map_err(|source| io("create", path, source))?;
    set_private(temporary.as_file(), path)?;
    temporary
        .write_all(value)
        .and_then(|_| temporary.as_file_mut().sync_all())
        .map_err(|source| io("write", path, source))?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| io("install", path, error.error))?;
    sync_parent(parent, path)
}

fn read_bounded(
    path: &Path,
    maximum: usize,
    permissions: InputPermissions,
) -> Result<Vec<u8>, CutoverFileError> {
    validate_absolute(path)?;
    let mut file = open_no_follow(path)?;
    let metadata = file
        .metadata()
        .map_err(|source| io("inspect", path, source))?;
    if !metadata.is_file() || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX) {
        return Err(CutoverFileError::InvalidInput {
            path: path.to_path_buf(),
            maximum,
        });
    }
    if matches!(permissions, InputPermissions::OwnerOnly) {
        validate_private(&metadata, path)?;
    }
    let limit = u64::try_from(maximum).unwrap_or(u64::MAX).saturating_add(1);
    let mut value = Vec::new();
    Read::by_ref(&mut file)
        .take(limit)
        .read_to_end(&mut value)
        .map_err(|source| io("read", path, source))?;
    if value.len() > maximum {
        return Err(CutoverFileError::InvalidInput {
            path: path.to_path_buf(),
            maximum,
        });
    }
    Ok(value)
}

fn open_no_follow(path: &Path) -> Result<File, CutoverFileError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options
        .open(path)
        .map_err(|source| io("open", path, source))
}

fn validate_absolute(path: &Path) -> Result<(), CutoverFileError> {
    if path.is_absolute()
        && path
            .components()
            .all(|component| !matches!(component, std::path::Component::ParentDir))
    {
        Ok(())
    } else {
        Err(CutoverFileError::InvalidPath {
            path: path.to_path_buf(),
        })
    }
}

#[cfg(unix)]
fn validate_private(metadata: &std::fs::Metadata, path: &Path) -> Result<(), CutoverFileError> {
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(CutoverFileError::PublicInput {
            path: path.to_path_buf(),
        })
    }
}

#[cfg(not(unix))]
fn validate_private(_metadata: &std::fs::Metadata, _path: &Path) -> Result<(), CutoverFileError> {
    Ok(())
}

#[cfg(unix)]
fn set_private(file: &File, path: &Path) -> Result<(), CutoverFileError> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|source| io("protect", path, source))
}

#[cfg(not(unix))]
fn set_private(_file: &File, _path: &Path) -> Result<(), CutoverFileError> {
    Ok(())
}

fn sync_parent(parent: &Path, output: &Path) -> Result<(), CutoverFileError> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io("sync parent of", output, source))
}

fn io(action: &'static str, path: &Path, source: std::io::Error) -> CutoverFileError {
    CutoverFileError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CutoverFileError {
    #[error("cutover path must be absolute without parent traversal: {}", path.display())]
    InvalidPath { path: PathBuf },
    #[error("cutover input `{}` is not a bounded regular file (maximum {maximum} bytes)", path.display())]
    InvalidInput { path: PathBuf, maximum: usize },
    #[error("secret-bearing cutover input `{}` must have owner-only permissions", path.display())]
    PublicInput { path: PathBuf },
    #[error("cutover master secret must be UTF-8, contain no NUL, and have at least 32 characters")]
    InvalidSecret,
    #[error("could not {action} cutover path `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
