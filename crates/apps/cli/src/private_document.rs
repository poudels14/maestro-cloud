use std::io::Write;
use std::path::Path;

use serde::Serialize;

use crate::CliError;

#[derive(Clone, Copy)]
pub(crate) enum Persisted {
    Created,
    Reused,
}

impl Persisted {
    pub(crate) fn verb(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Reused => "verified existing",
        }
    }
}

pub(crate) fn persist_private_exact(
    path: &Path,
    value: &impl Serialize,
    description: &str,
) -> Result<Persisted, CliError> {
    let encoded = encode_document(value, description)?;
    match read_private(path, description) {
        Ok(existing) if existing == encoded => Ok(Persisted::Reused),
        Ok(_) => Err(CliError::invalid_input(format!(
            "refusing to overwrite a different {description} `{}`",
            path.display()
        ))),
        Err(CliError::NotFound { .. }) => {
            persist_private_new_bytes(path, &encoded, description)?;
            Ok(Persisted::Created)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn persist_private_new(
    path: &Path,
    value: &impl Serialize,
    description: &str,
) -> Result<(), CliError> {
    persist_private_new_bytes(path, &encode_document(value, description)?, description)
}

pub(crate) fn replace_private(
    path: &Path,
    value: &impl Serialize,
    description: &str,
) -> Result<(), CliError> {
    read_private(path, description)?;
    let encoded = encode_document(value, description)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|source| {
        CliError::io(
            format!(
                "failed to create replacement {description} in `{}`",
                parent.display()
            ),
            source,
        )
    })?;
    temporary
        .write_all(&encoded)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| {
            CliError::io(
                format!("failed to persist {description} `{}`", path.display()),
                source,
            )
        })?;
    temporary.persist(path).map_err(|error| {
        CliError::io(
            format!("failed to replace {description} `{}`", path.display()),
            error.error,
        )
    })?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            CliError::io(
                format!(
                    "failed to sync {description} directory `{}`",
                    parent.display()
                ),
                source,
            )
        })
}

fn encode_document(value: &impl Serialize, description: &str) -> Result<Vec<u8>, CliError> {
    let mut encoded = serde_json::to_vec_pretty(value)
        .map_err(|source| CliError::json(format!("failed to encode {description}"), source))?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn persist_private_new_bytes(
    path: &Path,
    encoded: &[u8],
    description: &str,
) -> Result<(), CliError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|source| {
        CliError::io(
            format!(
                "failed to create {description} directory `{}`",
                parent.display()
            ),
            source,
        )
    })?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|source| {
        CliError::io(
            format!(
                "failed to create temporary {description} in `{}`",
                parent.display()
            ),
            source,
        )
    })?;
    temporary
        .write_all(encoded)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| {
            CliError::io(
                format!("failed to persist {description} `{}`", path.display()),
                source,
            )
        })?;
    temporary.persist_noclobber(path).map_err(|error| {
        CliError::io(
            format!("refusing to overwrite {description} `{}`", path.display()),
            error.error,
        )
    })?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            CliError::io(
                format!(
                    "failed to sync {description} directory `{}`",
                    parent.display()
                ),
                source,
            )
        })
}

pub(crate) fn read_private(path: &Path, description: &str) -> Result<Vec<u8>, CliError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CliError::not_found(format!(
                "{description} `{}`",
                path.display()
            )));
        }
        Err(source) => {
            return Err(CliError::io(
                format!("failed to inspect {description} `{}`", path.display()),
                source,
            ));
        }
    };
    if !metadata.file_type().is_file() {
        return Err(CliError::invalid_input(format!(
            "{description} `{}` must be a regular file",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(CliError::invalid_input(format!(
                "{description} `{}` has insecure permissions {mode:#o}",
                path.display()
            )));
        }
    }
    std::fs::read(path).map_err(|source| {
        CliError::io(
            format!("failed to read {description} `{}`", path.display()),
            source,
        )
    })
}
