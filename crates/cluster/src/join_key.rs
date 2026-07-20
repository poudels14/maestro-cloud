use std::io::Write;
use std::path::{Path, PathBuf};

use crate::JoinPrivateKey;

/// Loads a persisted join key or creates it with owner-only permissions.
pub fn load_or_create_join_key(path: &Path) -> Result<JoinPrivateKey, JoinKeyError> {
    match std::fs::read_to_string(path) {
        Ok(encoded) => {
            validate_private_permissions(path)?;
            decode_key(encoded.trim())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create_join_key(path),
        Err(source) => Err(JoinKeyError::Io {
            action: "read",
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Why protected local join-key persistence failed.
#[derive(Debug, thiserror::Error)]
pub enum JoinKeyError {
    /// The requested path had no usable parent directory.
    #[error("join key path `{}` has no parent directory", path.display())]
    InvalidPath { path: PathBuf },
    /// Persisted text was not hexadecimal.
    #[error("persisted join key is not valid hexadecimal")]
    InvalidEncoding,
    /// Persisted key material did not contain exactly 32 bytes.
    #[error("persisted join key must contain 32 bytes, found {observed}")]
    InvalidLength { observed: usize },
    /// A persisted key was readable by users other than its owner.
    #[error("join key `{}` has insecure permissions {mode:#o}", path.display())]
    InsecurePermissions { path: PathBuf, mode: u32 },
    /// Filesystem work failed at the private-key persistence boundary.
    #[error("failed to {action} join key `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn create_join_key(path: &Path) -> Result<JoinPrivateKey, JoinKeyError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| JoinKeyError::InvalidPath {
            path: path.to_path_buf(),
        })?;
    std::fs::create_dir_all(parent).map_err(|source| JoinKeyError::Io {
        action: "create parent directory for",
        path: path.to_path_buf(),
        source,
    })?;

    let private_key = JoinPrivateKey::generate();
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return load_or_create_join_key(path);
        }
        Err(source) => {
            return Err(JoinKeyError::Io {
                action: "create",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    file.write_all(hex::encode(private_key.secret().to_bytes()).as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| JoinKeyError::Io {
            action: "persist",
            path: path.to_path_buf(),
            source,
        })?;
    let directory = std::fs::File::open(parent).map_err(|source| JoinKeyError::Io {
        action: "open parent directory for sync of",
        path: path.to_path_buf(),
        source,
    })?;
    directory.sync_all().map_err(|source| JoinKeyError::Io {
        action: "sync parent directory for",
        path: path.to_path_buf(),
        source,
    })?;
    Ok(private_key)
}

fn decode_key(encoded: &str) -> Result<JoinPrivateKey, JoinKeyError> {
    let decoded = hex::decode(encoded).map_err(|_| JoinKeyError::InvalidEncoding)?;
    let observed = decoded.len();
    let key = decoded
        .try_into()
        .map_err(|_| JoinKeyError::InvalidLength { observed })?;
    Ok(JoinPrivateKey::from_bytes(key))
}

fn validate_private_permissions(path: &Path) -> Result<(), JoinKeyError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(path)
            .map_err(|source| JoinKeyError::Io {
                action: "inspect permissions of",
                path: path.to_path_buf(),
                source,
            })?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(JoinKeyError::InsecurePermissions {
                path: path.to_path_buf(),
                mode,
            });
        }
    }
    Ok(())
}
