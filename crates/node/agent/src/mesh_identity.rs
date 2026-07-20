use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use x25519_dalek::{PublicKey, StaticSecret};

const WIREGUARD_KEY_FILE: &str = "wireguard.key";

/// A WireGuard public key validated as exactly 32 bytes.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WireGuardPublicKey([u8; 32]);

impl WireGuardPublicKey {
    /// Returns the raw public key required by a kernel mesh backend.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Debug for WireGuardPublicKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("WireGuardPublicKey")
            .field(&self.to_string())
            .finish()
    }
}

impl Display for WireGuardPublicKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&BASE64.encode(self.0))
    }
}

impl FromStr for WireGuardPublicKey {
    type Err = MeshIdentityError;

    fn from_str(encoded: &str) -> Result<Self, Self::Err> {
        let bytes = decode_key(encoded)?;
        if bytes.iter().all(|byte| *byte == 0) {
            Err(MeshIdentityError::InvalidPublicKeyMaterial)
        } else {
            Ok(Self(bytes))
        }
    }
}

/// A zeroized WireGuard private key whose debug representation is redacted.
#[derive(Clone)]
pub struct WireGuardPrivateKey(StaticSecret);

impl WireGuardPrivateKey {
    /// Generates a new key from operating-system cryptographic randomness.
    pub fn generate() -> Self {
        Self(StaticSecret::random())
    }

    /// Returns the public half safe to publish in a `NodeNetwork` resource.
    pub fn public_key(&self) -> WireGuardPublicKey {
        WireGuardPublicKey(*PublicKey::from(&self.0).as_bytes())
    }

    /// Exposes private bytes only to the persistence and kernel boundaries.
    pub fn expose_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(StaticSecret::from(bytes))
    }
}

impl Debug for WireGuardPrivateKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WireGuardPrivateKey([REDACTED])")
    }
}

impl PartialEq for WireGuardPrivateKey {
    fn eq(&self, other: &Self) -> bool {
        self.expose_bytes() == other.expose_bytes()
    }
}

impl Eq for WireGuardPrivateKey {}

/// Stable node-local WireGuard identity loaded from protected storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshIdentity {
    private_key: WireGuardPrivateKey,
}

impl MeshIdentity {
    /// Loads a persisted identity or creates it without overwriting a winner.
    pub fn load_or_generate(data_directory: &Path) -> Result<Self, MeshIdentityError> {
        let path = data_directory.join(WIREGUARD_KEY_FILE);
        match std::fs::read_to_string(&path) {
            Ok(encoded) => {
                validate_private_permissions(&path)?;
                decode_key(encoded.trim()).map(|bytes| Self {
                    private_key: WireGuardPrivateKey::from_bytes(bytes),
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_identity(data_directory, &path)
            }
            Err(source) => Err(MeshIdentityError::Io {
                action: "read",
                path,
                source,
            }),
        }
    }

    /// Returns the publishable public key for this node.
    pub fn public_key(&self) -> WireGuardPublicKey {
        self.private_key.public_key()
    }

    /// Returns the protected private key used in desired backend state.
    pub fn private_key(&self) -> &WireGuardPrivateKey {
        &self.private_key
    }

    #[cfg(test)]
    pub(crate) fn from_private_key(private_key: WireGuardPrivateKey) -> Self {
        Self { private_key }
    }
}

/// Why a node-local WireGuard identity could not be parsed or persisted.
#[derive(Debug, thiserror::Error)]
pub enum MeshIdentityError {
    /// A key was not valid padded base64.
    #[error("WireGuard key is not valid base64")]
    InvalidEncoding,
    /// Decoded key material was not exactly 32 bytes.
    #[error("WireGuard key must contain 32 bytes, found {observed}")]
    InvalidLength { observed: usize },
    /// The encoded public key was the invalid all-zero X25519 point.
    #[error("WireGuard public key cannot be all zero")]
    InvalidPublicKeyMaterial,
    /// An existing private key was readable by users other than its owner.
    #[error("WireGuard key `{}` has insecure permissions {mode:#o}", path.display())]
    InsecurePermissions { path: PathBuf, mode: u32 },
    /// Filesystem work failed at the identity boundary.
    #[error("failed to {action} WireGuard key `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn create_identity(data_directory: &Path, path: &Path) -> Result<MeshIdentity, MeshIdentityError> {
    create_private_directory(data_directory, path)?;
    let private_key = WireGuardPrivateKey::generate();
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
            return MeshIdentity::load_or_generate(data_directory);
        }
        Err(source) => {
            return Err(MeshIdentityError::Io {
                action: "create",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    file.write_all(BASE64.encode(private_key.expose_bytes()).as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| MeshIdentityError::Io {
            action: "persist",
            path: path.to_path_buf(),
            source,
        })?;
    sync_directory(data_directory, path)?;
    Ok(MeshIdentity { private_key })
}

fn create_private_directory(
    data_directory: &Path,
    key_path: &Path,
) -> Result<(), MeshIdentityError> {
    std::fs::create_dir_all(data_directory).map_err(|source| MeshIdentityError::Io {
        action: "create parent directory for",
        path: key_path.to_path_buf(),
        source,
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(data_directory, std::fs::Permissions::from_mode(0o700)).map_err(
            |source| MeshIdentityError::Io {
                action: "restrict parent directory for",
                path: key_path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

fn sync_directory(data_directory: &Path, key_path: &Path) -> Result<(), MeshIdentityError> {
    let directory =
        std::fs::File::open(data_directory).map_err(|source| MeshIdentityError::Io {
            action: "open parent directory for sync of",
            path: key_path.to_path_buf(),
            source,
        })?;
    directory
        .sync_all()
        .map_err(|source| MeshIdentityError::Io {
            action: "sync parent directory for",
            path: key_path.to_path_buf(),
            source,
        })
}

fn decode_key(encoded: &str) -> Result<[u8; 32], MeshIdentityError> {
    let decoded = BASE64
        .decode(encoded)
        .map_err(|_| MeshIdentityError::InvalidEncoding)?;
    let observed = decoded.len();
    decoded
        .try_into()
        .map_err(|_| MeshIdentityError::InvalidLength { observed })
}

fn validate_private_permissions(path: &Path) -> Result<(), MeshIdentityError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|source| MeshIdentityError::Io {
                action: "inspect permissions of",
                path: path.to_path_buf(),
                source,
            })?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(MeshIdentityError::InsecurePermissions {
                path: path.to_path_buf(),
                mode,
            });
        }
    }
    Ok(())
}
