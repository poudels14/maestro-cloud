use std::collections::BTreeMap;
use std::path::Path;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use maestro_core::{EnvSpec, ServiceSpec};
use thiserror::Error;

const SEALED_PREFIX: &str = "enc:v1:";
const MASK: &str = "********";
const KEY_BYTES: usize = 32;
const NONCE_BYTES: usize = 12;

pub struct SecretKey {
    cipher: Aes256Gcm,
}

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("failed to access the encryption key: {0}")]
    Io(#[from] std::io::Error),
    #[error("the encryption key at {0} must be exactly {KEY_BYTES} bytes")]
    InvalidKey(String),
    #[error("failed to decrypt a sealed value; was the encryption key changed?")]
    Decrypt,
}

impl SecretKey {
    pub fn load_or_create(path: &Path) -> Result<Self, SecretError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let generated = rand::random::<[u8; KEY_BYTES]>().to_vec();
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                write_private(path, &generated)?;
                generated
            }
            Err(error) => Err(error)?,
        };
        match bytes.len() == KEY_BYTES {
            true => Ok(Self::from_bytes(&bytes)),
            false => Err(SecretError::InvalidKey(path.display().to_string())),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(bytes)),
        }
    }

    pub fn seal(&self, plaintext: &str) -> String {
        match plaintext.starts_with(SEALED_PREFIX) {
            true => plaintext.to_string(),
            false => {
                let nonce = rand::random::<[u8; NONCE_BYTES]>();
                let ciphertext = self
                    .cipher
                    .encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes())
                    .unwrap_or_default();
                let payload = [nonce.as_slice(), ciphertext.as_slice()].concat();
                format!("{SEALED_PREFIX}{}", STANDARD.encode(payload))
            }
        }
    }

    pub fn open(&self, value: &str) -> Result<String, SecretError> {
        match value.strip_prefix(SEALED_PREFIX) {
            None => Ok(value.to_string()),
            Some(encoded) => {
                let payload = STANDARD.decode(encoded).map_err(|_| SecretError::Decrypt)?;
                let (nonce, ciphertext) = payload
                    .split_at_checked(NONCE_BYTES)
                    .ok_or(SecretError::Decrypt)?;
                let plaintext = self
                    .cipher
                    .decrypt(Nonce::from_slice(nonce), ciphertext)
                    .map_err(|_| SecretError::Decrypt)?;
                String::from_utf8(plaintext).map_err(|_| SecretError::Decrypt)
            }
        }
    }

    pub fn seal_spec(&self, spec: &ServiceSpec) -> ServiceSpec {
        map_env_values(spec, |value| Ok::<_, SecretError>(self.seal(value)))
            .unwrap_or_else(|_| spec.clone())
    }

    pub fn open_spec(&self, spec: &ServiceSpec) -> Result<ServiceSpec, SecretError> {
        map_env_values(spec, |value| self.open(value))
    }
}

pub fn mask_spec(spec: &ServiceSpec) -> ServiceSpec {
    map_env_values(spec, |_| Ok::<_, SecretError>(MASK.to_string()))
        .unwrap_or_else(|_| spec.clone())
}

fn map_env_values<E>(
    spec: &ServiceSpec,
    transform: impl Fn(&str) -> Result<String, E>,
) -> Result<ServiceSpec, E> {
    let map_env = |env: &EnvSpec| -> Result<EnvSpec, E> {
        let items = env
            .items
            .iter()
            .map(|(name, value)| Ok((name.clone(), transform(value)?)))
            .collect::<Result<BTreeMap<_, _>, E>>()?;
        Ok(EnvSpec {
            source: env.source.clone(),
            items,
        })
    };
    let mut mapped = spec.clone();
    mapped.deploy.env = map_env(&spec.deploy.env)?;
    if let (Some(original), Some(build)) = (&spec.build, mapped.build.as_mut()) {
        build.env = map_env(&original.env)?;
        build.secrets = map_env(&original.secrets)?;
    }
    Ok(mapped)
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
}

#[cfg(test)]
mod tests {
    use maestro_core::{BuildSpec, DeploySpec};

    use super::*;

    fn key() -> SecretKey {
        SecretKey::from_bytes(&[7u8; KEY_BYTES])
    }

    fn spec() -> ServiceSpec {
        ServiceSpec {
            name: "API".to_string(),
            image: None,
            build: Some(BuildSpec {
                repo: None,
                branch: None,
                dockerfile: "Dockerfile".to_string(),
                watch: false,
                registry: None,
                env: EnvSpec::default(),
                secrets: EnvSpec {
                    source: None,
                    items: BTreeMap::from([("NPM_TOKEN".to_string(), "npm-secret".to_string())]),
                },
            }),
            deploy: DeploySpec {
                env: EnvSpec {
                    source: Some("aws-secret://prod/api".to_string()),
                    items: BTreeMap::from([("DB_PASSWORD".to_string(), "hunter2".to_string())]),
                },
                ..DeploySpec::default()
            },
            ingress: None,
        }
    }

    #[test]
    fn seals_and_opens_round_trip() {
        let key = key();
        let sealed = key.seal("hunter2");
        assert!(sealed.starts_with(SEALED_PREFIX));
        assert_ne!(sealed, key.seal("hunter2"));
        assert_eq!(key.seal(&sealed), sealed);
        assert_eq!(key.open(&sealed).unwrap(), "hunter2");
        assert_eq!(key.open("plain").unwrap(), "plain");
    }

    #[test]
    fn rejects_values_sealed_with_another_key() {
        let sealed = key().seal("hunter2");
        let other = SecretKey::from_bytes(&[9u8; KEY_BYTES]);
        assert!(matches!(other.open(&sealed), Err(SecretError::Decrypt)));
    }

    #[test]
    fn seals_every_env_value_in_a_spec() {
        let key = key();
        let sealed = key.seal_spec(&spec());
        assert!(sealed.deploy.env.items["DB_PASSWORD"].starts_with(SEALED_PREFIX));
        assert!(
            sealed.build.as_ref().unwrap().secrets.items["NPM_TOKEN"].starts_with(SEALED_PREFIX)
        );
        assert_eq!(
            sealed.deploy.env.source.as_deref(),
            Some("aws-secret://prod/api")
        );
        assert_eq!(key.open_spec(&sealed).unwrap(), spec());
        assert_eq!(mask_spec(&sealed).deploy.env.items["DB_PASSWORD"], MASK);
    }

    #[test]
    fn creates_a_private_key_file_once() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("keys/encryption.key");
        let first = SecretKey::load_or_create(&path).unwrap();
        let sealed = first.seal("value");
        let second = SecretKey::load_or_create(&path).unwrap();
        assert_eq!(second.open(&sealed).unwrap(), "value");
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
