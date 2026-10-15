use std::collections::BTreeMap;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use argon2::Argon2;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use maestro_core::{EnvSpec, ServiceSpec};
use sha2::{Digest, Sha256};
use thiserror::Error;

const SEALED_PREFIX: &str = "enc:v1:";
const MASK: &str = "********";
const KEY_BYTES: usize = 32;
const KDF_SALT: &[u8] = b"maestro-v1-key-derivation";
const NONCE_BYTES: usize = 12;

pub struct SecretKey {
    cipher: Aes256Gcm,
}

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("failed to decrypt a sealed value; was the encryption key changed?")]
    Decrypt,
}

impl SecretKey {
    pub fn derive(master_secret: &str) -> Self {
        let salt = Sha256::digest(KDF_SALT);
        let mut key = [0u8; KEY_BYTES];
        Argon2::default()
            .hash_password_into(master_secret.as_bytes(), &salt, &mut key)
            .unwrap_or_else(|_| unreachable!("a 32-byte output and 32-byte salt are valid"));
        Self::from_bytes(&key)
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
    if let (Some(original), Some(secrets)) = (&spec.deploy.secrets, mapped.deploy.secrets.as_mut())
    {
        secrets.items = map_env(&EnvSpec {
            source: None,
            items: original.items.clone(),
        })?
        .items;
    }
    if let (Some(original), Some(preview)) = (&spec.preview, mapped.preview.as_mut()) {
        preview.env = map_env(&original.env)?;
    }
    if let (Some(original), Some(build)) = (&spec.build, mapped.build.as_mut()) {
        build.env = map_env(&original.env)?;
        build.secrets = map_env(&original.secrets)?;
    }
    Ok(mapped)
}

#[cfg(test)]
mod tests {
    use maestro_core::{BuildSpec, DeploySpec, PreviewSpec, SecretsSpec};

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
                depot: None,
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
                secrets: Some(SecretsSpec {
                    mount_path: "/run/secrets".to_string(),
                    source: None,
                    items: BTreeMap::from([("tls.key".to_string(), "private".to_string())]),
                }),
                ..DeploySpec::default()
            },
            ingress: None,
            preview: Some(PreviewSpec {
                enabled: true,
                close_grace_period: "1h".to_string(),
                replicas: 1,
                env: EnvSpec {
                    source: None,
                    items: BTreeMap::from([("PREVIEW_TOKEN".to_string(), "preview".to_string())]),
                },
            }),
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
        let secret_files = &sealed.deploy.secrets.as_ref().unwrap().items;
        assert!(secret_files["tls.key"].starts_with(SEALED_PREFIX));
        assert!(
            sealed.preview.as_ref().unwrap().env.items["PREVIEW_TOKEN"].starts_with(SEALED_PREFIX)
        );
        let masked = mask_spec(&sealed);
        assert_eq!(masked.deploy.env.items["DB_PASSWORD"], MASK);
        assert_eq!(masked.deploy.secrets.unwrap().items["tls.key"], MASK);
        assert_eq!(masked.preview.unwrap().env.items["PREVIEW_TOKEN"], MASK);
    }

    #[test]
    fn derives_the_same_key_from_the_same_secret() {
        let sealed = SecretKey::derive("cluster-secret").seal("value");
        assert_eq!(SecretKey::derive("cluster-secret").open(&sealed).unwrap(), "value");
        assert!(SecretKey::derive("other-secret").open(&sealed).is_err());
    }
}
