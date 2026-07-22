use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use argon2::Argon2;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::LegacyEntry;

const KDF_SALT: &[u8] = b"maestro-v1-key-derivation";
const KEY_LENGTH: usize = 32;
const NONCE_LENGTH: usize = 12;

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub(crate) struct LegacyDecryptor {
    key: [u8; KEY_LENGTH],
}

impl LegacyDecryptor {
    pub(crate) fn new(master_secret: &str) -> Result<Self, LegacyCryptoError> {
        let salt = Sha256::digest(KDF_SALT);
        let mut key = [0; KEY_LENGTH];
        Argon2::default()
            .hash_password_into(master_secret.as_bytes(), &salt, &mut key)
            .map_err(|_| LegacyCryptoError::KeyDerivation)?;
        Ok(Self { key })
    }

    pub(crate) fn decode_json<Value>(&self, entry: &LegacyEntry) -> Result<Value, LegacyCryptoError>
    where
        Value: serde::de::DeserializeOwned,
    {
        let plaintext = if looks_like_json(entry.value()) {
            entry.value().to_vec()
        } else {
            let encoded = std::str::from_utf8(entry.value()).map_err(|_| {
                LegacyCryptoError::InvalidEncoding {
                    key: entry.key().to_owned(),
                }
            })?;
            let encrypted =
                STANDARD
                    .decode(encoded)
                    .map_err(|_| LegacyCryptoError::InvalidEncoding {
                        key: entry.key().to_owned(),
                    })?;
            self.decrypt(entry.key(), &encrypted)?
        };
        serde_json::from_slice(&plaintext).map_err(|error| LegacyCryptoError::InvalidJson {
            key: entry.key().to_owned(),
            message: error.to_string(),
        })
    }

    fn decrypt(&self, key: &str, encrypted: &[u8]) -> Result<Vec<u8>, LegacyCryptoError> {
        if encrypted.len() < NONCE_LENGTH {
            return Err(LegacyCryptoError::InvalidEnvelope {
                key: key.to_owned(),
            });
        }
        let (nonce, ciphertext) = encrypted.split_at(NONCE_LENGTH);
        let cipher = Aes256Gcm::new_from_slice(&self.key).map_err(|_| {
            LegacyCryptoError::InvalidEnvelope {
                key: key.to_owned(),
            }
        })?;
        cipher
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| LegacyCryptoError::Authentication {
                key: key.to_owned(),
            })
    }
}

impl std::fmt::Debug for LegacyDecryptor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LegacyDecryptor([REDACTED])")
    }
}

fn looks_like_json(value: &[u8]) -> bool {
    value
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| matches!(byte, b'{' | b'['))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyCryptoError {
    #[error("could not derive the legacy Maestro encryption key")]
    KeyDerivation,
    #[error("legacy encrypted value at `{key}` is not valid base64 text")]
    InvalidEncoding { key: String },
    #[error("legacy encrypted value at `{key}` has an invalid envelope")]
    InvalidEnvelope { key: String },
    #[error("legacy encrypted value at `{key}` failed authentication")]
    Authentication { key: String },
    #[error("legacy value at `{key}` contains invalid JSON: {message}")]
    InvalidJson { key: String, message: String },
}

#[cfg(test)]
pub(crate) fn encrypt_for_test(
    master_secret: &str,
    plaintext: &[u8],
) -> Result<Vec<u8>, LegacyCryptoError> {
    use aes_gcm::aead::OsRng;
    use aes_gcm::aead::rand_core::RngCore;

    let decryptor = LegacyDecryptor::new(master_secret)?;
    let cipher =
        Aes256Gcm::new_from_slice(&decryptor.key).map_err(|_| LegacyCryptoError::KeyDerivation)?;
    let mut nonce = [0; NONCE_LENGTH];
    OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| LegacyCryptoError::KeyDerivation)?;
    let mut envelope = nonce.to_vec();
    envelope.extend(ciphertext);
    Ok(STANDARD.encode(envelope).into_bytes())
}
