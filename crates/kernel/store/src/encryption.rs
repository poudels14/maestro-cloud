use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::rand_core::RngCore,
    aead::{Aead, KeyInit, OsRng, Payload},
};
use argon2::Argon2;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop};

const KDF_SALT: &[u8] = b"maestro-v1-key-derivation";
const KEY_LENGTH: usize = 32;
const NONCE_LENGTH: usize = 12;
const ENVELOPE_MAGIC: &[u8; 4] = b"MAE1";
const ENVELOPE_HEADER_LENGTH: usize = ENVELOPE_MAGIC.len() + NONCE_LENGTH;

/// A derived AES-256 key that zeroizes its bytes on drop.
///
/// v1 deliberately supports one active key; online key rotation is outside
/// the rewrite scope and requires a versioned envelope before being added.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct EncryptionKey([u8; KEY_LENGTH]);

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EncryptionKey([REDACTED])")
    }
}

/// Versioned authenticated ciphertext safe to persist as store bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct EncryptedValue(Vec<u8>);

impl EncryptedValue {
    /// Parses a persisted versioned ciphertext envelope.
    pub fn from_bytes(value: Vec<u8>) -> Result<Self, EncryptionError> {
        if value.len() < ENVELOPE_HEADER_LENGTH || !value.starts_with(ENVELOPE_MAGIC) {
            Err(EncryptionError::InvalidEnvelope)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the authenticated envelope bytes for persistence.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the envelope into persistence bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl std::fmt::Debug for EncryptedValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EncryptedValue")
            .field("length", &self.0.len())
            .finish()
    }
}

/// Matchable encryption and authentication failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EncryptionError {
    /// Argon2 could not derive the fixed-size v1 key.
    #[error("could not derive the Maestro encryption key")]
    KeyDerivation,
    /// The key bytes could not initialize AES-256-GCM.
    #[error("could not initialize the Maestro encryption cipher")]
    InvalidKey,
    /// Randomized authenticated encryption failed.
    #[error("could not encrypt the secret-bearing store value")]
    Encryption,
    /// Persisted data is not a supported versioned envelope.
    #[error("encrypted store value has an invalid or unsupported envelope")]
    InvalidEnvelope,
    /// Ciphertext authentication failed, including when the wrong key is used.
    #[error("encrypted store value failed authentication")]
    Authentication,
}

/// Derives the v1 store encryption key from an operator-supplied master secret.
pub fn derive_key(master_secret: &str) -> Result<EncryptionKey, EncryptionError> {
    let salt = Sha256::digest(KDF_SALT);
    let mut key = [0; KEY_LENGTH];
    Argon2::default()
        .hash_password_into(master_secret.as_bytes(), &salt, &mut key)
        .map_err(|_| EncryptionError::KeyDerivation)?;
    Ok(EncryptionKey(key))
}

/// Encrypts plaintext with a fresh nonce and authenticated AES-256-GCM.
pub fn seal(key: &EncryptionKey, plaintext: &[u8]) -> Result<EncryptedValue, EncryptionError> {
    seal_with_context(key, plaintext, &[])
}

/// Encrypts plaintext and authenticates it against non-secret storage context.
pub fn seal_with_context(
    key: &EncryptionKey,
    plaintext: &[u8],
    context: &[u8],
) -> Result<EncryptedValue, EncryptionError> {
    let cipher = Aes256Gcm::new_from_slice(&key.0).map_err(|_| EncryptionError::InvalidKey)?;
    let mut nonce_bytes = [0; NONCE_LENGTH];
    OsRng.fill_bytes(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: plaintext,
                aad: context,
            },
        )
        .map_err(|_| EncryptionError::Encryption)?;
    let mut envelope = Vec::with_capacity(ENVELOPE_HEADER_LENGTH + ciphertext.len());
    envelope.extend_from_slice(ENVELOPE_MAGIC);
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend(ciphertext);
    Ok(EncryptedValue(envelope))
}

/// Authenticates and decrypts one v1 nonce-prefixed store value.
pub fn open(key: &EncryptionKey, encrypted: &EncryptedValue) -> Result<Vec<u8>, EncryptionError> {
    open_with_context(key, encrypted, &[])
}

/// Authenticates storage context and decrypts one v1 envelope.
pub fn open_with_context(
    key: &EncryptionKey,
    encrypted: &EncryptedValue,
    context: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
    let (_, body) = encrypted.0.split_at(ENVELOPE_MAGIC.len());
    let (nonce, ciphertext) = body.split_at(NONCE_LENGTH);
    let cipher = Aes256Gcm::new_from_slice(&key.0).map_err(|_| EncryptionError::InvalidKey)?;
    cipher
        .decrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: context,
            },
        )
        .map_err(|_| EncryptionError::Authentication)
}
