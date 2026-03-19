use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::rand_core::RngCore,
    aead::{Aead, KeyInit, OsRng},
};
use argon2::Argon2;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SecretString {
    inner: String,
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString(***)")
    }
}

impl SecretString {
    pub fn new(value: String) -> Self {
        Self { inner: value }
    }

    pub fn as_str(&self) -> &str {
        &self.inner
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct EncryptionKey {
    inner: Vec<u8>,
}

const KDF_SALT: &[u8] = b"maestro-v1-key-derivation";

impl EncryptionKey {
    fn aes_key(&self) -> &Key<Aes256Gcm> {
        Key::<Aes256Gcm>::from_slice(&self.inner)
    }
}

pub fn derive_key(master_secret: &str) -> EncryptionKey {
    let salt = Sha256::digest(KDF_SALT);
    let argon2 = Argon2::default();
    let mut key = vec![0u8; 32];
    argon2
        .hash_password_into(master_secret.as_bytes(), &salt, &mut key)
        .expect("argon2 key derivation failed");
    EncryptionKey { inner: key }
}

pub fn encrypt(key: &EncryptionKey, plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new(key.aes_key());
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|err| format!("encryption failed: {err}"))?;
    let mut result = nonce_bytes.to_vec();
    result.extend(ciphertext);
    Ok(result)
}

#[allow(dead_code)]
pub fn decrypt(key: &EncryptionKey, data: &[u8]) -> Result<Vec<u8>, String> {
    if data.len() < 12 {
        return Err("encrypted data too short".to_string());
    }
    let (nonce_bytes, ciphertext) = data.split_at(12);
    let cipher = Aes256Gcm::new(key.aes_key());
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|err| format!("decryption failed: {err}"))
}

pub fn encrypt_string(key: &EncryptionKey, plaintext: &str) -> Result<String, String> {
    let encrypted = encrypt(key, plaintext.as_bytes())?;
    Ok(base64_encode(&encrypted))
}

#[allow(dead_code)]
pub fn decrypt_string(key: &EncryptionKey, encoded: &str) -> Result<String, String> {
    let data = base64_decode(encoded)?;
    let decrypted = decrypt(key, &data)?;
    String::from_utf8(decrypted).map_err(|err| format!("decrypted data is not utf8: {err}"))
}

fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(data.len() * 4 / 3 + 4);
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        let _ = write!(result, "{}", CHARS[((n >> 18) & 0x3F) as usize] as char);
        let _ = write!(result, "{}", CHARS[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            let _ = write!(result, "{}", CHARS[((n >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            let _ = write!(result, "{}", CHARS[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[allow(dead_code)]
fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let input = input.trim_end_matches('=');
    let mut result = Vec::with_capacity(input.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0;
    for c in input.chars() {
        let val = match c {
            'A'..='Z' => c as u32 - 'A' as u32,
            'a'..='z' => c as u32 - 'a' as u32 + 26,
            '0'..='9' => c as u32 - '0' as u32 + 52,
            '+' => 62,
            '/' => 63,
            _ => return Err(format!("invalid base64 character: {c}")),
        };
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            result.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    Ok(result)
}
