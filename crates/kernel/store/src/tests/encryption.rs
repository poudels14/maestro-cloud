use crate::{EncryptedValue, EncryptionError, derive_key, open, seal};

#[test]
fn encrypted_values_round_trip_without_exposing_key_or_ciphertext() {
    let key = derive_key("operator master secret").expect("derive encryption key");
    let first = seal(&key, b"database-password").expect("encrypt first value");
    let second = seal(&key, b"database-password").expect("encrypt second value");

    assert_ne!(first, second, "fresh nonces must vary ciphertext");
    assert_eq!(
        open(&key, &first).expect("decrypt value"),
        b"database-password"
    );
    assert_eq!(format!("{key:?}"), "EncryptionKey([REDACTED])");
    assert!(!format!("{first:?}").contains("database-password"));
}

#[test]
fn wrong_keys_and_truncated_envelopes_fail_closed() {
    let encryption_key = derive_key("correct key").expect("derive correct key");
    let wrong_key = derive_key("wrong key").expect("derive wrong key");
    let encrypted = seal(&encryption_key, b"secret").expect("encrypt value");

    assert_eq!(
        open(&wrong_key, &encrypted),
        Err(EncryptionError::Authentication)
    );
    assert_eq!(
        EncryptedValue::from_bytes(vec![0; 11]),
        Err(EncryptionError::InvalidEnvelope)
    );
}
