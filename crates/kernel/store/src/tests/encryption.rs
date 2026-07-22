use crate::{
    EncryptedValue, EncryptionError, derive_key, open, open_with_context, seal, seal_with_context,
};

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

#[test]
fn encrypted_values_are_bound_to_their_storage_context() {
    let key = derive_key("operator master secret").expect("derive encryption key");
    let encrypted =
        seal_with_context(&key, b"secret", b"/maestro/first").expect("encrypt contextual value");

    assert_eq!(
        open_with_context(&key, &encrypted, b"/maestro/first").expect("decrypt contextual value"),
        b"secret"
    );
    assert!(matches!(
        open_with_context(&key, &encrypted, b"/maestro/second"),
        Err(EncryptionError::Authentication)
    ));
    assert!(matches!(
        EncryptedValue::from_bytes(vec![0; 64]),
        Err(EncryptionError::InvalidEnvelope)
    ));
}
