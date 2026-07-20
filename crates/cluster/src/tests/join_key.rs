use crate::{load_or_create_join_key, public_key_fingerprint};

#[test]
fn persists_and_reloads_the_join_private_key() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("system/join-key");
    let first = load_or_create_join_key(&path)?;
    let second = load_or_create_join_key(&path)?;

    assert_eq!(
        public_key_fingerprint(&first.public_key_hex())?,
        public_key_fingerprint(&second.public_key_hex())?
    );
    assert_eq!(format!("{first:?}"), "JoinPrivateKey([REDACTED])");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
    }
    Ok(())
}
