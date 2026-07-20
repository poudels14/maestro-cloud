use crate::{MeshIdentity, WireGuardPrivateKey, WireGuardPublicKey};

#[test]
fn identity_persists_with_owner_only_permissions() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let data_directory = directory.path().join("node");
    let first = MeshIdentity::load_or_generate(&data_directory)?;
    let second = MeshIdentity::load_or_generate(&data_directory)?;

    assert_eq!(first.public_key(), second.public_key());
    assert_eq!(
        format!("{:?}", first.private_key()),
        "WireGuardPrivateKey([REDACTED])"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(data_directory.join("wireguard.key"))?
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(data_directory)?.permissions().mode() & 0o777,
            0o700
        );
    }
    Ok(())
}

#[test]
fn identity_rejects_insecure_existing_key() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let data_directory = directory.path().join("node");
    MeshIdentity::load_or_generate(&data_directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let key_path = data_directory.join("wireguard.key");
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o644))?;
        assert!(MeshIdentity::load_or_generate(&data_directory).is_err());
    }
    Ok(())
}

#[test]
fn public_key_parser_rejects_the_all_zero_point() {
    assert!(
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
            .parse::<WireGuardPublicKey>()
            .is_err()
    );
}

#[test]
fn generated_private_keys_use_wireguard_clamping() {
    let private_key = WireGuardPrivateKey::generate();
    let bytes = private_key.expose_bytes();
    assert_eq!(bytes.first().copied().unwrap_or_default() & 7, 0);
    assert_eq!(bytes.last().copied().unwrap_or_default() & 128, 0);
    assert_eq!(bytes.last().copied().unwrap_or_default() & 64, 64);
}
