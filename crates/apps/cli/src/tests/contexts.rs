use std::fs;

use kernel_api::SecretValue;

use crate::contexts::{ContextStore, normalize_origin};

#[test]
fn contexts_normalize_select_preserve_tokens_and_remove() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("contexts.json");
    let store = ContextStore::at(path.clone());
    assert_eq!(
        store.set("dev", "127.0.0.1:3000", None)?,
        "http://127.0.0.1:3000"
    );
    store.save_active_token(SecretValue::new("token-one"))?;
    store.set("dev", "http://localhost:4000", None)?;
    store.set("prod", "https://maestro.example.test", None)?;
    store.use_context("prod")?;
    let listed = store.list()?;
    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .any(|context| context.name == "prod" && context.active)
    );
    store.remove("prod")?;
    assert!(store.active_name().is_err());
    let encoded = fs::read_to_string(path)?;
    assert!(encoded.contains("token-one"));
    Ok(())
}

#[test]
fn contexts_reject_unsafe_or_ambiguous_origins() {
    assert_eq!(
        normalize_origin("maestro.example.test").ok().as_deref(),
        Some("https://maestro.example.test")
    );
    assert!(normalize_origin("http://maestro.example.test").is_err());
    assert!(normalize_origin("https://user@maestro.example.test").is_err());
    assert!(normalize_origin("https://maestro.example.test/api").is_err());
    assert!(normalize_origin("ftp://maestro.example.test").is_err());
}

#[cfg(unix)]
#[test]
fn contexts_file_is_owner_only() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("contexts.json");
    ContextStore::at(path.clone()).set("dev", "http://127.0.0.1:3000", None)?;
    assert_eq!(fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
    Ok(())
}
