use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;

use kernel_api::{SecretMountSpec, SecretValue, WorkloadId};
use runtime::{MountAccess, MountSource};

use crate::secret_mount::{SecretMountError, SecretMountManager};

#[tokio::test]
async fn secret_mount_is_private_idempotent_and_zeroized_on_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("secrets");
    let manager = SecretMountManager::new(root.clone())?;
    let workload_id = WorkloadId::new("workload-1")?;
    let spec = SecretMountSpec {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        items: BTreeMap::from([
            ("MULTILINE".to_owned(), SecretValue::new("line1\nline2")),
            ("QUOTED".to_owned(), SecretValue::new("say \"hi\"")),
            ("SIMPLE".to_owned(), SecretValue::new("hello")),
        ]),
    };

    let mount = manager.materialize(&workload_id, &spec).await?;
    assert_eq!(mount.target, std::path::Path::new(&spec.mount_path));
    assert_eq!(mount.access, MountAccess::ReadOnly);
    let MountSource::HostPath(ref secret_path) = mount.source else {
        return Err("secret was not rendered into a host file".into());
    };
    assert_eq!(
        fs::read_to_string(secret_path)?,
        "MULTILINE=\"line1\\nline2\"\nQUOTED=\"say \\\"hi\\\"\"\nSIMPLE=\"hello\"\n"
    );
    assert_eq!(
        fs::metadata(secret_path)?.permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(secret_path.parent().unwrap())?
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(manager.materialize(&workload_id, &spec).await?, mount);

    let observer = temporary.path().join("secret-observer");
    fs::hard_link(secret_path, &observer)?;
    let secret_length = fs::metadata(secret_path)?.len() as usize;
    manager.cleanup(&workload_id).await?;
    assert!(!secret_path.exists());
    assert_eq!(fs::read(&observer)?, vec![0_u8; secret_length]);
    Ok(())
}

#[tokio::test]
async fn secret_mount_rejects_identity_mutation_and_collects_only_stale_workloads()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let manager = SecretMountManager::new(temporary.path().join("secrets"))?;
    let first = WorkloadId::new("workload-1")?;
    let second = WorkloadId::new("workload-2")?;
    let mut spec = SecretMountSpec {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        items: BTreeMap::from([("TOKEN".to_owned(), SecretValue::new("first"))]),
    };
    let first_mount = manager.materialize(&first, &spec).await?;
    manager.materialize(&second, &spec).await?;
    spec.items
        .insert("TOKEN".to_owned(), SecretValue::new("changed"));
    assert!(matches!(
        manager.materialize(&first, &spec).await,
        Err(SecretMountError::ContentConflict { .. })
    ));

    let active = BTreeSet::from([first.to_string()]);
    assert_eq!(manager.cleanup_stale(&active).await?, 1);
    let MountSource::HostPath(first_path) = first_mount.source else {
        return Err("secret was not rendered into a host file".into());
    };
    assert!(first_path.exists());
    assert!(!temporary.path().join("secrets/workload-2").exists());
    Ok(())
}

#[tokio::test]
async fn secret_mount_rejects_unsafe_targets_and_keys() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let manager = SecretMountManager::new(temporary.path().join("secrets"))?;
    let workload_id = WorkloadId::new("workload-1")?;
    let invalid_target = SecretMountSpec {
        mount_path: "relative.env".to_owned(),
        items: BTreeMap::new(),
    };
    assert!(matches!(
        manager.materialize(&workload_id, &invalid_target).await,
        Err(SecretMountError::InvalidTarget { .. })
    ));
    let invalid_key = SecretMountSpec {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        items: BTreeMap::from([("BAD-KEY".to_owned(), SecretValue::new("value"))]),
    };
    assert!(matches!(
        manager.materialize(&workload_id, &invalid_key).await,
        Err(SecretMountError::InvalidKey { .. })
    ));
    Ok(())
}
