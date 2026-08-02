use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use kernel_api::{SecretMountSpec, SecretValue, WorkloadId};
use runtime::{MountAccess, MountSource, WorkloadMount};
use tokio::sync::oneshot;

use crate::secret_mount::{
    SecretMountError, SecretMountFileSystem, SecretMountManager, cleanup_directory, cleanup_stale,
    materialize,
};

#[tokio::test]
async fn secret_mount_is_private_idempotent_and_zeroized_on_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("secrets");
    let manager = SecretMountManager::new(root.clone())?;
    let workload_id = WorkloadId::new("workload-1")?;
    let spec = SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: None,
        items: BTreeMap::from([
            ("MULTILINE".to_owned(), SecretValue::new("line1\nline2")),
            ("QUOTED".to_owned(), SecretValue::new("say \"hi\"")),
            ("SIMPLE".to_owned(), SecretValue::new("hello")),
        ]),
    };

    let mount = manager.materialize(&workload_id, &spec).await?;
    assert_eq!(mount.target, std::path::Path::new(spec.mount_path()));
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
async fn secret_file_set_preserves_exact_bytes_and_zeroizes_every_file()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let manager = SecretMountManager::new(temporary.path().join("secrets"))?;
    let workload_id = WorkloadId::new("workload-files")?;
    let mut spec = SecretMountSpec::Files {
        mount_path: "/run/secrets/etcd".to_owned(),
        files: BTreeMap::from([
            (
                "ca.pem".to_owned(),
                SecretValue::new("-----BEGIN CERTIFICATE-----\nca\n"),
            ),
            (
                "client-key.pem".to_owned(),
                SecretValue::new("-----BEGIN PRIVATE KEY-----\nkey\n"),
            ),
        ]),
    };

    let mount = manager.materialize(&workload_id, &spec).await?;
    assert_eq!(mount.target, std::path::Path::new("/run/secrets/etcd"));
    assert_eq!(mount.access, MountAccess::ReadOnly);
    let MountSource::HostPath(ref directory) = mount.source else {
        return Err("secret file set was not rendered into a host directory".into());
    };
    let ca_path = directory.join("ca.pem");
    let key_path = directory.join("client-key.pem");
    assert_eq!(
        fs::read_to_string(&ca_path)?,
        "-----BEGIN CERTIFICATE-----\nca\n"
    );
    assert_eq!(
        fs::read_to_string(&key_path)?,
        "-----BEGIN PRIVATE KEY-----\nkey\n"
    );
    assert_eq!(fs::metadata(directory)?.permissions().mode() & 0o777, 0o700);
    assert_eq!(fs::metadata(&ca_path)?.permissions().mode() & 0o777, 0o600);
    assert_eq!(manager.materialize(&workload_id, &spec).await?, mount);

    let SecretMountSpec::Files { files, .. } = &mut spec else {
        return Err("test secret unexpectedly changed representation".into());
    };
    files.insert("ca.pem".to_owned(), SecretValue::new("changed"));
    assert!(matches!(
        manager.materialize(&workload_id, &spec).await,
        Err(SecretMountError::ContentConflict { .. })
    ));

    let ca_observer = temporary.path().join("ca-observer");
    let key_observer = temporary.path().join("key-observer");
    fs::hard_link(&ca_path, &ca_observer)?;
    fs::hard_link(&key_path, &key_observer)?;
    let ca_length = fs::metadata(&ca_path)?.len() as usize;
    let key_length = fs::metadata(&key_path)?.len() as usize;
    manager.cleanup(&workload_id).await?;
    assert_eq!(fs::read(ca_observer)?, vec![0_u8; ca_length]);
    assert_eq!(fs::read(key_observer)?, vec![0_u8; key_length]);
    Ok(())
}

#[tokio::test]
async fn secret_mount_rejects_identity_mutation_and_collects_only_stale_workloads()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let manager = SecretMountManager::new(temporary.path().join("secrets"))?;
    let first = WorkloadId::new("workload-1")?;
    let second = WorkloadId::new("workload-2")?;
    let mut spec = SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: None,
        items: BTreeMap::from([("TOKEN".to_owned(), SecretValue::new("first"))]),
    };
    let first_mount = manager.materialize(&first, &spec).await?;
    manager.materialize(&second, &spec).await?;
    let SecretMountSpec::Dotenv { items, .. } = &mut spec else {
        return Err("test secret unexpectedly changed representation".into());
    };
    items.insert("TOKEN".to_owned(), SecretValue::new("changed"));
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
    let invalid_target = SecretMountSpec::Dotenv {
        mount_path: "relative.env".to_owned(),
        source: None,
        items: BTreeMap::new(),
    };
    assert!(matches!(
        manager.materialize(&workload_id, &invalid_target).await,
        Err(SecretMountError::InvalidTarget { .. })
    ));
    let invalid_key = SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: None,
        items: BTreeMap::from([("BAD-KEY".to_owned(), SecretValue::new("value"))]),
    };
    assert!(matches!(
        manager.materialize(&workload_id, &invalid_key).await,
        Err(SecretMountError::InvalidKey { .. })
    ));
    let invalid_file = SecretMountSpec::Files {
        mount_path: "/run/secrets/files".to_owned(),
        files: BTreeMap::from([("../key.pem".to_owned(), SecretValue::new("value"))]),
    };
    assert!(matches!(
        manager.materialize(&workload_id, &invalid_file).await,
        Err(SecretMountError::InvalidFileName { .. })
    ));

    let outside = temporary.path().join("outside");
    fs::create_dir(&outside)?;
    fs::write(outside.join("do-not-touch"), "outside")?;
    let linked = temporary.path().join("linked-secret-directory");
    symlink(&outside, &linked)?;
    assert!(matches!(
        cleanup_directory(&linked),
        Err(SecretMountError::UnsafePath { .. })
    ));
    assert_eq!(fs::read_to_string(outside.join("do-not-touch"))?, "outside");
    Ok(())
}

#[tokio::test]
async fn slow_materialization_does_not_block_an_unrelated_workload()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("secrets");
    let blocked_workload = WorkloadId::new("workload-1")?;
    let unrelated_workload = WorkloadId::new("workload-2")?;
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let files = Arc::new(BlockingSecretMountFileSystem {
        blocked_workload: blocked_workload.clone(),
        started_sender,
        release_receiver: Mutex::new(Some(release_receiver)),
        cleanup_started_sender: Mutex::new(None),
    });
    let manager = Arc::new(SecretMountManager::with_file_system(root, files)?);
    let spec = secret_spec("value");

    let blocked_manager = manager.clone();
    let blocked_spec = spec.clone();
    let blocked = tokio::spawn(async move {
        blocked_manager
            .materialize(&blocked_workload, &blocked_spec)
            .await
    });
    tokio::task::spawn_blocking(move || started_receiver.recv()).await??;

    let unrelated = tokio::time::timeout(
        Duration::from_secs(2),
        manager.materialize(&unrelated_workload, &spec),
    )
    .await;
    release_sender.send(())?;
    blocked.await??;
    unrelated.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "unrelated secret materialization waited for another workload",
        )
    })??;
    Ok(())
}

#[tokio::test]
async fn cleanup_waits_for_same_workload_materialization() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("secrets");
    let blocked_workload = WorkloadId::new("workload-1")?;
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let (cleanup_started_sender, mut cleanup_started_receiver) = oneshot::channel();
    let files = Arc::new(BlockingSecretMountFileSystem {
        blocked_workload: blocked_workload.clone(),
        started_sender,
        release_receiver: Mutex::new(Some(release_receiver)),
        cleanup_started_sender: Mutex::new(Some(cleanup_started_sender)),
    });
    let manager = Arc::new(SecretMountManager::with_file_system(root.clone(), files)?);

    let blocked_manager = manager.clone();
    let materialized_workload = blocked_workload.clone();
    let blocked = tokio::spawn(async move {
        blocked_manager
            .materialize(&materialized_workload, &secret_spec("value"))
            .await
    });
    tokio::task::spawn_blocking(move || started_receiver.recv()).await??;

    let cleanup_manager = manager.clone();
    let cleanup = tokio::spawn(async move { cleanup_manager.cleanup(&blocked_workload).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut cleanup_started_receiver)
            .await
            .is_err(),
        "cleanup entered the filesystem while materialization was active"
    );

    release_sender.send(())?;
    blocked.await??;
    tokio::time::timeout(Duration::from_secs(2), &mut cleanup_started_receiver).await??;
    cleanup.await??;
    assert!(!root.join("workload-1").exists());
    Ok(())
}

struct BlockingSecretMountFileSystem {
    blocked_workload: WorkloadId,
    started_sender: mpsc::SyncSender<()>,
    release_receiver: Mutex<Option<mpsc::Receiver<()>>>,
    cleanup_started_sender: Mutex<Option<oneshot::Sender<()>>>,
}

impl SecretMountFileSystem for BlockingSecretMountFileSystem {
    fn materialize(
        &self,
        root: &std::path::Path,
        workload_id: &WorkloadId,
        spec: &SecretMountSpec,
    ) -> Result<WorkloadMount, SecretMountError> {
        if workload_id == &self.blocked_workload {
            self.started_sender
                .send(())
                .map_err(|error| task_failure(error.to_string()))?;
            self.release_receiver
                .lock()
                .map_err(|error| task_failure(error.to_string()))?
                .take()
                .ok_or_else(|| task_failure("release receiver was already consumed"))?
                .recv()
                .map_err(|error| task_failure(error.to_string()))?;
        }
        materialize(root, workload_id, spec)
    }

    fn cleanup(&self, directory: &std::path::Path) -> Result<(), SecretMountError> {
        if let Some(sender) = self
            .cleanup_started_sender
            .lock()
            .map_err(|error| task_failure(error.to_string()))?
            .take()
        {
            sender
                .send(())
                .map_err(|()| task_failure("cleanup observer was dropped"))?;
        }
        cleanup_directory(directory)
    }

    fn cleanup_stale(
        &self,
        root: &std::path::Path,
        active_workloads: &BTreeSet<String>,
    ) -> Result<usize, SecretMountError> {
        cleanup_stale(root, active_workloads)
    }
}

fn secret_spec(value: &str) -> SecretMountSpec {
    SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: None,
        items: BTreeMap::from([("TOKEN".to_owned(), SecretValue::new(value))]),
    }
}

fn task_failure(message: impl Into<String>) -> SecretMountError {
    SecretMountError::Task {
        message: message.into(),
    }
}
