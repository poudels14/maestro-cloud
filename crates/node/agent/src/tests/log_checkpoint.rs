use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use kernel_api::WorkloadId;
use runtime::LogCursor;

use crate::log_checkpoint::{LogCheckpointFileSystem, cleanup_stale, commit_cursor, load_cursor};
use crate::{FileLogCheckpointStore, LogCheckpointError, LogCheckpointStore};

#[tokio::test]
async fn file_checkpoints_replace_atomically_survive_restart_and_clean_stale_entries()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("log-checkpoints");
    let first = workload_id("workload-1");
    let stale = workload_id("workload-2");
    let store = FileLogCheckpointStore::new(root.clone())?;

    store.commit(&first, &LogCursor::new("cursor-a")).await?;
    store.commit(&first, &LogCursor::new("cursor-b")).await?;
    store
        .commit(&stale, &LogCursor::new("cursor-stale"))
        .await?;

    assert_eq!(store.load(&first).await?, Some(LogCursor::new("cursor-b")));
    assert_eq!(fs::metadata(&root)?.permissions().mode() & 0o777, 0o700);
    assert_eq!(
        fs::metadata(root.join("workload-1.cursor"))?
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        store
            .cleanup_stale(&BTreeSet::from([first.clone()]))
            .await?,
        1
    );
    assert_eq!(store.load(&stale).await?, None);

    let restarted = FileLogCheckpointStore::new(root)?;
    assert_eq!(
        restarted.load(&first).await?,
        Some(LogCursor::new("cursor-b"))
    );
    Ok(())
}

#[tokio::test]
async fn file_checkpoints_reject_unsafe_roots_and_checkpoint_paths()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        FileLogCheckpointStore::new("relative".into()),
        Err(LogCheckpointError::InvalidRoot { .. })
    ));
    let temporary = tempfile::tempdir()?;
    let real_root = temporary.path().join("real");
    fs::create_dir(&real_root)?;
    fs::set_permissions(&real_root, fs::Permissions::from_mode(0o700))?;
    let linked_root = temporary.path().join("linked");
    symlink(&real_root, &linked_root)?;
    let linked_store = FileLogCheckpointStore::new(linked_root)?;
    assert!(matches!(
        linked_store.load(&workload_id("workload-1")).await,
        Err(LogCheckpointError::UnsafePath { .. })
    ));

    let root = temporary.path().join("checkpoints");
    fs::create_dir(&root)?;
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
    let outside = temporary.path().join("outside");
    fs::write(&outside, "do not overwrite")?;
    symlink(&outside, root.join("workload-1.cursor"))?;
    let store = FileLogCheckpointStore::new(root)?;
    assert!(matches!(
        store
            .commit(&workload_id("workload-1"), &LogCursor::new("cursor"))
            .await,
        Err(LogCheckpointError::UnsafePath { .. })
    ));
    assert_eq!(fs::read_to_string(outside)?, "do not overwrite");
    Ok(())
}

#[tokio::test]
async fn file_checkpoints_reject_empty_oversized_and_public_cursors()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("checkpoints");
    let store = FileLogCheckpointStore::new(root.clone())?;
    let id = workload_id("workload-1");
    assert!(matches!(
        store.commit(&id, &LogCursor::new("")).await,
        Err(LogCheckpointError::InvalidCursor { .. })
    ));
    assert!(matches!(
        store.commit(&id, &LogCursor::new("x".repeat(4097))).await,
        Err(LogCheckpointError::InvalidCursor { .. })
    ));

    fs::create_dir(&root)?;
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
    let path = root.join("workload-1.cursor");
    fs::write(&path, "cursor")?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644))?;
    assert!(matches!(
        store.load(&id).await,
        Err(LogCheckpointError::UnsafePath { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn slow_commit_does_not_block_an_unrelated_workload() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("checkpoints");
    let blocked_workload = workload_id("workload-1");
    let unrelated_workload = workload_id("workload-2");
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let files = Arc::new(BlockingLogCheckpointFileSystem {
        blocked_workload: blocked_workload.clone(),
        started_sender,
        release_receiver: Mutex::new(Some(release_receiver)),
    });
    let store = Arc::new(FileLogCheckpointStore::with_file_system(root, files)?);

    let blocked_store = store.clone();
    let blocked = tokio::spawn(async move {
        blocked_store
            .commit(&blocked_workload, &LogCursor::new("cursor-1"))
            .await
    });
    tokio::task::spawn_blocking(move || started_receiver.recv()).await??;

    let unrelated = tokio::time::timeout(
        Duration::from_secs(2),
        store.commit(&unrelated_workload, &LogCursor::new("cursor-2")),
    )
    .await;
    release_sender.send(())?;
    blocked.await??;
    unrelated.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "unrelated log checkpoint waited for another workload",
        )
    })??;
    assert_eq!(
        store.load(&workload_id("workload-1")).await?,
        Some(LogCursor::new("cursor-1"))
    );
    assert_eq!(
        store.load(&workload_id("workload-2")).await?,
        Some(LogCursor::new("cursor-2"))
    );
    Ok(())
}

struct BlockingLogCheckpointFileSystem {
    blocked_workload: WorkloadId,
    started_sender: mpsc::SyncSender<()>,
    release_receiver: Mutex<Option<mpsc::Receiver<()>>>,
}

impl LogCheckpointFileSystem for BlockingLogCheckpointFileSystem {
    fn load(
        &self,
        root: &std::path::Path,
        workload_id: &WorkloadId,
    ) -> Result<Option<LogCursor>, LogCheckpointError> {
        load_cursor(root, workload_id)
    }

    fn commit(
        &self,
        root: &std::path::Path,
        workload_id: &WorkloadId,
        cursor: &LogCursor,
    ) -> Result<(), LogCheckpointError> {
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
        commit_cursor(root, workload_id, cursor)
    }

    fn cleanup_stale(
        &self,
        root: &std::path::Path,
        active_workloads: &BTreeSet<WorkloadId>,
    ) -> Result<usize, LogCheckpointError> {
        cleanup_stale(root, active_workloads)
    }
}

fn task_failure(message: impl Into<String>) -> LogCheckpointError {
    LogCheckpointError::Task {
        message: message.into(),
    }
}

fn workload_id(value: &str) -> WorkloadId {
    WorkloadId::new(value).expect("workload id")
}
