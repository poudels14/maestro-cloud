use std::os::unix::fs::{FileTypeExt, PermissionsExt};

use crate::ExecMode;
use crate::containerd_exec_io::{prepare_exec_paths, remove_exec_directory};

#[tokio::test]
async fn containerd_exec_paths_are_private_fifos_with_mode_specific_stderr() {
    let state_root = tempfile::tempdir().unwrap();
    let paths = prepare_exec_paths(state_root.path(), "pipes", ExecMode::Pipes)
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(&paths.directory)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert!(
        std::fs::metadata(&paths.stdin)
            .unwrap()
            .file_type()
            .is_fifo()
    );
    assert!(
        std::fs::metadata(&paths.stdout)
            .unwrap()
            .file_type()
            .is_fifo()
    );
    assert!(
        std::fs::metadata(paths.stderr.as_ref().unwrap())
            .unwrap()
            .file_type()
            .is_fifo()
    );
    remove_exec_directory(&paths.directory).await;

    let terminal = prepare_exec_paths(
        state_root.path(),
        "terminal",
        ExecMode::Terminal {
            columns: 80,
            rows: 24,
        },
    )
    .await
    .unwrap();
    assert!(terminal.stderr.is_none());
    remove_exec_directory(&terminal.directory).await;
}
