use std::collections::BTreeMap;
use std::path::Path;

use crate::{
    EnvironmentInheritance, ProcessCommand, ProcessEnvironment, ProcessHandle, ProcessLogFiles,
    ProcessSignal, ProcessSpec, ProcessStatus, ProcessSupervisor, SupervisorError,
};

#[tokio::test]
async fn spawned_process_writes_protected_logs_and_retains_exit_status() {
    let root = tempfile::tempdir().unwrap();
    let supervisor = ProcessSupervisor::new();
    let handle = supervisor
        .spawn(shell_spec(
            root.path(),
            "printf stdout-value; printf stderr-value >&2; exit 7",
        ))
        .await
        .unwrap();

    let exit = supervisor.wait(handle).await.unwrap();
    assert_eq!(exit.code, Some(7));
    assert_eq!(
        supervisor.status(handle).await.unwrap(),
        ProcessStatus::Exited(exit)
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("stdout.log")).unwrap(),
        "stdout-value"
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("stderr.log")).unwrap(),
        "stderr-value"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(root.path().join("stdout.log"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn another_supervisor_adopts_and_signals_the_exact_process() {
    let root = tempfile::tempdir().unwrap();
    let owner = ProcessSupervisor::new();
    let handle = owner
        .spawn(ProcessSpec {
            command: ProcessCommand {
                executable: "/bin/sleep".into(),
                arguments: vec!["30".to_owned()],
            },
            ..base_spec(root.path())
        })
        .await
        .unwrap();
    let adopter = ProcessSupervisor::new();

    assert_eq!(
        adopter.status(handle).await.unwrap(),
        ProcessStatus::Running
    );
    adopter
        .signal(handle, ProcessSignal::Terminate)
        .await
        .unwrap();
    let exit = owner.wait(handle).await.unwrap();
    assert_eq!(exit.signal, Some(15));
}

#[tokio::test]
async fn persisted_handle_rejects_pid_reuse_before_signaling() {
    let root = tempfile::tempdir().unwrap();
    let supervisor = ProcessSupervisor::new();
    let handle = supervisor
        .spawn(ProcessSpec {
            command: ProcessCommand {
                executable: "/bin/sleep".into(),
                arguments: vec!["30".to_owned()],
            },
            ..base_spec(root.path())
        })
        .await
        .unwrap();
    let stale =
        ProcessHandle::from_parts(handle.pid(), handle.start_time_ticks().saturating_add(1))
            .unwrap();

    assert!(matches!(
        supervisor.signal(stale, ProcessSignal::Kill).await,
        Err(SupervisorError::IdentityMismatch { .. })
    ));
    supervisor
        .signal(handle, ProcessSignal::Kill)
        .await
        .unwrap();
    assert_eq!(supervisor.wait(handle).await.unwrap().signal, Some(9));
}

fn shell_spec(root: &Path, script: &str) -> ProcessSpec {
    ProcessSpec {
        command: ProcessCommand {
            executable: "/bin/sh".into(),
            arguments: vec!["-c".to_owned(), script.to_owned()],
        },
        ..base_spec(root)
    }
}

fn base_spec(root: &Path) -> ProcessSpec {
    ProcessSpec {
        command: ProcessCommand {
            executable: "/bin/true".into(),
            arguments: Vec::new(),
        },
        environment: ProcessEnvironment {
            inheritance: EnvironmentInheritance::Clear,
            variables: BTreeMap::new(),
        },
        working_directory: Some(root.to_path_buf()),
        logs: ProcessLogFiles {
            stdout: root.join("stdout.log"),
            stderr: root.join("stderr.log"),
        },
        user: None,
    }
}
