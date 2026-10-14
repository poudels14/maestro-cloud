#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use maestro_core::{ByteSize, Millicores};
use maestro_runtime::{
    CniNetwork, ContainerLimits, ContainerSpec, ContainerState, ContainerdRuntime,
    ContainerdSettings, ExecRequest, NetworkMode, Runtime, RuntimeEvent,
};

const IMAGE: &str = "busybox:1.36";
const EVENT_TIMEOUT: Duration = Duration::from_secs(20);

fn enabled() -> bool {
    std::env::var("MAESTRO_CONTAINERD_TESTS").is_ok_and(|value| value == "1")
}

async fn runtime(state_dir: &std::path::Path) -> ContainerdRuntime {
    let plugin_dirs = std::env::var("CNI_PATH")
        .map(|paths| std::env::split_paths(&paths).collect())
        .unwrap_or_else(|_| {
            vec![
                PathBuf::from("/usr/local/libexec/cni"),
                PathBuf::from("/opt/cni/bin"),
            ]
        });
    let network = CniNetwork::bridge("maestro-test0", "10.254.7.0/24", plugin_dirs, state_dir);
    ContainerdRuntime::connect(
        ContainerdSettings {
            socket: PathBuf::from("/run/containerd/containerd.sock"),
            namespace: "maestro-test".to_string(),
            snapshotter: "overlayfs".to_string(),
            state_dir: state_dir.to_path_buf(),
            resolv_conf: PathBuf::from("/etc/resolv.conf"),
            log_uri: None,
        },
        network,
    )
    .await
    .unwrap()
}

fn container(id: &str, args: &[&str], memory: ByteSize) -> ContainerSpec {
    ContainerSpec {
        id: id.to_string(),
        image: IMAGE.to_string(),
        entrypoint: None,
        args: Some(args.iter().map(|arg| arg.to_string()).collect()),
        env: BTreeMap::new(),
        labels: BTreeMap::from([("maestro.test".to_string(), "true".to_string())]),
        limits: ContainerLimits {
            cpu: Millicores::new(250),
            memory,
            pids: 128,
            oom_score_adj: 500,
        },
        cgroup_parent: "maestro-workloads.slice".to_string(),
        mounts: vec![],
        network: NetworkMode::Bridge,
    }
}

fn cgroup_file(id: &str, name: &str) -> String {
    let path =
        format!("/sys/fs/cgroup/maestro.slice/maestro-workloads.slice/maestro-{id}.scope/{name}");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
        .trim()
        .to_string()
}

async fn next_event_for(
    events: &mut tokio::sync::mpsc::Receiver<RuntimeEvent>,
    id: &str,
) -> RuntimeEvent {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            let event = events.recv().await.expect("event stream closed");
            let matches = match &event {
                RuntimeEvent::Exited { id: event_id, .. } => event_id == id,
                RuntimeEvent::OomKilled { id: event_id } => event_id == id,
            };
            if matches {
                break event;
            }
        }
    })
    .await
    .expect("timed out waiting for runtime event")
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_container_with_limits_and_reports_exit() {
    if enabled() {
        limits_and_exit().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reports_oom_kills() {
    if enabled() {
        oom_kill().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ships_images_through_export_and_import() {
    if enabled() {
        export_and_import().await;
    }
}

async fn export_and_import() {
    let state = tempfile::tempdir().unwrap();
    let runtime = runtime(state.path()).await;
    runtime.pull(IMAGE).await.unwrap();
    let mut exported = tokio::time::timeout(EVENT_TIMEOUT, runtime.export_image(IMAGE))
        .await
        .expect("export did not open")
        .unwrap();
    let mut archive = Vec::new();
    while let Some(chunk) = tokio::time::timeout(EVENT_TIMEOUT, exported.recv())
        .await
        .unwrap_or_else(|_| panic!("export stalled after {} bytes", archive.len()))
    {
        archive.extend(chunk.unwrap());
    }
    assert!(
        archive.len() > 1 << 20,
        "archive is only {} bytes",
        archive.len()
    );
    assert!(
        archive[archive.len() - 1024..]
            .iter()
            .all(|byte| *byte == 0),
        "export ended before the tar trailer"
    );

    runtime.remove_image(IMAGE).await.unwrap();
    assert!(!runtime.has_image(IMAGE).await.unwrap());
    let (chunks, receiver) = tokio::sync::mpsc::channel(4);
    let feeder = tokio::spawn(async move {
        for piece in archive.chunks(100_000) {
            chunks.send(piece.to_vec()).await.unwrap();
        }
    });
    tokio::time::timeout(EVENT_TIMEOUT, runtime.import_image(IMAGE, receiver))
        .await
        .expect("import stalled")
        .unwrap();
    feeder.await.unwrap();
    assert!(runtime.has_image(IMAGE).await.unwrap());

    let spec = container("imported-1", &["true"], ByteSize::new(64 << 20));
    let mut events = runtime.events().await.unwrap();
    runtime.start(&spec).await.unwrap();
    assert!(matches!(
        next_event_for(&mut events, "imported-1").await,
        RuntimeEvent::Exited { exit_code: 0, .. }
    ));
    runtime.remove("imported-1").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn execs_commands_in_running_containers() {
    if enabled() {
        exec_in_container().await;
    }
}

async fn exec_in_container() {
    let state = tempfile::tempdir().unwrap();
    let runtime = runtime(state.path()).await;
    runtime.pull(IMAGE).await.unwrap();
    let spec = container("exec-1", &["sleep", "3600"], ByteSize::new(64 << 20));
    runtime.start(&spec).await.unwrap();

    let mut session = runtime
        .exec(ExecRequest {
            container_id: "exec-1".to_string(),
            argv: vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo out; echo err >&2; exit 3".to_string(),
            ],
            tty: false,
            size: None,
        })
        .await
        .unwrap();
    let exit = tokio::time::timeout(EVENT_TIMEOUT, &mut session.exit)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let output = drain(&mut session.output).await;
    assert_eq!(exit, 3);
    assert!(
        output.contains("out") && output.contains("err"),
        "{output:?}"
    );
    runtime
        .remove_exec(&session.container_id, &session.exec_id)
        .await;

    let mut session = runtime
        .exec(ExecRequest {
            container_id: "exec-1".to_string(),
            argv: vec!["sh".to_string()],
            tty: true,
            size: Some((120, 40)),
        })
        .await
        .unwrap();
    let stdin = session.stdin.as_mut().unwrap();
    tokio::io::AsyncWriteExt::write_all(stdin, b"stty size; exit 5\n")
        .await
        .unwrap();
    let exit = tokio::time::timeout(EVENT_TIMEOUT, &mut session.exit)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let output = drain(&mut session.output).await;
    assert_eq!(exit, 5);
    assert!(output.contains("40 120"), "{output:?}");
    runtime
        .remove_exec(&session.container_id, &session.exec_id)
        .await;
    runtime
        .stop("exec-1", Duration::from_secs(1))
        .await
        .unwrap();
    runtime.remove("exec-1").await.unwrap();
}

async fn drain(output: &mut tokio::sync::mpsc::Receiver<Vec<u8>>) -> String {
    let mut collected = Vec::new();
    while let Ok(Some(chunk)) =
        tokio::time::timeout(Duration::from_millis(300), output.recv()).await
    {
        collected.extend(chunk);
    }
    String::from_utf8_lossy(&collected).to_string()
}

async fn limits_and_exit() {
    let state = tempfile::tempdir().unwrap();
    let runtime = runtime(state.path()).await;
    runtime.pull(IMAGE).await.unwrap();
    let mut events = runtime.events().await.unwrap();

    let spec = container("limits-1", &["sleep", "3600"], ByteSize::new(64 << 20));
    let started = runtime.start(&spec).await.unwrap();
    assert!(
        started
            .ip
            .is_some_and(|ip| ip.to_string().starts_with("10.254.7."))
    );

    assert_eq!(cgroup_file("limits-1", "cpu.max"), "25000 100000");
    assert_eq!(
        cgroup_file("limits-1", "memory.max"),
        (64u64 << 20).to_string()
    );
    assert_eq!(cgroup_file("limits-1", "memory.swap.max"), "0");
    assert_eq!(cgroup_file("limits-1", "pids.max"), "128");

    let listed = runtime.list().await.unwrap();
    let status = listed
        .iter()
        .find(|status| status.id == "limits-1")
        .unwrap();
    assert_eq!(status.state, ContainerState::Running);
    assert_eq!(status.labels["maestro.test"], "true");

    runtime
        .stop("limits-1", Duration::from_secs(2))
        .await
        .unwrap();
    assert!(matches!(
        next_event_for(&mut events, "limits-1").await,
        RuntimeEvent::Exited { .. }
    ));
    runtime.remove("limits-1").await.unwrap();
    assert!(
        runtime
            .list()
            .await
            .unwrap()
            .iter()
            .all(|status| status.id != "limits-1")
    );
}

async fn oom_kill() {
    let state = tempfile::tempdir().unwrap();
    let runtime = runtime(state.path()).await;
    runtime.pull(IMAGE).await.unwrap();
    let mut events = runtime.events().await.unwrap();

    let spec = container(
        "oom-1",
        &["sh", "-c", "head -c 200m /dev/zero | tail"],
        ByteSize::new(32 << 20),
    );
    runtime.start(&spec).await.unwrap();
    let mut saw_oom = false;
    let exit_code = loop {
        match next_event_for(&mut events, "oom-1").await {
            RuntimeEvent::OomKilled { .. } => saw_oom = true,
            RuntimeEvent::Exited { exit_code, .. } => break exit_code,
        }
    };
    assert!(saw_oom, "expected an OOM event before exit");
    assert_ne!(exit_code, 0);
    runtime.remove("oom-1").await.unwrap();
}
