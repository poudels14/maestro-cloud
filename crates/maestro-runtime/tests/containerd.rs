#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use maestro_core::{ByteSize, Millicores};
use maestro_runtime::{
    CniNetwork, ContainerLimits, ContainerSpec, ContainerState, ContainerdRuntime,
    ContainerdSettings, NetworkMode, Runtime, RuntimeEvent,
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
