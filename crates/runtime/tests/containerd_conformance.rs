#![allow(clippy::expect_used, clippy::unwrap_used)]

#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use std::collections::BTreeMap;
#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use std::path::PathBuf;
#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use std::sync::Arc;
#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use std::time::Duration;

#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, WorkloadId};
#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use runtime::conformance::{
    ExecConformanceFixture, RunningWorkloadFixture, WorkloadRuntimeFixture,
    assert_running_workload_adoptable, exercise_running_workload, exercise_workload_runtime,
};
#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use runtime::{
    ArtifactReference, ContainerWorkload, ContainerdRuntime, ContainerdRuntimeSettings, ExecMode,
    ExecRequest, TokioRuntimeClock, WorkloadConfiguration, WorkloadMetadata, WorkloadRuntime,
    WorkloadSpec,
};

#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
#[tokio::test]
#[ignore = "requires containerd and registry access for MAESTRO_CONTAINERD_TEST_IMAGE"]
async fn containerd_backend_passes_workload_runtime_conformance() {
    let image = std::env::var("MAESTRO_CONTAINERD_TEST_IMAGE")
        .expect("MAESTRO_CONTAINERD_TEST_IMAGE must name a pullable image");
    let temporary_root = tempfile::tempdir().unwrap();
    let process_id = std::process::id();
    let settings = ContainerdRuntimeSettings {
        socket: std::env::var_os("MAESTRO_CONTAINERD_SOCKET").map_or_else(
            || PathBuf::from("/run/containerd/containerd.sock"),
            PathBuf::from,
        ),
        namespace: std::env::var("MAESTRO_CONTAINERD_NAMESPACE")
            .unwrap_or_else(|_| "maestro-test".to_owned()),
        snapshotter: std::env::var("MAESTRO_CONTAINERD_SNAPSHOTTER")
            .unwrap_or_else(|_| "overlayfs".to_owned()),
        state_root: temporary_root.path().to_path_buf(),
        ..ContainerdRuntimeSettings::default()
    };
    let runtime = ContainerdRuntime::connect(settings.clone(), Arc::new(TokioRuntimeClock::new()))
        .await
        .unwrap();
    let workload_id = format!("containerd-conformance-{process_id}");
    let spec = container_spec(&image, &workload_id, "containerd-conformance");
    let conflicting_spec = container_spec(&image, &workload_id, "containerd-conflict");

    exercise_workload_runtime(
        &runtime,
        &WorkloadRuntimeFixture {
            spec,
            conflicting_spec,
        },
    )
    .await
    .unwrap();

    let streaming_id = format!("containerd-streaming-{process_id}");
    let streaming_spec = container_spec(&image, &streaming_id, "containerd-streaming");
    let handle = runtime.create(&streaming_spec).await.unwrap();
    runtime.start(&handle).await.unwrap();
    let replacement = ContainerdRuntime::connect(settings, Arc::new(TokioRuntimeClock::new()))
        .await
        .unwrap();
    let fixture = running_fixture("containerd-conformance");
    assert_running_workload_adoptable(&replacement, &handle, &fixture)
        .await
        .unwrap();
    exercise_running_workload(&replacement, &handle, &fixture)
        .await
        .unwrap();
}

#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
fn container_spec(image: &str, workload_id: &str, hostname: &str) -> WorkloadSpec {
    WorkloadSpec::Container(ContainerWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("containerd-conformance").unwrap(),
                node_id: NodeId::new("node-1").unwrap(),
                service_id: kernel_api::ServiceId::new("api").unwrap(),
                deployment_id: kernel_api::DeploymentId::new("deployment-1").unwrap(),
                assignment_id: AssignmentId::new("assignment-1").unwrap(),
                workload_id: WorkloadId::new(workload_id).unwrap(),
                labels: BTreeMap::new(),
            },
            hostname: hostname.to_owned(),
            environment: BTreeMap::new(),
            mounts: Vec::new(),
            workload_address: None,
            dns_server: None,
            user: None,
        },
        image: ArtifactReference::new(image).unwrap(),
        command: Some(CommandSpec {
            executable: "/bin/sh".to_owned(),
            arguments: vec![
                "-c".to_owned(),
                "printf runtime-stdout; printf runtime-stderr >&2; trap 'exit 0' TERM; while :; do sleep 60; done"
                    .to_owned(),
            ],
        }),
    })
}

#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
fn running_fixture(cluster: &str) -> RunningWorkloadFixture {
    RunningWorkloadFixture {
        cluster_id: ClusterId::new(cluster).unwrap(),
        node_id: NodeId::new("node-1").unwrap(),
        stdout_marker: b"runtime-stdout".to_vec(),
        stderr_marker: b"runtime-stderr".to_vec(),
        exec: Some(ExecConformanceFixture {
            request: ExecRequest {
                command: CommandSpec {
                    executable: "/bin/sh".to_owned(),
                    arguments: vec![
                        "-c".to_owned(),
                        "printf exec-stdout; printf exec-stderr >&2".to_owned(),
                    ],
                },
                environment: BTreeMap::new(),
                mode: ExecMode::Pipes,
            },
            stdout_marker: b"exec-stdout".to_vec(),
            stderr_marker: Some(b"exec-stderr".to_vec()),
        }),
        timeout: Duration::from_secs(10),
    }
}
