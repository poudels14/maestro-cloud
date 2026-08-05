#![allow(clippy::expect_used, clippy::unwrap_used)]

#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use std::collections::BTreeMap;
#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
use std::os::unix::fs::MetadataExt;
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
    ExecOutput, ExecRequest, MountAccess, MountSource, TokioRuntimeClock, WorkloadConfiguration,
    WorkloadMetadata, WorkloadMount, WorkloadRuntime, WorkloadSpec, WorkloadUser,
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
    let spec = container_spec(&runtime, &image, &workload_id, "containerd-conformance").await;
    let conflicting_spec =
        container_spec(&runtime, &image, &workload_id, "containerd-conflict").await;

    exercise_workload_runtime(
        &runtime,
        &WorkloadRuntimeFixture {
            spec,
            conflicting_spec,
        },
    )
    .await
    .unwrap();

    assert_user_namespace_isolation(&runtime, &settings, &image, temporary_root.path()).await;

    let streaming_id = format!("containerd-streaming-{process_id}");
    let streaming_spec =
        container_spec(&runtime, &image, &streaming_id, "containerd-streaming").await;
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
async fn assert_user_namespace_isolation(
    runtime: &ContainerdRuntime,
    settings: &ContainerdRuntimeSettings,
    image: &str,
    temporary_root: &std::path::Path,
) {
    let workload_id = format!("containerd-userns-{}", std::process::id());
    let source = temporary_root.join("idmapped-volume");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("input"), "mounted-secret\n").unwrap();
    let mut spec = container_spec(runtime, image, &workload_id, "containerd-userns").await;
    let WorkloadSpec::Container(workload) = &mut spec else {
        unreachable!();
    };
    workload.configuration.user = Some(WorkloadUser {
        user_id: 0,
        group_id: 0,
    });
    workload.configuration.mounts.push(WorkloadMount {
        source: MountSource::HostPath(source.clone()),
        target: "/maestro-userns".into(),
        access: MountAccess::ReadWrite,
    });
    let namespace = workload.configuration.user_namespace.unwrap();
    let handle = runtime.create(&spec).await.unwrap();
    runtime.start(&handle).await.unwrap();

    let output = exec_stdout(
        runtime,
        &handle,
        "cat /proc/self/uid_map; printf 'input='; cat /maestro-userns/input; printf written > /maestro-userns/output; stat -c 'inside=%u:%g' /maestro-userns/output",
    )
    .await;
    let first_line = output.lines().next().unwrap();
    assert_eq!(
        first_line.split_whitespace().collect::<Vec<_>>(),
        vec![
            namespace.uid.container_id.to_string(),
            namespace.uid.host_id.to_string(),
            namespace.uid.size.to_string(),
        ]
    );
    assert!(output.contains("input=mounted-secret"));
    assert!(output.contains("inside=0:0"));
    let output_metadata = std::fs::metadata(source.join("output")).unwrap();
    assert_eq!(output_metadata.uid(), 0);
    assert_eq!(output_metadata.gid(), 0);

    let channel = containerd::connect(&settings.socket).await.unwrap();
    let mut request =
        containerd::tonic::Request::new(containerd::services::v1::ListTasksRequest::default());
    request
        .metadata_mut()
        .insert("containerd-namespace", settings.namespace.parse().unwrap());
    let tasks = containerd::services::v1::tasks_client::TasksClient::new(channel)
        .list(request)
        .await
        .unwrap()
        .into_inner()
        .tasks;
    let expected_container_id = format!("maestro-{workload_id}");
    let task = tasks
        .iter()
        .find(|task| task.id == expected_container_id || task.container_id == expected_container_id)
        .unwrap();
    let status = std::fs::read_to_string(format!("/proc/{}/status", task.pid)).unwrap();
    let host_uid = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|line| line.split_whitespace().next())
        .and_then(|uid| uid.parse::<u32>().ok())
        .unwrap();
    assert_eq!(host_uid, namespace.uid.host_id);

    runtime.kill(&handle).await.unwrap();
    runtime.remove(&handle).await.unwrap();
}

#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
async fn exec_stdout(
    runtime: &ContainerdRuntime,
    handle: &runtime::WorkloadHandle,
    command: &str,
) -> String {
    let mut session = runtime
        .exec(
            handle,
            ExecRequest {
                command: CommandSpec {
                    executable: "/bin/sh".to_owned(),
                    arguments: vec!["-c".to_owned(), command.to_owned()],
                },
                environment: BTreeMap::new(),
                mode: ExecMode::Pipes,
            },
        )
        .await
        .unwrap();
    let mut stdout = Vec::new();
    while let Some(output) = session.next().await.unwrap() {
        match output {
            ExecOutput::Stdout(payload) => stdout.extend(payload),
            ExecOutput::Stderr(payload) => {
                panic!(
                    "user-namespace acceptance exec wrote stderr: {}",
                    String::from_utf8_lossy(&payload)
                );
            }
            ExecOutput::Exited { code } => assert_eq!(code, Some(0)),
        }
    }
    String::from_utf8(stdout).unwrap()
}

#[cfg(all(feature = "containerd", feature = "test-util", target_os = "linux"))]
async fn container_spec(
    runtime: &ContainerdRuntime,
    image: &str,
    workload_id: &str,
    hostname: &str,
) -> WorkloadSpec {
    let workload_id = WorkloadId::new(workload_id).unwrap();
    let user_namespace = runtime.prepare_user_namespace(&workload_id).await.unwrap();
    WorkloadSpec::Container(ContainerWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("containerd-conformance").unwrap(),
                node_id: NodeId::new("node-1").unwrap(),
                service_id: kernel_api::ServiceId::new("api").unwrap(),
                deployment_id: kernel_api::DeploymentId::new("deployment-1").unwrap(),
                assignment_id: AssignmentId::new("assignment-1").unwrap(),
                workload_id,
                labels: BTreeMap::new(),
            },
            hostname: hostname.to_owned(),
            environment: BTreeMap::new(),
            mounts: Vec::new(),
            workload_address: None,
            dns_server: None,
            user: None,
            user_namespace,
            capabilities: Default::default(),
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
        published_ports: Vec::new(),
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
