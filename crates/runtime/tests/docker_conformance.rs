#![allow(clippy::expect_used, clippy::unwrap_used)]

#[cfg(all(feature = "docker", feature = "test-util", unix))]
use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, WorkloadId};
#[cfg(all(feature = "docker", feature = "test-util", unix))]
use runtime::conformance::{WorkloadRuntimeFixture, exercise_workload_runtime};
#[cfg(all(feature = "docker", feature = "test-util", unix))]
use runtime::{
    ArtifactReference, ContainerWorkload, DockerRuntime, ExecMode, ExecRequest,
    WorkloadConfiguration, WorkloadMetadata, WorkloadRuntime, WorkloadSpec,
};
#[cfg(all(feature = "docker", feature = "test-util", unix))]
use std::collections::BTreeMap;
#[cfg(all(feature = "docker", feature = "test-util", unix))]
use std::time::Duration;

#[cfg(all(feature = "docker", feature = "test-util", unix))]
use runtime::conformance::{
    ExecConformanceFixture, RunningWorkloadFixture, assert_running_workload_adoptable,
    exercise_running_workload,
};

#[cfg(all(feature = "docker", feature = "test-util", unix))]
#[tokio::test]
#[ignore = "requires a Docker daemon and registry access for MAESTRO_DOCKER_TEST_IMAGE"]
async fn docker_backend_passes_workload_runtime_conformance() {
    let image = std::env::var("MAESTRO_DOCKER_TEST_IMAGE")
        .expect("MAESTRO_DOCKER_TEST_IMAGE must name a pullable image");
    let workload_id = format!("docker-conformance-{}", std::process::id());
    let spec = container_spec(&image, &workload_id, "docker-conformance");
    let conflicting_spec = container_spec(&image, &workload_id, "docker-conflict");
    let runtime = DockerRuntime::connect_with_defaults().unwrap();

    exercise_workload_runtime(
        &runtime,
        &WorkloadRuntimeFixture {
            spec,
            conflicting_spec,
        },
    )
    .await
    .unwrap();

    let streaming_id = format!("docker-streaming-{}", std::process::id());
    let streaming_spec = container_spec(&image, &streaming_id, "docker-streaming");
    let handle = runtime.create(&streaming_spec).await.unwrap();
    runtime.start(&handle).await.unwrap();
    let replacement = DockerRuntime::connect_with_defaults().unwrap();
    let fixture = running_fixture();
    assert_running_workload_adoptable(&replacement, &handle, &fixture)
        .await
        .unwrap();
    exercise_running_workload(&replacement, &handle, &fixture)
        .await
        .unwrap();
}

#[cfg(all(feature = "docker", feature = "test-util", unix))]
fn container_spec(image: &str, workload_id: &str, hostname: &str) -> WorkloadSpec {
    WorkloadSpec::Container(ContainerWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("docker-conformance").unwrap(),
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

#[cfg(all(feature = "docker", feature = "test-util", unix))]
fn running_fixture() -> RunningWorkloadFixture {
    RunningWorkloadFixture {
        cluster_id: ClusterId::new("docker-conformance").unwrap(),
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
