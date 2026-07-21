#![allow(clippy::expect_used, clippy::unwrap_used)]

#[cfg(all(feature = "docker", feature = "test-util", target_os = "linux"))]
use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, WorkloadId};
#[cfg(all(feature = "docker", feature = "test-util", target_os = "linux"))]
use runtime::conformance::{WorkloadRuntimeFixture, exercise_workload_runtime};
#[cfg(all(feature = "docker", feature = "test-util", target_os = "linux"))]
use runtime::{
    ArtifactReference, ContainerWorkload, DockerRuntime, WorkloadConfiguration, WorkloadMetadata,
    WorkloadSpec,
};
#[cfg(all(feature = "docker", feature = "test-util", target_os = "linux"))]
use std::collections::BTreeMap;

#[cfg(all(feature = "docker", feature = "test-util", target_os = "linux"))]
#[tokio::test]
#[ignore = "requires a Docker daemon and MAESTRO_DOCKER_TEST_IMAGE already present locally"]
async fn docker_backend_passes_workload_runtime_conformance() {
    let image = std::env::var("MAESTRO_DOCKER_TEST_IMAGE")
        .expect("MAESTRO_DOCKER_TEST_IMAGE must name a local image");
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
}

#[cfg(all(feature = "docker", feature = "test-util", target_os = "linux"))]
fn container_spec(image: &str, workload_id: &str, hostname: &str) -> WorkloadSpec {
    WorkloadSpec::Container(ContainerWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("docker-conformance").unwrap(),
                node_id: NodeId::new("node-1").unwrap(),
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
                "trap 'exit 0' TERM; while :; do sleep 60; done".to_owned(),
            ],
        }),
    })
}
