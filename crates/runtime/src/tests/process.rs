use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, WorkloadId};
use supervisor::ProcessSupervisor;

use crate::conformance::{
    RunningWorkloadFixture, WorkloadRuntimeFixture, assert_running_workload_adoptable,
    exercise_running_workload, exercise_workload_runtime,
};
use crate::{
    ProcessRuntime, ProcessRuntimeSettings, ProcessWorkload, TokioRuntimeClock,
    WorkloadConfiguration, WorkloadMetadata, WorkloadRuntime, WorkloadSpec,
};

#[tokio::test]
async fn process_backend_passes_workload_runtime_conformance() {
    let root = tempfile::tempdir().unwrap();
    let runtime = process_runtime(root.path());
    let fixture = WorkloadRuntimeFixture {
        spec: process_spec("/bin/sleep", &["30"]),
        conflicting_spec: process_spec("/bin/false", &[]),
    };

    exercise_workload_runtime(&runtime, &fixture).await.unwrap();
}

#[tokio::test]
async fn process_backend_streams_logs_and_adopts_across_runtime_restart() {
    let root = tempfile::tempdir().unwrap();
    let first = process_runtime(root.path());
    let spec = process_spec(
        "/bin/sh",
        &[
            "-c",
            "printf process-stdout; printf process-stderr >&2; exec /bin/sleep 30",
        ],
    );
    let handle = first.create(&spec).await.unwrap();
    first.start(&handle).await.unwrap();
    let stats = first.stats(&handle).await.unwrap();
    assert!(
        stats
            .cgroup_path()
            .is_some_and(|path| path.as_path().is_absolute())
    );

    drop(first);

    let replacement = process_runtime(root.path());
    let fixture = RunningWorkloadFixture {
        cluster_id: ClusterId::new("cluster-1").unwrap(),
        node_id: NodeId::new("node-1").unwrap(),
        stdout_marker: b"process-stdout".to_vec(),
        stderr_marker: b"process-stderr".to_vec(),
        exec: None,
        timeout: Duration::from_secs(5),
    };
    assert_running_workload_adoptable(&replacement, &handle, &fixture)
        .await
        .unwrap();
    exercise_running_workload(&replacement, &handle, &fixture)
        .await
        .unwrap();
}

#[tokio::test]
async fn process_mutations_are_serialized_per_workload_only() {
    let root = tempfile::tempdir().unwrap();
    let runtime = Arc::new(process_runtime(root.path()));
    let blocked_workload = WorkloadId::new("workload-1").unwrap();
    let blocked_operation = runtime.operation(&blocked_workload).await.unwrap();

    let unrelated = tokio::time::timeout(
        Duration::from_secs(2),
        runtime.create(&process_spec_for("workload-2", "/bin/true", &[])),
    )
    .await
    .expect("unrelated process mutation waited for another workload")
    .unwrap();

    let blocked_runtime = runtime.clone();
    let mut same_workload = tokio::spawn(async move {
        blocked_runtime
            .create(&process_spec_for("workload-1", "/bin/true", &[]))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut same_workload)
            .await
            .is_err(),
        "same-workload mutation bypassed its operation gate"
    );
    drop(blocked_operation);
    let blocked = tokio::time::timeout(Duration::from_secs(2), same_workload)
        .await
        .expect("same-workload mutation did not resume")
        .unwrap()
        .unwrap();

    runtime.remove(&unrelated).await.unwrap();
    runtime.remove(&blocked).await.unwrap();
}

fn process_runtime(root: &std::path::Path) -> ProcessRuntime {
    ProcessRuntime::new(
        root.to_path_buf(),
        ProcessSupervisor::new(),
        Arc::new(TokioRuntimeClock::new()),
        ProcessRuntimeSettings {
            poll_interval: Duration::from_millis(10),
            kill_timeout: Duration::from_secs(5),
        },
    )
    .unwrap()
}

fn process_spec(executable: &str, arguments: &[&str]) -> WorkloadSpec {
    process_spec_for("workload-1", executable, arguments)
}

fn process_spec_for(workload_id: &str, executable: &str, arguments: &[&str]) -> WorkloadSpec {
    WorkloadSpec::Process(ProcessWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("cluster-1").unwrap(),
                node_id: NodeId::new("node-1").unwrap(),
                service_id: kernel_api::ServiceId::new("api").unwrap(),
                deployment_id: kernel_api::DeploymentId::new("deployment-1").unwrap(),
                assignment_id: AssignmentId::new(format!("assignment-{workload_id}")).unwrap(),
                workload_id: WorkloadId::new(workload_id).unwrap(),
                labels: BTreeMap::new(),
            },
            hostname: workload_id.to_owned(),
            environment: BTreeMap::new(),
            mounts: Vec::new(),
            workload_address: None,
            dns_server: None,
            user: None,
            capabilities: Default::default(),
        },
        command: CommandSpec {
            executable: executable.to_owned(),
            arguments: arguments
                .iter()
                .map(|argument| (*argument).to_owned())
                .collect(),
        },
    })
}
