use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, WorkloadId};
use supervisor::ProcessSupervisor;

use crate::conformance::{WorkloadRuntimeFixture, exercise_workload_runtime};
use crate::{
    EventRequest, LogMode, LogRequest, ProcessRuntime, ProcessRuntimeSettings, ProcessWorkload,
    ShutdownRequest, TokioRuntimeClock, WorkloadConfiguration, WorkloadMetadata, WorkloadRuntime,
    WorkloadSpec, WorkloadState,
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
        &["-c", "printf adopted-output; exec /bin/sleep 30"],
    );
    let handle = first.create(&spec).await.unwrap();
    first.start(&handle).await.unwrap();
    assert!(
        first
            .stats_handle(&handle)
            .await
            .unwrap()
            .as_path()
            .is_absolute()
    );

    let mut events = first
        .events(EventRequest {
            cluster_id: ClusterId::new("cluster-1").unwrap(),
            node_id: NodeId::new("node-1").unwrap(),
            after: None,
        })
        .await
        .unwrap();
    assert!(events.next().await.unwrap().is_some());
    assert!(events.next().await.unwrap().is_some());
    let mut logs = first
        .logs(
            &handle,
            LogRequest {
                after: None,
                mode: LogMode::Follow,
            },
        )
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), logs.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(frame.payload, b"adopted-output");
    drop(first);

    let replacement = process_runtime(root.path());
    let observed = replacement
        .list(
            &ClusterId::new("cluster-1").unwrap(),
            &NodeId::new("node-1").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed.first().unwrap().status.state,
        WorkloadState::Running
    );
    replacement
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(5),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        replacement.status(&handle).await.unwrap().state,
        WorkloadState::Stopped
    );

    replacement.create(&spec).await.unwrap();
    replacement.start(&handle).await.unwrap();
    assert_eq!(
        replacement.status(&handle).await.unwrap().state,
        WorkloadState::Running
    );
    replacement.kill(&handle).await.unwrap();
    replacement.remove(&handle).await.unwrap();
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
    WorkloadSpec::Process(ProcessWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("cluster-1").unwrap(),
                node_id: NodeId::new("node-1").unwrap(),
                service_id: kernel_api::ServiceId::new("api").unwrap(),
                deployment_id: kernel_api::DeploymentId::new("deployment-1").unwrap(),
                assignment_id: AssignmentId::new("assignment-1").unwrap(),
                workload_id: WorkloadId::new("workload-1").unwrap(),
                labels: BTreeMap::new(),
            },
            hostname: "workload-1".to_owned(),
            environment: BTreeMap::new(),
            mounts: Vec::new(),
            workload_address: None,
            dns_server: None,
            user: None,
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
