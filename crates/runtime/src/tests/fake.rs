use std::collections::BTreeMap;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, WorkloadId};

use crate::conformance::{
    ExecConformanceFixture, RunningWorkloadFixture, WorkloadRuntimeFixture,
    exercise_running_workload, exercise_workload_runtime,
};
use crate::{
    EventRequest, ExecMode, ExecOutput, ExecRequest, FakeRuntime, FakeRuntimeOperation, LogMode,
    LogRequest, LogSource, ProcessWorkload, RuntimeError, RuntimeEventKind, ShutdownRequest,
    WorkloadConfiguration, WorkloadMetadata, WorkloadRuntime, WorkloadSpec,
};

#[tokio::test]
async fn fake_passes_workload_runtime_conformance() {
    let runtime = FakeRuntime::new();
    exercise_workload_runtime(&runtime, &fixture())
        .await
        .unwrap();
}

#[tokio::test]
async fn fake_passes_running_workload_conformance() {
    let runtime = FakeRuntime::new();
    let fixture = fixture();
    let handle = runtime.create(&fixture.spec).await.unwrap();
    runtime.start(&handle).await.unwrap();
    runtime
        .append_log(
            handle.workload_id(),
            LogSource::Stdout,
            b"runtime-stdout".to_vec(),
        )
        .unwrap();
    runtime
        .append_log(
            handle.workload_id(),
            LogSource::Stderr,
            b"runtime-stderr".to_vec(),
        )
        .unwrap();

    exercise_running_workload(&runtime, &handle, &running_fixture())
        .await
        .unwrap();
}

#[tokio::test]
async fn fake_replays_events_logs_and_exec_without_hiding_event_gaps() {
    let runtime = FakeRuntime::new();
    let fixture = fixture();
    let configuration = fixture.spec.configuration();
    let handle = runtime.create(&fixture.spec).await.unwrap();
    runtime.start(&handle).await.unwrap();
    runtime
        .append_log(handle.workload_id(), LogSource::Stdout, b"ready\n".to_vec())
        .unwrap();

    let mut events = runtime
        .events(EventRequest {
            cluster_id: configuration.metadata.cluster_id.clone(),
            node_id: configuration.metadata.node_id.clone(),
            after: None,
        })
        .await
        .unwrap();
    assert_eq!(
        events.next().await.unwrap().unwrap().kind,
        RuntimeEventKind::Created
    );
    let started = events.next().await.unwrap().unwrap();
    assert_eq!(started.kind, RuntimeEventKind::Started);

    let mut resumed = runtime
        .events(EventRequest {
            cluster_id: configuration.metadata.cluster_id.clone(),
            node_id: configuration.metadata.node_id.clone(),
            after: Some(started.cursor),
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), resumed.next())
            .await
            .is_err()
    );

    let mut logs = runtime
        .logs(
            &handle,
            LogRequest {
                after: None,
                mode: LogMode::Snapshot,
            },
        )
        .await
        .unwrap();
    assert_eq!(logs.next().await.unwrap().unwrap().payload, b"ready\n");

    let mut exec = runtime
        .exec(
            &handle,
            ExecRequest {
                command: CommandSpec {
                    executable: "/bin/echo".to_owned(),
                    arguments: vec!["hello".to_owned()],
                },
                environment: BTreeMap::new(),
                mode: ExecMode::Pipes,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        exec.next().await.unwrap(),
        Some(ExecOutput::Stdout(b"/bin/echo".to_vec()))
    );
    exec.kill().await.unwrap();
    assert_eq!(
        exec.next().await.unwrap(),
        Some(ExecOutput::Exited { code: Some(137) })
    );

    runtime.clear_event_history().unwrap();
    assert_eq!(
        runtime
            .list(
                &configuration.metadata.cluster_id,
                &configuration.metadata.node_id,
            )
            .await
            .unwrap()
            .len(),
        1
    );
    runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(1),
            },
        )
        .await
        .unwrap();
    runtime.remove(&handle).await.unwrap();
    let mut removal_events = runtime
        .events(EventRequest {
            cluster_id: configuration.metadata.cluster_id.clone(),
            node_id: configuration.metadata.node_id.clone(),
            after: None,
        })
        .await
        .unwrap();
    assert_eq!(
        removal_events.next().await.unwrap().unwrap().kind,
        RuntimeEventKind::Exited
    );
    assert_eq!(
        removal_events.next().await.unwrap().unwrap().kind,
        RuntimeEventKind::Removed
    );
}

#[tokio::test]
async fn fake_consumes_failure_injection_once() {
    let runtime = FakeRuntime::new();
    let fixture = fixture();
    runtime
        .fail_next(
            FakeRuntimeOperation::Create,
            RuntimeError::Unavailable {
                message: "injected".to_owned(),
            },
        )
        .unwrap();

    assert!(matches!(
        runtime.create(&fixture.spec).await,
        Err(RuntimeError::Unavailable { .. })
    ));
    let handle = runtime.create(&fixture.spec).await.unwrap();
    runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(1),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn canceled_hang_injection_does_not_block_later_operations() {
    let runtime = FakeRuntime::new();
    let fixture = fixture();
    runtime.hang_next(FakeRuntimeOperation::Create).unwrap();
    {
        let mut future = runtime.create(&fixture.spec);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    }

    assert!(runtime.create(&fixture.spec).await.is_ok());
}

fn fixture() -> WorkloadRuntimeFixture {
    WorkloadRuntimeFixture {
        spec: process_spec("/bin/true"),
        conflicting_spec: process_spec("/bin/false"),
    }
}

fn running_fixture() -> RunningWorkloadFixture {
    RunningWorkloadFixture {
        cluster_id: ClusterId::new("cluster-1").unwrap(),
        node_id: NodeId::new("node-1").unwrap(),
        stdout_marker: b"runtime-stdout".to_vec(),
        stderr_marker: b"runtime-stderr".to_vec(),
        exec: Some(ExecConformanceFixture {
            request: ExecRequest {
                command: CommandSpec {
                    executable: "fake-exec-stdout".to_owned(),
                    arguments: Vec::new(),
                },
                environment: BTreeMap::new(),
                mode: ExecMode::Pipes,
            },
            stdout_marker: b"fake-exec-stdout".to_vec(),
            stderr_marker: None,
        }),
        timeout: Duration::from_secs(1),
    }
}

fn process_spec(executable: &str) -> WorkloadSpec {
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
            arguments: Vec::new(),
        },
    })
}
