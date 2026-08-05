use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, Timestamp, WorkloadId};
use kernel_store::{Clock, MonotonicTime};
use runtime::{
    FakeRuntime, FakeRuntimeOperation, LogCursor, LogSource, ProcessWorkload, RuntimeError,
    WorkloadConfiguration, WorkloadMetadata, WorkloadRuntime, WorkloadSpec,
};

use crate::{
    FileLogCheckpointStore, LogCheckpointStore, RuntimeLogAgent, RuntimeLogAgentError,
    RuntimeLogAgentSettings, RuntimeLogFailureStage, StatusClock, WorkloadLogEntry,
    WorkloadLogSink, WorkloadLogSinkError,
};
use tokio::sync::{Notify, watch};

#[tokio::test]
async fn log_agent_run_collects_immediately_and_owns_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    runtime.append_log(
        &workload_id("workload-1"),
        LogSource::Stdout,
        b"ready\n".to_vec(),
    )?;
    let sink = Arc::new(RecordingSink::default());
    let agent = agent(
        runtime,
        &temporary.path().join("checkpoints"),
        sink.clone(),
        16,
    )?;
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move { agent.run(receiver).await });

    sink.wait_for_entry().await;
    assert_eq!(sink.entries().len(), 1);
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[tokio::test]
async fn log_agent_run_retries_a_transient_runtime_snapshot_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    runtime.append_log(
        &workload_id("workload-1"),
        LogSource::Stdout,
        b"after recovery\n".to_vec(),
    )?;
    runtime.fail_next(
        FakeRuntimeOperation::List,
        RuntimeError::Unavailable {
            message: "injected snapshot outage".to_owned(),
        },
    )?;
    let sink = Arc::new(RecordingSink::default());
    let clock = Arc::new(ManualClock::default());
    let agent = RuntimeLogAgent::new(
        runtime,
        Arc::new(FileLogCheckpointStore::new(
            temporary.path().join("checkpoints"),
        )?),
        sink.clone(),
        RuntimeLogAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id(),
            max_frames_per_workload: 16,
            poll_interval: Duration::from_secs(1),
        },
        Arc::new(FixedClock),
        clock.clone(),
    )?;
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move { agent.run(receiver).await });

    wait_for_sleeps(&clock, 1).await?;
    assert!(!task.is_finished());
    assert!(sink.entries().is_empty());
    let delivered = sink.delivered.notified();
    clock.advance(Duration::from_secs(1));
    delivered.await;
    assert_eq!(sink.entries().len(), 1);

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[tokio::test]
async fn log_agent_resumes_from_durable_cursors_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("log-checkpoints");
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    let first = runtime.append_log(
        &workload_id("workload-1"),
        LogSource::Stdout,
        b"first\n".to_vec(),
    )?;
    let second = runtime.append_log(
        &workload_id("workload-1"),
        LogSource::Stderr,
        b"second\n".to_vec(),
    )?;
    let initial_sink = Arc::new(RecordingSink::default());
    let initial_agent = agent(runtime.clone(), &root, initial_sink.clone(), 16)?;

    let first_report = initial_agent.collect_once().await?;
    assert_eq!(first_report.observed, 1);
    assert_eq!(first_report.delivered, 2);
    assert!(first_report.failures.is_empty());
    assert_eq!(initial_agent.collect_once().await?.delivered, 0);
    let initial_entries = initial_sink.entries();
    let mut initial_entries = initial_entries.iter();
    let first_entry = initial_entries.next().expect("first initial entry");
    let second_entry = initial_entries.next().expect("second initial entry");
    assert!(initial_entries.next().is_none());
    assert_eq!(first_entry.cursor, first);
    assert_eq!(second_entry.cursor, second);
    assert_eq!(first_entry.metadata.assignment_id.as_str(), "workload-1");
    assert_eq!(first_entry.received_at, Timestamp(1_750_000_000_000));

    let third = runtime.append_log(
        &workload_id("workload-1"),
        LogSource::Stdout,
        b"third\n".to_vec(),
    )?;
    let restarted_sink = Arc::new(RecordingSink::default());
    let restarted = agent(runtime, &root, restarted_sink.clone(), 16)?;
    assert_eq!(restarted.collect_once().await?.delivered, 1);
    let restarted_entries = restarted_sink.entries();
    assert_eq!(restarted_entries.len(), 1);
    assert_eq!(
        restarted_entries.first().expect("restarted entry").cursor,
        third
    );
    Ok(())
}

#[tokio::test]
async fn log_agent_replays_when_sink_delivery_did_not_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("log-checkpoints");
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    let cursor = runtime.append_log(
        &workload_id("workload-1"),
        LogSource::Stdout,
        b"retry me\n".to_vec(),
    )?;
    let failing_sink = Arc::new(RecordingSink {
        fail_next: AtomicBool::new(true),
        ..Default::default()
    });
    let first_agent = agent(runtime.clone(), &root, failing_sink, 16)?;

    let failed = first_agent.collect_once().await?;
    assert_eq!(failed.delivered, 0);
    assert_eq!(failed.failures.len(), 1);
    assert_eq!(
        failed.failures.first().expect("sink failure").stage,
        RuntimeLogFailureStage::Deliver
    );
    let store = FileLogCheckpointStore::new(root.clone())?;
    assert_eq!(store.load(&workload_id("workload-1")).await?, None);

    let healthy_sink = Arc::new(RecordingSink::default());
    let second_agent = agent(runtime, &root, healthy_sink.clone(), 16)?;
    assert_eq!(second_agent.collect_once().await?.delivered, 1);
    assert_eq!(
        healthy_sink
            .entries()
            .first()
            .expect("replayed entry")
            .cursor,
        cursor
    );
    Ok(())
}

#[tokio::test]
async fn log_agent_bounds_workloads_and_isolates_stream_open_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    create_and_start(runtime.as_ref(), "workload-2").await?;
    runtime.append_log(
        &workload_id("workload-1"),
        LogSource::Stdout,
        b"one-a\n".to_vec(),
    )?;
    runtime.append_log(
        &workload_id("workload-1"),
        LogSource::Stdout,
        b"one-b\n".to_vec(),
    )?;
    runtime.append_log(
        &workload_id("workload-2"),
        LogSource::Stdout,
        b"two\n".to_vec(),
    )?;
    runtime.fail_next(
        FakeRuntimeOperation::Logs,
        RuntimeError::Unavailable {
            message: "injected stream failure".to_owned(),
        },
    )?;
    let sink = Arc::new(RecordingSink::default());
    let root = temporary.path().join("log-checkpoints");
    let first_agent = agent(runtime.clone(), &root, sink.clone(), 1)?;

    let first = first_agent.collect_once().await?;
    assert_eq!(first.observed, 2);
    assert_eq!(first.delivered, 1);
    assert_eq!(first.saturated, 1);
    assert_eq!(first.failures.len(), 1);
    let failure = first.failures.first().expect("stream failure");
    assert_eq!(failure.workload_id, workload_id("workload-1"));
    assert_eq!(failure.stage, RuntimeLogFailureStage::OpenStream);

    let second = first_agent.collect_once().await?;
    assert_eq!(second.delivered, 1);
    assert_eq!(second.saturated, 1);
    let third = first_agent.collect_once().await?;
    assert_eq!(third.delivered, 1);
    assert_eq!(third.saturated, 1);
    assert_eq!(first_agent.collect_once().await?.delivered, 0);
    assert_eq!(sink.entries().len(), 3);
    Ok(())
}

#[tokio::test]
async fn log_agent_cleans_stale_checkpoints_and_rejects_invalid_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("log-checkpoints");
    let store = FileLogCheckpointStore::new(root.clone())?;
    store
        .commit(&workload_id("removed-workload"), &LogCursor::new("old"))
        .await?;
    let runtime = Arc::new(FakeRuntime::new());
    let sink = Arc::new(RecordingSink::default());
    let cleanup_agent = agent(runtime.clone(), &root, sink.clone(), 16)?;
    assert_eq!(
        cleanup_agent
            .collect_once()
            .await?
            .stale_checkpoints_removed,
        1
    );
    assert_eq!(store.load(&workload_id("removed-workload")).await?, None);

    let invalid = RuntimeLogAgent::new(
        runtime.clone(),
        Arc::new(FileLogCheckpointStore::new(root.clone())?),
        sink.clone(),
        RuntimeLogAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id(),
            max_frames_per_workload: 0,
            poll_interval: Duration::from_secs(1),
        },
        Arc::new(FixedClock),
        Arc::new(PausedClock),
    );
    assert!(matches!(
        invalid,
        Err(RuntimeLogAgentError::InvalidFrameLimit)
    ));
    let invalid_poll = RuntimeLogAgent::new(
        runtime,
        Arc::new(FileLogCheckpointStore::new(root)?),
        sink,
        RuntimeLogAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id(),
            max_frames_per_workload: 16,
            poll_interval: Duration::ZERO,
        },
        Arc::new(FixedClock),
        Arc::new(PausedClock),
    );
    assert!(matches!(
        invalid_poll,
        Err(RuntimeLogAgentError::InvalidPollInterval)
    ));
    Ok(())
}

#[derive(Default)]
struct RecordingSink {
    entries: Mutex<Vec<WorkloadLogEntry>>,
    fail_next: AtomicBool,
    delivered: Notify,
}

impl RecordingSink {
    fn entries(&self) -> Vec<WorkloadLogEntry> {
        self.entries.lock().expect("recording sink lock").clone()
    }

    async fn wait_for_entry(&self) {
        self.delivered.notified().await;
    }
}

#[async_trait]
impl WorkloadLogSink for RecordingSink {
    async fn ingest(&self, entry: WorkloadLogEntry) -> Result<(), WorkloadLogSinkError> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(WorkloadLogSinkError::Unavailable {
                message: "injected sink failure".to_owned(),
            });
        }
        self.entries
            .lock()
            .expect("recording sink lock")
            .push(entry);
        self.delivered.notify_one();
        Ok(())
    }
}

struct FixedClock;

impl StatusClock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(1_750_000_000_000)
    }
}

struct PausedClock;

#[async_trait]
impl Clock for PausedClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::ZERO)
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}

#[derive(Default)]
struct ManualClock {
    milliseconds: AtomicU64,
    sleeps: AtomicU64,
    advanced: Notify,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        let milliseconds = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.milliseconds.fetch_add(milliseconds, Ordering::SeqCst);
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(
            self.milliseconds.load(Ordering::SeqCst),
        ))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        loop {
            let advanced = self.advanced.notified();
            if self.now() >= deadline {
                return;
            }
            advanced.await;
        }
    }
}

async fn wait_for_sleeps(
    clock: &ManualClock,
    expected: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for _attempt in 0..128 {
        if clock.sleeps.load(Ordering::SeqCst) >= expected {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(format!("runtime log agent did not begin sleep {expected}").into())
}

fn agent(
    runtime: Arc<FakeRuntime>,
    root: &std::path::Path,
    sink: Arc<RecordingSink>,
    max_frames_per_workload: usize,
) -> Result<RuntimeLogAgent, RuntimeLogAgentError> {
    let runtime: Arc<dyn WorkloadRuntime> = runtime;
    let checkpoints = Arc::new(FileLogCheckpointStore::new(root.to_path_buf())?);
    RuntimeLogAgent::new(
        runtime,
        checkpoints,
        sink,
        RuntimeLogAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id(),
            max_frames_per_workload,
            poll_interval: Duration::from_secs(1),
        },
        Arc::new(FixedClock),
        Arc::new(PausedClock),
    )
}

async fn create_and_start(
    runtime: &FakeRuntime,
    id: &str,
) -> Result<runtime::WorkloadHandle, RuntimeError> {
    let handle = runtime.create(&process_spec(id)).await?;
    runtime.start(&handle).await?;
    Ok(handle)
}

fn process_spec(id: &str) -> WorkloadSpec {
    WorkloadSpec::Process(ProcessWorkload {
        configuration: WorkloadConfiguration {
            metadata: WorkloadMetadata {
                cluster_id: cluster_id(),
                node_id: node_id(),
                service_id: kernel_api::ServiceId::new("api").expect("service id"),
                deployment_id: kernel_api::DeploymentId::new("deployment-1")
                    .expect("deployment id"),
                assignment_id: AssignmentId::new(id).expect("assignment id"),
                workload_id: workload_id(id),
                labels: BTreeMap::from([("service".to_owned(), "api".to_owned())]),
            },
            hostname: id.to_owned(),
            environment: BTreeMap::new(),
            mounts: Vec::new(),
            workload_address: None,
            dns_server: None,
            user: None,
            capabilities: Default::default(),
        },
        command: CommandSpec {
            executable: "/bin/true".to_owned(),
            arguments: Vec::new(),
        },
    })
}

fn cluster_id() -> ClusterId {
    ClusterId::new("cluster-1").expect("cluster id")
}

fn node_id() -> NodeId {
    NodeId::new("node-1").expect("node id")
}

fn workload_id(value: &str) -> WorkloadId {
    WorkloadId::new(value).expect("workload id")
}
