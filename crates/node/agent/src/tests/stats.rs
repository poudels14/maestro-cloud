use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, Timestamp, WorkloadId};
use kernel_store::{Clock, MonotonicTime};
use runtime::{
    CgroupPath, FakeRuntime, FakeRuntimeOperation, ProcessWorkload, RuntimeError, ShutdownRequest,
    WorkloadConfiguration, WorkloadMetadata, WorkloadRuntime, WorkloadSpec, WorkloadStatsSnapshot,
};

use crate::{
    CgroupCpuStats, CgroupIoStats, CgroupMemoryEvents, CgroupMemoryStats, CgroupProcessStats,
    CgroupStats, CgroupStatsError, CgroupStatsReader, StatusClock, WorkloadNetworkStats,
    WorkloadNetworkStatsError, WorkloadNetworkStatsReader, WorkloadStatsAgent,
    WorkloadStatsFailureStage, WorkloadStatsSample, WorkloadStatsSettings, WorkloadStatsSink,
    WorkloadStatsSinkError,
};
use tokio::sync::{Mutex, Notify, watch};

#[tokio::test]
async fn stats_agent_collects_running_workloads_and_isolates_reader_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(FakeRuntime::new());
    let running = create_and_start(runtime.as_ref(), "workload-1").await?;
    create_and_start(runtime.as_ref(), "workload-2").await?;
    let stopped = create_and_start(runtime.as_ref(), "workload-3").await?;
    runtime
        .stop(
            &stopped,
            ShutdownRequest {
                timeout: std::time::Duration::from_secs(1),
            },
        )
        .await?;
    let reader: Arc<dyn CgroupStatsReader> = Arc::new(SelectiveReader {
        rejected: Some(workload_id("workload-2")),
    });
    let runtime_trait: Arc<dyn WorkloadRuntime> = runtime.clone();
    let sink = Arc::new(RecordingSink::default());
    let agent = WorkloadStatsAgent::new(
        runtime_trait,
        reader,
        Arc::new(FixedNetworkReader::default()),
        sink.clone(),
        settings(),
        Arc::new(FixedClock(Timestamp(1_750_000_000_000))),
        Arc::new(ManualClock::default()),
    )?;

    let report = agent.collect().await?;

    assert_eq!(report.observed, 2);
    assert_eq!(report.samples.len(), 1);
    assert_eq!(report.delivered, 1);
    assert_eq!(sink.batches.lock().await.len(), 1);
    let collected = report.samples.first().expect("one collected sample");
    assert_eq!(collected.metadata.workload_id, workload_id("workload-1"));
    assert_eq!(collected.collected_at, Timestamp(1_750_000_000_000));
    assert_eq!(collected.stats, sample());
    assert_eq!(
        collected.network,
        Some(WorkloadNetworkStats {
            receive_bytes: 100,
            transmit_bytes: 200,
        })
    );
    assert_eq!(report.failures.len(), 1);
    let failure = report.failures.first().expect("one failed sample");
    assert_eq!(failure.workload_id, workload_id("workload-2"));
    assert_eq!(failure.stage, WorkloadStatsFailureStage::ReadCgroup);
    assert_eq!(running.workload_id(), &workload_id("workload-1"));
    Ok(())
}

#[tokio::test]
async fn stats_agent_isolates_handle_failures_but_not_snapshot_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    runtime.fail_next(
        FakeRuntimeOperation::Stats,
        RuntimeError::Unavailable {
            message: "cgroup lookup unavailable".to_owned(),
        },
    )?;
    let runtime_trait: Arc<dyn WorkloadRuntime> = runtime.clone();
    let agent = WorkloadStatsAgent::new(
        runtime_trait,
        Arc::new(SelectiveReader { rejected: None }),
        Arc::new(FixedNetworkReader::default()),
        Arc::new(RecordingSink::default()),
        settings(),
        Arc::new(FixedClock(Timestamp(1))),
        Arc::new(ManualClock::default()),
    )?;

    let report = agent.collect().await?;
    assert!(report.samples.is_empty());
    assert_eq!(report.failures.len(), 1);
    let failure = report.failures.first().expect("one failed sample");
    assert_eq!(failure.stage, WorkloadStatsFailureStage::ReadRuntime);

    runtime.fail_next(
        FakeRuntimeOperation::List,
        RuntimeError::Unavailable {
            message: "runtime snapshot unavailable".to_owned(),
        },
    )?;
    assert!(agent.collect().await.is_err());
    Ok(())
}

#[tokio::test]
async fn stats_agent_isolates_sink_failure_and_retries_on_the_next_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    let sink = Arc::new(RecordingSink::default());
    sink.fail_next.store(true, Ordering::Release);
    let agent = WorkloadStatsAgent::new(
        runtime,
        Arc::new(SelectiveReader { rejected: None }),
        Arc::new(FixedNetworkReader::default()),
        sink.clone(),
        settings(),
        Arc::new(FixedClock(Timestamp(1))),
        Arc::new(ManualClock::default()),
    )?;

    let failed = agent.collect().await?;
    assert_eq!(failed.delivered, 0);
    assert!(matches!(
        failed.delivery_failure,
        Some(WorkloadStatsSinkError::Unavailable { .. })
    ));
    assert!(sink.batches.lock().await.is_empty());

    let recovered = agent.collect().await?;
    assert_eq!(recovered.delivered, 1);
    assert!(recovered.delivery_failure.is_none());
    assert_eq!(sink.batches.lock().await.len(), 1);
    Ok(())
}

#[tokio::test]
async fn stats_agent_retains_cgroup_sample_when_optional_network_reading_fails()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    let sink = Arc::new(RecordingSink::default());
    let agent = WorkloadStatsAgent::new(
        runtime,
        Arc::new(SelectiveReader { rejected: None }),
        Arc::new(FixedNetworkReader { fail: true }),
        sink,
        settings(),
        Arc::new(FixedClock(Timestamp(1))),
        Arc::new(ManualClock::default()),
    )?;

    let report = agent.collect().await?;

    assert_eq!(report.samples.len(), 1);
    assert_eq!(
        report
            .samples
            .first()
            .ok_or("collected sample missing")?
            .network,
        None
    );
    assert_eq!(report.delivered, 1);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(
        report
            .failures
            .first()
            .ok_or("network collection failure missing")?
            .stage,
        WorkloadStatsFailureStage::ReadNetwork
    );
    Ok(())
}

#[tokio::test]
async fn stats_agent_uses_one_runtime_native_snapshot_without_host_readers()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    runtime.set_stats_snapshot(WorkloadStatsSnapshot {
        resources: sample(),
        network: Some(WorkloadNetworkStats {
            receive_bytes: 300,
            transmit_bytes: 500,
        }),
    });
    let agent = WorkloadStatsAgent::new(
        runtime,
        Arc::new(SelectiveReader {
            rejected: Some(workload_id("workload-1")),
        }),
        Arc::new(FixedNetworkReader { fail: true }),
        Arc::new(RecordingSink::default()),
        settings(),
        Arc::new(FixedClock(Timestamp(1))),
        Arc::new(ManualClock::default()),
    )?;

    let report = agent.collect().await?;

    assert!(report.failures.is_empty());
    assert_eq!(report.delivered, 1);
    let collected = report.samples.first().ok_or("native sample missing")?;
    assert_eq!(collected.stats, sample());
    assert_eq!(
        collected.network,
        Some(WorkloadNetworkStats {
            receive_bytes: 300,
            transmit_bytes: 500,
        })
    );
    Ok(())
}

#[tokio::test]
async fn stats_agent_runs_immediately_then_on_each_injected_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    let sink = Arc::new(RecordingSink::default());
    let clock = Arc::new(ManualClock::default());
    let agent = WorkloadStatsAgent::new(
        runtime,
        Arc::new(SelectiveReader { rejected: None }),
        Arc::new(FixedNetworkReader::default()),
        sink.clone(),
        settings(),
        Arc::new(FixedClock(Timestamp(1))),
        clock.clone(),
    )?;
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let first_delivery = sink.delivered.notified();
    let task = tokio::spawn(async move { agent.run(shutdown_receiver).await });

    first_delivery.await;
    assert_eq!(sink.batches.lock().await.len(), 1);
    let second_delivery = sink.delivered.notified();
    clock.advance(Duration::from_secs(5));
    second_delivery.await;
    assert_eq!(sink.batches.lock().await.len(), 2);

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[tokio::test]
async fn stats_agent_run_retries_a_transient_runtime_snapshot_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(FakeRuntime::new());
    create_and_start(runtime.as_ref(), "workload-1").await?;
    runtime.fail_next(
        FakeRuntimeOperation::List,
        RuntimeError::Unavailable {
            message: "injected snapshot outage".to_owned(),
        },
    )?;
    let sink = Arc::new(RecordingSink::default());
    let clock = Arc::new(ManualClock::default());
    let agent = WorkloadStatsAgent::new(
        runtime,
        Arc::new(SelectiveReader { rejected: None }),
        Arc::new(FixedNetworkReader::default()),
        sink.clone(),
        settings(),
        Arc::new(FixedClock(Timestamp(1))),
        clock.clone(),
    )?;
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move { agent.run(receiver).await });

    wait_for_sleeps(&clock, 1).await?;
    assert!(!task.is_finished());
    assert!(sink.batches.lock().await.is_empty());
    let delivered = sink.delivered.notified();
    clock.advance(Duration::from_secs(5));
    delivered.await;
    assert_eq!(sink.batches.lock().await.len(), 1);

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

#[test]
fn stats_agent_rejects_a_zero_poll_interval() {
    let mut settings = settings();
    settings.poll_interval = Duration::ZERO;
    assert!(
        WorkloadStatsAgent::new(
            Arc::new(FakeRuntime::new()),
            Arc::new(SelectiveReader { rejected: None }),
            Arc::new(FixedNetworkReader::default()),
            Arc::new(RecordingSink::default()),
            settings,
            Arc::new(FixedClock(Timestamp(1))),
            Arc::new(ManualClock::default()),
        )
        .is_err()
    );
}

#[derive(Default)]
struct RecordingSink {
    batches: Mutex<Vec<Vec<WorkloadStatsSample>>>,
    delivered: Notify,
    fail_next: AtomicBool,
}

#[async_trait]
impl WorkloadStatsSink for RecordingSink {
    async fn ingest(&self, samples: &[WorkloadStatsSample]) -> Result<(), WorkloadStatsSinkError> {
        if self.fail_next.swap(false, Ordering::AcqRel) {
            return Err(WorkloadStatsSinkError::Unavailable {
                message: "injected sink outage".to_owned(),
            });
        }
        self.batches.lock().await.push(samples.to_vec());
        self.delivered.notify_one();
        Ok(())
    }
}

#[derive(Default)]
struct ManualClock {
    now_millis: AtomicU64,
    sleeps: AtomicU64,
    advanced: Notify,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.now_millis.fetch_add(millis, Ordering::AcqRel);
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(
            self.now_millis.load(Ordering::Acquire),
        ))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        self.sleeps.fetch_add(1, Ordering::AcqRel);
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
        if clock.sleeps.load(Ordering::Acquire) >= expected {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(format!("workload stats agent did not begin sleep {expected}").into())
}

struct SelectiveReader {
    rejected: Option<WorkloadId>,
}

#[derive(Default)]
struct FixedNetworkReader {
    fail: bool,
}

#[async_trait]
impl WorkloadNetworkStatsReader for FixedNetworkReader {
    async fn read(
        &self,
        _workload: &runtime::WorkloadHandle,
    ) -> Result<Option<WorkloadNetworkStats>, WorkloadNetworkStatsError> {
        if self.fail {
            Err(WorkloadNetworkStatsError::InvalidInterface {
                name: "injected".to_owned(),
            })
        } else {
            Ok(Some(WorkloadNetworkStats {
                receive_bytes: 100,
                transmit_bytes: 200,
            }))
        }
    }
}

#[async_trait]
impl CgroupStatsReader for SelectiveReader {
    async fn read(&self, path: &CgroupPath) -> Result<CgroupStats, CgroupStatsError> {
        if self
            .rejected
            .as_ref()
            .is_some_and(|workload_id| path.as_path().ends_with(workload_id.as_str()))
        {
            Err(CgroupStatsError::InvalidValue {
                file: "cpu.stat",
                value: "injected".to_owned(),
            })
        } else {
            Ok(sample())
        }
    }
}

struct FixedClock(Timestamp);

impl StatusClock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

async fn create_and_start(
    runtime: &FakeRuntime,
    id: &str,
) -> Result<runtime::WorkloadHandle, RuntimeError> {
    let spec = process_spec(id);
    let handle = runtime.create(&spec).await?;
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
                labels: BTreeMap::new(),
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

fn sample() -> CgroupStats {
    CgroupStats {
        cpu: CgroupCpuStats {
            usage_usec: 10,
            user_usec: 7,
            system_usec: 3,
            periods: 2,
            throttled_periods: 1,
            throttled_usec: 4,
        },
        memory: CgroupMemoryStats {
            current_bytes: 1024,
            maximum_bytes: None,
            events: CgroupMemoryEvents {
                low: 0,
                high: 0,
                maximum: 0,
                out_of_memory: 0,
                out_of_memory_kills: 0,
                out_of_memory_group_kills: 0,
            },
        },
        io: CgroupIoStats::default(),
        processes: CgroupProcessStats {
            current: 1,
            maximum: Some(32),
        },
    }
}

fn settings() -> WorkloadStatsSettings {
    WorkloadStatsSettings {
        cluster_id: cluster_id(),
        node_id: node_id(),
        poll_interval: Duration::from_secs(5),
    }
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
