use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{AssignmentId, ClusterId, CommandSpec, NodeId, Timestamp, WorkloadId};
use runtime::{
    CgroupPath, FakeRuntime, FakeRuntimeOperation, ProcessWorkload, RuntimeError, ShutdownRequest,
    WorkloadConfiguration, WorkloadMetadata, WorkloadRuntime, WorkloadSpec,
};

use crate::{
    CgroupCpuStats, CgroupIoStats, CgroupMemoryEvents, CgroupMemoryStats, CgroupProcessStats,
    CgroupStats, CgroupStatsError, CgroupStatsReader, StatusClock, WorkloadStatsAgent,
    WorkloadStatsFailureStage, WorkloadStatsSettings,
};

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
    let agent = WorkloadStatsAgent::new(
        runtime_trait,
        reader,
        settings(),
        Arc::new(FixedClock(Timestamp(1_750_000_000_000))),
    );

    let report = agent.collect().await?;

    assert_eq!(report.observed, 2);
    assert_eq!(report.samples.len(), 1);
    let collected = report.samples.first().expect("one collected sample");
    assert_eq!(collected.metadata.workload_id, workload_id("workload-1"));
    assert_eq!(collected.collected_at, Timestamp(1_750_000_000_000));
    assert_eq!(collected.stats, sample());
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
        FakeRuntimeOperation::StatsHandle,
        RuntimeError::Unavailable {
            message: "cgroup lookup unavailable".to_owned(),
        },
    )?;
    let runtime_trait: Arc<dyn WorkloadRuntime> = runtime.clone();
    let agent = WorkloadStatsAgent::new(
        runtime_trait,
        Arc::new(SelectiveReader { rejected: None }),
        settings(),
        Arc::new(FixedClock(Timestamp(1))),
    );

    let report = agent.collect().await?;
    assert!(report.samples.is_empty());
    assert_eq!(report.failures.len(), 1);
    let failure = report.failures.first().expect("one failed sample");
    assert_eq!(failure.stage, WorkloadStatsFailureStage::ResolveCgroup);

    runtime.fail_next(
        FakeRuntimeOperation::List,
        RuntimeError::Unavailable {
            message: "runtime snapshot unavailable".to_owned(),
        },
    )?;
    assert!(agent.collect().await.is_err());
    Ok(())
}

struct SelectiveReader {
    rejected: Option<WorkloadId>,
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
