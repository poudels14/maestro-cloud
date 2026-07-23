#![cfg(unix)]
#![allow(clippy::unwrap_used)]

#[path = "support/fixture.rs"]
mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{AssignmentId, AssignmentPhase, CommandSpec, DeploymentPhase, WorkloadId};
use node_agent::{
    FileLogCheckpointStore, HealthAgent, HealthAgentSettings, HealthProbeError, HealthProbeTarget,
    HealthProber, NodeExecService, NodeExecSettings, RuntimeLogAgent, RuntimeLogAgentSettings,
    WorkloadLogEntry, WorkloadLogSink, WorkloadLogSinkError,
};
use runtime::{
    ExecMode, ExecOutput, ExecRequest, FakeRuntime, LogSource, WorkloadRuntime, WorkloadState,
};

use support::{ExitWorld, FakeNetworkProvider, WORKLOAD_ADDRESS, cluster_id, node_id};

#[tokio::test]
async fn handwritten_assignment_survives_agent_and_node_restarts()
-> Result<(), Box<dyn std::error::Error>> {
    let world = ExitWorld::new();
    world.seed().await?;
    let runtime = Arc::new(FakeRuntime::new());
    let network = Arc::new(FakeNetworkProvider::default());

    let started = world
        .assignment_agent(runtime.clone(), network.clone())
        .reconcile_once()
        .await?;
    assert_eq!((started.desired, started.running), (1, 1));
    assert_running(&world, runtime.as_ref()).await?;

    let prober = Arc::new(HealthyProber::default());
    let health = HealthAgent::new(
        world.store.clone(),
        prober.clone(),
        HealthAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id(),
            poll_interval: Duration::from_secs(5),
        },
        world.monotonic_clock.clone(),
        world.status_clock.clone(),
    )?;
    let health_report = health.reconcile_once().await?;
    assert_eq!((health_report.probed, health_report.ready), (1, 1));
    assert_eq!(world.replica().await?.status.phase, DeploymentPhase::Ready);
    assert_eq!(
        prober.targets(),
        vec![HealthProbeTarget::Http {
            address: WORKLOAD_ADDRESS,
            port: 8080,
            path: "/ready".to_owned(),
        }]
    );

    let workload_id = WorkloadId::new("assignment-1")?;
    runtime.append_log(&workload_id, LogSource::Stdout, b"ready\n")?;
    let sink = Arc::new(MemoryLogSink::default());
    log_agent(&world, runtime.clone(), sink.clone())
        .collect_once()
        .await?;
    assert_eq!(sink.payloads(), vec![b"ready\n".to_vec()]);

    let exec = NodeExecService::new(
        world.store.clone(),
        runtime.clone(),
        NodeExecSettings {
            cluster_id: cluster_id(),
            node_id: node_id(),
            maximum_sessions: 2,
        },
    )?;
    let mut session = exec
        .open(
            &AssignmentId::new("assignment-1")?,
            ExecRequest {
                command: CommandSpec {
                    executable: "/bin/echo".to_owned(),
                    arguments: vec!["hello".to_owned()],
                },
                environment: BTreeMap::new(),
                mode: ExecMode::Pipes,
            },
        )
        .await?;
    assert_eq!(
        session.next().await?,
        Some(ExecOutput::Stdout(b"/bin/echo".to_vec()))
    );
    assert_eq!(
        session.next().await?,
        Some(ExecOutput::Exited { code: Some(0) })
    );

    let adopted = world
        .assignment_agent(runtime.clone(), network)
        .reconcile_once()
        .await?;
    assert_eq!((adopted.running, adopted.garbage_collected), (1, 0));
    assert_eq!(runtime.list(&cluster_id(), &node_id()).await?.len(), 1);
    runtime.append_log(&workload_id, LogSource::Stderr, b"after-agent-restart\n")?;
    let restarted_sink = Arc::new(MemoryLogSink::default());
    let log_report = log_agent(&world, runtime, restarted_sink.clone())
        .collect_once()
        .await?;
    assert_eq!(log_report.delivered, 1);
    assert_eq!(
        restarted_sink.payloads(),
        vec![b"after-agent-restart\n".to_vec()]
    );

    let recovered_runtime = Arc::new(FakeRuntime::new());
    let recovered = world
        .assignment_agent(
            recovered_runtime.clone(),
            Arc::new(FakeNetworkProvider::default()),
        )
        .reconcile_once()
        .await?;
    assert_eq!((recovered.desired, recovered.running), (1, 1));
    assert_running(&world, recovered_runtime.as_ref()).await?;
    Ok(())
}

async fn assert_running(
    world: &ExitWorld,
    runtime: &FakeRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    let assignment = world.assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Running);
    assert_eq!(
        assignment
            .status
            .workload_id
            .as_ref()
            .map(WorkloadId::as_str),
        Some("assignment-1")
    );
    let workloads = runtime.list(&cluster_id(), &node_id()).await?;
    assert_eq!(workloads.len(), 1);
    assert_eq!(
        workloads
            .first()
            .ok_or("runtime workload missing")?
            .status
            .state,
        WorkloadState::Running
    );
    Ok(())
}

fn log_agent(
    world: &ExitWorld,
    runtime: Arc<FakeRuntime>,
    sink: Arc<MemoryLogSink>,
) -> RuntimeLogAgent {
    RuntimeLogAgent::new(
        runtime,
        Arc::new(FileLogCheckpointStore::new(world.checkpoint_root()).unwrap()),
        sink,
        RuntimeLogAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id(),
            max_frames_per_workload: 16,
            poll_interval: Duration::from_secs(1),
        },
        world.status_clock.clone(),
        world.monotonic_clock.clone(),
    )
    .unwrap()
}

#[derive(Default)]
struct HealthyProber(Mutex<Vec<HealthProbeTarget>>);

impl HealthyProber {
    fn targets(&self) -> Vec<HealthProbeTarget> {
        self.0.lock().unwrap().clone()
    }
}

#[async_trait]
impl HealthProber for HealthyProber {
    async fn probe(&self, target: &HealthProbeTarget) -> Result<(), HealthProbeError> {
        self.0.lock().unwrap().push(target.clone());
        Ok(())
    }
}

#[derive(Default)]
struct MemoryLogSink(Mutex<Vec<WorkloadLogEntry>>);

impl MemoryLogSink {
    fn payloads(&self) -> Vec<Vec<u8>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|entry| entry.payload.clone())
            .collect()
    }
}

#[async_trait]
impl WorkloadLogSink for MemoryLogSink {
    async fn ingest(&self, entry: WorkloadLogEntry) -> Result<(), WorkloadLogSinkError> {
        self.0.lock().unwrap().push(entry);
        Ok(())
    }
}
