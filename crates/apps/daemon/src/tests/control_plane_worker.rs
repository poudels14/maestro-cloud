use std::sync::{Arc, Mutex};

use build::LocalBuildSourceProvider;
use kernel_api::{AssignmentPhase, NodeId, NodeInstanceId, NodeRole};
use kernel_store::InMemoryStore;
use logs::InMemoryLogStoreRuntime;
use metrics::InMemoryMetricStoreRuntime;
use node_agent::MeshIdentity;
use runtime::{ArtifactStore, FakeNetworkProvider, FakeRuntime, WorkloadRuntime};
use semver::Version;

use crate::{
    AgentStore, Daemon, DaemonPlan, DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings,
    OperatorSettings,
};

use super::build_backend::FakeBuildBackend;
use super::cluster_with_nodes;
use super::control_plane::{
    FixedHostDiskReader, FixedHostStatsReader, FixedNetworkStatsReader, FixedStatsReader,
    FixedStatusClock, PausedClock, RecordingBridgeBackend, RecordingDnsBinder,
    RecordingFirewallBackend, RecordingHealthProber, RecordingMeshBackend, test_api_settings,
};
use super::control_plane_resources::{load_assignment, seed_agent_resources};

#[tokio::test]
async fn worker_agent_uses_remote_store_without_starting_a_controller()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(PausedClock::new());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let cluster =
        cluster_with_nodes(&[("master", NodeRole::Master), ("worker", NodeRole::Worker)])?;
    let worker_id = NodeId::new("worker")?;
    let worker = cluster
        .nodes
        .get(&worker_id)
        .ok_or("worker topology missing")?;
    seed_agent_resources(
        &store,
        &cluster.cluster_id,
        &worker_id,
        worker.workload_subnet,
        None,
    )
    .await?;
    let directory = tempfile::tempdir()?;
    let plan = DaemonPlan::new(
        cluster.clone(),
        worker_id.clone(),
        directory.path().to_path_buf(),
    )?;
    let workload_runtime = Arc::new(FakeRuntime::new());
    let network_provider = Arc::new(FakeNetworkProvider::default());
    let factory = DaemonRoleFactory::new(
        DaemonRoleDependencies {
            agent_store: AgentStore::Remote(store.clone()),
            mesh_backend: RecordingMeshBackend {
                applications: Arc::new(Mutex::new(Vec::new())),
            },
            firewall_backend: RecordingFirewallBackend {
                applications: Arc::new(Mutex::new(Vec::new())),
            },
            bridge_backend: RecordingBridgeBackend {
                applications: Arc::new(Mutex::new(Vec::new())),
            },
            dns_server_binder: Arc::new(RecordingDnsBinder {
                bindings: Arc::new(Mutex::new(Vec::new())),
            }),
            workload_runtime: workload_runtime.clone(),
            artifact_store: Arc::new(FakeBuildBackend::default()) as Arc<dyn ArtifactStore>,
            artifact_archives: Arc::new(LocalBuildSourceProvider::new(
                directory.path().join("archive-workspaces"),
                directory.path().join("archives"),
            )?),
            log_store_runtime: Box::new(InMemoryLogStoreRuntime::new()),
            log_sinks: Vec::new(),
            metric_store_runtime: Box::new(InMemoryMetricStoreRuntime::new()),
            metric_sinks: Vec::new(),
            host_metric_sinks: Vec::new(),
            stats_reader: Arc::new(FixedStatsReader),
            network_stats_reader: Arc::new(FixedNetworkStatsReader),
            host_stats_reader: Arc::new(FixedHostStatsReader),
            host_disk_reader: Arc::new(FixedHostDiskReader),
            network_provider: network_provider.clone(),
            health_prober: Arc::new(RecordingHealthProber {
                targets: Arc::new(Mutex::new(Vec::new())),
            }),
            volatile_root: directory.path().join("volatile"),
            mesh_identity: MeshIdentity::load_or_generate(&directory.path().join("mesh"))?,
            instance_id: NodeInstanceId::new("worker-instance")?,
            running_version: Version::new(0, 1, 0),
            monotonic_clock: clock,
            status_clock: Arc::new(FixedStatusClock),
            node_upgrade: None,
            api_settings: test_api_settings("127.0.0.1:0".parse()?)?,
            firewall_settings: OperatorSettings::production(&cluster)?.firewall,
        },
        DaemonRoleSettings::default(),
    );

    let running = Daemon::new(plan, factory).start().await?;
    assert_eq!(
        load_assignment(&store, &cluster.cluster_id)
            .await?
            .status
            .phase,
        AssignmentPhase::Running
    );
    assert_eq!(
        workload_runtime
            .list(&cluster.cluster_id, &worker_id)
            .await?
            .len(),
        1
    );
    assert_eq!(
        (
            network_provider.lease_count(),
            network_provider.attachment_count()
        ),
        (1, 1)
    );
    running.shutdown().await?;
    Ok(())
}
