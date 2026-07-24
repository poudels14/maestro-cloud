use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use build::LocalBuildSourceProvider;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kernel_api::{
    NodeFirewallSpec, NodeId, NodeRole, ResourceKind, ResourceName, WorkloadUserSpec,
};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock,
};
use logs::{InMemoryLogStoreRuntime, LiveControllerStats, LogStoreRuntime, SinkRuntimeRegistry};
use metrics::{InMemoryMetricStoreRuntime, MetricStoreRuntime};
use node_agent::{MeshConfiguration, MeshIdentity, WorkloadBridge};
use runtime::{ArtifactStore, FakeNetworkProvider, FakeRuntime};
use semver::Version;
use serde::Serialize;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::agent_api::{AgentApiInputs, bind_agent_api};
use crate::{
    AgentStore, DaemonPlan, DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings,
    OperatorSettings,
};

use super::build_backend::FakeBuildBackend;
use super::control_plane::{
    FixedHostDiskReader, FixedHostStatsReader, FixedNetworkStatsReader, FixedStatsReader,
    FixedStatusClock, RecordingBridgeBackend, RecordingDnsBinder, RecordingFirewallBackend,
    RecordingHealthProber, RecordingMeshBackend, test_api_settings,
};
use super::control_plane_resources::{load_assignment, seed_agent_resources};
use super::{cluster_with_nodes, control_plane_store::FakeProvider};

#[tokio::test]
async fn voter_agent_proxies_exec_to_workload_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let api_probe = TcpListener::bind("127.0.0.1:0")?;
    let api_address = api_probe.local_addr()?;
    drop(api_probe);
    let cluster = cluster_with_nodes(&[
        ("master", NodeRole::Master),
        ("hybrid", NodeRole::Hybrid),
        ("voter", NodeRole::ControlPlane),
        ("worker", NodeRole::Worker),
    ])?;
    let voter_id = NodeId::new("voter")?;
    let worker_id = NodeId::new("worker")?;

    let directory = tempfile::tempdir()?;
    let plan = DaemonPlan::new(
        cluster.clone(),
        voter_id.clone(),
        directory.path().to_path_buf(),
    )?;
    let spec = plan.roles().first().ok_or("agent role missing")?;
    assert!(!spec.workload_enabled);

    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let voter_subnet = cluster
        .nodes
        .get(&voter_id)
        .ok_or("voter topology missing")?
        .workload_subnet;
    seed_agent_resources(
        &store,
        &cluster.cluster_id,
        &voter_id,
        voter_subnet,
        Some(WorkloadUserSpec {
            user_id: 1_000,
            group_id: 1_000,
        }),
    )
    .await?;
    move_assignment(&store, &cluster.cluster_id, worker_id).await?;

    let log_runtime = InMemoryLogStoreRuntime::new();
    let metric_runtime = InMemoryMetricStoreRuntime::new();
    let controller_stats = Arc::new(LiveControllerStats::new(
        log_runtime.stats_store(),
        Vec::new(),
        SinkRuntimeRegistry::default(),
        "test",
    ));
    let api_inputs = AgentApiInputs {
        store: store.clone(),
        local_log_queries: log_runtime.query_store(),
        local_traffic_queries: log_runtime.traffic_query_store(),
        workload_metric_queries: metric_runtime.query_store(),
        host_metric_queries: metric_runtime.host_query_store(),
        controller_stats,
        backup_stats: None,
        stats_metric_queries: log_runtime.stats_metric_store(),
    };
    let runtime = Arc::new(FakeRuntime::new());
    let artifacts = Arc::new(FakeBuildBackend::default());
    let factory = DaemonRoleFactory::new(
        DaemonRoleDependencies {
            agent_store: AgentStore::Managed {
                provider: Arc::new(FakeProvider::new(store, Arc::new(Mutex::new(0)))),
                start_mode: cluster::StoreStartMode::Restart,
            },
            mesh_backend: RecordingMeshBackend {
                applications: Arc::new(Mutex::new(Vec::<MeshConfiguration>::new())),
            },
            firewall_backend: RecordingFirewallBackend {
                applications: Arc::new(Mutex::new(Vec::<NodeFirewallSpec>::new())),
            },
            bridge_backend: RecordingBridgeBackend {
                applications: Arc::new(Mutex::new(Vec::<WorkloadBridge>::new())),
            },
            dns_server_binder: Arc::new(RecordingDnsBinder {
                bindings: Arc::new(Mutex::new(Vec::new())),
            }),
            workload_runtime: runtime,
            artifact_store: artifacts.clone() as Arc<dyn ArtifactStore>,
            artifact_archives: Arc::new(LocalBuildSourceProvider::new(
                directory.path().join("workspaces"),
                directory.path().join("archives"),
            )?),
            log_store_runtime: Box::new(log_runtime),
            log_sinks: Vec::new(),
            metric_store_runtime: Box::new(metric_runtime),
            metric_sinks: Vec::new(),
            host_metric_sinks: Vec::new(),
            stats_reader: Arc::new(FixedStatsReader),
            network_stats_reader: Arc::new(FixedNetworkStatsReader),
            host_stats_reader: Arc::new(FixedHostStatsReader),
            host_disk_reader: Arc::new(FixedHostDiskReader),
            network_provider: Arc::new(FakeNetworkProvider::default()),
            health_prober: Arc::new(RecordingHealthProber {
                targets: Arc::new(Mutex::new(Vec::new())),
            }),
            volatile_root: directory.path().join("volatile"),
            mesh_identity: MeshIdentity::load_or_generate(&directory.path().join("mesh"))?,
            instance_id: kernel_api::NodeInstanceId::new("voter-instance")?,
            running_version: Version::new(0, 1, 0),
            monotonic_clock: Arc::new(TokioClock::new()),
            status_clock: Arc::new(FixedStatusClock),
            node_upgrade: None,
            api_settings: test_api_settings(api_address)?,
            firewall_settings: OperatorSettings::production(&cluster)?.firewall,
        },
        DaemonRoleSettings::default(),
    );
    let server = bind_agent_api(&factory, &plan, spec, api_inputs).await?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(server.serve(receiver));

    let endpoint = format!(
        "ws://{api_address}/api/services/api/deployments/deployment-1/assignments/assignment-1/exec"
    );
    let mut request = endpoint.into_client_request()?;
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", operator_token()?).parse()?,
    );
    let (mut socket, response) = tokio_tungstenite::connect_async(request).await?;
    assert_eq!(
        response.status(),
        tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
    );
    let _ = socket.close(None).await;

    shutdown.send(true)?;
    server_task.await??;
    Ok(())
}

async fn move_assignment(
    store: &InMemoryStore,
    cluster_id: &kernel_api::ClusterId,
    node_id: NodeId,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut assignment = load_assignment(store, cluster_id).await?;
    assignment.spec.node_id = node_id;
    let key = Keyspace::new(cluster_id).resource(
        &ResourceKind::new("Assignment")?,
        &ResourceName::new("assignment-1")?,
    );
    let current = store.get(&key).await?.ok_or("assignment missing")?;
    let outcome = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&assignment)?,
            expected: ExpectedVersion::Exact(current.version),
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("assignment update conflicted".into())
    }
}

fn operator_token() -> Result<String, jsonwebtoken::errors::Error> {
    let issued_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &OperatorClaims {
            sub: "voter-exec-test",
            scope: "operator",
            exp: issued_at.saturating_add(60),
        },
        &EncodingKey::from_secret(b"daemon-test-cluster-log-secret-key"),
    )
}

#[derive(Serialize)]
struct OperatorClaims<'a> {
    sub: &'a str,
    scope: &'a str,
    exp: u64,
}
