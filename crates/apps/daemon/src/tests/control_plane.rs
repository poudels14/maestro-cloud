use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cluster::StoreStartMode;
use kernel_api::{
    AssignmentPhase, DeploymentPhase, NodeFirewallSpec, NodeId, NodeInstanceId, NodeRole, Timestamp,
};
use kernel_controller::{FencedStore, LeaderIdentity};
use kernel_store::{Clock, InMemoryStore, Keyspace, MonotonicTime, Store};
use logs::{
    InMemoryLogStoreRuntime, LogBody, LogOrigin, LogSequence, LogSinkId, RecordingLogSink,
    SinkWorkerSettings,
};
use metrics::{
    HostMetricSequence, InMemoryMetricStoreRuntime, MetricSequence, MetricSinkId,
    RecordingHostMetricSink, RecordingMetricSink,
};
use node_agent::{
    AuthoritativeDnsResolver, CgroupCpuStats, CgroupIoStats, CgroupMemoryEvents, CgroupMemoryStats,
    CgroupProcessStats, CgroupStats, CgroupStatsError, CgroupStatsReader, DnsQueryType,
    DnsServerBinder, DnsServerError, DnsServerRuntime, DnsServerSettings, FirewallBackend,
    FirewallBackendError, HealthProbeError, HealthProbeTarget, HealthProber, HostCpuStats,
    HostDiskError, HostDiskReader, HostDiskReport, HostDiskStats, HostMemoryStats,
    HostNetworkStats, HostResourceStats, HostStatsError, HostStatsReader, MeshBackend,
    MeshBackendError, MeshConfiguration, MeshIdentity, StatusClock, WorkloadBridge,
    WorkloadBridgeBackend, WorkloadBridgeBackendError, WorkloadNetworkStats,
    WorkloadNetworkStatsError, WorkloadNetworkStatsReader,
};
use runtime::{CgroupPath, FakeNetworkProvider, FakeRuntime, LogSource, WorkloadRuntime};
use tokio::sync::{Notify, watch};

use crate::{
    AgentStore, Daemon, DaemonPlan, DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings,
    LeaderWorkload, RoleError,
};

use super::cluster_with_nodes;
use super::control_plane_resources::{load_assignment, load_replica, seed_agent_resources};
use super::control_plane_store::FakeProvider;

#[tokio::test]
async fn concrete_roles_establish_mesh_leadership_and_owned_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(PausedClock::new());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let shutdowns = Arc::new(Mutex::new(0_u32));
    let provider = Arc::new(FakeProvider::new(store.clone(), shutdowns.clone()));
    let applications = Arc::new(Mutex::new(Vec::new()));
    let backend = RecordingMeshBackend {
        applications: applications.clone(),
    };
    let workload = Arc::new(RecordingLeaderWorkload::default());
    let firewall_applications = Arc::new(Mutex::new(Vec::new()));
    let bridge_applications = Arc::new(Mutex::new(Vec::new()));
    let dns_bindings = Arc::new(Mutex::new(Vec::new()));
    let workload_runtime = Arc::new(FakeRuntime::new());
    let network_provider = Arc::new(FakeNetworkProvider::default());
    let health_targets = Arc::new(Mutex::new(Vec::new()));
    let log_store_runtime = InMemoryLogStoreRuntime::new();
    let log_store = log_store_runtime.store_handle();
    let delivery_sink = Arc::new(RecordingLogSink::new(LogSinkId::new("test-delivery")?, []));
    let metric_store_runtime = InMemoryMetricStoreRuntime::new();
    let metric_store = metric_store_runtime.store_handle();
    let host_metric_store = metric_store_runtime.host_store_handle();
    let metric_delivery_sink = Arc::new(RecordingMetricSink::new(
        MetricSinkId::new("test-metrics")?,
        [],
    ));
    let host_metric_delivery_sink = Arc::new(RecordingHostMetricSink::new(
        MetricSinkId::new("test-host-metrics")?,
        [],
    ));
    let directory = tempfile::tempdir()?;
    let cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    let plan = DaemonPlan::new(
        cluster.clone(),
        NodeId::new("master")?,
        directory.path().to_path_buf(),
    )?;
    seed_agent_resources(
        &store,
        &cluster.cluster_id,
        &NodeId::new("master")?,
        cluster
            .nodes
            .get(&NodeId::new("master")?)
            .ok_or("master topology missing")?
            .workload_subnet,
    )
    .await?;
    let factory = DaemonRoleFactory::new(
        DaemonRoleDependencies {
            agent_store: AgentStore::Managed {
                provider,
                start_mode: StoreStartMode::Bootstrap,
            },
            mesh_backend: backend,
            firewall_backend: RecordingFirewallBackend {
                applications: firewall_applications.clone(),
            },
            bridge_backend: RecordingBridgeBackend {
                applications: bridge_applications.clone(),
            },
            dns_server_binder: Arc::new(RecordingDnsBinder {
                bindings: dns_bindings.clone(),
            }),
            workload_runtime: workload_runtime.clone(),
            log_store_runtime: Box::new(log_store_runtime),
            log_sinks: vec![delivery_sink.clone()],
            metric_store_runtime: Box::new(metric_store_runtime),
            metric_sinks: vec![metric_delivery_sink.clone()],
            host_metric_sinks: vec![host_metric_delivery_sink.clone()],
            stats_reader: Arc::new(FixedStatsReader),
            network_stats_reader: Arc::new(FixedNetworkStatsReader),
            host_stats_reader: Arc::new(FixedHostStatsReader),
            host_disk_reader: Arc::new(FixedHostDiskReader),
            network_provider: network_provider.clone(),
            health_prober: Arc::new(RecordingHealthProber {
                targets: health_targets.clone(),
            }),
            volatile_root: directory.path().join("volatile"),
            mesh_identity: MeshIdentity::load_or_generate(&directory.path().join("mesh"))?,
            instance_id: NodeInstanceId::new("instance-1")?,
            monotonic_clock: clock.clone(),
            status_clock: Arc::new(FixedStatusClock),
        },
        DaemonRoleSettings::default().with_sink_worker_settings(SinkWorkerSettings {
            poll_interval: Duration::from_millis(1),
            ..SinkWorkerSettings::default()
        })?,
    )
    .with_leader_workload(workload.clone());

    let running = Daemon::new(plan, factory).start().await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if host_metric_store
                .points()
                .is_ok_and(|points| points.len() == 1)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    tokio::time::timeout(Duration::from_secs(1), workload.started.notified()).await?;
    {
        let applied = applications
            .lock()
            .map_err(|_| "mesh application lock poisoned")?;
        assert!(!applied.is_empty());
        assert!(applied.first().is_some_and(|mesh| mesh.peers.is_empty()));
    }
    assert_eq!(
        firewall_applications
            .lock()
            .map_err(|_| "firewall application lock poisoned")?
            .len(),
        2
    );
    let bridge = bridge_applications
        .lock()
        .map_err(|_| "bridge application lock poisoned")?
        .first()
        .cloned()
        .ok_or("workload bridge was not applied")?;
    assert_eq!(bridge.name, "maestro0");
    assert_eq!(bridge.gateway, Ipv4Addr::new(172, 22, 0, 1));
    assert_eq!(bridge.prefix_length, 24);
    assert_eq!(bridge.mtu_bytes, cluster::WIREGUARD_MTU_BYTES);
    let (dns_settings, resolver) = dns_bindings
        .lock()
        .map_err(|_| "DNS binding lock poisoned")?
        .first()
        .map(|(settings, resolver)| (*settings, resolver.clone()))
        .ok_or("authoritative DNS server was not bound")?;
    assert_eq!(dns_settings.bind_address(), "172.22.0.1:53".parse()?);
    assert_eq!(
        resolver
            .lookup("api.maestro.internal.", DnsQueryType::A)
            .await?
            .answers
            .len(),
        1
    );
    assert_eq!(
        load_assignment(&store, &cluster.cluster_id)
            .await?
            .status
            .phase,
        AssignmentPhase::Running
    );
    assert_eq!(
        workload_runtime
            .list(&cluster.cluster_id, &NodeId::new("master")?)
            .await?
            .len(),
        1
    );
    assert_eq!(network_provider.lease_count(), 1);
    assert_eq!(network_provider.attachment_count(), 1);
    let metric_points = metric_store.points()?;
    assert_eq!(metric_points.len(), 1);
    let metric_point = metric_points
        .first()
        .ok_or("normalized workload metric is missing")?;
    assert_eq!(metric_point.metadata.service_id.as_str(), "api");
    assert_eq!(metric_point.metadata.deployment_id.as_str(), "deployment-1");
    assert_eq!(metric_point.cpu_usage_usec, 10);
    assert_eq!(metric_point.memory_current_bytes, 1_024);
    assert_eq!(metric_point.network_receive_bytes, Some(100));
    assert_eq!(metric_point.network_transmit_bytes, Some(200));
    let metric_attempts = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let attempts = metric_delivery_sink.attempts()?;
            if !attempts.is_empty() {
                return Ok::<_, metrics::MetricSinkError>(attempts);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(metric_attempts, vec![vec![MetricSequence(1)]]);
    let host_metric_attempts = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let attempts = host_metric_delivery_sink.attempts()?;
            if !attempts.is_empty() {
                return Ok::<_, metrics::MetricSinkError>(attempts);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(host_metric_attempts, vec![vec![HostMetricSequence(1)]]);
    let workload_id = load_assignment(&store, &cluster.cluster_id)
        .await?
        .status
        .workload_id
        .ok_or("running assignment has no workload identity")?;
    workload_runtime.append_log(
        &workload_id,
        LogSource::Stdout,
        br#"{"level":"info","message":"daemon log"}"#,
    )?;
    clock.advance(Duration::from_secs(1));
    let entries = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let entries = log_store.entries()?;
            if !entries.is_empty() {
                return Ok::<_, logs::LogStoreError>(entries);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(entries.len(), 1);
    let entry = entries.first().ok_or("normalized runtime log is missing")?;
    assert_eq!(entry.body, LogBody::Text("daemon log".to_owned()));
    let LogOrigin::Workload { metadata } = &entry.origin else {
        return Err("runtime log lost its workload ownership".into());
    };
    assert_eq!(metadata.service_id.as_str(), "api");
    assert_eq!(metadata.deployment_id.as_str(), "deployment-1");
    let delivered_batches = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let batches = delivery_sink.attempts()?;
            if !batches.is_empty() {
                return Ok::<_, logs::LogSinkError>(batches);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(delivered_batches, vec![vec![LogSequence(1)]]);
    assert_eq!(
        load_replica(&store, &cluster.cluster_id)
            .await?
            .status
            .phase,
        DeploymentPhase::Ready
    );
    let workload_address = cluster
        .nodes
        .get(&NodeId::new("master")?)
        .and_then(|node| node.workload_subnet.workload_addresses().next())
        .ok_or("master workload address missing")?;
    assert_eq!(
        health_targets
            .lock()
            .map_err(|_| "health target lock poisoned")?
            .as_slice(),
        &[HealthProbeTarget::Http {
            address: workload_address.into(),
            port: 8080,
            path: "/ready".to_owned(),
        }]
    );

    let leader_key = Keyspace::new(&cluster.cluster_id).leader();
    let stored_leader = store.get(&leader_key).await?.ok_or("leader key missing")?;
    let leader: LeaderIdentity = serde_json::from_slice(&stored_leader.value)?;
    assert_eq!(leader.node_id, NodeId::new("master")?);
    assert_eq!(
        workload
            .terms
            .lock()
            .map_err(|_| "leader term lock poisoned")?
            .as_slice(),
        &[leader]
    );

    running.shutdown().await?;
    assert_eq!(store.get(&leader_key).await?, None);
    assert_eq!(
        *shutdowns
            .lock()
            .map_err(|_| "shutdown count lock poisoned")?,
        1
    );
    assert_eq!(
        workload
            .stopped_while_fenced
            .lock()
            .map_err(|_| "leader stop lock poisoned")?
            .as_slice(),
        &[true]
    );
    Ok(())
}

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
            monotonic_clock: clock,
            status_clock: Arc::new(FixedStatusClock),
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

#[test]
fn settings_reject_keepalive_at_or_after_leadership_ttl() {
    assert!(
        DaemonRoleSettings::new(
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(1),
            1_000,
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
        .is_err()
    );
}

struct PausedClock {
    now: Mutex<MonotonicTime>,
    advanced: Notify,
}

impl PausedClock {
    fn new() -> Self {
        Self {
            now: Mutex::new(MonotonicTime::from_duration(Duration::ZERO)),
            advanced: Notify::new(),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut now = self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *now = now.saturating_add(duration);
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for PausedClock {
    fn now(&self) -> MonotonicTime {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        loop {
            let advanced = self.advanced.notified();
            if self.now() >= deadline {
                return;
            }
            advanced.await;
        }
    }
}

struct FixedStatusClock;

impl StatusClock for FixedStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(1_750_000_000_000)
    }
}

struct FixedStatsReader;

#[async_trait]
impl CgroupStatsReader for FixedStatsReader {
    async fn read(&self, _path: &CgroupPath) -> Result<CgroupStats, CgroupStatsError> {
        Ok(CgroupStats {
            cpu: CgroupCpuStats {
                usage_usec: 10,
                user_usec: 7,
                system_usec: 3,
                periods: 2,
                throttled_periods: 1,
                throttled_usec: 4,
            },
            memory: CgroupMemoryStats {
                current_bytes: 1_024,
                maximum_bytes: Some(2_048),
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
        })
    }
}

struct FixedNetworkStatsReader;

#[async_trait]
impl WorkloadNetworkStatsReader for FixedNetworkStatsReader {
    async fn read(
        &self,
        _workload: &runtime::WorkloadHandle,
    ) -> Result<Option<WorkloadNetworkStats>, WorkloadNetworkStatsError> {
        Ok(Some(WorkloadNetworkStats {
            receive_bytes: 100,
            transmit_bytes: 200,
        }))
    }
}

struct FixedHostStatsReader;

#[async_trait]
impl HostStatsReader for FixedHostStatsReader {
    async fn read(&self) -> Result<HostResourceStats, HostStatsError> {
        Ok(HostResourceStats {
            cpu: HostCpuStats {
                total_ticks: 1_000,
                idle_ticks: 250,
            },
            memory: HostMemoryStats {
                used_bytes: 3_000,
                total_bytes: 4_000,
            },
            network: HostNetworkStats {
                receive_bytes: 5_000,
                transmit_bytes: 6_000,
            },
        })
    }
}

struct FixedHostDiskReader;

#[async_trait]
impl HostDiskReader for FixedHostDiskReader {
    async fn read(&self) -> Result<HostDiskReport, HostDiskError> {
        Ok(HostDiskReport {
            disks: vec![HostDiskStats {
                name: "/dev/test".to_owned(),
                mount_point: "/".to_owned(),
                total_bytes: 10_000,
                available_bytes: 4_000,
                file_system: "ext4".to_owned(),
            }],
            failures: Vec::new(),
        })
    }
}

struct RecordingHealthProber {
    targets: Arc<Mutex<Vec<HealthProbeTarget>>>,
}

#[async_trait]
impl HealthProber for RecordingHealthProber {
    async fn probe(&self, target: &HealthProbeTarget) -> Result<(), HealthProbeError> {
        self.targets
            .lock()
            .map_err(|_| HealthProbeError::Unavailable {
                message: "health target lock poisoned".to_owned(),
            })?
            .push(target.clone());
        Ok(())
    }
}

struct RecordingMeshBackend {
    applications: Arc<Mutex<Vec<MeshConfiguration>>>,
}

#[derive(Default)]
struct RecordingLeaderWorkload {
    started: Notify,
    terms: Mutex<Vec<LeaderIdentity>>,
    stopped_while_fenced: Mutex<Vec<bool>>,
}

#[async_trait]
impl LeaderWorkload for RecordingLeaderWorkload {
    async fn run(
        &self,
        store: Arc<FencedStore>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), RoleError> {
        self.terms
            .lock()
            .map_err(|_| RoleError::new("leader term lock poisoned"))?
            .push(store.token().identity().clone());
        self.started.notify_one();
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
        let fenced = store.verify_leadership().await.is_ok();
        self.stopped_while_fenced
            .lock()
            .map_err(|_| RoleError::new("leader stop lock poisoned"))?
            .push(fenced);
        Ok(())
    }
}

#[async_trait]
impl MeshBackend for RecordingMeshBackend {
    async fn apply(&self, desired: &MeshConfiguration) -> Result<(), MeshBackendError> {
        self.applications
            .lock()
            .map_err(|_| MeshBackendError::new("mesh application lock poisoned"))?
            .push(desired.clone());
        Ok(())
    }
}

struct RecordingFirewallBackend {
    applications: Arc<Mutex<Vec<NodeFirewallSpec>>>,
}

#[async_trait]
impl FirewallBackend for RecordingFirewallBackend {
    async fn apply(&self, desired: &NodeFirewallSpec) -> Result<(), FirewallBackendError> {
        self.applications
            .lock()
            .map_err(|_| FirewallBackendError::new("firewall application lock poisoned"))?
            .push(desired.clone());
        Ok(())
    }
}

struct RecordingBridgeBackend {
    applications: Arc<Mutex<Vec<WorkloadBridge>>>,
}

#[async_trait]
impl WorkloadBridgeBackend for RecordingBridgeBackend {
    async fn apply(&self, desired: &WorkloadBridge) -> Result<(), WorkloadBridgeBackendError> {
        self.applications
            .lock()
            .map_err(|_| WorkloadBridgeBackendError::new("bridge application lock poisoned"))?
            .push(desired.clone());
        Ok(())
    }
}

struct RecordingDnsBinder {
    bindings: Arc<Mutex<Vec<(DnsServerSettings, AuthoritativeDnsResolver)>>>,
}

#[async_trait]
impl DnsServerBinder for RecordingDnsBinder {
    async fn bind(
        &self,
        settings: DnsServerSettings,
        resolver: AuthoritativeDnsResolver,
    ) -> Result<Box<dyn DnsServerRuntime>, DnsServerError> {
        let mut bindings = match self.bindings.lock() {
            Ok(bindings) => bindings,
            Err(poisoned) => poisoned.into_inner(),
        };
        bindings.push((settings, resolver));
        Ok(Box::new(WaitingDnsServer))
    }
}

struct WaitingDnsServer;

#[async_trait]
impl DnsServerRuntime for WaitingDnsServer {
    async fn serve(
        self: Box<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), DnsServerError> {
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
        Ok(())
    }
}
