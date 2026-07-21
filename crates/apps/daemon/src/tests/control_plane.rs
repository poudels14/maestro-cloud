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
use node_agent::{
    AuthoritativeDnsResolver, DnsQueryType, DnsServerBinder, DnsServerError, DnsServerRuntime,
    DnsServerSettings, FirewallBackend, FirewallBackendError, HealthProbeError, HealthProbeTarget,
    HealthProber, MeshBackend, MeshBackendError, MeshConfiguration, MeshIdentity, StatusClock,
    WorkloadBridge, WorkloadBridgeBackend, WorkloadBridgeBackendError,
};
use runtime::{FakeNetworkProvider, FakeRuntime, WorkloadRuntime};
use tokio::sync::{Notify, watch};

use crate::{
    AgentStore, ControlPlaneRoleDependencies, ControlPlaneRoleFactory, ControlPlaneRoleSettings,
    Daemon, DaemonPlan, LeaderWorkload, RoleError,
};

use super::cluster_with_nodes;
use super::control_plane_resources::{load_assignment, load_replica, seed_agent_resources};
use super::control_plane_store::FakeProvider;

#[tokio::test]
async fn concrete_roles_establish_mesh_leadership_and_owned_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    let clock: Arc<dyn Clock> = Arc::new(PausedClock);
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
    let factory = ControlPlaneRoleFactory::new(
        ControlPlaneRoleDependencies {
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
            network_provider: network_provider.clone(),
            health_prober: Arc::new(RecordingHealthProber {
                targets: health_targets.clone(),
            }),
            volatile_root: directory.path().join("volatile"),
            mesh_identity: MeshIdentity::load_or_generate(&directory.path().join("mesh"))?,
            instance_id: NodeInstanceId::new("instance-1")?,
            monotonic_clock: clock,
            status_clock: Arc::new(FixedStatusClock),
        },
        ControlPlaneRoleSettings::default(),
    )
    .with_leader_workload(workload.clone());

    let running = Daemon::new(plan, factory).start().await?;
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
    let clock: Arc<dyn Clock> = Arc::new(PausedClock);
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
    let factory = ControlPlaneRoleFactory::new(
        ControlPlaneRoleDependencies {
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
        ControlPlaneRoleSettings::default(),
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
        ControlPlaneRoleSettings::new(
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
        .is_err()
    );
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

struct FixedStatusClock;

impl StatusClock for FixedStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(1_750_000_000_000)
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
