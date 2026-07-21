use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cluster::{
    MemberActivation, StoreJoinTicket, StoreMember, StoreProvider, StoreProviderError,
    StoreRecovery, StoreRecoveryPermit, StoreRuntime, StoreShutdown, StoreStartMode,
};
use kernel_api::{
    DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, DnsRecordValue, Generation,
    NodeFirewall, NodeFirewallId, NodeFirewallSpec, NodeFirewallStatus, NodeId, NodeInstanceId,
    NodeRole, Object, ObjectMeta, ResourceRevision, Timestamp,
};
use kernel_controller::{FencedStore, LeaderIdentity};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest, Store,
};
use node_agent::{
    AuthoritativeDnsResolver, DnsQueryType, DnsServerBinder, DnsServerError, DnsServerRuntime,
    DnsServerSettings, FirewallBackend, FirewallBackendError, MeshBackend, MeshBackendError,
    MeshConfiguration, MeshIdentity, StatusClock, WorkloadBridge, WorkloadBridgeBackend,
    WorkloadBridgeBackendError,
};
use tokio::sync::{Notify, watch};

use crate::{
    ControlPlaneRoleDependencies, ControlPlaneRoleFactory, ControlPlaneRoleSettings, Daemon,
    DaemonPlan, LeaderWorkload, RoleError,
};

use super::cluster_with_nodes;

#[tokio::test]
async fn concrete_roles_establish_mesh_leadership_and_owned_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    let clock: Arc<dyn Clock> = Arc::new(PausedClock);
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let shutdowns = Arc::new(Mutex::new(0_u32));
    let provider = Arc::new(FakeProvider {
        store: store.clone(),
        shutdowns: shutdowns.clone(),
    });
    let applications = Arc::new(Mutex::new(Vec::new()));
    let backend = RecordingMeshBackend {
        applications: applications.clone(),
    };
    let workload = Arc::new(RecordingLeaderWorkload::default());
    let firewall_applications = Arc::new(Mutex::new(Vec::new()));
    let bridge_applications = Arc::new(Mutex::new(Vec::new()));
    let dns_bindings = Arc::new(Mutex::new(Vec::new()));
    let directory = tempfile::tempdir()?;
    let cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    let plan = DaemonPlan::new(
        cluster.clone(),
        NodeId::new("master")?,
        directory.path().to_path_buf(),
    )?;
    put_firewall(&store, &cluster.cluster_id, "master").await?;
    put_dns_record(&store, &cluster.cluster_id).await?;
    let factory = ControlPlaneRoleFactory::new(
        ControlPlaneRoleDependencies {
            provider,
            store_start_mode: StoreStartMode::Bootstrap,
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

async fn put_dns_record(
    store: &InMemoryStore,
    cluster_id: &kernel_api::ClusterId,
) -> Result<(), Box<dyn std::error::Error>> {
    let resource: DnsRecord = Object {
        meta: ObjectMeta {
            id: DnsRecordId::new("api")?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: DnsRecordSpec {
            name: "api.maestro.internal.".to_owned(),
            values: vec![DnsRecordValue::A(Ipv4Addr::new(172, 22, 0, 11))],
            ttl_secs: 30,
        },
        status: DnsRecordStatus {
            applied_generation: Generation::default(),
            published_nodes: Vec::new(),
            conditions: Vec::new(),
        },
    };
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(cluster_id).resource(
                &kernel_api::ResourceKind::new("DnsRecord")?,
                &kernel_api::ResourceName::new("api")?,
            ),
            value: serde_json::to_vec(&resource)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("DnsRecord create conflicted".into())
    }
}

async fn put_firewall(
    store: &InMemoryStore,
    cluster_id: &kernel_api::ClusterId,
    node_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let resource: NodeFirewall = Object {
        meta: ObjectMeta {
            id: NodeFirewallId::new(node_id)?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: NodeFirewallSpec {
            node_id: NodeId::new(node_id)?,
            table_name: "maestro_firewall".to_string(),
            script: String::new(),
            digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
        },
        status: NodeFirewallStatus {
            applied_generation: Generation::default(),
            applied_digest: None,
            conditions: Vec::new(),
        },
    };
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(cluster_id).resource(
                &kernel_api::ResourceKind::new("NodeFirewall")?,
                &kernel_api::ResourceName::new(node_id)?,
            ),
            value: serde_json::to_vec(&resource)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("NodeFirewall create conflicted".into())
    }
}

struct FakeProvider {
    store: Arc<InMemoryStore>,
    shutdowns: Arc<Mutex<u32>>,
}

#[async_trait]
impl StoreProvider for FakeProvider {
    async fn start(
        &self,
        _mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        Ok(Box::new(FakeStoreRuntime {
            store: self.store.clone(),
            shutdowns: self.shutdowns.clone(),
        }))
    }

    async fn stage_member(
        &self,
        _member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
        Err(unsupported())
    }

    async fn activate_member(
        &self,
        _ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError> {
        Err(unsupported())
    }

    async fn remove_member(&self, _node_id: &NodeId) -> Result<(), StoreProviderError> {
        Err(unsupported())
    }

    async fn recover(
        &self,
        _permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError> {
        Err(unsupported())
    }
}

struct FakeStoreRuntime {
    store: Arc<InMemoryStore>,
    shutdowns: Arc<Mutex<u32>>,
}

#[async_trait]
impl StoreRuntime for FakeStoreRuntime {
    fn store(&self) -> Arc<dyn Store> {
        self.store.clone()
    }

    async fn shutdown(self: Box<Self>, _request: StoreShutdown) -> Result<(), StoreProviderError> {
        let mut shutdowns = self
            .shutdowns
            .lock()
            .map_err(|_| StoreProviderError::Lifecycle {
                reason: "shutdown count lock poisoned".to_owned(),
            })?;
        *shutdowns = shutdowns.saturating_add(1);
        Ok(())
    }
}

fn unsupported() -> StoreProviderError {
    StoreProviderError::InvalidConfiguration {
        reason: "operation is not used by the daemon role test".to_owned(),
    }
}
