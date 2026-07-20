use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cluster::{
    MemberActivation, StoreJoinTicket, StoreMember, StoreProvider, StoreProviderError,
    StoreRecovery, StoreRecoveryPermit, StoreRuntime, StoreShutdown, StoreStartMode,
};
use kernel_api::{NodeId, NodeInstanceId, NodeRole, Timestamp};
use kernel_controller::LeaderIdentity;
use kernel_store::{Clock, InMemoryStore, Keyspace, MonotonicTime, Store};
use node_agent::{MeshBackend, MeshBackendError, MeshConfiguration, MeshIdentity, StatusClock};

use crate::{
    ControlPlaneRoleDependencies, ControlPlaneRoleFactory, ControlPlaneRoleSettings, Daemon,
    DaemonPlan,
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
    let directory = tempfile::tempdir()?;
    let cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    let plan = DaemonPlan::new(
        cluster.clone(),
        NodeId::new("master")?,
        directory.path().to_path_buf(),
    )?;
    let factory = ControlPlaneRoleFactory::new(
        ControlPlaneRoleDependencies {
            provider,
            store_start_mode: StoreStartMode::Bootstrap,
            mesh_backend: backend,
            mesh_identity: MeshIdentity::load_or_generate(&directory.path().join("mesh"))?,
            instance_id: NodeInstanceId::new("instance-1")?,
            monotonic_clock: clock,
            status_clock: Arc::new(FixedStatusClock),
        },
        ControlPlaneRoleSettings::default(),
    );

    let running = Daemon::new(plan, factory).start().await?;
    {
        let applied = applications
            .lock()
            .map_err(|_| "mesh application lock poisoned")?;
        assert!(!applied.is_empty());
        assert!(applied.first().is_some_and(|mesh| mesh.peers.is_empty()));
    }

    let leader_key = Keyspace::new(&cluster.cluster_id).leader();
    let stored_leader = store.get(&leader_key).await?.ok_or("leader key missing")?;
    let leader: LeaderIdentity = serde_json::from_slice(&stored_leader.value)?;
    assert_eq!(leader.node_id, NodeId::new("master")?);

    running.shutdown().await?;
    assert_eq!(store.get(&leader_key).await?, None);
    assert_eq!(
        *shutdowns
            .lock()
            .map_err(|_| "shutdown count lock poisoned")?,
        1
    );
    Ok(())
}

#[test]
fn settings_reject_keepalive_at_or_after_leadership_ttl() {
    assert!(
        ControlPlaneRoleSettings::new(
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
