use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, ConditionState, NodeId, NodeNetwork, ResourceKind, ResourceName, Timestamp,
};
use kernel_store::{
    Clock, DeleteRequest, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Store,
};

use crate::{
    MeshBackend, MeshBackendError, MeshConfiguration, MeshIdentity, MeshPlanner, MeshResourceAgent,
    MeshSubnet, StatusClock, WireGuardPrivateKey,
};

use super::fake_mesh::FakeMeshBackend;

#[tokio::test]
async fn resource_agents_publish_apply_and_remove_stale_peers()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestMonotonicClock)));
    let cluster_id = ClusterId::new("mesh-resource-test")?;
    let node_1 = agent(
        store.clone(),
        &cluster_id,
        "node-1",
        1,
        Ipv4Addr::new(10, 20, 0, 11),
        "172.22.1.0/24",
    )?;
    let node_2 = agent(
        store.clone(),
        &cluster_id,
        "node-2",
        2,
        Ipv4Addr::new(10, 20, 0, 12),
        "172.22.2.0/24",
    )?;

    let first = node_1.reconcile_once().await?;
    assert!(first.peers.is_empty());
    let second = node_2.reconcile_once().await?;
    assert_eq!(peer_ids(&second), vec!["node-1"]);
    let formed = node_1.reconcile_once().await?;
    assert_eq!(peer_ids(&formed), vec!["node-2"]);

    let keyspace = Keyspace::new(&cluster_id);
    let node_2_key = keyspace.resource(
        &ResourceKind::new("NodeNetwork")?,
        &ResourceName::new("node-2")?,
    );
    let stored = store
        .get(&node_2_key)
        .await?
        .ok_or("node-2 publication missing")?;
    store
        .delete_cas(DeleteRequest {
            key: node_2_key,
            expected: stored.version,
        })
        .await?;

    let converged = node_1.reconcile_once().await?;
    assert!(converged.peers.is_empty());
    assert_eq!(node_1.backend().applied().last(), Some(&converged));

    let node_1_key = keyspace.resource(
        &ResourceKind::new("NodeNetwork")?,
        &ResourceName::new("node-1")?,
    );
    let stored = store
        .get(&node_1_key)
        .await?
        .ok_or("node-1 publication missing")?;
    let resource: NodeNetwork = serde_json::from_slice(&stored.value)?;
    assert_eq!(resource.status.applied_generation, resource.meta.generation);
    assert_eq!(
        resource
            .status
            .conditions
            .first()
            .map(|condition| condition.state),
        Some(ConditionState::True)
    );
    Ok(())
}

#[tokio::test]
async fn backend_failure_is_reported_without_advancing_applied_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestMonotonicClock)));
    let cluster_id = ClusterId::new("mesh-resource-failure")?;
    let node_id = NodeId::new("node-1")?;
    let planner = MeshPlanner::new(
        node_id.clone(),
        MeshIdentity::from_private_key(WireGuardPrivateKey::from_bytes([1; 32])),
        51_820,
    )?;
    let publication =
        planner.publication(Ipv4Addr::new(10, 20, 0, 11), "172.22.1.0/24".parse()?)?;
    let agent = MeshResourceAgent::new(
        store.clone(),
        &cluster_id,
        planner,
        publication,
        FailingBackend,
        Arc::new(TestMonotonicClock),
        Arc::new(FixedStatusClock),
        Duration::from_secs(30),
    )?;

    assert!(agent.reconcile_once().await.is_err());
    let key = Keyspace::new(&cluster_id).resource(
        &ResourceKind::new("NodeNetwork")?,
        &ResourceName::new(node_id.as_str())?,
    );
    let stored = store.get(&key).await?.ok_or("publication missing")?;
    let resource: NodeNetwork = serde_json::from_slice(&stored.value)?;
    assert_eq!(resource.status.applied_generation.0, 0);
    assert_eq!(
        resource
            .status
            .conditions
            .first()
            .map(|condition| (condition.state, condition.reason.0.as_str())),
        Some((ConditionState::False, "MeshApplyFailed"))
    );
    Ok(())
}

#[tokio::test]
async fn removed_identity_cannot_recreate_its_mesh_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestMonotonicClock)));
    let cluster_id = ClusterId::new("mesh-resource-removed")?;
    let node_id = NodeId::new("node-1")?;
    store
        .put_cas(PutRequest {
            key: Keyspace::new(&cluster_id).node_tombstone(&node_id),
            value: b"removed".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    let agent = agent(
        store.clone(),
        &cluster_id,
        node_id.as_str(),
        1,
        Ipv4Addr::new(10, 20, 0, 11),
        "172.22.1.0/24",
    )?;

    assert!(matches!(
        agent.reconcile_once().await,
        Err(crate::MeshResourceError::LocalNodeRemoved)
    ));
    let network_key = Keyspace::new(&cluster_id).resource(
        &ResourceKind::new("NodeNetwork")?,
        &ResourceName::new(node_id.as_str())?,
    );
    assert!(store.get(&network_key).await?.is_none());
    Ok(())
}

fn agent(
    store: Arc<InMemoryStore>,
    cluster_id: &ClusterId,
    node_id: &str,
    key_byte: u8,
    host_address: Ipv4Addr,
    subnet: &str,
) -> Result<MeshResourceAgent<FakeMeshBackend>, Box<dyn std::error::Error>> {
    let planner = MeshPlanner::new(
        NodeId::new(node_id)?,
        MeshIdentity::from_private_key(WireGuardPrivateKey::from_bytes([key_byte; 32])),
        51_820,
    )?;
    let publication = planner.publication(host_address, subnet.parse::<MeshSubnet>()?)?;
    Ok(MeshResourceAgent::new(
        store,
        cluster_id,
        planner,
        publication,
        FakeMeshBackend::default(),
        Arc::new(TestMonotonicClock),
        Arc::new(FixedStatusClock),
        Duration::from_secs(30),
    )?)
}

fn peer_ids(configuration: &MeshConfiguration) -> Vec<&str> {
    configuration
        .peers
        .iter()
        .map(|peer| peer.node_id.as_str())
        .collect()
}

struct TestMonotonicClock;

#[async_trait]
impl Clock for TestMonotonicClock {
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

struct FailingBackend;

#[async_trait]
impl MeshBackend for FailingBackend {
    async fn apply(&self, _desired: &MeshConfiguration) -> Result<(), MeshBackendError> {
        Err(MeshBackendError::new("injected netlink failure"))
    }
}
