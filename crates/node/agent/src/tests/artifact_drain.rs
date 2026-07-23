use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{
    ClusterId, Condition, ConditionReason, ConditionState, ConditionType, Generation, Node, NodeId,
    NodeInstanceId, NodeRole, NodeSpec, NodeStatus, ObjectMeta, ResourceKind, ResourceName,
    ResourceRevision, Timestamp,
};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock,
};
use runtime::ArtifactDigest;

use crate::ArtifactHolderRegistry;
use crate::artifact_drain::{
    ARTIFACT_REPLICATION_READY_CONDITION, ArtifactDrainReadiness, DRAIN_REQUEST_REASON,
    DRAINING_CONDITION, PeerCopyPolicy, update_artifact_drain_status,
};

#[tokio::test]
async fn requested_drain_waits_for_a_peer_copy_then_becomes_unschedulable()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(TokioClock::new());
    let store = Arc::new(InMemoryStore::new(clock));
    let cluster_id = ClusterId::new("artifact-drain-test")?;
    let local_node_id = NodeId::new("node-1")?;
    let peer_node_id = NodeId::new("node-2")?;
    put_node(&store, &cluster_id, pending_node(local_node_id.clone())?).await?;
    let local_session = store.session(Duration::from_secs(30)).await?;
    let peer_session = store.session(Duration::from_secs(30)).await?;
    let local_holders = ArtifactHolderRegistry::new(
        store.clone(),
        &cluster_id,
        local_node_id.clone(),
        local_session.id(),
    );
    let peer_holders =
        ArtifactHolderRegistry::new(store.clone(), &cluster_id, peer_node_id, peer_session.id());
    let digest = ArtifactDigest::new("sha256:drain")?;
    let retained = BTreeSet::from([digest.clone()]);

    let single_node_ready = ArtifactDrainReadiness::inspect(
        &retained,
        &local_holders,
        &local_node_id,
        PeerCopyPolicy::LocalCopySufficient,
    )
    .await?;
    assert!(single_node_ready.ready());

    let pending = ArtifactDrainReadiness::inspect(
        &retained,
        &local_holders,
        &local_node_id,
        PeerCopyPolicy::RequirePeerCopy,
    )
    .await?;
    assert!(!pending.ready());
    update_artifact_drain_status(
        store.as_ref(),
        &Keyspace::new(&cluster_id),
        &local_node_id,
        &pending,
        Timestamp(20_000),
    )
    .await?;
    let node = read_node(&store, &cluster_id, &local_node_id).await?;
    assert_eq!(
        condition(&node, DRAINING_CONDITION).map(|value| value.state),
        Some(ConditionState::Unknown)
    );
    assert_eq!(
        condition(&node, ARTIFACT_REPLICATION_READY_CONDITION).map(|value| value.state),
        Some(ConditionState::False)
    );

    peer_holders.publish(&digest).await?;
    let ready = ArtifactDrainReadiness::inspect(
        &retained,
        &local_holders,
        &local_node_id,
        PeerCopyPolicy::RequirePeerCopy,
    )
    .await?;
    assert!(ready.ready());
    update_artifact_drain_status(
        store.as_ref(),
        &Keyspace::new(&cluster_id),
        &local_node_id,
        &ready,
        Timestamp(21_000),
    )
    .await?;
    let node = read_node(&store, &cluster_id, &local_node_id).await?;
    assert_eq!(
        condition(&node, DRAINING_CONDITION).map(|value| value.state),
        Some(ConditionState::True)
    );
    assert_eq!(
        condition(&node, DRAINING_CONDITION).map(|value| value.reason.0.as_str()),
        Some("ArtifactsReplicated")
    );
    assert_eq!(
        condition(&node, ARTIFACT_REPLICATION_READY_CONDITION).map(|value| value.state),
        Some(ConditionState::True)
    );
    Ok(())
}

fn pending_node(node_id: NodeId) -> Result<Node, kernel_api::InvalidIdentifier> {
    Ok(Node {
        meta: ObjectMeta {
            id: node_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: NodeSpec {
            hostname: "node-1.internal".to_string(),
            host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            role: NodeRole::Worker,
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new("instance-1")?,
            version: "1.0.0".to_string(),
            last_seen: Timestamp(10_000),
            conditions: vec![Condition {
                condition_type: ConditionType(DRAINING_CONDITION.to_string()),
                state: ConditionState::Unknown,
                reason: ConditionReason(DRAIN_REQUEST_REASON.to_string()),
                message: "drain requested".to_string(),
                observed_generation: Generation(1),
                last_transition_time: Timestamp(19_000),
            }],
        },
    })
}

async fn put_node(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    node: Node,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key: node_key(cluster_id, &node.meta.id)?,
            value: serde_json::to_vec(&node)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("node seed conflicted".into())
    }
}

async fn read_node(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    node_id: &NodeId,
) -> Result<Node, Box<dyn std::error::Error>> {
    let stored = store
        .get(&node_key(cluster_id, node_id)?)
        .await?
        .ok_or("node missing")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

fn node_key(
    cluster_id: &ClusterId,
    node_id: &NodeId,
) -> Result<kernel_store::StoreKey, kernel_api::InvalidIdentifier> {
    Ok(Keyspace::new(cluster_id).resource(
        &ResourceKind::new("Node")?,
        &ResourceName::new(node_id.as_str())?,
    ))
}

fn condition<'a>(node: &'a Node, condition_type: &str) -> Option<&'a Condition> {
    node.status
        .conditions
        .iter()
        .find(|condition| condition.condition_type.0 == condition_type)
}
