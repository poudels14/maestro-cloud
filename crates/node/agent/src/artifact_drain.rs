use std::collections::BTreeSet;

use kernel_api::{
    Condition, ConditionReason, ConditionState, ConditionType, Node, NodeId, ResourceName,
    Timestamp,
};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store};
use runtime::ArtifactDigest;

use crate::{ArtifactHolderRegistry, ArtifactReplicationError};

pub(crate) const ARTIFACT_REPLICATION_READY_CONDITION: &str = "ArtifactReplicationReady";
pub(crate) const DRAINING_CONDITION: &str = "Draining";
pub(crate) const DRAIN_REQUEST_REASON: &str = "ReplicatingArtifacts";

const MAX_NODE_BYTES: usize = 256 * 1_024;
const MAX_CAS_ATTEMPTS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerCopyPolicy {
    LocalCopySufficient,
    RequirePeerCopy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactDrainReadiness {
    retained: usize,
    missing_peer_copies: Vec<ArtifactDigest>,
}

impl ArtifactDrainReadiness {
    pub(crate) async fn inspect(
        retained: &BTreeSet<ArtifactDigest>,
        holders: &ArtifactHolderRegistry,
        local_node_id: &NodeId,
        peer_copy_policy: PeerCopyPolicy,
    ) -> Result<Self, ArtifactReplicationError> {
        let mut missing_peer_copies = Vec::new();
        if peer_copy_policy == PeerCopyPolicy::RequirePeerCopy {
            for digest in retained {
                let has_peer = holders
                    .holders(digest)
                    .await?
                    .iter()
                    .any(|holder| &holder.node_id != local_node_id);
                if !has_peer {
                    missing_peer_copies.push(digest.clone());
                }
            }
        }
        Ok(Self {
            retained: retained.len(),
            missing_peer_copies,
        })
    }

    pub(crate) fn ready(&self) -> bool {
        self.missing_peer_copies.is_empty()
    }

    pub(crate) fn missing_peer_copies(&self) -> usize {
        self.missing_peer_copies.len()
    }
}

pub(crate) async fn update_artifact_drain_status(
    store: &dyn Store,
    keyspace: &Keyspace,
    node_id: &NodeId,
    readiness: &ArtifactDrainReadiness,
    now: Timestamp,
) -> Result<(), ArtifactReplicationError> {
    let kind = kernel_api::ResourceKind::new("Node")?;
    let key = keyspace.resource(&kind, &ResourceName::new(node_id.as_str())?);
    for _attempt in 0..MAX_CAS_ATTEMPTS {
        let stored =
            store
                .get(&key)
                .await?
                .ok_or_else(|| ArtifactReplicationError::LocalNodeMissing {
                    node_id: node_id.clone(),
                })?;
        let mut node = decode_node(&stored.value, node_id)?;
        if node.meta.deletion_timestamp.is_some() {
            return Ok(());
        }
        let original = node.status.conditions.clone();
        upsert_replication_condition(&mut node, readiness, now);
        complete_requested_drain(&mut node, readiness, now);
        if node.status.conditions == original {
            return Ok(());
        }
        node.meta.revision = stored.version.resource_revision();
        let outcome = store
            .put_cas(PutRequest {
                key: key.clone(),
                value: encode_node(&node)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            return Ok(());
        }
    }
    Err(ArtifactReplicationError::DrainStatusContention {
        node_id: node_id.clone(),
    })
}

fn upsert_replication_condition(
    node: &mut Node,
    readiness: &ArtifactDrainReadiness,
    now: Timestamp,
) {
    let (state, reason, message) = if readiness.ready() {
        (
            ConditionState::True,
            "PeerCopiesReady",
            format!(
                "all {} retained registry-free artifacts satisfy drain replication policy",
                readiness.retained
            ),
        )
    } else {
        (
            ConditionState::False,
            "PeerCopiesPending",
            format!(
                "{} retained registry-free artifacts still need a peer copy",
                readiness.missing_peer_copies()
            ),
        )
    };
    upsert_condition(
        node,
        ARTIFACT_REPLICATION_READY_CONDITION,
        state,
        reason,
        message,
        now,
    );
}

fn complete_requested_drain(node: &mut Node, readiness: &ArtifactDrainReadiness, now: Timestamp) {
    let pending = node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == DRAINING_CONDITION
            && condition.state == ConditionState::Unknown
            && condition.reason.0 == DRAIN_REQUEST_REASON
    });
    if !pending {
        return;
    }
    let (state, reason, message) = if readiness.ready() {
        (
            ConditionState::True,
            "ArtifactsReplicated",
            "node drain started after retained artifacts acquired peer copies".to_string(),
        )
    } else {
        (
            ConditionState::Unknown,
            DRAIN_REQUEST_REASON,
            format!(
                "waiting for peer copies of {} retained registry-free artifacts",
                readiness.missing_peer_copies()
            ),
        )
    };
    upsert_condition(node, DRAINING_CONDITION, state, reason, message, now);
}

fn upsert_condition(
    node: &mut Node,
    condition_type: &str,
    state: ConditionState,
    reason: &str,
    message: String,
    now: Timestamp,
) {
    let transitioned = node
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type.0 == condition_type)
        .filter(|condition| condition.state == state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    node.status
        .conditions
        .retain(|condition| condition.condition_type.0 != condition_type);
    node.status.conditions.push(Condition {
        condition_type: ConditionType(condition_type.to_string()),
        state,
        reason: ConditionReason(reason.to_string()),
        message,
        observed_generation: node.meta.generation,
        last_transition_time: transitioned,
    });
}

fn decode_node(value: &[u8], node_id: &NodeId) -> Result<Node, ArtifactReplicationError> {
    if value.len() > MAX_NODE_BYTES {
        return Err(ArtifactReplicationError::MalformedResource {
            key: node_id.to_string(),
            message: format!("document exceeds {MAX_NODE_BYTES} bytes"),
        });
    }
    let node: Node = serde_json::from_slice(value).map_err(|error| {
        ArtifactReplicationError::MalformedResource {
            key: node_id.to_string(),
            message: error.to_string(),
        }
    })?;
    if &node.meta.id == node_id {
        Ok(node)
    } else {
        Err(ArtifactReplicationError::LocalNodeIdentityMismatch {
            expected: node_id.clone(),
            actual: node.meta.id,
        })
    }
}

fn encode_node(node: &Node) -> Result<Vec<u8>, ArtifactReplicationError> {
    let encoded =
        serde_json::to_vec(node).map_err(|error| ArtifactReplicationError::SerializeNode {
            message: error.to_string(),
        })?;
    if encoded.len() > MAX_NODE_BYTES {
        Err(ArtifactReplicationError::SerializeNode {
            message: format!("document exceeds {MAX_NODE_BYTES} bytes"),
        })
    } else {
        Ok(encoded)
    }
}
