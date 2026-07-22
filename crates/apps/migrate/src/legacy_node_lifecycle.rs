use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    AnnotationKey, BuiltinResource, Generation, NodeId, NodeTombstoneSpec, NodeTombstoneStatus,
    Object, ObjectMeta, ResourceRevision, Timestamp,
};
use serde::Serialize;

use crate::LegacyEntry;
use crate::legacy_convert::LegacyPlanError;
use crate::legacy_membership::LegacyMembershipCatalog;
use crate::legacy_node_lifecycle_schema::{
    LegacyJoinAdmission, LegacyJoinIntent, LegacyMembershipHistory, LegacyRemovedNode,
};
use crate::legacy_node_schema::convert_role;
use crate::legacy_nodes::LegacyNodeCatalog;

const NONCE_PREFIX: &str = "/maetro/cluster/join-nonces/";
const INTENT_PREFIX: &str = "/maetro/cluster/join-intents/";
const ADMISSION_PREFIX: &str = "/maetro/cluster/admissions/";
const REMOVAL_PREFIX: &str = "/maetro/cluster/removals/";
const HISTORY_PREFIX: &str = "/maetro/cluster/membership-history/";
const REMOVED_PREFIX: &str = "/maetro/cluster/removed/";
const JOIN_ANNOTATION: &str = "migration.maestro.dev/legacy-join-state";
const HISTORY_ANNOTATION: &str = "migration.maestro.dev/legacy-membership-history";
const REMOVED_ANNOTATION: &str = "migration.maestro.dev/legacy-removal-tombstone";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyNodeLifecycleCatalog {
    joins: BTreeMap<NodeId, SettledJoin>,
    removed: BTreeMap<NodeId, RemovedBundle>,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyNodeLifecycleCatalog {
    pub(crate) fn decode(
        entries: &[LegacyEntry],
        nodes: &LegacyNodeCatalog,
        membership: &LegacyMembershipCatalog,
    ) -> Result<Self, LegacyNodeLifecycleError> {
        let mut intents = BTreeMap::new();
        let mut admissions = BTreeMap::new();
        let mut history = BTreeMap::new();
        let mut tombstones = BTreeMap::new();
        let mut unclaimed = Vec::new();
        for entry in entries {
            match classify_key(entry.key())? {
                Some(LifecycleKey::Nonce) => {
                    return Err(active(entry.key(), "a join replay lease is still active"));
                }
                Some(LifecycleKey::Removal(_)) => {
                    return Err(active(entry.key(), "node removal has not completed"));
                }
                Some(LifecycleKey::Intent(node_id)) => {
                    insert(entry, &mut intents, node_id, decode_json(entry)?)?;
                }
                Some(LifecycleKey::Admission(node_id)) => {
                    insert(entry, &mut admissions, node_id, decode_json(entry)?)?;
                }
                Some(LifecycleKey::History(node_id)) => {
                    insert(entry, &mut history, node_id, decode_json(entry)?)?;
                }
                Some(LifecycleKey::Removed(node_id)) => {
                    insert(entry, &mut tombstones, node_id, decode_json(entry)?)?;
                }
                None => unclaimed.push(entry.clone()),
            }
        }
        let joins = validate_joins(intents, admissions, nodes, membership)?;
        let removed = validate_removed(history, tombstones, nodes)?;
        Ok(Self {
            joins,
            removed,
            unclaimed,
        })
    }

    pub(crate) fn annotate_nodes(
        &self,
        resources: &mut [BuiltinResource],
    ) -> Result<(), LegacyPlanError> {
        for (node_id, state) in &self.joins {
            let node = resources.iter_mut().find_map(|resource| match resource {
                BuiltinResource::Node(node) if &node.meta.id == node_id => Some(node),
                _ => None,
            });
            let node = node.ok_or_else(|| LegacyPlanError::InvalidClusterState {
                resource_id: node_id.to_string(),
                message: "converted joined node is missing".to_owned(),
            })?;
            let value = encode(state, node_id, "legacy join state")?;
            node.meta
                .annotations
                .insert(AnnotationKey(JOIN_ANNOTATION.to_owned()), value);
        }
        Ok(())
    }

    pub(crate) fn convert_removed(&self) -> Result<Vec<BuiltinResource>, LegacyPlanError> {
        self.removed
            .iter()
            .map(|(node_id, bundle)| convert_removed_node(node_id, bundle))
            .collect()
    }
}

fn validate_joins(
    intents: BTreeMap<NodeId, LegacyJoinIntent>,
    mut admissions: BTreeMap<NodeId, LegacyJoinAdmission>,
    nodes: &LegacyNodeCatalog,
    membership: &LegacyMembershipCatalog,
) -> Result<BTreeMap<NodeId, SettledJoin>, LegacyNodeLifecycleError> {
    let mut joins = BTreeMap::new();
    for (node_id, intent) in intents {
        let key = format!("{INTENT_PREFIX}{node_id}");
        let bundle = nodes.bundle(&node_id).ok_or_else(|| {
            active(
                &key,
                "join intent has no completed durable node registration",
            )
        })?;
        let info = &bundle.record.last_info;
        if intent.node_id != node_id
            || intent.role != info.role
            || intent.cluster_host_ip != info.cluster_host_ip
            || intent.cluster_api_port != info.cluster_api_port
            || intent.etcd_peer_port != bundle.control.etcd_peer_port
            || intent.subnet != info.subnet
            || !valid_sha256(&intent.public_key_sha256)
        {
            return Err(invalid(&key, "join intent disagrees with the durable node"));
        }
        let current_member = membership.member_id(&node_id);
        if info.role.is_control_plane() {
            if intent.member_id.is_none() || intent.member_id != current_member {
                return Err(active(
                    &key,
                    "voter join intent is not bound to the current store member",
                ));
            }
        } else if intent.member_id.is_some() || current_member.is_some() {
            return Err(invalid(
                &key,
                "worker join intent unexpectedly owns a store member",
            ));
        }
        let admission = admissions.remove(&node_id);
        if let Some(admission) = &admission {
            validate_admission(&key, &intent, admission)?;
        }
        joins.insert(node_id, SettledJoin { intent, admission });
    }
    if let Some((node_id, _)) = admissions.first_key_value() {
        return Err(active(
            format!("{ADMISSION_PREFIX}{node_id}"),
            "voter admission has no completed join intent",
        ));
    }
    Ok(joins)
}

fn validate_admission(
    key: &str,
    intent: &LegacyJoinIntent,
    admission: &LegacyJoinAdmission,
) -> Result<(), LegacyNodeLifecycleError> {
    if admission.node_id != intent.node_id
        || admission.role != intent.role
        || !admission.role.is_control_plane()
        || admission.cluster_host_ip != intent.cluster_host_ip
        || admission.cluster_api_port != intent.cluster_api_port
        || admission.subnet != intent.subnet
        || admission.public_key_sha256 != intent.public_key_sha256
        || admission.created_at_ms < 0
    {
        return Err(invalid(
            key,
            "voter admission disagrees with its join intent",
        ));
    }
    Ok(())
}

fn validate_removed(
    history: BTreeMap<NodeId, LegacyMembershipHistory>,
    tombstones: BTreeMap<NodeId, LegacyRemovedNode>,
    nodes: &LegacyNodeCatalog,
) -> Result<BTreeMap<NodeId, RemovedBundle>, LegacyNodeLifecycleError> {
    let history_ids = history.keys().collect::<BTreeSet<_>>();
    let tombstone_ids = tombstones.keys().collect::<BTreeSet<_>>();
    if history_ids != tombstone_ids {
        return Err(invalid(
            REMOVED_PREFIX,
            "removed-node history and tombstone identity sets disagree",
        ));
    }
    let mut removed = BTreeMap::new();
    for (node_id, history) in history {
        let tombstone = tombstones
            .get(&node_id)
            .cloned()
            .ok_or_else(|| invalid(node_id.to_string(), "removed-node tombstone is missing"))?;
        if nodes.contains(node_id.as_str())
            || history.node_id != node_id
            || tombstone.node_id != node_id
            || history.host_ip != tombstone.host_ip
            || history.role != tombstone.role
            || tombstone.requested_at_ms < 0
            || history.removed_at_ms < tombstone.requested_at_ms
        {
            return Err(invalid(
                node_id.to_string(),
                "removed-node history is inconsistent or still has a durable node",
            ));
        }
        removed.insert(node_id, RemovedBundle { history, tombstone });
    }
    Ok(removed)
}

fn convert_removed_node(
    node_id: &NodeId,
    bundle: &RemovedBundle,
) -> Result<BuiltinResource, LegacyPlanError> {
    let mut annotations = BTreeMap::new();
    annotations.insert(
        AnnotationKey(HISTORY_ANNOTATION.to_owned()),
        encode(&bundle.history, node_id, "legacy membership history")?,
    );
    annotations.insert(
        AnnotationKey(REMOVED_ANNOTATION.to_owned()),
        encode(&bundle.tombstone, node_id, "legacy removal tombstone")?,
    );
    Ok(BuiltinResource::NodeTombstone(Object {
        meta: ObjectMeta {
            id: node_id.clone(),
            labels: BTreeMap::new(),
            annotations,
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: NodeTombstoneSpec {
            host_address: bundle.tombstone.host_ip.into(),
            role: convert_role(bundle.tombstone.role),
            requested_at: Timestamp(bundle.tombstone.requested_at_ms),
        },
        status: NodeTombstoneStatus {
            removed_at: Timestamp(bundle.history.removed_at_ms),
        },
    }))
}

fn classify_key(key: &str) -> Result<Option<LifecycleKey>, LegacyNodeLifecycleError> {
    if let Some(nonce) = key.strip_prefix(NONCE_PREFIX) {
        if nonce.len() != 32
            || !nonce
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid(key, "join nonce key is malformed"));
        }
        return Ok(Some(LifecycleKey::Nonce));
    }
    for (prefix, constructor) in [
        (INTENT_PREFIX, LifecycleKey::Intent as fn(NodeId) -> _),
        (ADMISSION_PREFIX, LifecycleKey::Admission),
        (REMOVAL_PREFIX, LifecycleKey::Removal),
        (HISTORY_PREFIX, LifecycleKey::History),
        (REMOVED_PREFIX, LifecycleKey::Removed),
    ] {
        if let Some(raw_node_id) = key.strip_prefix(prefix) {
            if raw_node_id.is_empty() || raw_node_id.contains('/') {
                return Err(invalid(key, "node lifecycle key is malformed"));
            }
            let node_id = NodeId::new(raw_node_id).map_err(|error| {
                invalid(key, format!("node lifecycle identity is invalid: {error}"))
            })?;
            return Ok(Some(constructor(node_id)));
        }
    }
    Ok(None)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn encode<Value: Serialize>(
    value: &Value,
    node_id: &NodeId,
    field: &str,
) -> Result<String, LegacyPlanError> {
    serde_json::to_string(value).map_err(|error| LegacyPlanError::InvalidClusterState {
        resource_id: node_id.to_string(),
        message: format!("could not preserve {field}: {error}"),
    })
}

fn insert<Value>(
    entry: &LegacyEntry,
    values: &mut BTreeMap<NodeId, Value>,
    node_id: NodeId,
    value: Value,
) -> Result<(), LegacyNodeLifecycleError> {
    if values.insert(node_id, value).is_some() {
        Err(invalid(entry.key(), "node occurs twice in this key family"))
    } else {
        Ok(())
    }
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyNodeLifecycleError> {
    serde_json::from_slice(entry.value())
        .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))
}

fn active(key: impl Into<String>, message: impl Into<String>) -> LegacyNodeLifecycleError {
    LegacyNodeLifecycleError::ActiveTransition {
        key: key.into(),
        message: message.into(),
    }
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyNodeLifecycleError {
    LegacyNodeLifecycleError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyNodeLifecycleError {
    #[error("legacy node lifecycle state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
    #[error("legacy node lifecycle is not quiescent at `{key}`: {message}")]
    ActiveTransition { key: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct SettledJoin {
    intent: LegacyJoinIntent,
    #[serde(skip_serializing_if = "Option::is_none")]
    admission: Option<LegacyJoinAdmission>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemovedBundle {
    history: LegacyMembershipHistory,
    tombstone: LegacyRemovedNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LifecycleKey {
    Nonce,
    Intent(NodeId),
    Admission(NodeId),
    Removal(NodeId),
    History(NodeId),
    Removed(NodeId),
}
