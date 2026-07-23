use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;

use kernel_api::{
    Assignment, BuiltinKind, ClusterId, ConditionState, Generation, InvalidIdentifier, Node,
    NodeId, NodeRemovalResponse, NodeRemovalState, NodeRole, NodeTombstone, NodeTombstoneSpec,
    NodeTombstoneStatus, ObjectMeta, ResourceKind, ResourceName, ResourceRevision, Timestamp,
};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, Store, StoreError, StoredValue, Transaction,
};
use serde::{Deserialize, Serialize};

use crate::{StoreProvider, StoreProviderError, set_node_draining};

const MAXIMUM_RESOURCE_BYTES: usize = 1024 * 1_024;
const MAXIMUM_REMOVAL_INTENT_BYTES: usize = 16 * 1_024;
const DRAINING_CONDITION: &str = "Draining";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeRemovalIntent {
    node_id: NodeId,
    host_address: IpAddr,
    role: NodeRole,
    requested_at: Timestamp,
}

struct DrainPreparation<'a> {
    node_id: &'a NodeId,
    node_key: kernel_store::StoreKey,
    stored_node: StoredValue,
    node: Node,
    intent_key: kernel_store::StoreKey,
    stored_intent: Option<&'a StoredValue>,
    intent: NodeRemovalIntent,
    drain_changed: bool,
}

/// One CAS-protected removal phase ready to commit with an API request receipt.
pub struct NodeRemovalPlan {
    response: NodeRemovalResponse,
    transaction: Transaction,
}

impl NodeRemovalPlan {
    /// Separates the public progress receipt from its atomic persistence plan.
    pub fn into_parts(self) -> (NodeRemovalResponse, Transaction) {
        (self.response, self.transaction)
    }
}

/// Store-backed planner for drain-gated, provider-neutral node retirement.
pub struct NodeRemovalCoordinator {
    provider: Option<Arc<dyn StoreProvider>>,
    store: Arc<dyn Store>,
    keys: Keyspace,
    node_kind: ResourceKind,
    assignment_kind: ResourceKind,
    network_kind: ResourceKind,
    firewall_kind: ResourceKind,
    tombstone_kind: ResourceKind,
}

impl NodeRemovalCoordinator {
    /// Binds optional membership authority and canonical resources for one cluster.
    pub fn new(
        cluster_id: &ClusterId,
        provider: Option<Arc<dyn StoreProvider>>,
        store: Arc<dyn Store>,
    ) -> Result<Self, NodeRemovalError> {
        Ok(Self {
            provider,
            store,
            keys: Keyspace::new(cluster_id),
            node_kind: resource_kind(BuiltinKind::Node)?,
            assignment_kind: resource_kind(BuiltinKind::Assignment)?,
            network_kind: resource_kind(BuiltinKind::NodeNetwork)?,
            firewall_kind: resource_kind(BuiltinKind::NodeFirewall)?,
            tombstone_kind: resource_kind(BuiltinKind::NodeTombstone)?,
        })
    }

    /// Advances one removal by at most one durable phase.
    ///
    /// Provider membership removal precedes the final transaction and is deliberately
    /// idempotent. If the final CAS conflicts, a retry repeats that safe provider call.
    pub async fn progress(
        &self,
        node_id: &NodeId,
        now: Timestamp,
    ) -> Result<NodeRemovalPlan, NodeRemovalError> {
        if let Some(stored) = self.store.get(&self.keys.node_tombstone(node_id)).await? {
            let tombstone = self.decode_tombstone(&stored)?;
            return Ok(plan(
                tombstone.meta.id,
                NodeRemovalState::Removed,
                vec![exact_compare(&stored)],
                Vec::new(),
            ));
        }

        let node_key = self
            .keys
            .resource(&self.node_kind, &ResourceName::from(node_id.clone()));
        let stored_node =
            self.store
                .get(&node_key)
                .await?
                .ok_or_else(|| NodeRemovalError::UnknownNode {
                    node_id: node_id.clone(),
                })?;
        let mut node = self.decode_node(&stored_node, node_id)?;
        if node.meta.deletion_timestamp.is_some() {
            return Err(NodeRemovalError::DeletionInProgress {
                node_id: node_id.clone(),
            });
        }

        let intent_key = self.keys.node_removal(node_id);
        let stored_intent = self.store.get(&intent_key).await?;
        let intent = match stored_intent.as_ref() {
            Some(stored) => decode_intent(stored, &node)?,
            None => NodeRemovalIntent {
                node_id: node_id.clone(),
                host_address: node.spec.host_address,
                role: node.spec.role,
                requested_at: now,
            },
        };
        let drain_changed = set_node_draining(&mut node, true, now);
        if stored_intent.is_none() || drain_changed {
            return self.prepare_drain(DrainPreparation {
                node_id,
                node_key,
                stored_node,
                node,
                intent_key,
                stored_intent: stored_intent.as_ref(),
                intent,
                drain_changed,
            });
        }

        let scheduler_generation = self.store.get(&self.keys.scheduler_generation()).await?;
        let assignments = self.list_assignments().await?;
        let scheduler_compare = optional_compare(
            self.keys.scheduler_generation(),
            scheduler_generation.as_ref(),
        );
        if !drain_complete(&node)
            || assignments
                .iter()
                .any(|assignment| &assignment.spec.node_id == node_id)
        {
            return Ok(plan(
                node_id.clone(),
                NodeRemovalState::Draining,
                vec![exact_compare(&stored_node), scheduler_compare],
                Vec::new(),
            ));
        }

        let provider = self
            .provider
            .as_ref()
            .ok_or(NodeRemovalError::ProviderUnavailable)?;
        provider.remove_member(node_id).await?;
        let tombstone = NodeTombstone {
            meta: ObjectMeta {
                id: node_id.clone(),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: NodeTombstoneSpec {
                host_address: intent.host_address,
                role: intent.role,
                requested_at: intent.requested_at,
            },
            status: NodeTombstoneStatus { removed_at: now },
        };
        let tombstone_key = self.keys.node_tombstone(node_id);
        let stored_intent = stored_intent.ok_or(NodeRemovalError::IntentDisappeared)?;
        Ok(plan(
            node_id.clone(),
            NodeRemovalState::Removed,
            vec![
                exact_compare(&stored_node),
                exact_compare(&stored_intent),
                scheduler_compare,
                Compare {
                    key: tombstone_key.clone(),
                    expected: ExpectedVersion::Missing,
                },
            ],
            self.cleanup_mutations(
                node_id,
                node_key,
                intent_key,
                tombstone_key,
                encode(&tombstone, "node tombstone")?,
            ),
        ))
    }

    fn prepare_drain(
        &self,
        preparation: DrainPreparation<'_>,
    ) -> Result<NodeRemovalPlan, NodeRemovalError> {
        let mut compares = vec![exact_compare(&preparation.stored_node)];
        let mut mutations = Vec::new();
        match preparation.stored_intent {
            Some(stored) => compares.push(exact_compare(stored)),
            None => {
                compares.push(Compare {
                    key: preparation.intent_key.clone(),
                    expected: ExpectedVersion::Missing,
                });
                mutations.push(Mutation::Put {
                    key: preparation.intent_key,
                    value: encode(&preparation.intent, "node removal intent")?,
                    session: None,
                });
            }
        }
        if preparation.drain_changed {
            mutations.push(Mutation::Put {
                key: preparation.node_key,
                value: encode(&preparation.node, "Node resource")?,
                session: None,
            });
        }
        Ok(plan(
            preparation.node_id.clone(),
            NodeRemovalState::Draining,
            compares,
            mutations,
        ))
    }

    async fn list_assignments(&self) -> Result<Vec<Assignment>, NodeRemovalError> {
        self.store
            .list(&self.keys.resource_kind(&self.assignment_kind))
            .await?
            .values
            .iter()
            .map(|stored| decode_resource(stored, MAXIMUM_RESOURCE_BYTES, "Assignment"))
            .collect()
    }

    fn decode_node(
        &self,
        stored: &StoredValue,
        node_id: &NodeId,
    ) -> Result<Node, NodeRemovalError> {
        let mut node: Node = decode_resource(stored, MAXIMUM_RESOURCE_BYTES, "Node")?;
        let expected = self
            .keys
            .resource(&self.node_kind, &ResourceName::from(node.meta.id.clone()));
        if stored.key != expected || &node.meta.id != node_id {
            return Err(NodeRemovalError::ResourceIdentityMismatch {
                kind: "Node",
                key: stored.key.to_string(),
            });
        }
        node.meta.revision = stored.version.resource_revision();
        Ok(node)
    }

    fn decode_tombstone(&self, stored: &StoredValue) -> Result<NodeTombstone, NodeRemovalError> {
        let mut tombstone: NodeTombstone =
            decode_resource(stored, MAXIMUM_RESOURCE_BYTES, "NodeTombstone")?;
        let expected = self.keys.resource(
            &self.tombstone_kind,
            &ResourceName::from(tombstone.meta.id.clone()),
        );
        if stored.key != expected {
            return Err(NodeRemovalError::ResourceIdentityMismatch {
                kind: "NodeTombstone",
                key: stored.key.to_string(),
            });
        }
        tombstone.meta.revision = stored.version.resource_revision();
        Ok(tombstone)
    }

    fn cleanup_mutations(
        &self,
        node_id: &NodeId,
        node_key: kernel_store::StoreKey,
        intent_key: kernel_store::StoreKey,
        tombstone_key: kernel_store::StoreKey,
        tombstone: Vec<u8>,
    ) -> Vec<Mutation> {
        let resource_name = ResourceName::from(node_id.clone());
        vec![
            Mutation::Delete { key: node_key },
            Mutation::Delete {
                key: self.keys.resource(&self.network_kind, &resource_name),
            },
            Mutation::Delete {
                key: self.keys.resource(&self.firewall_kind, &resource_name),
            },
            Mutation::Delete {
                key: self.keys.node_liveness(node_id),
            },
            Mutation::Delete {
                key: self.keys.node_upgrade_command(node_id),
            },
            Mutation::Delete {
                key: self.keys.join_approval(node_id),
            },
            Mutation::Delete { key: intent_key },
            Mutation::Put {
                key: tombstone_key,
                value: tombstone,
                session: None,
            },
        ]
    }
}

fn drain_complete(node: &Node) -> bool {
    node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == DRAINING_CONDITION && condition.state == ConditionState::True
    })
}

fn decode_intent(stored: &StoredValue, node: &Node) -> Result<NodeRemovalIntent, NodeRemovalError> {
    let intent: NodeRemovalIntent =
        decode_resource(stored, MAXIMUM_REMOVAL_INTENT_BYTES, "node removal intent")?;
    if intent.node_id == node.meta.id
        && intent.host_address == node.spec.host_address
        && intent.role == node.spec.role
    {
        Ok(intent)
    } else {
        Err(NodeRemovalError::IntentIdentityConflict)
    }
}

fn decode_resource<Value: serde::de::DeserializeOwned>(
    stored: &StoredValue,
    limit: usize,
    subject: &'static str,
) -> Result<Value, NodeRemovalError> {
    if stored.value.len() > limit {
        Err(NodeRemovalError::Oversized {
            subject,
            size: stored.value.len(),
            limit,
        })
    } else {
        serde_json::from_slice(&stored.value).map_err(|error| NodeRemovalError::Malformed {
            subject,
            message: error.to_string(),
        })
    }
}

fn encode(value: &impl Serialize, subject: &'static str) -> Result<Vec<u8>, NodeRemovalError> {
    serde_json::to_vec(value).map_err(|error| NodeRemovalError::Serialize {
        subject,
        message: error.to_string(),
    })
}

fn plan(
    node_id: NodeId,
    state: NodeRemovalState,
    compares: Vec<Compare>,
    mutations: Vec<Mutation>,
) -> NodeRemovalPlan {
    NodeRemovalPlan {
        response: NodeRemovalResponse { node_id, state },
        transaction: Transaction {
            compares,
            mutations,
        },
    }
}

fn exact_compare(stored: &StoredValue) -> Compare {
    Compare {
        key: stored.key.clone(),
        expected: ExpectedVersion::Exact(stored.version),
    }
}

fn optional_compare(key: kernel_store::StoreKey, stored: Option<&StoredValue>) -> Compare {
    Compare {
        key,
        expected: stored.map_or(ExpectedVersion::Missing, |stored| {
            ExpectedVersion::Exact(stored.version)
        }),
    }
}

fn resource_kind(kind: BuiltinKind) -> Result<ResourceKind, InvalidIdentifier> {
    ResourceKind::new(kind.as_str())
}

/// Why a permanent node removal could not safely advance.
#[derive(Debug, thiserror::Error)]
pub enum NodeRemovalError {
    /// A canonical resource kind constant was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] InvalidIdentifier),
    /// The named node has no active durable resource.
    #[error("node `{node_id}` does not exist")]
    UnknownNode { node_id: NodeId },
    /// Another deletion flow already owns this node.
    #[error("node `{node_id}` deletion is already in progress")]
    DeletionInProgress { node_id: NodeId },
    /// Persisted removal identity disagreed with the immutable Node definition.
    #[error("node definition differs from its persisted removal intent")]
    IntentIdentityConflict,
    /// The intent vanished between planning reads.
    #[error("node removal intent disappeared")]
    IntentDisappeared,
    /// A typed resource was stored beneath a non-canonical identity.
    #[error("stored {kind} identity does not match key `{key}`")]
    ResourceIdentityMismatch { kind: &'static str, key: String },
    /// A persisted document exceeded its explicit bound.
    #[error("stored {subject} document is {size} bytes; limit is {limit}")]
    Oversized {
        subject: &'static str,
        size: usize,
        limit: usize,
    },
    /// A persisted document did not match its typed schema.
    #[error("stored {subject} document is malformed: {message}")]
    Malformed {
        subject: &'static str,
        message: String,
    },
    /// A planned resource could not be serialized.
    #[error("failed to encode {subject}: {message}")]
    Serialize {
        subject: &'static str,
        message: String,
    },
    /// Store access failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Backend membership removal failed.
    #[error(transparent)]
    Provider(#[from] StoreProviderError),
    /// This store-connected process does not own membership mutation.
    #[error("node removal requires a store-owning control-plane endpoint")]
    ProviderUnavailable,
}
