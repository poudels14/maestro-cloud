use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    FirewallPolicyId, Generation, NodeFirewall, NodeFirewallId, NodeFirewallSpec,
    NodeFirewallStatus, Object, ObjectMeta, ResourceKind, ResourceName, ResourceRevision,
};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction, TransactionOutcome};

use crate::snapshot::ResourceSnapshot;
use crate::{FirewallPlan, FirewallPolicyStatusUpdate, FirewallRuleset};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FirewallWriteReport {
    pub(crate) published_rulesets: usize,
    pub(crate) pending_rulesets: usize,
    pub(crate) desired_state_changed: bool,
    pub(crate) updated_policies: usize,
    pub(crate) conflict: bool,
}

pub(crate) struct FirewallWriter;

impl FirewallWriter {
    /// Publishes desired per-node rulesets before acknowledging their complete bundle.
    pub(crate) async fn apply(
        store: &FencedStore,
        keyspace: &Keyspace,
        snapshot: &ResourceSnapshot,
        plan: &FirewallPlan,
    ) -> Result<FirewallWriteReport, FirewallWriteError> {
        let desired = desired_rulesets(&plan.rulesets)?;
        let mut compares = snapshot.dependency_compares();
        let mut mutations = Vec::new();
        let mut published_rulesets = 0_usize;
        let mut pending_rulesets = 0_usize;

        for (id, (ruleset, spec)) in &desired {
            match snapshot.node_firewalls.get(id) {
                Some(current) if current.resource.spec == *spec => {
                    if !acknowledged(&current.resource) {
                        pending_rulesets = pending_rulesets.saturating_add(1);
                    }
                }
                Some(current) => {
                    let mut resource = current.resource.clone();
                    resource.meta.generation =
                        Generation(resource.meta.generation.0.saturating_add(1));
                    resource.spec = spec.clone();
                    mutations.push(put_node_firewall(keyspace, &resource, ruleset)?);
                    published_rulesets = published_rulesets.saturating_add(1);
                    pending_rulesets = pending_rulesets.saturating_add(1);
                }
                None => {
                    let resource = new_node_firewall(id.clone(), spec.clone());
                    let kind = ResourceKind::new("NodeFirewall")?;
                    let key = keyspace.resource(&kind, &ResourceName::from(id.clone()));
                    compares.push(Compare {
                        key,
                        expected: ExpectedVersion::Missing,
                    });
                    mutations.push(put_node_firewall(keyspace, &resource, ruleset)?);
                    published_rulesets = published_rulesets.saturating_add(1);
                    pending_rulesets = pending_rulesets.saturating_add(1);
                }
            }
        }
        for (id, current) in &snapshot.node_firewalls {
            if !desired.contains_key(id) {
                mutations.push(Mutation::Delete {
                    key: current.stored.key.clone(),
                });
            }
        }

        let rulesets_changed = !mutations.is_empty();
        let updated_policies = if rulesets_changed || pending_rulesets > 0 {
            0
        } else {
            mutations.extend(
                plan.policy_updates
                    .iter()
                    .map(|update| status_put(snapshot, update))
                    .collect::<Result<Vec<_>, _>>()?,
            );
            plan.policy_updates.len()
        };
        if mutations.is_empty() {
            return Ok(FirewallWriteReport {
                published_rulesets,
                pending_rulesets,
                desired_state_changed: rulesets_changed,
                updated_policies,
                conflict: false,
            });
        }
        let outcome = store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        Ok(if outcome == TransactionOutcome::Conflict {
            FirewallWriteReport {
                conflict: true,
                ..Default::default()
            }
        } else {
            FirewallWriteReport {
                published_rulesets,
                pending_rulesets,
                desired_state_changed: rulesets_changed,
                updated_policies,
                conflict: false,
            }
        })
    }
}

fn desired_rulesets(
    rulesets: &[FirewallRuleset],
) -> Result<BTreeMap<NodeFirewallId, (FirewallRuleset, NodeFirewallSpec)>, FirewallWriteError> {
    rulesets
        .iter()
        .map(|ruleset| {
            let id = NodeFirewallId::new(ruleset.node_id.as_str())?;
            Ok((
                id,
                (
                    ruleset.clone(),
                    NodeFirewallSpec {
                        node_id: ruleset.node_id.clone(),
                        table_name: ruleset.table_name.clone(),
                        script: ruleset.script.clone(),
                        digest: ruleset.digest.clone(),
                    },
                ),
            ))
        })
        .collect()
}

fn acknowledged(resource: &NodeFirewall) -> bool {
    resource.status.applied_generation == resource.meta.generation
        && resource.status.applied_digest.as_deref() == Some(resource.spec.digest.as_str())
}

fn new_node_firewall(id: NodeFirewallId, spec: NodeFirewallSpec) -> NodeFirewall {
    Object {
        meta: ObjectMeta {
            id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec,
        status: NodeFirewallStatus {
            applied_generation: Generation::default(),
            applied_digest: None,
            conditions: Vec::new(),
        },
    }
}

fn put_node_firewall(
    keyspace: &Keyspace,
    resource: &NodeFirewall,
    ruleset: &FirewallRuleset,
) -> Result<Mutation, FirewallWriteError> {
    let kind = ResourceKind::new("NodeFirewall")?;
    Ok(Mutation::Put {
        key: keyspace.resource(&kind, &ResourceName::from(resource.meta.id.clone())),
        value: serde_json::to_vec(resource).map_err(|error| {
            FirewallWriteError::SerializeNodeFirewall {
                node_id: ruleset.node_id.to_string(),
                message: error.to_string(),
            }
        })?,
        session: None,
    })
}

fn status_put(
    snapshot: &ResourceSnapshot,
    update: &FirewallPolicyStatusUpdate,
) -> Result<Mutation, FirewallWriteError> {
    let current = snapshot.policies.get(&update.policy_id).ok_or_else(|| {
        FirewallWriteError::MissingPlannedPolicy {
            policy_id: update.policy_id.clone(),
        }
    })?;
    let actual = current.stored.version.resource_revision();
    if update.observed_revision != actual {
        return Err(FirewallWriteError::ObservedRevisionMismatch {
            policy_id: update.policy_id.clone(),
            planned: update.observed_revision,
            actual,
        });
    }
    let mut resource = current.resource.clone();
    resource.meta.revision = actual;
    resource.status = update.status.clone();
    Ok(Mutation::Put {
        key: current.stored.key.clone(),
        value: serde_json::to_vec(&resource).map_err(|error| {
            FirewallWriteError::SerializePolicy {
                policy_id: update.policy_id.clone(),
                message: error.to_string(),
            }
        })?,
        session: None,
    })
}

/// Atomic desired-state publication and policy acknowledgement failures.
#[derive(Debug, thiserror::Error)]
pub enum FirewallWriteError {
    /// The controller kernel rejected the fenced transaction.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// A generated per-node resource identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// A planner update referenced a policy absent from its input snapshot.
    #[error("planned FirewallPolicy `{policy_id}` is absent from the resource snapshot")]
    MissingPlannedPolicy { policy_id: FirewallPolicyId },
    /// A planner update did not retain the exact observed policy revision.
    #[error(
        "planned FirewallPolicy `{policy_id}` revision {planned:?} does not match snapshot {actual:?}"
    )]
    ObservedRevisionMismatch {
        policy_id: FirewallPolicyId,
        planned: ResourceRevision,
        actual: ResourceRevision,
    },
    /// A desired node ruleset could not be serialized.
    #[error("failed to serialize NodeFirewall for `{node_id}`: {message}")]
    SerializeNodeFirewall { node_id: String, message: String },
    /// A complete policy status replacement could not be serialized.
    #[error("failed to serialize FirewallPolicy `{policy_id}`: {message}")]
    SerializePolicy {
        policy_id: FirewallPolicyId,
        message: String,
    },
}
