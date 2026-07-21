use kernel_api::{FirewallPolicy, FirewallPolicyId, ResourceRevision};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{Mutation, Transaction, TransactionOutcome};

use crate::snapshot::{ResourceSnapshot, StoredResource};
use crate::{FirewallPlan, FirewallPolicyStatusUpdate};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FirewallWriteReport {
    pub(crate) updated_policies: usize,
    pub(crate) conflict: bool,
}

pub(crate) struct FirewallWriter;

impl FirewallWriter {
    /// Verifies that every backend input and the leadership fence are still exact.
    pub(crate) async fn preflight(
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
    ) -> Result<bool, FirewallWriteError> {
        let outcome = store
            .txn(Transaction {
                compares: snapshot.dependency_compares(),
                mutations: Vec::new(),
            })
            .await?;
        Ok(matches!(outcome, TransactionOutcome::Applied { .. }))
    }

    /// Atomically acknowledges policies only after the exact bundle was applied.
    ///
    /// A backend can be ahead after cancellation or a resource conflict. The next
    /// level-triggered pass reapplies the idempotent bundle before acknowledging it.
    pub(crate) async fn apply(
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
        plan: &FirewallPlan,
    ) -> Result<FirewallWriteReport, FirewallWriteError> {
        let mutations = plan
            .policy_updates
            .iter()
            .map(|update| status_put(snapshot, update))
            .collect::<Result<Vec<_>, _>>()?;
        let outcome = store
            .txn(Transaction {
                compares: snapshot.dependency_compares(),
                mutations,
            })
            .await?;
        if outcome == TransactionOutcome::Conflict {
            return Ok(FirewallWriteReport {
                conflict: true,
                ..Default::default()
            });
        }
        Ok(FirewallWriteReport {
            updated_policies: plan.policy_updates.len(),
            conflict: false,
        })
    }
}

fn status_put(
    snapshot: &ResourceSnapshot,
    update: &FirewallPolicyStatusUpdate,
) -> Result<Mutation, FirewallWriteError> {
    let current = required(&snapshot.policies, &update.policy_id)?;
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
        value: serde_json::to_vec(&resource).map_err(|error| FirewallWriteError::Serialize {
            policy_id: update.policy_id.clone(),
            message: error.to_string(),
        })?,
        session: None,
    })
}

fn required<'a>(
    policies: &'a std::collections::BTreeMap<FirewallPolicyId, StoredResource<FirewallPolicy>>,
    id: &FirewallPolicyId,
) -> Result<&'a StoredResource<FirewallPolicy>, FirewallWriteError> {
    policies
        .get(id)
        .ok_or_else(|| FirewallWriteError::MissingPlannedPolicy {
            policy_id: id.clone(),
        })
}

/// Atomic firewall status validation and persistence failures.
#[derive(Debug, thiserror::Error)]
pub enum FirewallWriteError {
    /// The controller kernel rejected the fenced transaction.
    #[error(transparent)]
    Controller(#[from] ControllerError),
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
    /// A complete policy status replacement could not be serialized.
    #[error("failed to serialize FirewallPolicy `{policy_id}`: {message}")]
    Serialize {
        policy_id: FirewallPolicyId,
        message: String,
    },
}
