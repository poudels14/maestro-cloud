use std::collections::BTreeSet;

use kernel_api::{ResourceKind, ResourceName, UpgradeRun};
use kernel_controller::FencedStore;
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, Transaction, TransactionOutcome, Version,
};

use crate::snapshot::UpgradeSnapshot;
use crate::{UpgradeError, UpgradePlan};

pub(crate) struct UpgradeWriter {
    keyspace: Keyspace,
    run_kind: ResourceKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpgradeWriteOutcome {
    Noop,
    Applied,
    Conflict,
}

impl UpgradeWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            run_kind: ResourceKind::new("UpgradeRun")?,
        })
    }

    pub(crate) async fn apply(
        &self,
        store: &FencedStore,
        observed_version: Version,
        observed_run: &UpgradeRun,
        snapshot: &UpgradeSnapshot,
        plan: &UpgradePlan,
    ) -> Result<UpgradeWriteOutcome, UpgradeError> {
        let run_key = self.keyspace.resource(
            &self.run_kind,
            &ResourceName::from(observed_run.meta.id.clone()),
        );
        let mut compares = vec![Compare {
            key: run_key.clone(),
            expected: ExpectedVersion::Exact(observed_version),
        }];
        compares.extend(snapshot.dependency_compares());
        let mut mutations = Vec::new();
        let mut updated_nodes = BTreeSet::new();
        for desired in &plan.node_updates {
            if !updated_nodes.insert(desired.meta.id.clone()) {
                return Err(UpgradeError::DuplicateNodeUpdate {
                    node_id: desired.meta.id.clone(),
                });
            }
            let current = snapshot.nodes.get(&desired.meta.id).ok_or_else(|| {
                UpgradeError::PlannedNodeMissing {
                    node_id: desired.meta.id.clone(),
                }
            })?;
            if current.resource != *desired {
                mutations.push(Mutation::Put {
                    key: current.stored.key.clone(),
                    value: serialize(desired)?,
                    session: None,
                });
            }
        }
        if plan.run != *observed_run {
            mutations.push(Mutation::Put {
                key: run_key,
                value: serialize(&plan.run)?,
                session: None,
            });
        }
        let no_mutations = mutations.is_empty();
        let outcome = store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        Ok(match outcome {
            TransactionOutcome::Applied { .. } if no_mutations => UpgradeWriteOutcome::Noop,
            TransactionOutcome::Applied { .. } => UpgradeWriteOutcome::Applied,
            TransactionOutcome::Conflict => UpgradeWriteOutcome::Conflict,
        })
    }
}

fn serialize(value: &impl serde::Serialize) -> Result<Vec<u8>, UpgradeError> {
    serde_json::to_vec(value).map_err(|error| UpgradeError::Serialize {
        message: error.to_string(),
    })
}
