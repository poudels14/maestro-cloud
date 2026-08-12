use kernel_api::{Preview, ResourceKind, ResourceName};
use kernel_controller::FencedStore;
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction, TransactionOutcome};

use crate::PreviewError;
use crate::source_plan::PreviewSourcePlan;
use crate::source_snapshot::PreviewSourceSnapshot;

pub(crate) struct PreviewSourceWriter {
    keyspace: Keyspace,
    preview_kind: ResourceKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreviewSourceWriteOutcome {
    Noop,
    Applied,
    Conflict,
}

impl PreviewSourceWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            preview_kind: ResourceKind::new("Preview")?,
        })
    }

    pub(crate) async fn apply(
        &self,
        store: &FencedStore,
        snapshot: &PreviewSourceSnapshot,
        plan: &PreviewSourcePlan,
    ) -> Result<PreviewSourceWriteOutcome, PreviewError> {
        let mut compares = snapshot
            .nodes
            .values()
            .map(|resource| Compare {
                key: resource.stored.key.clone(),
                expected: ExpectedVersion::Exact(resource.stored.version),
            })
            .collect::<Vec<_>>();
        compares.extend(snapshot.services.values().map(|service| Compare {
            key: service.stored.key.clone(),
            expected: ExpectedVersion::Exact(service.stored.version),
        }));
        compares.extend(snapshot.previews.values().map(|preview| Compare {
            key: preview.stored.key.clone(),
            expected: ExpectedVersion::Exact(preview.stored.version),
        }));
        let mut mutations = Vec::new();
        for preview in &plan.creates {
            let key = self.preview_key(preview);
            compares.push(Compare {
                key: key.clone(),
                expected: ExpectedVersion::Missing,
            });
            mutations.push(Mutation::Put {
                key,
                value: serialize(preview)?,
                session: None,
            });
        }
        for preview in &plan.updates {
            let Some(current) = snapshot.previews.get(&preview.meta.id) else {
                return Err(PreviewError::InvalidDefinition {
                    message: format!(
                        "planned update references missing preview `{}`",
                        preview.meta.id
                    ),
                });
            };
            mutations.push(Mutation::Put {
                key: current.stored.key.clone(),
                value: serialize(preview)?,
                session: None,
            });
        }
        if mutations.is_empty() {
            return Ok(PreviewSourceWriteOutcome::Noop);
        }
        let outcome = store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        Ok(match outcome {
            TransactionOutcome::Applied { .. } => PreviewSourceWriteOutcome::Applied,
            TransactionOutcome::Conflict => PreviewSourceWriteOutcome::Conflict,
        })
    }

    fn preview_key(&self, preview: &Preview) -> kernel_store::StoreKey {
        self.keyspace.resource(
            &self.preview_kind,
            &ResourceName::from(preview.meta.id.clone()),
        )
    }
}

fn serialize(preview: &Preview) -> Result<Vec<u8>, PreviewError> {
    serde_json::to_vec(preview).map_err(|error| PreviewError::Serialize {
        message: error.to_string(),
    })
}
