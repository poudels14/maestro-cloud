use std::fmt::Display;

use kernel_api::{Object, ResourceKind, ResourceName, ResourceRevision};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoredValue, Transaction, TransactionOutcome,
};
use serde::Serialize;

use crate::snapshot::{ResourceSnapshot, StoredResource};
use crate::{IngressPlan, ResourceStatusUpdate};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct IngressWriteReport {
    pub(crate) created_generations: usize,
    pub(crate) updated_generations: usize,
    pub(crate) updated_routes: usize,
    pub(crate) deleted_generations: usize,
    pub(crate) conflict: bool,
}

pub(crate) struct IngressWriter {
    keyspace: Keyspace,
    generation_kind: ResourceKind,
}

impl IngressWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            generation_kind: ResourceKind::new("TrafficGeneration")?,
        })
    }

    /// Verifies that every backend input and the leadership fence are still exact.
    pub(crate) async fn preflight(
        &self,
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
    ) -> Result<bool, IngressWriteError> {
        let outcome = store
            .txn(Transaction {
                compares: snapshot.dependency_compares(),
                mutations: Vec::new(),
            })
            .await?;
        Ok(matches!(outcome, TransactionOutcome::Applied { .. }))
    }

    /// Commits one all-or-nothing traffic resource generation after publication.
    ///
    /// Backend publication intentionally precedes this transaction. Cancellation
    /// or a resource conflict can therefore leave the idempotent backend ahead of
    /// status; the next level-triggered pass republishes and acknowledges it.
    pub(crate) async fn apply(
        &self,
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
        plan: &IngressPlan,
    ) -> Result<IngressWriteReport, IngressWriteError> {
        let mut compares = snapshot.dependency_compares();
        let mut mutations = Vec::new();
        for generation in &plan.create_generations {
            let key = self.keyspace.resource(
                &self.generation_kind,
                &ResourceName::from(generation.meta.id.clone()),
            );
            compares.push(Compare {
                key: key.clone(),
                expected: ExpectedVersion::Missing,
            });
            mutations.push(Mutation::Put {
                key,
                value: serialize("TrafficGeneration", &generation.meta.id, generation)?,
                session: None,
            });
        }
        for update in &plan.generation_updates {
            let current = required(&snapshot.generations, &update.id, "TrafficGeneration")?;
            let resource = status_replacement(current, update, "TrafficGeneration")?;
            mutations.push(put(
                &current.stored,
                &resource,
                "TrafficGeneration",
                &update.id,
            )?);
        }
        for update in &plan.route_updates {
            let current = required(&snapshot.routes, &update.id, "IngressRoute")?;
            let resource = status_replacement(current, update, "IngressRoute")?;
            mutations.push(put(&current.stored, &resource, "IngressRoute", &update.id)?);
        }
        for id in &plan.delete_generations {
            mutations.push(delete(required(
                &snapshot.generations,
                id,
                "TrafficGeneration",
            )?));
        }

        let outcome = store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        if outcome == TransactionOutcome::Conflict {
            return Ok(IngressWriteReport {
                conflict: true,
                ..Default::default()
            });
        }
        Ok(IngressWriteReport {
            created_generations: plan.create_generations.len(),
            updated_generations: plan.generation_updates.len(),
            updated_routes: plan.route_updates.len(),
            deleted_generations: plan.delete_generations.len(),
            conflict: false,
        })
    }
}

fn required<'a, Id: Ord + Display, Resource>(
    resources: &'a std::collections::BTreeMap<Id, StoredResource<Resource>>,
    id: &Id,
    kind: &'static str,
) -> Result<&'a StoredResource<Resource>, IngressWriteError> {
    resources
        .get(id)
        .ok_or_else(|| IngressWriteError::MissingPlannedResource {
            kind,
            resource_id: id.to_string(),
        })
}

fn status_replacement<Id, Spec, Status>(
    current: &StoredResource<Object<Id, Spec, Status>>,
    update: &ResourceStatusUpdate<Id, Status>,
    kind: &'static str,
) -> Result<Object<Id, Spec, Status>, IngressWriteError>
where
    Id: Clone + Display,
    Spec: Clone,
    Status: Clone,
{
    let actual = current.stored.version.resource_revision();
    if update.observed_revision != actual {
        return Err(IngressWriteError::ObservedRevisionMismatch {
            kind,
            resource_id: update.id.to_string(),
            planned: update.observed_revision,
            actual,
        });
    }
    let mut resource = current.resource.clone();
    resource.meta.revision = actual;
    resource.status = update.status.clone();
    Ok(resource)
}

fn put(
    current: &StoredValue,
    resource: &impl Serialize,
    kind: &'static str,
    id: &impl Display,
) -> Result<Mutation, IngressWriteError> {
    Ok(Mutation::Put {
        key: current.key.clone(),
        value: serialize(kind, id, resource)?,
        session: None,
    })
}

fn delete<Resource>(current: &StoredResource<Resource>) -> Mutation {
    Mutation::Delete {
        key: current.stored.key.clone(),
    }
}

fn serialize(
    kind: &str,
    id: &impl Display,
    resource: &impl Serialize,
) -> Result<Vec<u8>, IngressWriteError> {
    serde_json::to_vec(resource).map_err(|error| IngressWriteError::Serialize {
        kind: kind.to_string(),
        resource_id: id.to_string(),
        message: error.to_string(),
    })
}

/// Atomic traffic mutation validation and persistence failures.
#[derive(Debug, thiserror::Error)]
pub enum IngressWriteError {
    /// The controller kernel rejected the fenced transaction.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// A planner mutation referenced a resource absent from its input snapshot.
    #[error("planned {kind} `{resource_id}` is absent from the resource snapshot")]
    MissingPlannedResource {
        kind: &'static str,
        resource_id: String,
    },
    /// A planner update did not retain the revision from its input resource.
    #[error(
        "planned {kind} `{resource_id}` revision {planned:?} does not match snapshot {actual:?}"
    )]
    ObservedRevisionMismatch {
        kind: &'static str,
        resource_id: String,
        planned: ResourceRevision,
        actual: ResourceRevision,
    },
    /// A complete resource replacement could not be serialized.
    #[error("failed to serialize {kind} `{resource_id}`: {message}")]
    Serialize {
        kind: String,
        resource_id: String,
        message: String,
    },
}
