use std::fmt::Display;

use kernel_api::{Object, ResourceKind, ResourceName, ResourceRevision};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoredValue, Transaction, TransactionOutcome,
};
use serde::Serialize;

use crate::snapshot::{ResourceSnapshot, StoredResource};
use crate::{DeploymentPlan, ResourceStatusUpdate};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DeploymentWriteReport {
    pub(crate) created_deployments: usize,
    pub(crate) created_builds: usize,
    pub(crate) updated_deployments: usize,
    pub(crate) updated_services: usize,
    pub(crate) deleted_deployments: usize,
    pub(crate) deleted_builds: usize,
    pub(crate) deleted_replicas: usize,
    pub(crate) conflict: bool,
}

pub(crate) struct DeploymentWriter {
    keyspace: Keyspace,
    deployment_kind: ResourceKind,
    build_kind: ResourceKind,
}

impl DeploymentWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            deployment_kind: ResourceKind::new("Deployment")?,
            build_kind: ResourceKind::new("Build")?,
        })
    }

    /// Commits one lifecycle generation atomically under the active leader fence.
    ///
    /// Cancellation may leave the entire transaction committed. A subsequent
    /// relist observes that generation and computes the next level-triggered step;
    /// no partial combination of child, status, or deletion mutations is visible.
    pub(crate) async fn apply(
        &self,
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
        plan: &DeploymentPlan,
    ) -> Result<DeploymentWriteReport, DeploymentWriteError> {
        let mut compares = snapshot.dependency_compares();
        let mut mutations = Vec::new();

        for deployment in &plan.create_deployments {
            self.create(
                &self.deployment_kind,
                &deployment.meta.id,
                deployment,
                &mut compares,
                &mut mutations,
            )?;
        }
        for build in &plan.create_builds {
            self.create(
                &self.build_kind,
                &build.meta.id,
                build,
                &mut compares,
                &mut mutations,
            )?;
        }
        for update in &plan.deployment_updates {
            let current = required(&snapshot.deployments, &update.id, "Deployment")?;
            let resource = status_replacement(current, update, "Deployment")?;
            mutations.push(put(&current.stored, &resource, "Deployment", &update.id)?);
        }
        for update in &plan.service_updates {
            let current = required(&snapshot.services, &update.id, "Service")?;
            let resource = status_replacement(current, update, "Service")?;
            mutations.push(put(&current.stored, &resource, "Service", &update.id)?);
        }
        for id in &plan.delete_replicas {
            mutations.push(delete(required(&snapshot.replicas, id, "ReplicaState")?));
        }
        for id in &plan.delete_builds {
            mutations.push(delete(required(&snapshot.builds, id, "Build")?));
        }
        for id in &plan.delete_deployments {
            mutations.push(delete(required(&snapshot.deployments, id, "Deployment")?));
        }

        let outcome = store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        if outcome == TransactionOutcome::Conflict {
            return Ok(DeploymentWriteReport {
                conflict: true,
                ..Default::default()
            });
        }
        Ok(DeploymentWriteReport {
            created_deployments: plan.create_deployments.len(),
            created_builds: plan.create_builds.len(),
            updated_deployments: plan.deployment_updates.len(),
            updated_services: plan.service_updates.len(),
            deleted_deployments: plan.delete_deployments.len(),
            deleted_builds: plan.delete_builds.len(),
            deleted_replicas: plan.delete_replicas.len(),
            conflict: false,
        })
    }

    fn create<Id: Clone + Display + Into<ResourceName>, Resource: Serialize>(
        &self,
        kind: &ResourceKind,
        id: &Id,
        resource: &Resource,
        compares: &mut Vec<Compare>,
        mutations: &mut Vec<Mutation>,
    ) -> Result<(), DeploymentWriteError> {
        let key = self.keyspace.resource(kind, &id.clone().into());
        compares.push(Compare {
            key: key.clone(),
            expected: ExpectedVersion::Missing,
        });
        mutations.push(Mutation::Put {
            key,
            value: serialize(kind.as_str(), id, resource)?,
            session: None,
        });
        Ok(())
    }
}

fn required<'a, Id: Ord + Display, Resource>(
    resources: &'a std::collections::BTreeMap<Id, StoredResource<Resource>>,
    id: &Id,
    kind: &'static str,
) -> Result<&'a StoredResource<Resource>, DeploymentWriteError> {
    resources
        .get(id)
        .ok_or_else(|| DeploymentWriteError::MissingPlannedResource {
            kind,
            resource_id: id.to_string(),
        })
}

fn status_replacement<Id, Spec, Status>(
    current: &StoredResource<Object<Id, Spec, Status>>,
    update: &ResourceStatusUpdate<Id, Status>,
    kind: &'static str,
) -> Result<Object<Id, Spec, Status>, DeploymentWriteError>
where
    Id: Clone + Display,
    Spec: Clone,
    Status: Clone,
{
    let actual = current.stored.version.resource_revision();
    if update.observed_revision != actual {
        return Err(DeploymentWriteError::ObservedRevisionMismatch {
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

fn put<Id: Display, Resource: Serialize>(
    current: &StoredValue,
    resource: &Resource,
    kind: &'static str,
    id: &Id,
) -> Result<Mutation, DeploymentWriteError> {
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
) -> Result<Vec<u8>, DeploymentWriteError> {
    serde_json::to_vec(resource).map_err(|error| DeploymentWriteError::Serialize {
        kind: kind.to_string(),
        resource_id: id.to_string(),
        message: error.to_string(),
    })
}

/// Atomic lifecycle mutation validation and persistence failures.
#[derive(Debug, thiserror::Error)]
pub enum DeploymentWriteError {
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
