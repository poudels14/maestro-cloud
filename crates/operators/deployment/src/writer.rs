use std::fmt::Display;

use kernel_api::{Object, ResourceKind, ResourceName, ResourceRevision};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoredValue, Transaction, TransactionOutcome,
};
use serde::Serialize;

use crate::snapshot::{ResourceSnapshot, StoredResource};
use crate::{DeploymentPlan, ResourceStatusUpdate, ServiceUpdate};

// FencedStore adds one leader compare. Each simple deletion consumes one
// compare and one mutation; replica deletion also proves its Assignment is
// still absent. These batch sizes stay below etcd's default 128-op limit while
// leaving room for the primary Service compare.
const SIMPLE_GC_BATCH_SIZE: usize = 60;
const REPLICA_GC_BATCH_SIZE: usize = 40;

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
    assignment_kind: ResourceKind,
    deployment_kind: ResourceKind,
    build_kind: ResourceKind,
}

impl DeploymentWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            assignment_kind: ResourceKind::new("Assignment")?,
            deployment_kind: ResourceKind::new("Deployment")?,
            build_kind: ResourceKind::new("Build")?,
        })
    }

    /// Commits one lifecycle generation under the active leader fence.
    ///
    /// Status and creation mutations remain one atomic transaction. Independent
    /// garbage collection is bounded and level-triggered so retained history can
    /// never exceed a backend transaction limit.
    pub(crate) async fn apply(
        &self,
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
        plan: &DeploymentPlan,
    ) -> Result<DeploymentWriteReport, DeploymentWriteError> {
        let garbage = self.collect_garbage(store, snapshot, plan).await?;
        if garbage.conflict {
            return Ok(DeploymentWriteReport {
                deleted_deployments: garbage.deleted_deployments,
                deleted_builds: garbage.deleted_builds,
                deleted_replicas: garbage.deleted_replicas,
                conflict: true,
                ..Default::default()
            });
        }

        let mut compares = snapshot.primary_compares();
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
            compares.push(exact(&current.stored));
            mutations.push(put(&current.stored, &resource, "Deployment", &update.id)?);
        }
        for update in &plan.service_updates {
            let current = required(&snapshot.services, &update.id, "Service")?;
            let resource = service_replacement(current, update)?;
            compares.push(exact(&current.stored));
            mutations.push(put(&current.stored, &resource, "Service", &update.id)?);
        }

        let outcome = store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        if outcome == TransactionOutcome::Conflict {
            return Ok(DeploymentWriteReport {
                deleted_deployments: garbage.deleted_deployments,
                deleted_builds: garbage.deleted_builds,
                deleted_replicas: garbage.deleted_replicas,
                conflict: true,
                ..Default::default()
            });
        }
        Ok(DeploymentWriteReport {
            created_deployments: plan.create_deployments.len(),
            created_builds: plan.create_builds.len(),
            updated_deployments: plan.deployment_updates.len(),
            updated_services: plan.service_updates.len(),
            deleted_deployments: garbage.deleted_deployments,
            deleted_builds: garbage.deleted_builds,
            deleted_replicas: garbage.deleted_replicas,
            conflict: false,
        })
    }

    async fn collect_garbage(
        &self,
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
        plan: &DeploymentPlan,
    ) -> Result<GarbageCollectionReport, DeploymentWriteError> {
        let mut report = GarbageCollectionReport::default();
        let primary = snapshot.primary_compares();

        for ids in plan.delete_replicas.chunks(REPLICA_GC_BATCH_SIZE) {
            let mut compares = primary.clone();
            let mut mutations = Vec::with_capacity(ids.len());
            for id in ids {
                let replica = required(&snapshot.replicas, id, "ReplicaState")?;
                compares.push(exact(&replica.stored));
                compares.push(Compare {
                    key: self.keyspace.resource(
                        &self.assignment_kind,
                        &ResourceName::from(replica.resource.spec.assignment_id.clone()),
                    ),
                    expected: ExpectedVersion::Missing,
                });
                mutations.push(delete(replica));
            }
            if store
                .txn(Transaction {
                    compares,
                    mutations,
                })
                .await?
                == TransactionOutcome::Conflict
            {
                report.conflict = true;
                return Ok(report);
            }
            report.deleted_replicas += ids.len();
        }

        let builds = self
            .delete_batches(
                store,
                &primary,
                &snapshot.builds,
                &plan.delete_builds,
                "Build",
            )
            .await?;
        report.deleted_builds = builds.applied;
        if builds.conflict {
            report.conflict = true;
            return Ok(report);
        }

        let deployments = self
            .delete_batches(
                store,
                &primary,
                &snapshot.deployments,
                &plan.delete_deployments,
                "Deployment",
            )
            .await?;
        report.deleted_deployments = deployments.applied;
        report.conflict = deployments.conflict;
        Ok(report)
    }

    async fn delete_batches<Id, Resource>(
        &self,
        store: &FencedStore,
        primary: &[Compare],
        resources: &std::collections::BTreeMap<Id, StoredResource<Resource>>,
        ids: &[Id],
        kind: &'static str,
    ) -> Result<BatchReport, DeploymentWriteError>
    where
        Id: Ord + Display,
    {
        let mut report = BatchReport::default();
        for ids in ids.chunks(SIMPLE_GC_BATCH_SIZE) {
            let mut compares = primary.to_vec();
            let mut mutations = Vec::with_capacity(ids.len());
            for id in ids {
                let resource = required(resources, id, kind)?;
                compares.push(exact(&resource.stored));
                mutations.push(delete(resource));
            }
            if store
                .txn(Transaction {
                    compares,
                    mutations,
                })
                .await?
                == TransactionOutcome::Conflict
            {
                report.conflict = true;
                return Ok(report);
            }
            report.applied += ids.len();
        }
        Ok(report)
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

#[derive(Debug, Clone, Copy, Default)]
struct GarbageCollectionReport {
    deleted_deployments: usize,
    deleted_builds: usize,
    deleted_replicas: usize,
    conflict: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct BatchReport {
    applied: usize,
    conflict: bool,
}

fn exact(current: &StoredValue) -> Compare {
    Compare {
        key: current.key.clone(),
        expected: ExpectedVersion::Exact(current.version),
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

fn service_replacement(
    current: &StoredResource<kernel_api::Service>,
    update: &ServiceUpdate,
) -> Result<kernel_api::Service, DeploymentWriteError> {
    let actual = current.stored.version.resource_revision();
    if update.observed_revision != actual {
        return Err(DeploymentWriteError::ObservedRevisionMismatch {
            kind: "Service",
            resource_id: update.id.to_string(),
            planned: update.observed_revision,
            actual,
        });
    }
    let mut resource = current.resource.clone();
    resource.meta.revision = actual;
    resource.meta.generation = update.generation;
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
