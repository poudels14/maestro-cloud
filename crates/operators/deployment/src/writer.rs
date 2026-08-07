use std::collections::BTreeSet;
use std::fmt::Display;

use kernel_api::{Object, ResourceKind, ResourceName, ResourceRevision};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoredValue, Transaction, TransactionOutcome,
};
use serde::Serialize;

use crate::snapshot::{ResourceSnapshot, StoredResource};
use crate::{DeploymentPlan, ResourceStatusUpdate, ServiceUpdate};

// etcd counts every compare and mutation against one transaction's operation
// limit. FencedStore contributes one additional leadership compare.
const ETCD_TRANSACTION_OPERATION_LIMIT: usize = 128;
const FENCED_STORE_COMPARE_COUNT: usize = 1;

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
    /// Independent history and garbage-collection writes are bounded and
    /// level-triggered. The Service pointer and both sides of an active cutover
    /// remain one atomic transaction.
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

        let primary = snapshot.primary_compares();
        let batch_size = transaction_batch_size(primary.len(), 2)?;
        let mut writes = Vec::new();
        let mut cutover_writes = Vec::new();
        let cutover_deployments = cutover_deployments(snapshot, plan);

        for deployment in &plan.create_deployments {
            writes.push(self.create(
                &self.deployment_kind,
                &deployment.meta.id,
                deployment,
                WriteKind::DeploymentCreated,
            )?);
        }
        for build in &plan.create_builds {
            writes.push(self.create(
                &self.build_kind,
                &build.meta.id,
                build,
                WriteKind::BuildCreated,
            )?);
        }
        for update in &plan.deployment_updates {
            let current = required(&snapshot.deployments, &update.id, "Deployment")?;
            let resource = status_replacement(current, update, "Deployment")?;
            let write = PlannedWrite {
                compare: exact(&current.stored),
                mutation: put(&current.stored, &resource, "Deployment", &update.id)?,
                kind: WriteKind::DeploymentUpdated,
            };
            if cutover_deployments.contains(&update.id) {
                cutover_writes.push(write);
            } else {
                writes.push(write);
            }
        }
        for update in &plan.service_updates {
            let current = required(&snapshot.services, &update.id, "Service")?;
            let resource = service_replacement(current, update)?;
            cutover_writes.push(PlannedWrite {
                compare: exact(&current.stored),
                mutation: put(&current.stored, &resource, "Service", &update.id)?,
                kind: WriteKind::ServiceUpdated,
            });
        }

        let mut report = DeploymentWriteReport {
            deleted_deployments: garbage.deleted_deployments,
            deleted_builds: garbage.deleted_builds,
            deleted_replicas: garbage.deleted_replicas,
            ..Default::default()
        };
        self.apply_write_batches(store, &primary, &writes, batch_size, &mut report)
            .await?;
        if report.conflict {
            return Ok(report);
        }
        if cutover_writes.len() > batch_size {
            return Err(DeploymentWriteError::AtomicGroupTooLarge {
                operations: transaction_operations(primary.len(), cutover_writes.len(), 2),
                limit: ETCD_TRANSACTION_OPERATION_LIMIT,
            });
        }
        self.apply_write_batches(store, &primary, &cutover_writes, batch_size, &mut report)
            .await?;
        Ok(report)
    }

    async fn apply_write_batches(
        &self,
        store: &FencedStore,
        primary: &[Compare],
        writes: &[PlannedWrite],
        batch_size: usize,
        report: &mut DeploymentWriteReport,
    ) -> Result<(), DeploymentWriteError> {
        for batch in writes.chunks(batch_size) {
            let mut compares = primary.to_vec();
            compares.extend(batch.iter().map(|write| write.compare.clone()));
            let mutations = batch.iter().map(|write| write.mutation.clone()).collect();
            if store
                .txn(Transaction {
                    compares,
                    mutations,
                })
                .await?
                == TransactionOutcome::Conflict
            {
                report.conflict = true;
                return Ok(());
            }
            for write in batch {
                write.kind.record(report);
            }
        }
        Ok(())
    }

    async fn collect_garbage(
        &self,
        store: &FencedStore,
        snapshot: &ResourceSnapshot,
        plan: &DeploymentPlan,
    ) -> Result<GarbageCollectionReport, DeploymentWriteError> {
        let mut report = GarbageCollectionReport::default();
        let primary = snapshot.primary_compares();

        let replica_batch_size = transaction_batch_size(primary.len(), 3)?;
        let simple_batch_size = transaction_batch_size(primary.len(), 2)?;

        for ids in plan.delete_replicas.chunks(replica_batch_size) {
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
                simple_batch_size,
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
                simple_batch_size,
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
        batch_size: usize,
    ) -> Result<BatchReport, DeploymentWriteError>
    where
        Id: Ord + Display,
    {
        let mut report = BatchReport::default();
        for ids in ids.chunks(batch_size) {
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
        write_kind: WriteKind,
    ) -> Result<PlannedWrite, DeploymentWriteError> {
        let key = self.keyspace.resource(kind, &id.clone().into());
        Ok(PlannedWrite {
            compare: Compare {
                key: key.clone(),
                expected: ExpectedVersion::Missing,
            },
            mutation: Mutation::Put {
                key,
                value: serialize(kind.as_str(), id, resource)?,
                session: None,
            },
            kind: write_kind,
        })
    }
}

#[derive(Debug, Clone)]
struct PlannedWrite {
    compare: Compare,
    mutation: Mutation,
    kind: WriteKind,
}

#[derive(Debug, Clone, Copy)]
enum WriteKind {
    DeploymentCreated,
    BuildCreated,
    DeploymentUpdated,
    ServiceUpdated,
}

impl WriteKind {
    fn record(self, report: &mut DeploymentWriteReport) {
        match self {
            Self::DeploymentCreated => {
                report.created_deployments = report.created_deployments.saturating_add(1);
            }
            Self::BuildCreated => {
                report.created_builds = report.created_builds.saturating_add(1);
            }
            Self::DeploymentUpdated => {
                report.updated_deployments = report.updated_deployments.saturating_add(1);
            }
            Self::ServiceUpdated => {
                report.updated_services = report.updated_services.saturating_add(1);
            }
        }
    }
}

fn cutover_deployments(
    snapshot: &ResourceSnapshot,
    plan: &DeploymentPlan,
) -> BTreeSet<kernel_api::DeploymentId> {
    let mut deployments = snapshot
        .services
        .values()
        .filter_map(|service| service.resource.status.active_deployment_id.clone())
        .collect::<BTreeSet<_>>();
    deployments.extend(
        plan.service_updates
            .iter()
            .filter_map(|update| update.status.active_deployment_id.clone()),
    );
    deployments
}

fn transaction_batch_size(
    primary_compares: usize,
    operations_per_item: usize,
) -> Result<usize, DeploymentWriteError> {
    let fixed = primary_compares.saturating_add(FENCED_STORE_COMPARE_COUNT);
    let available = ETCD_TRANSACTION_OPERATION_LIMIT.saturating_sub(fixed);
    let capacity = available / operations_per_item;
    if capacity == 0 {
        Err(DeploymentWriteError::AtomicGroupTooLarge {
            operations: fixed.saturating_add(operations_per_item),
            limit: ETCD_TRANSACTION_OPERATION_LIMIT,
        })
    } else {
        Ok(capacity)
    }
}

fn transaction_operations(
    primary_compares: usize,
    items: usize,
    operations_per_item: usize,
) -> usize {
    primary_compares
        .saturating_add(FENCED_STORE_COMPARE_COUNT)
        .saturating_add(items.saturating_mul(operations_per_item))
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
    /// One lifecycle group that must remain atomic cannot fit in etcd.
    #[error("deployment lifecycle atomic group requires {operations} operations; limit is {limit}")]
    AtomicGroupTooLarge { operations: usize, limit: usize },
}
