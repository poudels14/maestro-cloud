use std::collections::BTreeMap;

use kernel_api::{
    Assignment, AssignmentId, ClusterId, InvalidIdentifier, ResourceKind, ResourceName, ServiceId,
    UnschedulableReplica,
};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, StoredValue, TRANSACTION_OPERATION_LIMIT,
    Transaction, TransactionOutcome, Version,
};

// FencedStore adds the leadership compare to the store operation count.
const FENCED_STORE_COMPARE_COUNT: usize = 1;

/// Atomic assignment changes applied by one successfully fenced scheduler pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AssignmentWriteReport {
    pub(crate) created: usize,
    pub(crate) deleted: usize,
    pub(crate) conflict: bool,
}

pub(crate) struct AssignmentWriteFence {
    pub(crate) scheduler_generation: ExpectedVersion,
    pub(crate) dependency_compares: Vec<Compare>,
}

pub(crate) struct AssignmentWriter {
    keyspace: Keyspace,
    assignment_kind: ResourceKind,
}

impl AssignmentWriter {
    pub(crate) fn new(cluster_id: &ClusterId) -> Result<Self, InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            assignment_kind: ResourceKind::new("Assignment")?,
        })
    }

    /// Applies one Service's all-or-nothing assignment diff while preserving agent-owned status.
    ///
    /// Cancellation may leave the complete transaction committed. A subsequent resource relist
    /// observes that result and computes an empty diff; partial assignment generations are never
    /// visible.
    pub(crate) async fn apply(
        &self,
        fenced_store: &FencedStore,
        service_id: &ServiceId,
        current: &[StoredValue],
        desired: &[Assignment],
        observation: &[UnschedulableReplica],
        fence: AssignmentWriteFence,
    ) -> Result<AssignmentWriteReport, AssignmentWriteError> {
        let mut current = self.decode_current(current)?;
        current.retain(|_, assignment| assignment.resource.spec.service_id == *service_id);
        let mut desired = self.index_desired(desired)?;
        desired.retain(|_, assignment| assignment.spec.service_id == *service_id);
        let mut compares = fence.dependency_compares;
        let mut mutations = Vec::new();
        let mut created = 0_usize;
        let mut deleted = 0_usize;

        for (assignment_id, resource) in &desired {
            let key = self.assignment_key(assignment_id);
            match current.get(assignment_id) {
                Some(stored) if stored.resource.spec == resource.spec => {}
                Some(stored) => {
                    return Err(AssignmentWriteError::IdentityCollision {
                        assignment_id: assignment_id.clone(),
                        existing_version: stored.version,
                    });
                }
                None => {
                    compares.push(Compare {
                        key: key.clone(),
                        expected: ExpectedVersion::Missing,
                    });
                    mutations.push(Mutation::Put {
                        key,
                        value: serde_json::to_vec(resource).map_err(|error| {
                            AssignmentWriteError::Serialize {
                                assignment_id: assignment_id.clone(),
                                message: error.to_string(),
                            }
                        })?,
                        session: None,
                    });
                    created = created.saturating_add(1);
                }
            }
        }
        for (assignment_id, stored) in &current {
            if !desired.contains_key(assignment_id) {
                compares.push(Compare {
                    key: stored.key.clone(),
                    expected: ExpectedVersion::Exact(stored.version),
                });
                mutations.push(Mutation::Delete {
                    key: stored.key.clone(),
                });
                deleted = deleted.saturating_add(1);
            }
        }
        compares.push(Compare {
            key: self.keyspace.scheduler_generation(),
            expected: fence.scheduler_generation,
        });
        let assignments_changed = !mutations.is_empty();
        if assignments_changed {
            mutations.push(Mutation::Put {
                key: self.keyspace.scheduler_generation(),
                value: b"assignment generation".to_vec(),
                session: None,
            });
        }
        self.append_observation(
            fenced_store,
            service_id,
            observation,
            &mut compares,
            &mut mutations,
        )
        .await?;

        if mutations.is_empty() {
            return Ok(AssignmentWriteReport::default());
        }
        let operations = compares
            .len()
            .saturating_add(mutations.len())
            .saturating_add(FENCED_STORE_COMPARE_COUNT);
        if operations > TRANSACTION_OPERATION_LIMIT {
            return Err(AssignmentWriteError::AtomicGroupTooLarge {
                service_id: service_id.clone(),
                operations,
                limit: TRANSACTION_OPERATION_LIMIT,
            });
        }

        let outcome = fenced_store
            .txn(Transaction {
                compares,
                mutations,
            })
            .await?;
        match outcome {
            TransactionOutcome::Applied { .. } => Ok(AssignmentWriteReport {
                created,
                deleted,
                conflict: false,
            }),
            TransactionOutcome::Conflict => Ok(AssignmentWriteReport {
                conflict: true,
                ..Default::default()
            }),
        }
    }

    fn decode_current(
        &self,
        current: &[StoredValue],
    ) -> Result<BTreeMap<AssignmentId, StoredAssignment>, AssignmentWriteError> {
        let mut decoded = BTreeMap::new();
        for stored in current {
            let resource: Assignment = serde_json::from_slice(&stored.value).map_err(|error| {
                AssignmentWriteError::MalformedResource {
                    key: stored.key.to_string(),
                    message: error.to_string(),
                }
            })?;
            let expected_key = self.assignment_key(&resource.meta.id);
            if stored.key != expected_key {
                return Err(AssignmentWriteError::ResourceIdentityMismatch {
                    assignment_id: resource.meta.id,
                    key: stored.key.to_string(),
                });
            }
            let assignment_id = resource.meta.id.clone();
            if decoded
                .insert(
                    assignment_id.clone(),
                    StoredAssignment {
                        key: stored.key.clone(),
                        version: stored.version,
                        resource,
                    },
                )
                .is_some()
            {
                return Err(AssignmentWriteError::DuplicateAssignment { assignment_id });
            }
        }
        Ok(decoded)
    }

    fn index_desired<'a>(
        &self,
        desired: &'a [Assignment],
    ) -> Result<BTreeMap<AssignmentId, &'a Assignment>, AssignmentWriteError> {
        let mut indexed = BTreeMap::new();
        for resource in desired {
            let assignment_id = resource.meta.id.clone();
            if indexed.insert(assignment_id.clone(), resource).is_some() {
                return Err(AssignmentWriteError::DuplicateAssignment { assignment_id });
            }
        }
        Ok(indexed)
    }

    fn assignment_key(&self, assignment_id: &AssignmentId) -> kernel_store::StoreKey {
        self.keyspace.resource(
            &self.assignment_kind,
            &ResourceName::from(assignment_id.clone()),
        )
    }

    async fn append_observation(
        &self,
        fenced_store: &FencedStore,
        service_id: &ServiceId,
        observation: &[UnschedulableReplica],
        compares: &mut Vec<Compare>,
        mutations: &mut Vec<Mutation>,
    ) -> Result<(), AssignmentWriteError> {
        let key = self.keyspace.scheduler_observation();
        let current = fenced_store.get(&key).await?;
        let mut merged = match current.as_ref() {
            Some(stored) => serde_json::from_slice::<Vec<UnschedulableReplica>>(&stored.value)
                .map_err(|error| AssignmentWriteError::MalformedObservation {
                    message: error.to_string(),
                })?,
            None => Vec::new(),
        };
        merged.retain(|failure| failure.service_id != *service_id);
        merged.extend(
            observation
                .iter()
                .filter(|failure| failure.service_id == *service_id)
                .cloned(),
        );
        merged.sort_by(|left, right| {
            left.service_id
                .cmp(&right.service_id)
                .then_with(|| left.deployment_id.cmp(&right.deployment_id))
                .then_with(|| left.replica_index.cmp(&right.replica_index))
        });
        let value = serde_json::to_vec(&merged).map_err(|error| {
            AssignmentWriteError::SerializeObservation {
                message: error.to_string(),
            }
        })?;
        match current {
            Some(stored) if stored.value == value => {}
            Some(stored) => {
                compares.push(Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Exact(stored.version),
                });
                mutations.push(Mutation::Put {
                    key,
                    value,
                    session: None,
                });
            }
            None => {
                compares.push(Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Missing,
                });
                mutations.push(Mutation::Put {
                    key,
                    value,
                    session: None,
                });
            }
        }
        Ok(())
    }
}

struct StoredAssignment {
    key: kernel_store::StoreKey,
    version: Version,
    resource: Assignment,
}

/// Matchable assignment-set decoding, identity, serialization, and fencing failures.
#[derive(Debug, thiserror::Error)]
pub enum AssignmentWriteError {
    /// The controller kernel rejected the fenced transaction.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// Stored assignment JSON was invalid.
    #[error("malformed Assignment resource at `{key}`: {message}")]
    MalformedResource { key: String, message: String },
    /// Assignment metadata did not match its canonical key.
    #[error("Assignment `{assignment_id}` does not match store key `{key}`")]
    ResourceIdentityMismatch {
        assignment_id: AssignmentId,
        key: String,
    },
    /// The same assignment identity appeared more than once.
    #[error("Assignment `{assignment_id}` occurs more than once in one scheduler snapshot")]
    DuplicateAssignment { assignment_id: AssignmentId },
    /// A generated stable identity resolved to a different immutable specification.
    #[error(
        "generated Assignment `{assignment_id}` conflicts with immutable stored version {existing_version:?}"
    )]
    IdentityCollision {
        assignment_id: AssignmentId,
        existing_version: Version,
    },
    /// A desired assignment could not be serialized for persistence.
    #[error("failed to serialize Assignment `{assignment_id}`: {message}")]
    Serialize {
        assignment_id: AssignmentId,
        message: String,
    },
    /// The scheduler observation could not be serialized for persistence.
    #[error("failed to serialize scheduler observation: {message}")]
    SerializeObservation { message: String },
    /// The existing scheduler observation could not be decoded for a scoped update.
    #[error("stored scheduler observation is malformed: {message}")]
    MalformedObservation { message: String },
    /// One Service's assignment generation cannot fit in one etcd transaction.
    #[error(
        "Service `{service_id}` assignment generation requires {operations} operations; limit is {limit}"
    )]
    AtomicGroupTooLarge {
        service_id: ServiceId,
        operations: usize,
        limit: usize,
    },
}
