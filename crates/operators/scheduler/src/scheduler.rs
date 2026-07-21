use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, InvalidIdentifier, NodeId, Timestamp};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Store, StoreError};

use crate::assignment::{AssignmentWriteError, AssignmentWriter};
use crate::model::UnschedulableReplica;
use crate::projection::project;
use crate::resource::ResourceSnapshot;

/// Node replacement policy for one scheduler instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerSettings {
    /// Time an unavailable node retains its assignments before replacement is allowed.
    pub replacement_grace: Duration,
}

impl SchedulerSettings {
    /// Validates scheduler timing policy.
    pub fn validate(self) -> Result<Self, SchedulerError> {
        if self.replacement_grace.is_zero() {
            Err(SchedulerError::ZeroReplacementGrace)
        } else {
            Ok(self)
        }
    }
}

/// Result of one finite, globally fenced scheduling pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulerReport {
    /// Fully addressed assignments desired by the pure plan.
    pub desired: usize,
    /// Assignments created by the committed generation.
    pub created: usize,
    /// Assignments deleted by the committed generation.
    pub deleted: usize,
    /// Whether concurrent input or another scheduler invalidated the snapshot.
    pub conflict: bool,
    /// Replica slots that could not receive a new placement.
    pub unschedulable: Vec<UnschedulableReplica>,
}

/// Store-backed scheduler that projects resources, plans placements, and commits one generation.
pub struct Scheduler {
    store: Arc<dyn Store>,
    cluster_id: ClusterId,
    keyspace: Keyspace,
    settings: SchedulerSettings,
    writer: AssignmentWriter,
}

impl Scheduler {
    /// Constructs a scheduler without reading or mutating cluster state.
    pub fn new(
        store: Arc<dyn Store>,
        cluster_id: ClusterId,
        settings: SchedulerSettings,
    ) -> Result<Self, SchedulerError> {
        Ok(Self {
            writer: AssignmentWriter::new(&cluster_id)?,
            keyspace: Keyspace::new(&cluster_id),
            store,
            cluster_id,
            settings: settings.validate()?,
        })
    }

    /// Reconciles one linearizable resource snapshot under the active leadership fence.
    ///
    /// The assignment generation marker is sampled before and after resource projection. A
    /// concurrent scheduler pass causes this invocation to report a conflict without mutating;
    /// cancellation during the final transaction can commit only the complete assignment diff.
    pub async fn reconcile_once(
        &self,
        fenced_store: &FencedStore,
        now: Timestamp,
    ) -> Result<SchedulerReport, SchedulerError> {
        let generation_before = self.scheduler_generation().await?;
        let mut snapshot = ResourceSnapshot::load(self.store.as_ref(), &self.keyspace).await?;
        let (live_nodes, liveness_compares) = self.live_nodes(&snapshot).await?;
        let generation_after = self.scheduler_generation().await?;
        if generation_before != generation_after {
            return Ok(SchedulerReport {
                conflict: true,
                ..Default::default()
            });
        }

        let projection = project(
            self.cluster_id.clone(),
            &snapshot,
            &live_nodes,
            now,
            self.settings.replacement_grace,
        )?;
        let mut schedule = crate::plan(projection.input);
        schedule.assignments.extend(projection.retained_on_error);
        schedule.unschedulable.extend(projection.validation_errors);
        schedule.unschedulable.sort_by(|left, right| {
            left.service_id
                .cmp(&right.service_id)
                .then_with(|| left.deployment_id.cmp(&right.deployment_id))
                .then_with(|| left.replica_index.cmp(&right.replica_index))
        });
        snapshot.dependency_compares.extend(liveness_compares);
        let write = self
            .writer
            .apply(
                fenced_store,
                &snapshot.assignment_values,
                &schedule.assignments,
                generation_before,
                snapshot.dependency_compares,
            )
            .await?;
        Ok(SchedulerReport {
            desired: schedule.assignments.len(),
            created: write.created,
            deleted: write.deleted,
            conflict: write.conflict,
            unschedulable: schedule.unschedulable,
        })
    }

    async fn scheduler_generation(&self) -> Result<ExpectedVersion, StoreError> {
        Ok(
            match self
                .store
                .get(&self.keyspace.scheduler_generation())
                .await?
            {
                Some(stored) => ExpectedVersion::Exact(stored.version),
                None => ExpectedVersion::Missing,
            },
        )
    }

    async fn live_nodes(
        &self,
        snapshot: &ResourceSnapshot,
    ) -> Result<(BTreeSet<NodeId>, Vec<Compare>), StoreError> {
        let mut live = BTreeSet::new();
        let mut compares = Vec::new();
        for node_id in snapshot.nodes.keys() {
            let key = self.keyspace.node_liveness(node_id);
            match self.store.get(&key).await? {
                Some(stored) => {
                    live.insert(node_id.clone());
                    compares.push(Compare {
                        key,
                        expected: ExpectedVersion::Exact(stored.version),
                    });
                }
                None => compares.push(Compare {
                    key,
                    expected: ExpectedVersion::Missing,
                }),
            }
        }
        Ok((live, compares))
    }
}

/// Matchable scheduler construction, snapshot, and mutation failures.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// A zero grace period would replace nodes immediately on transient loss.
    #[error("scheduler replacement grace must be greater than zero")]
    ZeroReplacementGrace,
    /// A static or stored resource identifier was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] InvalidIdentifier),
    /// Linearizable resource or liveness access failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The leadership fence was lost or rejected a transaction.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// Assignment decoding, identity validation, or atomic writing failed.
    #[error(transparent)]
    AssignmentWrite(#[from] AssignmentWriteError),
    /// A relevant stored resource could not be decoded.
    #[error("malformed {kind} resource at `{key}`: {message}")]
    MalformedResource {
        kind: &'static str,
        key: String,
        message: String,
    },
    /// Typed metadata identity did not match its canonical store key.
    #[error("{kind} `{resource_id}` does not match store key `{key}`")]
    ResourceIdentityMismatch {
        kind: &'static str,
        resource_id: String,
        key: String,
    },
    /// One typed identity occurred under more than one key.
    #[error("{kind} `{resource_id}` occurs more than once in one resource snapshot")]
    DuplicateResource {
        kind: &'static str,
        resource_id: String,
    },
    /// More than one active node network claimed the same node.
    #[error("multiple active NodeNetwork resources claim node `{node_id}`")]
    DuplicateNodeNetwork { node_id: NodeId },
}
