use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use kernel_api::{
    ClusterId, Condition, ConditionReason, ConditionState, ConditionType, Generation,
    InvalidIdentifier, NodeNetwork, NodeNetworkId, NodeNetworkSpec, NodeNetworkStatus, ObjectMeta,
    ResourceKind, ResourceName, ResourceRevision, Timestamp,
};
use kernel_store::{
    CasOutcome, Clock, Compare, ExpectedVersion, Keyspace, Mutation, PutRequest, Store, StoreError,
    StoredValue, Transaction, TransactionOutcome, WatchCursor, WatchStart,
};
use tokio::sync::watch;

use crate::{MeshBackend, MeshConfiguration, MeshError, MeshPlanner, MeshReconciler};

const NODE_NETWORK_KIND: &str = "NodeNetwork";
const MESH_READY_CONDITION: &str = "MeshReady";
const MESH_APPLIED_REASON: &str = "MeshApplied";
const MESH_FAILED_REASON: &str = "MeshApplyFailed";
const MAX_CAS_ATTEMPTS: usize = 16;

/// Result of one complete store-backed mesh reconciliation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MeshReconcileReport {
    /// Exact configuration accepted by the backend, when this pass succeeded.
    pub configuration: Option<MeshConfiguration>,
    /// Stored publications skipped because decoding or identity validation failed.
    pub malformed_resources: usize,
    /// Whether the backend accepted and applied the complete snapshot.
    pub applied: bool,
    /// Whether a rejected snapshot or backend failure was surfaced in local status.
    pub failure_reported: bool,
}

/// Wall-clock source used only for resource condition transition timestamps.
pub trait StatusClock: Send + Sync {
    /// Returns the current UTC Unix timestamp in milliseconds.
    fn now(&self) -> Timestamp;
}

/// Production status clock backed by the operating-system wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemStatusClock;

impl StatusClock for SystemStatusClock {
    fn now(&self) -> Timestamp {
        let milliseconds = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => bounded_milliseconds(duration),
            Err(error) => bounded_milliseconds(error.duration()).saturating_neg(),
        };
        Timestamp(milliseconds)
    }
}

/// Store-driven node network publisher and complete-snapshot mesh reconciler.
pub struct MeshResourceAgent<Backend> {
    store: Arc<dyn Store>,
    keyspace: Keyspace,
    kind: ResourceKind,
    tombstone_key: kernel_store::StoreKey,
    resource_name: ResourceName,
    resource_id: NodeNetworkId,
    local_publication: NodeNetworkSpec,
    reconciler: MeshReconciler<Backend>,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
    resync_interval: Duration,
}

impl<Backend> MeshResourceAgent<Backend>
where
    Backend: MeshBackend,
{
    /// Binds one local publication and mesh backend to the cluster resource store.
    pub fn new(
        store: Arc<dyn Store>,
        cluster_id: &ClusterId,
        planner: MeshPlanner,
        local_publication: NodeNetworkSpec,
        backend: Backend,
        monotonic_clock: Arc<dyn Clock>,
        status_clock: Arc<dyn StatusClock>,
        resync_interval: Duration,
    ) -> Result<Self, MeshResourceError> {
        if resync_interval.is_zero() {
            return Err(MeshResourceError::ZeroResyncInterval);
        }
        if &local_publication.node_id != planner.local_node_id() {
            return Err(MeshResourceError::LocalPublicationMismatch);
        }
        let node_id = planner.local_node_id().as_str();
        let keyspace = Keyspace::new(cluster_id);
        Ok(Self {
            store,
            tombstone_key: keyspace.node_tombstone(planner.local_node_id()),
            keyspace,
            kind: ResourceKind::new(NODE_NETWORK_KIND)?,
            resource_name: ResourceName::new(node_id)?,
            resource_id: NodeNetworkId::new(node_id)?,
            local_publication,
            reconciler: MeshReconciler::new(planner, backend),
            monotonic_clock,
            status_clock,
            resync_interval,
        })
    }

    /// Returns the backend for diagnostics and exact-artifact assertions.
    pub fn backend(&self) -> &Backend {
        self.reconciler.backend()
    }

    /// Publishes local desired state, applies one linearizable snapshot, and reports status.
    pub async fn reconcile_once(&self) -> Result<MeshReconcileReport, MeshResourceError> {
        let (report, _cursor) = self.reconcile_with_cursor().await?;
        Ok(report)
    }

    /// Runs event-driven reconciliation with a level-triggered periodic resync.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), MeshResourceError> {
        let mut resync_at = self
            .monotonic_clock
            .now()
            .saturating_add(self.resync_interval);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let (_report, cursor) = self.reconcile_with_cursor().await?;
            let mut events = self.store.watch(
                self.keyspace.resource_kind(&self.kind),
                WatchStart::After(cursor),
            )?;

            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    event = events.next() => {
                        match event {
                            Ok(_) | Err(StoreError::CursorExpired { .. }) => break,
                            Err(error) => return Err(error.into()),
                        }
                    }
                    () = self.monotonic_clock.sleep_until(resync_at) => {
                        resync_at = self
                            .monotonic_clock
                            .now()
                            .saturating_add(self.resync_interval);
                        break;
                    }
                }
            }
        }
    }

    async fn reconcile_with_cursor(
        &self,
    ) -> Result<(MeshReconcileReport, WatchCursor), MeshResourceError> {
        self.ensure_local_publication().await?;
        let snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.kind))
            .await?;
        let mut publications = Vec::new();
        let mut first_malformed = None;
        let mut malformed_resources = 0_usize;
        for stored in &snapshot.values {
            match decode_publication(stored, &self.keyspace, &self.kind) {
                Ok(Some(publication)) => publications.push(publication),
                Ok(None) => {}
                Err(error) => {
                    self.warn_malformed(stored, &error);
                    malformed_resources = malformed_resources.saturating_add(1);
                    if first_malformed.is_none() {
                        first_malformed = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_malformed {
            return Ok((
                self.report_failure(error, malformed_resources).await?,
                snapshot.cursor,
            ));
        }
        match self.reconciler.reconcile(&publications).await {
            Ok(configuration) => {
                self.update_status(MeshStatus::Ready).await?;
                Ok((
                    MeshReconcileReport {
                        configuration: Some(configuration),
                        malformed_resources: 0,
                        applied: true,
                        failure_reported: false,
                    },
                    snapshot.cursor,
                ))
            }
            Err(error) => Ok((self.report_failure(error.into(), 0).await?, snapshot.cursor)),
        }
    }

    async fn report_failure(
        &self,
        failure: MeshResourceError,
        malformed_resources: usize,
    ) -> Result<MeshReconcileReport, MeshResourceError> {
        let detail = failure.to_string();
        self.update_status(MeshStatus::Failed(&detail))
            .await
            .map_err(|status_error| MeshResourceError::FailureStatus {
                failure: detail,
                status_error: status_error.to_string(),
            })?;
        tracing::warn!(
            kind = NODE_NETWORK_KIND,
            node_id = %self.local_publication.node_id,
            error = %failure,
            "mesh snapshot was rejected without replacing the last applied configuration"
        );
        Ok(MeshReconcileReport {
            configuration: None,
            malformed_resources,
            applied: false,
            failure_reported: true,
        })
    }

    async fn ensure_local_publication(&self) -> Result<(), MeshResourceError> {
        let key = self.keyspace.resource(&self.kind, &self.resource_name);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            if self.store.get(&self.tombstone_key).await?.is_some() {
                return Err(MeshResourceError::LocalNodeRemoved);
            }
            let current = self.store.get(&key).await?;
            let (mut resource, expected) = match current {
                Some(stored) => match decode_local_resource(&stored, &self.resource_id) {
                    Ok(mut resource) => {
                        if resource.spec == self.local_publication {
                            return Ok(());
                        }
                        resource.meta.generation =
                            Generation(resource.meta.generation.0.saturating_add(1));
                        resource.spec = self.local_publication.clone();
                        (resource, ExpectedVersion::Exact(stored.version))
                    }
                    Err(error) => {
                        self.warn_malformed(&stored, &error);
                        (
                            self.new_local_resource(),
                            ExpectedVersion::Exact(stored.version),
                        )
                    }
                },
                None => (self.new_local_resource(), ExpectedVersion::Missing),
            };
            resource.meta.revision = ResourceRevision::default();
            let outcome = self
                .store
                .txn(Transaction {
                    compares: vec![
                        Compare {
                            key: self.tombstone_key.clone(),
                            expected: ExpectedVersion::Missing,
                        },
                        Compare {
                            key: key.clone(),
                            expected,
                        },
                    ],
                    mutations: vec![Mutation::Put {
                        key: key.clone(),
                        value: encode_resource(&resource)?,
                        session: None,
                    }],
                })
                .await?;
            if matches!(outcome, TransactionOutcome::Applied { .. }) {
                return Ok(());
            }
        }
        Err(MeshResourceError::Contention {
            operation: "publish local NodeNetwork",
        })
    }

    async fn update_status(&self, state: MeshStatus<'_>) -> Result<(), MeshResourceError> {
        let key = self.keyspace.resource(&self.kind, &self.resource_name);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let stored = self
                .store
                .get(&key)
                .await?
                .ok_or(MeshResourceError::LocalPublicationDisappeared)?;
            let mut resource = decode_local_resource(&stored, &self.resource_id)?;
            let desired = desired_status(&resource, state, self.status_clock.now());
            if resource.status == desired {
                return Ok(());
            }
            resource.status = desired;
            resource.meta.revision = stored.version.resource_revision();
            let outcome = self
                .store
                .put_cas(PutRequest {
                    key: key.clone(),
                    value: encode_resource(&resource)?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?;
            if matches!(outcome, CasOutcome::Applied(_)) {
                return Ok(());
            }
        }
        Err(MeshResourceError::Contention {
            operation: "update local NodeNetwork status",
        })
    }

    fn new_local_resource(&self) -> NodeNetwork {
        NodeNetwork {
            meta: ObjectMeta {
                id: self.resource_id.clone(),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: self.local_publication.clone(),
            status: NodeNetworkStatus {
                applied_generation: Generation::default(),
                conditions: Vec::new(),
            },
        }
    }

    fn warn_malformed(&self, stored: &StoredValue, error: &MeshResourceError) {
        tracing::warn!(
            kind = NODE_NETWORK_KIND,
            node_id = %self.local_publication.node_id,
            resource_key = %stored.key,
            error = %error,
            "malformed mesh publication preserved the last applied configuration"
        );
    }
}

#[derive(Clone, Copy)]
enum MeshStatus<'a> {
    Ready,
    Failed(&'a str),
}

fn desired_status(
    resource: &NodeNetwork,
    state: MeshStatus<'_>,
    now: Timestamp,
) -> NodeNetworkStatus {
    let (condition_state, reason, message, applied_generation) = match state {
        MeshStatus::Ready => (
            ConditionState::True,
            MESH_APPLIED_REASON,
            "WireGuard peers and workload routes match the observed snapshot".to_owned(),
            resource.meta.generation,
        ),
        MeshStatus::Failed(detail) => (
            ConditionState::False,
            MESH_FAILED_REASON,
            detail.to_owned(),
            resource.status.applied_generation,
        ),
    };
    let previous = resource
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type.0 == MESH_READY_CONDITION);
    let last_transition_time = previous
        .filter(|condition| condition.state == condition_state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    NodeNetworkStatus {
        applied_generation,
        conditions: vec![Condition {
            condition_type: ConditionType(MESH_READY_CONDITION.to_owned()),
            state: condition_state,
            reason: ConditionReason(reason.to_owned()),
            message,
            observed_generation: resource.meta.generation,
            last_transition_time,
        }],
    }
}

fn decode_publication(
    stored: &StoredValue,
    keyspace: &Keyspace,
    kind: &ResourceKind,
) -> Result<Option<NodeNetworkSpec>, MeshResourceError> {
    let resource = decode_resource(stored)?;
    let expected_key = keyspace.resource(kind, &ResourceName::new(resource.meta.id.as_str())?);
    if stored.key != expected_key || resource.meta.id.as_str() != resource.spec.node_id.as_str() {
        return Err(MeshResourceError::ResourceIdentityMismatch {
            key: stored.key.to_string(),
            resource_id: resource.meta.id.as_str().to_owned(),
            node_id: resource.spec.node_id.as_str().to_owned(),
        });
    }
    if resource.meta.deletion_timestamp.is_some() {
        Ok(None)
    } else {
        Ok(Some(resource.spec))
    }
}

fn decode_local_resource(
    stored: &StoredValue,
    expected_id: &NodeNetworkId,
) -> Result<NodeNetwork, MeshResourceError> {
    let resource = decode_resource(stored)?;
    if &resource.meta.id != expected_id || resource.spec.node_id.as_str() != expected_id.as_str() {
        return Err(MeshResourceError::LocalResourceIdentityMismatch);
    }
    Ok(resource)
}

fn decode_resource(stored: &StoredValue) -> Result<NodeNetwork, MeshResourceError> {
    serde_json::from_slice(&stored.value).map_err(|error| MeshResourceError::MalformedResource {
        key: stored.key.to_string(),
        message: error.to_string(),
    })
}

fn encode_resource(resource: &NodeNetwork) -> Result<Vec<u8>, MeshResourceError> {
    serde_json::to_vec(resource).map_err(|error| MeshResourceError::SerializeResource {
        message: error.to_string(),
    })
}

fn bounded_milliseconds(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// Why store-backed mesh publication or reconciliation failed.
#[derive(Debug, thiserror::Error)]
pub enum MeshResourceError {
    /// A constant or node-derived resource identity was invalid.
    #[error("invalid NodeNetwork resource identity: {0}")]
    InvalidIdentifier(#[from] InvalidIdentifier),
    /// The local publication was constructed for a different planner identity.
    #[error("local NodeNetwork publication differs from the mesh planner identity")]
    LocalPublicationMismatch,
    /// A zero interval would create an unbounded reconciliation loop.
    #[error("mesh resource resync interval must be greater than zero")]
    ZeroResyncInterval,
    /// Store access or watch setup failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A stored NodeNetwork value was not valid JSON or schema.
    #[error("malformed NodeNetwork resource at `{key}`: {message}")]
    MalformedResource { key: String, message: String },
    /// Encoding a local resource failed before a conditional write.
    #[error("failed to serialize local NodeNetwork resource: {message}")]
    SerializeResource { message: String },
    /// A local key contained another resource or node identity.
    #[error("local NodeNetwork key contains a different resource identity")]
    LocalResourceIdentityMismatch,
    /// A stored publication's typed identities did not match its canonical key.
    #[error(
        "NodeNetwork resource `{resource_id}` for node `{node_id}` does not match store key `{key}`"
    )]
    ResourceIdentityMismatch {
        key: String,
        resource_id: String,
        node_id: String,
    },
    /// The local publication vanished between publication and status update.
    #[error("local NodeNetwork publication disappeared before status update")]
    LocalPublicationDisappeared,
    /// A removed identity must never recreate its mesh publication.
    #[error("local node identity was permanently removed from the cluster")]
    LocalNodeRemoved,
    /// Repeated compare-and-swap conflicts exceeded the bounded retry budget.
    #[error("store contention prevented operation: {operation}")]
    Contention { operation: &'static str },
    /// Desired-state validation or backend application failed.
    #[error(transparent)]
    Mesh(#[from] MeshError),
    /// Reporting a failed reconciliation also encountered a status error.
    #[error("mesh reconciliation failed ({failure}); status update also failed ({status_error})")]
    FailureStatus {
        failure: String,
        status_error: String,
    },
}
