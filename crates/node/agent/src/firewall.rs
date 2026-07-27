use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, Condition, ConditionReason, ConditionState, ConditionType, InvalidIdentifier,
    NodeFirewall, NodeFirewallId, NodeFirewallSpec, NodeId, ResourceKind, ResourceName, Timestamp,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, Keyspace, PutRequest, Store, StoreError, StoredValue,
    WatchCursor, WatchStart,
};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::StatusClock;

const NODE_FIREWALL_KIND: &str = "NodeFirewall";
const FIREWALL_READY: &str = "FirewallReady";
const APPLIED_REASON: &str = "RulesetApplied";
const FAILED_REASON: &str = "RulesetApplyFailed";
const MAX_CAS_ATTEMPTS: usize = 16;

/// Node-local side-effect boundary for one complete nftables ruleset.
#[async_trait]
pub trait FirewallBackend: Send + Sync {
    /// Checks and atomically applies the exact desired script.
    async fn apply(&self, desired: &NodeFirewallSpec) -> Result<(), FirewallBackendError>;
}

/// Matchable node-local firewall application failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("node firewall backend failed: {message}")]
pub struct FirewallBackendError {
    message: String,
}

impl FirewallBackendError {
    /// Creates an adapter-neutral backend error.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Returns backend failure detail suitable for a status condition.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Result of one local desired-state application pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FirewallReconcileReport {
    /// Whether a desired resource currently exists for the node.
    pub resource_present: bool,
    /// Whether the stored local resource could not be decoded or identified.
    pub malformed_resource: bool,
    /// Whether the backend accepted the exact desired script.
    pub applied: bool,
    /// Whether an application failure was surfaced in resource status.
    pub failure_reported: bool,
    /// Whether this pass changed the resource acknowledgement.
    pub acknowledgement_updated: bool,
    /// Whether desired state changed before acknowledgement could commit.
    pub stale: bool,
}

/// Store-driven reconciler applying only this node's desired firewall resource.
pub struct NodeFirewallAgent<Backend> {
    store: Arc<dyn Store>,
    keyspace: Keyspace,
    kind: ResourceKind,
    resource_name: ResourceName,
    resource_id: NodeFirewallId,
    node_id: NodeId,
    backend: Backend,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
    resync_interval: Duration,
}

impl<Backend> NodeFirewallAgent<Backend>
where
    Backend: FirewallBackend,
{
    /// Binds a node-local backend to its one canonical desired resource.
    pub fn new(
        store: Arc<dyn Store>,
        cluster_id: &ClusterId,
        node_id: NodeId,
        backend: Backend,
        monotonic_clock: Arc<dyn Clock>,
        status_clock: Arc<dyn StatusClock>,
        resync_interval: Duration,
    ) -> Result<Self, FirewallAgentError> {
        if resync_interval.is_zero() {
            return Err(FirewallAgentError::ZeroResyncInterval);
        }
        Ok(Self {
            store,
            keyspace: Keyspace::new(cluster_id),
            kind: ResourceKind::new(NODE_FIREWALL_KIND)?,
            resource_name: ResourceName::new(node_id.as_str())?,
            resource_id: NodeFirewallId::new(node_id.as_str())?,
            node_id,
            backend,
            monotonic_clock,
            status_clock,
            resync_interval,
        })
    }

    /// Returns the backend for diagnostics and exact-artifact assertions.
    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    /// Applies and acknowledges the current local desired resource once.
    pub async fn reconcile_once(&self) -> Result<FirewallReconcileReport, FirewallAgentError> {
        let (report, _cursor) = self.reconcile_with_cursor().await?;
        Ok(report)
    }

    /// Runs watch-triggered reconciliation with periodic drift repair.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), FirewallAgentError> {
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
    ) -> Result<(FirewallReconcileReport, WatchCursor), FirewallAgentError> {
        let snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.kind))
            .await?;
        let key = self.keyspace.resource(&self.kind, &self.resource_name);
        let Some(stored) = snapshot.values.iter().find(|stored| stored.key == key) else {
            return Ok((FirewallReconcileReport::default(), snapshot.cursor));
        };
        let resource = match self.decode(stored) {
            Ok(resource) => resource,
            Err(error) => {
                self.warn_malformed(stored, &error);
                return Ok((
                    FirewallReconcileReport {
                        resource_present: true,
                        malformed_resource: true,
                        ..Default::default()
                    },
                    snapshot.cursor,
                ));
            }
        };
        if resource.meta.deletion_timestamp.is_some() {
            return Ok((FirewallReconcileReport::default(), snapshot.cursor));
        }
        if let Err(error) = verify_digest(&resource.spec) {
            return Ok((
                self.report_failure(&resource, error).await?,
                snapshot.cursor,
            ));
        }
        if let Err(error) = self.backend.apply(&resource.spec).await {
            return Ok((
                self.report_failure(&resource, error.into()).await?,
                snapshot.cursor,
            ));
        }
        let outcome = self.update_status(&resource, ApplyStatus::Ready).await?;
        Ok((
            FirewallReconcileReport {
                resource_present: true,
                applied: true,
                acknowledgement_updated: outcome == StatusOutcome::Updated,
                stale: outcome == StatusOutcome::Stale,
                malformed_resource: outcome == StatusOutcome::Malformed,
                failure_reported: false,
            },
            snapshot.cursor,
        ))
    }

    async fn report_failure(
        &self,
        desired: &NodeFirewall,
        failure: FirewallAgentError,
    ) -> Result<FirewallReconcileReport, FirewallAgentError> {
        let detail = failure.to_string();
        let outcome = self
            .update_status(desired, ApplyStatus::Failed(&detail))
            .await
            .map_err(|status_error| FirewallAgentError::FailureStatus {
                failure: detail,
                status_error: status_error.to_string(),
            })?;
        tracing::warn!(
            kind = NODE_FIREWALL_KIND,
            node_id = %self.node_id,
            resource_id = %desired.meta.id,
            generation = desired.meta.generation.0,
            error = %failure,
            "firewall desired state was rejected without replacing the last applied ruleset"
        );
        Ok(FirewallReconcileReport {
            resource_present: true,
            malformed_resource: outcome == StatusOutcome::Malformed,
            applied: false,
            failure_reported: true,
            acknowledgement_updated: outcome == StatusOutcome::Updated,
            stale: outcome == StatusOutcome::Stale,
        })
    }

    async fn update_status(
        &self,
        applied: &NodeFirewall,
        status: ApplyStatus<'_>,
    ) -> Result<StatusOutcome, FirewallAgentError> {
        let expected_generation = applied.meta.generation;
        let expected_digest = applied.spec.digest.clone();
        let key = self.keyspace.resource(&self.kind, &self.resource_name);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let Some(stored) = self.store.get(&key).await? else {
                return Ok(StatusOutcome::Stale);
            };
            let mut current = match self.decode(&stored) {
                Ok(current) => current,
                Err(error) => {
                    self.warn_malformed(&stored, &error);
                    return Ok(StatusOutcome::Malformed);
                }
            };
            if current.meta.generation != expected_generation
                || current.spec.digest != expected_digest
                || current.meta.deletion_timestamp.is_some()
            {
                return Ok(StatusOutcome::Stale);
            }
            let desired = desired_status(&current, status, self.status_clock.now());
            if current.status == desired {
                return Ok(StatusOutcome::Current);
            }
            current.status = desired;
            current.meta.revision = stored.version.resource_revision();
            let outcome = self
                .store
                .put_cas(PutRequest {
                    key: key.clone(),
                    value: encode(&current)?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?;
            if matches!(outcome, CasOutcome::Applied(_)) {
                return Ok(StatusOutcome::Updated);
            }
        }
        Err(FirewallAgentError::Contention)
    }

    fn decode(&self, stored: &StoredValue) -> Result<NodeFirewall, FirewallAgentError> {
        let mut resource: NodeFirewall =
            serde_json::from_slice(&stored.value).map_err(|error| {
                FirewallAgentError::MalformedResource {
                    key: stored.key.to_string(),
                    message: error.to_string(),
                }
            })?;
        let expected_key = self.keyspace.resource(&self.kind, &self.resource_name);
        if stored.key != expected_key
            || resource.meta.id != self.resource_id
            || resource.spec.node_id != self.node_id
        {
            return Err(FirewallAgentError::LocalResourceIdentityMismatch);
        }
        resource.meta.revision = stored.version.resource_revision();
        Ok(resource)
    }

    fn warn_malformed(&self, stored: &StoredValue, error: &FirewallAgentError) {
        tracing::warn!(
            kind = NODE_FIREWALL_KIND,
            node_id = %self.node_id,
            resource_key = %stored.key,
            error = %error,
            "malformed firewall resource preserved the last applied ruleset"
        );
    }
}

#[derive(Clone, Copy)]
enum ApplyStatus<'a> {
    Ready,
    Failed(&'a str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatusOutcome {
    Updated,
    Current,
    Stale,
    Malformed,
}

fn desired_status(
    resource: &NodeFirewall,
    status: ApplyStatus<'_>,
    now: Timestamp,
) -> kernel_api::NodeFirewallStatus {
    let (state, reason, message, applied_generation, applied_digest) = match status {
        ApplyStatus::Ready => (
            ConditionState::True,
            APPLIED_REASON,
            "the exact desired nftables ruleset is applied".to_string(),
            resource.meta.generation,
            Some(resource.spec.digest.clone()),
        ),
        ApplyStatus::Failed(detail) => (
            ConditionState::False,
            FAILED_REASON,
            detail.to_string(),
            resource.status.applied_generation,
            resource.status.applied_digest.clone(),
        ),
    };
    let previous = resource
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type.0 == FIREWALL_READY);
    let last_transition_time = previous
        .filter(|condition| condition.state == state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    kernel_api::NodeFirewallStatus {
        applied_generation,
        applied_digest,
        conditions: vec![Condition {
            condition_type: ConditionType(FIREWALL_READY.to_string()),
            state,
            reason: ConditionReason(reason.to_string()),
            message,
            observed_generation: resource.meta.generation,
            last_transition_time,
        }],
    }
}

fn verify_digest(spec: &NodeFirewallSpec) -> Result<(), FirewallAgentError> {
    let actual = Sha256::digest(spec.script.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if actual == spec.digest {
        Ok(())
    } else {
        Err(FirewallAgentError::DigestMismatch {
            expected: spec.digest.clone(),
            actual,
        })
    }
}

fn encode(resource: &NodeFirewall) -> Result<Vec<u8>, FirewallAgentError> {
    serde_json::to_vec(resource).map_err(|error| FirewallAgentError::SerializeResource {
        message: error.to_string(),
    })
}

/// Failure to load, apply, watch, or acknowledge local firewall desired state.
#[derive(Debug, thiserror::Error)]
pub enum FirewallAgentError {
    /// A constant or node-derived resource identity was invalid.
    #[error("invalid NodeFirewall resource identity: {0}")]
    InvalidIdentifier(#[from] InvalidIdentifier),
    /// A zero interval would create an unbounded reconciliation loop.
    #[error("firewall resync interval must be greater than zero")]
    ZeroResyncInterval,
    /// Store access or watch setup failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A stored local resource was not valid JSON or schema.
    #[error("malformed NodeFirewall resource at `{key}`: {message}")]
    MalformedResource { key: String, message: String },
    /// The local key contained another resource or node identity.
    #[error("local NodeFirewall key contains a different resource or node identity")]
    LocalResourceIdentityMismatch,
    /// The stored digest did not authenticate the exact script.
    #[error("NodeFirewall script digest mismatch: expected {expected}, computed {actual}")]
    DigestMismatch { expected: String, actual: String },
    /// Encoding an acknowledgement failed before a conditional write.
    #[error("failed to serialize local NodeFirewall resource: {message}")]
    SerializeResource { message: String },
    /// Repeated compare-and-swap conflicts exceeded the bounded retry budget.
    #[error("store contention prevented NodeFirewall status acknowledgement")]
    Contention,
    /// The node-local backend rejected the desired script.
    #[error(transparent)]
    Backend(#[from] FirewallBackendError),
    /// Reporting a failed application also encountered a status error.
    #[error("firewall application failed ({failure}); status update also failed ({status_error})")]
    FailureStatus {
        failure: String,
        status_error: String,
    },
}
