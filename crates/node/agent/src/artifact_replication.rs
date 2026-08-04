use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, ConditionState, Deployment, Node, NodeId, ResourceKind, ResourceName};
use kernel_store::{Clock, Keyspace, Store, StoreError, StoredValue};
use runtime::{
    ArtifactByteStream, ArtifactDigest, ArtifactPrunePolicy, ArtifactStore, ArtifactStoreError,
};
use tokio::sync::watch;

use crate::artifact_drain::{
    ArtifactDrainReadiness, DRAINING_CONDITION, PeerCopyPolicy, update_artifact_drain_status,
};
use crate::artifact_retention::{preserved_digests, retained_digests};
use crate::retry::{retryable_store_error, wait_for_store_retry_or_shutdown};
use crate::{ArtifactHolderRegistry, ArtifactHolderRegistryError, StatusClock};

const DEPLOYMENT_KIND: &str = "Deployment";
const NODE_KIND: &str = "Node";
const MAX_RESOURCE_BYTES: usize = 512 * 1_024;

/// Static identity and resync policy for registry-free artifact replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReplicationSettings {
    /// Cluster whose build artifacts are retained and replicated.
    pub cluster_id: ClusterId,
    /// Local node receiving or publishing artifact copies.
    pub node_id: NodeId,
    /// Maximum delay before retrying incomplete replication.
    pub resync_interval: Duration,
}

/// One node-to-node artifact stream provider.
#[async_trait]
pub trait ArtifactPeerSource: Send + Sync {
    /// Opens a bounded stream from one live holder.
    async fn export(
        &self,
        node_id: &NodeId,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactPeerSourceError>;
}

/// Peer handshake failure before an artifact stream is available.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArtifactPeerSourceError {
    /// The peer rejected the digest or transfer contract.
    #[error("peer rejected the artifact request: {message}")]
    Rejected { message: String },
    /// The peer could not currently open the requested stream.
    #[error("peer artifact stream is unavailable: {message}")]
    Unavailable { message: String },
}

/// Registry-free artifact coordinator for one node.
pub struct ArtifactReplicationAgent {
    store: Arc<dyn Store>,
    artifacts: Arc<dyn ArtifactStore>,
    peers: Arc<dyn ArtifactPeerSource>,
    holders: ArtifactHolderRegistry,
    settings: ArtifactReplicationSettings,
    keyspace: Keyspace,
    deployment_kind: ResourceKind,
    node_kind: ResourceKind,
    clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
}

impl ArtifactReplicationAgent {
    /// Binds runtime storage, authenticated peers, and one active holder session.
    pub fn new(
        store: Arc<dyn Store>,
        artifacts: Arc<dyn ArtifactStore>,
        peers: Arc<dyn ArtifactPeerSource>,
        holders: ArtifactHolderRegistry,
        settings: ArtifactReplicationSettings,
        clock: Arc<dyn Clock>,
        status_clock: Arc<dyn StatusClock>,
    ) -> Result<Self, ArtifactReplicationError> {
        if settings.resync_interval.is_zero() {
            return Err(ArtifactReplicationError::ZeroResyncInterval);
        }
        Ok(Self {
            keyspace: Keyspace::new(&settings.cluster_id),
            deployment_kind: ResourceKind::new(DEPLOYMENT_KIND)?,
            node_kind: ResourceKind::new(NODE_KIND)?,
            store,
            artifacts,
            peers,
            holders,
            settings,
            clock,
            status_clock,
        })
    }

    /// Ensures one digest is local, then publishes it under the node session.
    pub async fn ensure_local(
        &self,
        digest: &ArtifactDigest,
    ) -> Result<ArtifactReplicationOutcome, ArtifactReplicationError> {
        if self.artifacts.contains(digest).await? {
            self.holders.publish(digest).await?;
            return Ok(ArtifactReplicationOutcome::AlreadyLocal);
        }
        let candidates = self
            .holders
            .holders(digest)
            .await?
            .into_iter()
            .filter(|holder| holder.node_id != self.settings.node_id)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(ArtifactReplicationError::NoHolder {
                digest: digest.clone(),
            });
        }
        let mut failures = Vec::new();
        for candidate in candidates {
            let stream = match self.peers.export(&candidate.node_id, digest).await {
                Ok(stream) => stream,
                Err(error) => {
                    failures.push(format!("{}: {error}", candidate.node_id));
                    continue;
                }
            };
            match self.artifacts.import(stream).await {
                Ok(imported) if imported == *digest => {
                    self.holders.publish(digest).await?;
                    return Ok(ArtifactReplicationOutcome::Imported {
                        source_node_id: candidate.node_id,
                    });
                }
                Ok(imported) => failures.push(format!(
                    "{}: imported digest `{}` instead of `{}`",
                    candidate.node_id,
                    imported.as_str(),
                    digest.as_str()
                )),
                Err(error) => failures.push(format!("{}: {error}", candidate.node_id)),
            }
        }
        Err(ArtifactReplicationError::PeerFailures {
            digest: digest.clone(),
            failures,
        })
    }

    /// Reconciles retained build artifacts and removes obsolete holder claims.
    pub async fn reconcile_once(
        &self,
    ) -> Result<ArtifactReplicationReport, ArtifactReplicationError> {
        let deployment_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.deployment_kind))
            .await?;
        let node_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.node_kind))
            .await?;
        let (deployments, malformed_deployments) = decode_resources(
            &deployment_snapshot.values,
            &self.keyspace,
            &self.deployment_kind,
            &self.settings.node_id,
            |deployment: &Deployment| deployment.meta.id.as_str(),
        );
        let (nodes, malformed_nodes) = decode_resources(
            &node_snapshot.values,
            &self.keyspace,
            &self.node_kind,
            &self.settings.node_id,
            |node: &Node| node.meta.id.as_str(),
        );
        let malformed_resources = malformed_deployments.saturating_add(malformed_nodes);
        if malformed_resources > 0 {
            return Ok(self.reject_snapshot(
                ArtifactReplicationReport::default(),
                malformed_resources,
                format!(
                    "{malformed_resources} malformed deployment or node resources preserved \
                     existing artifacts"
                ),
            ));
        }
        let live_keys = self
            .store
            .list(&self.keyspace.node_liveness_records())
            .await?
            .values
            .into_iter()
            .map(|stored| stored.key)
            .collect::<BTreeSet<_>>();
        let workload_node_count = nodes
            .iter()
            .filter(|node| {
                node.meta.deletion_timestamp.is_none() && node.spec.role.runs_workloads()
            })
            .count();
        let local_runs_workloads = nodes.iter().any(|node| {
            node.meta.id == self.settings.node_id
                && node.meta.deletion_timestamp.is_none()
                && node.spec.role.runs_workloads()
        });
        let eligible = nodes
            .iter()
            .filter(|node| {
                node.meta.deletion_timestamp.is_none()
                    && node.spec.role.runs_workloads()
                    && !is_draining(node)
                    && live_keys.contains(&self.keyspace.node_liveness(&node.meta.id))
            })
            .map(|node| node.meta.id.clone())
            .collect::<BTreeSet<_>>();
        let retained = match retained_digests(&deployments) {
            Ok(retained) => retained,
            Err(error) => {
                return Ok(self.reject_snapshot(
                    ArtifactReplicationReport::default(),
                    0,
                    error.to_string(),
                ));
            }
        };
        let preserved = match preserved_digests(&deployments) {
            Ok(preserved) => preserved,
            Err(error) => {
                return Ok(self.reject_snapshot(
                    ArtifactReplicationReport::default(),
                    0,
                    error.to_string(),
                ));
            }
        };
        let mut report = ArtifactReplicationReport {
            retained: retained.len(),
            eligible_nodes: eligible.len(),
            ..ArtifactReplicationReport::default()
        };
        let local_holders = match self.holders.local_holders().await {
            Ok(holders) => holders,
            Err(error) if error.is_malformed_store_data() => {
                return Ok(self.reject_snapshot(report, 1, error.to_string()));
            }
            Err(error) => return Err(error.into()),
        };
        for holder in local_holders {
            let present = self.artifacts.contains(&holder.digest).await;
            if !retained.contains(&holder.digest) || matches!(present, Ok(false)) {
                self.holders.remove(&holder.digest).await?;
                report.removed_claims = report.removed_claims.saturating_add(1);
            } else if let Err(error) = present {
                report.failures.push(ArtifactReplicationFailure {
                    digest: holder.digest,
                    message: error.to_string(),
                });
            }
        }
        for digest in &retained {
            match self.artifacts.contains(digest).await {
                Ok(true) => match self.holders.publish(digest).await {
                    Ok(()) => report.local = report.local.saturating_add(1),
                    Err(error) => report.failures.push(ArtifactReplicationFailure {
                        digest: digest.clone(),
                        message: error.to_string(),
                    }),
                },
                Ok(false) if eligible.contains(&self.settings.node_id) => {
                    match self.ensure_local(digest).await {
                        Ok(ArtifactReplicationOutcome::Imported { .. }) => {
                            report.imported = report.imported.saturating_add(1);
                        }
                        Ok(ArtifactReplicationOutcome::AlreadyLocal) => {
                            report.local = report.local.saturating_add(1);
                        }
                        Err(error) => report.failures.push(ArtifactReplicationFailure {
                            digest: digest.clone(),
                            message: error.to_string(),
                        }),
                    }
                }
                Ok(false) => {}
                Err(error) => report.failures.push(ArtifactReplicationFailure {
                    digest: digest.clone(),
                    message: error.to_string(),
                }),
            }
        }
        let peer_copy_policy = if local_runs_workloads && workload_node_count > 1 {
            PeerCopyPolicy::RequirePeerCopy
        } else {
            PeerCopyPolicy::LocalCopySufficient
        };
        let readiness = match ArtifactDrainReadiness::inspect(
            &retained,
            &self.holders,
            &self.settings.node_id,
            peer_copy_policy,
        )
        .await
        {
            Ok(readiness) => readiness,
            Err(error) if error.is_malformed_store_data() => {
                return Ok(self.reject_snapshot(report, 1, error.to_string()));
            }
            Err(error) => return Err(error),
        };
        report.drain_ready = readiness.ready();
        report.missing_peer_copies = readiness.missing_peer_copies();
        if let Err(error) = update_artifact_drain_status(
            self.store.as_ref(),
            &self.keyspace,
            &self.settings.node_id,
            &readiness,
            self.status_clock.now(),
        )
        .await
        {
            if error.is_malformed_store_data() {
                return Ok(self.reject_snapshot(report, 1, error.to_string()));
            }
            return Err(error);
        }
        match self
            .artifacts
            .prune(&ArtifactPrunePolicy::Preserve(
                preserved.into_iter().collect(),
            ))
            .await
        {
            Ok(pruned) => report.pruned = pruned.removed.len(),
            Err(error) => report.prune_failure = Some(error.to_string()),
        }
        Ok(report)
    }

    fn reject_snapshot(
        &self,
        mut report: ArtifactReplicationReport,
        malformed_resources: usize,
        message: String,
    ) -> ArtifactReplicationReport {
        tracing::warn!(
            kind = "ArtifactReplicationSnapshot",
            node_id = %self.settings.node_id,
            malformed_resources,
            error = %message,
            "artifact replication snapshot was rejected without pruning local state"
        );
        report.malformed_resources = report
            .malformed_resources
            .saturating_add(malformed_resources);
        report.snapshot_rejected = true;
        report.snapshot_failure = Some(message);
        report
    }

    /// Reconciles immediately and periodically until shutdown.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ArtifactReplicationError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let reconcile = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                result = self.reconcile_once() => result,
            };
            if let Err(error) = reconcile {
                if *shutdown.borrow() {
                    return Ok(());
                }
                if !error.retryable() {
                    return Err(error);
                }
                tracing::warn!(
                    node_id = %self.settings.node_id,
                    error = %error,
                    "transient artifact replication failure; retrying"
                );
                if wait_for_store_retry_or_shutdown(self.clock.as_ref(), &mut shutdown).await {
                    return Ok(());
                }
                continue;
            }
            let deadline = self
                .clock
                .now()
                .saturating_add(self.settings.resync_interval);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = self.clock.sleep_until(deadline) => {}
            }
        }
    }
}

fn is_draining(node: &Node) -> bool {
    node.status.conditions.iter().any(|condition| {
        condition.condition_type == DRAINING_CONDITION && condition.state == ConditionState::True
    })
}

fn decode_resources<Resource>(
    values: &[StoredValue],
    keyspace: &Keyspace,
    kind: &ResourceKind,
    node_id: &NodeId,
    resource_id: fn(&Resource) -> &str,
) -> (Vec<Resource>, usize)
where
    Resource: serde::de::DeserializeOwned,
{
    let mut decoded = Vec::new();
    let mut malformed = 0_usize;
    for stored in values {
        let resource = match decode_resource(stored) {
            Ok(resource) => resource,
            Err(error) => {
                malformed = malformed.saturating_add(1);
                warn_malformed(kind, node_id, stored, &error);
                continue;
            }
        };
        let id = resource_id(&resource);
        let expected_key = match ResourceName::new(id) {
            Ok(name) => keyspace.resource(kind, &name),
            Err(error) => {
                malformed = malformed.saturating_add(1);
                warn_malformed(kind, node_id, stored, &error);
                continue;
            }
        };
        if stored.key != expected_key {
            malformed = malformed.saturating_add(1);
            tracing::warn!(
                kind = %kind,
                node_id = %node_id,
                resource_key = %stored.key,
                resource_id = id,
                expected_key = %expected_key,
                "misidentified artifact retention resource was skipped"
            );
            continue;
        }
        decoded.push(resource);
    }
    (decoded, malformed)
}

fn decode_resource<Resource>(stored: &StoredValue) -> Result<Resource, ArtifactReplicationError>
where
    Resource: serde::de::DeserializeOwned,
{
    if stored.value.len() > MAX_RESOURCE_BYTES {
        return Err(ArtifactReplicationError::MalformedResource {
            key: stored.key.to_string(),
            message: format!("document exceeds {MAX_RESOURCE_BYTES} bytes"),
        });
    }
    serde_json::from_slice(&stored.value).map_err(|error| {
        ArtifactReplicationError::MalformedResource {
            key: stored.key.to_string(),
            message: error.to_string(),
        }
    })
}

fn warn_malformed(
    kind: &ResourceKind,
    node_id: &NodeId,
    stored: &StoredValue,
    error: &dyn std::fmt::Display,
) {
    tracing::warn!(
        kind = %kind,
        node_id = %node_id,
        resource_key = %stored.key,
        error = %error,
        "malformed artifact retention resource was skipped"
    );
}

/// Result of ensuring one immutable artifact locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactReplicationOutcome {
    /// The artifact was already present and its holder claim was refreshed.
    AlreadyLocal,
    /// The artifact was imported from a live peer.
    Imported { source_node_id: NodeId },
}

/// One retained digest that could not converge during a bounded pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReplicationFailure {
    /// Immutable artifact that remains unavailable or unadvertised.
    pub digest: ArtifactDigest,
    /// Bounded diagnostic safe for node logs and status projection.
    pub message: String,
}

/// Observable result of one replication reconciliation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArtifactReplicationReport {
    /// Deployment, node, or holder resources rejected during this pass.
    pub malformed_resources: usize,
    /// Whether an incomplete snapshot prevented every destructive cleanup step.
    pub snapshot_rejected: bool,
    /// Bounded reason the complete retention snapshot was rejected.
    pub snapshot_failure: Option<String>,
    /// Registry-free digests selected by retention policy.
    pub retained: usize,
    /// Live, schedulable workload nodes targeted for copies.
    pub eligible_nodes: usize,
    /// Retained artifacts already local and advertised.
    pub local: usize,
    /// Retained artifacts imported during this pass.
    pub imported: usize,
    /// Stale or obsolete holder claims removed during this pass.
    pub removed_claims: usize,
    /// Unreferenced runtime-managed artifacts removed during this pass.
    pub pruned: usize,
    /// Cleanup failure isolated from transfer and holder convergence.
    pub prune_failure: Option<String>,
    /// Per-digest failures isolated from other retained artifacts.
    pub failures: Vec<ArtifactReplicationFailure>,
    /// Whether the local node may leave service without the last retained copy.
    pub drain_ready: bool,
    /// Retained digests that still exist only on the local node.
    pub missing_peer_copies: usize,
}

/// Replication configuration, snapshot, transfer, or holder failure.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactReplicationError {
    /// A zero resync interval would hot-loop.
    #[error("artifact replication resync interval must be greater than zero")]
    ZeroResyncInterval,
    /// A fixed resource kind was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// The local runtime artifact backend failed.
    #[error(transparent)]
    Artifacts(#[from] ArtifactStoreError),
    /// Holder lookup or publication failed.
    #[error(transparent)]
    Holders(#[from] ArtifactHolderRegistryError),
    /// Store snapshot access failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A typed resource snapshot was malformed.
    #[error("malformed resource at `{key}`: {message}")]
    MalformedResource { key: String, message: String },
    /// No live holder currently advertises the requested digest.
    #[error("artifact `{digest}` has no live holder")]
    NoHolder { digest: ArtifactDigest },
    /// Every advertised holder failed or returned different content.
    #[error("artifact `{digest}` failed from every holder: {failures:?}")]
    PeerFailures {
        digest: ArtifactDigest,
        failures: Vec<String>,
    },
    /// The durable local node resource disappeared after registration.
    #[error("local Node `{node_id}` disappeared during artifact status publication")]
    LocalNodeMissing { node_id: NodeId },
    /// The local node key contained a different typed identity.
    #[error("local Node key expected `{expected}` but contained `{actual}`")]
    LocalNodeIdentityMismatch { expected: NodeId, actual: NodeId },
    /// The local node status could not be encoded within its contract bound.
    #[error("failed to serialize local Node artifact status: {message}")]
    SerializeNode { message: String },
    /// Bounded compare-and-swap retries could not publish local drain readiness.
    #[error("artifact drain status for Node `{node_id}` remained contended")]
    DrainStatusContention { node_id: NodeId },
}

impl ArtifactReplicationError {
    fn retryable(&self) -> bool {
        match self {
            Self::Artifacts(
                ArtifactStoreError::Unavailable { .. } | ArtifactStoreError::Stream { .. },
            ) => true,
            Self::Holders(ArtifactHolderRegistryError::Store(error)) | Self::Store(error) => {
                retryable_store_error(error)
            }
            Self::NoHolder { .. }
            | Self::PeerFailures { .. }
            | Self::LocalNodeMissing { .. }
            | Self::DrainStatusContention { .. } => true,
            _ => false,
        }
    }

    fn is_malformed_store_data(&self) -> bool {
        matches!(
            self,
            Self::MalformedResource { .. } | Self::LocalNodeIdentityMismatch { .. }
        ) || matches!(
            self,
            Self::Holders(error) if error.is_malformed_store_data()
        )
    }
}
