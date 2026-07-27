use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, DnsRecord, InvalidIdentifier, NodeId, ResourceKind, ResourceName};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, Keyspace, PutRequest, Store, StoreError, StoredValue,
    WatchCursor, WatchStart,
};
use tokio::sync::watch;

use crate::{AuthoritativeDnsResolver, DnsResolverError, DnsZoneSummary};

const DNS_RECORD_KIND: &str = "DnsRecord";
const MAX_CAS_ATTEMPTS: usize = 16;

/// Result of loading and acknowledging one complete DNS resource snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DnsReconcileReport {
    /// Active resources compiled into the resolver.
    pub observed_resources: usize,
    /// Stored resources skipped because decoding or identity validation failed.
    pub malformed_resources: usize,
    /// Complete snapshots rejected without replacing the last valid zone.
    pub snapshot_rejections: usize,
    /// Distinct record sets and records in the published zone.
    pub zone: DnsZoneSummary,
    /// Resource statuses changed to acknowledge this node.
    pub acknowledgements: usize,
    /// Resources changed after the compiled snapshot and were not acknowledged.
    pub stale_acknowledgements: usize,
}

/// Store-driven, level-triggered publisher for one node's authoritative resolver.
pub struct DnsResourceAgent {
    store: Arc<dyn Store>,
    keyspace: Keyspace,
    kind: ResourceKind,
    node_id: NodeId,
    resolver: AuthoritativeDnsResolver,
    clock: Arc<dyn Clock>,
    resync_interval: Duration,
}

impl DnsResourceAgent {
    /// Binds a node resolver to the cluster's `DnsRecord` prefix.
    pub fn new(
        store: Arc<dyn Store>,
        cluster_id: &ClusterId,
        node_id: NodeId,
        resolver: AuthoritativeDnsResolver,
        clock: Arc<dyn Clock>,
        resync_interval: Duration,
    ) -> Result<Self, DnsResourceError> {
        if resync_interval.is_zero() {
            return Err(DnsResourceError::ZeroResyncInterval);
        }
        Ok(Self {
            store,
            keyspace: Keyspace::new(cluster_id),
            kind: ResourceKind::new(DNS_RECORD_KIND)?,
            node_id,
            resolver,
            clock,
            resync_interval,
        })
    }

    /// Loads, atomically publishes, and acknowledges one linearizable snapshot.
    pub async fn reconcile_once(&self) -> Result<DnsReconcileReport, DnsResourceError> {
        let (report, _cursor) = self.reconcile_with_cursor().await?;
        Ok(report)
    }

    /// Runs watch-triggered reconciliation with a periodic level-triggered resync.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), DnsResourceError> {
        let mut resync_at = self.clock.now().saturating_add(self.resync_interval);
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
                    () = self.clock.sleep_until(resync_at) => {
                        resync_at = self.clock.now().saturating_add(self.resync_interval);
                        break;
                    }
                }
            }
        }
    }

    async fn reconcile_with_cursor(
        &self,
    ) -> Result<(DnsReconcileReport, WatchCursor), DnsResourceError> {
        let snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.kind))
            .await?;
        let mut active = Vec::new();
        let mut malformed_resources = 0_usize;
        for stored in &snapshot.values {
            match self.decode(stored) {
                Ok(entry) if entry.resource.meta.deletion_timestamp.is_none() => {
                    active.push(entry);
                }
                Ok(_) => {}
                Err(error) => {
                    malformed_resources = malformed_resources.saturating_add(1);
                    self.warn_malformed(stored, &error);
                }
            }
        }
        let resources = active
            .iter()
            .map(|entry| entry.resource.clone())
            .collect::<Vec<_>>();
        let mut report = DnsReconcileReport {
            observed_resources: resources.len(),
            malformed_resources,
            zone: self.resolver.summary().await,
            ..Default::default()
        };
        if malformed_resources > 0 {
            report.snapshot_rejections = 1;
            return Ok((report, snapshot.cursor));
        }
        report.zone = match self.resolver.replace(&resources).await {
            Ok(zone) => zone,
            Err(error) => {
                tracing::warn!(
                    kind = DNS_RECORD_KIND,
                    node_id = %self.node_id,
                    error = %error,
                    "invalid DNS resource snapshot preserved the last published zone"
                );
                report.snapshot_rejections = 1;
                return Ok((report, snapshot.cursor));
            }
        };
        for entry in active {
            match self.acknowledge(entry).await? {
                AcknowledgeOutcome::Applied => {
                    report.acknowledgements = report.acknowledgements.saturating_add(1);
                }
                AcknowledgeOutcome::Current => {}
                AcknowledgeOutcome::Stale => {
                    report.stale_acknowledgements = report.stale_acknowledgements.saturating_add(1);
                }
                AcknowledgeOutcome::Malformed => {
                    report.malformed_resources = report.malformed_resources.saturating_add(1);
                }
            }
        }
        Ok((report, snapshot.cursor))
    }

    async fn acknowledge(
        &self,
        snapshot: DecodedRecord,
    ) -> Result<AcknowledgeOutcome, DnsResourceError> {
        let expected_generation = snapshot.resource.meta.generation;
        let mut current = snapshot;
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            if current.resource.meta.deletion_timestamp.is_some()
                || current.resource.meta.generation != expected_generation
            {
                return Ok(AcknowledgeOutcome::Stale);
            }
            let mut published_nodes =
                if current.resource.status.applied_generation == expected_generation {
                    current.resource.status.published_nodes.clone()
                } else {
                    Vec::new()
                };
            published_nodes.sort();
            published_nodes.dedup();
            if !published_nodes.contains(&self.node_id) {
                published_nodes.push(self.node_id.clone());
                published_nodes.sort();
            }
            if current.resource.status.applied_generation == expected_generation
                && current.resource.status.published_nodes == published_nodes
            {
                return Ok(AcknowledgeOutcome::Current);
            }
            current.resource.status.applied_generation = expected_generation;
            current.resource.status.published_nodes = published_nodes;
            current.resource.meta.revision = current.stored.version.resource_revision();
            let outcome = self
                .store
                .put_cas(PutRequest {
                    key: current.stored.key.clone(),
                    value: encode(&current.resource)?,
                    expected: ExpectedVersion::Exact(current.stored.version),
                    session: None,
                })
                .await?;
            if matches!(outcome, CasOutcome::Applied(_)) {
                return Ok(AcknowledgeOutcome::Applied);
            }
            let Some(stored) = self.store.get(&current.stored.key).await? else {
                return Ok(AcknowledgeOutcome::Stale);
            };
            current = match self.decode(&stored) {
                Ok(current) => current,
                Err(error) => {
                    self.warn_malformed(&stored, &error);
                    return Ok(AcknowledgeOutcome::Malformed);
                }
            };
        }
        Err(DnsResourceError::Contention {
            resource_id: current.resource.meta.id.as_str().to_owned(),
        })
    }

    fn decode(&self, stored: &StoredValue) -> Result<DecodedRecord, DnsResourceError> {
        let mut resource: DnsRecord = serde_json::from_slice(&stored.value).map_err(|error| {
            DnsResourceError::MalformedResource {
                key: stored.key.to_string(),
                message: error.to_string(),
            }
        })?;
        let expected_key = self
            .keyspace
            .resource(&self.kind, &ResourceName::new(resource.meta.id.as_str())?);
        if stored.key != expected_key {
            return Err(DnsResourceError::ResourceIdentityMismatch {
                key: stored.key.to_string(),
                resource_id: resource.meta.id.as_str().to_owned(),
            });
        }
        resource.meta.revision = stored.version.resource_revision();
        Ok(DecodedRecord {
            stored: stored.clone(),
            resource,
        })
    }

    fn warn_malformed(&self, stored: &StoredValue, error: &DnsResourceError) {
        tracing::warn!(
            kind = DNS_RECORD_KIND,
            node_id = %self.node_id,
            resource_key = %stored.key,
            error = %error,
            "malformed DNS resource preserved the last published zone"
        );
    }
}

struct DecodedRecord {
    stored: StoredValue,
    resource: DnsRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcknowledgeOutcome {
    Applied,
    Current,
    Stale,
    Malformed,
}

fn encode(resource: &DnsRecord) -> Result<Vec<u8>, DnsResourceError> {
    serde_json::to_vec(resource).map_err(|error| DnsResourceError::SerializeResource {
        message: error.to_string(),
    })
}

/// Failure to load, publish, watch, or acknowledge authoritative DNS resources.
#[derive(Debug, thiserror::Error)]
pub enum DnsResourceError {
    /// A fixed kind or stored resource identity was invalid.
    #[error("invalid DnsRecord resource identity: {0}")]
    InvalidIdentifier(#[from] InvalidIdentifier),
    /// A zero interval would create an unbounded reconciliation loop.
    #[error("DNS resource resync interval must be greater than zero")]
    ZeroResyncInterval,
    /// Store access or watch setup failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A stored value was not valid `DnsRecord` JSON.
    #[error("malformed DnsRecord resource at `{key}`: {message}")]
    MalformedResource { key: String, message: String },
    /// A stored value's typed identity did not match its canonical key.
    #[error("DnsRecord `{resource_id}` does not match store key `{key}`")]
    ResourceIdentityMismatch { key: String, resource_id: String },
    /// A status update could not be serialized.
    #[error("failed to serialize DnsRecord status update: {message}")]
    SerializeResource { message: String },
    /// Repeated status conflicts exceeded the bounded retry budget.
    #[error("store contention prevented acknowledging DnsRecord `{resource_id}`")]
    Contention { resource_id: String },
    /// The complete zone snapshot was invalid.
    #[error(transparent)]
    Resolver(#[from] DnsResolverError),
}
