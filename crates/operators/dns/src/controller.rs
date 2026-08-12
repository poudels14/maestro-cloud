use std::collections::BTreeSet;

use kernel_api::{ClusterId, ServiceId};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::Keyspace;

use crate::snapshot::ResourceSnapshot;
use crate::writer::{DnsWriteError, DnsWriteReport, DnsWriter};
use crate::{DnsPlanError, DnsSettings};

/// Result of one finite, globally fenced DNS projection pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DnsReport {
    /// Missing authoritative record resources created atomically.
    pub created_records: usize,
    /// Changed record resources replaced atomically.
    pub replaced_records: usize,
    /// Stale managed record resources deleted atomically.
    pub deleted_records: usize,
    /// Whether concurrent input invalidated the snapshot without a resource commit.
    pub conflict: bool,
    services_with_records: BTreeSet<ServiceId>,
}

impl DnsReport {
    pub(crate) fn has_records(&self, service_id: &ServiceId) -> bool {
        self.services_with_records.contains(service_id)
    }
}

/// Store-backed controller for deterministic authoritative DNS resources.
pub struct DnsController {
    cluster_id: ClusterId,
    keyspace: Keyspace,
    settings: DnsSettings,
    writer: DnsWriter,
}

impl DnsController {
    /// Constructs a DNS controller without reading or mutating cluster state.
    pub fn new(cluster_id: ClusterId, settings: DnsSettings) -> Result<Self, DnsError> {
        if settings.ttl_secs == 0 {
            return Err(DnsPlanError::ZeroTtl.into());
        }
        Ok(Self {
            writer: DnsWriter::new(&cluster_id)?,
            keyspace: Keyspace::new(&cluster_id),
            cluster_id,
            settings,
        })
    }

    /// Projects and commits every Service's DNS generation independently.
    pub async fn reconcile_once(&self, store: &FencedStore) -> Result<DnsReport, DnsError> {
        let snapshot = ResourceSnapshot::load(store, &self.keyspace).await?;
        let service_ids = snapshot.services.keys().cloned().collect::<Vec<_>>();
        let mut aggregate = DnsReport::default();
        for service_id in service_ids {
            aggregate.merge(self.reconcile_service(store, &service_id).await?);
        }
        Ok(aggregate)
    }

    /// Projects and atomically commits one Service's DNS resource generation.
    pub async fn reconcile_service(
        &self,
        store: &FencedStore,
        service_id: &ServiceId,
    ) -> Result<DnsReport, DnsError> {
        let snapshot = ResourceSnapshot::load_service(store, &self.keyspace, service_id).await?;
        let plan = crate::plan(snapshot.input(self.cluster_id.clone(), self.settings))?;
        let services_with_records = services_with_records_after(&snapshot, &plan)?;
        let write = self.writer.apply(store, &snapshot, &plan).await?;
        Ok(report(write, services_with_records))
    }
}

impl DnsReport {
    fn merge(&mut self, report: Self) {
        self.created_records = self.created_records.saturating_add(report.created_records);
        self.replaced_records = self
            .replaced_records
            .saturating_add(report.replaced_records);
        self.deleted_records = self.deleted_records.saturating_add(report.deleted_records);
        self.conflict |= report.conflict;
        self.services_with_records
            .extend(report.services_with_records);
    }
}

fn services_with_records_after(
    snapshot: &ResourceSnapshot,
    plan: &crate::DnsPlan,
) -> Result<BTreeSet<ServiceId>, DnsError> {
    let deleted = plan.delete_records.iter().collect::<BTreeSet<_>>();
    let mut services = BTreeSet::new();
    for stored in snapshot
        .records
        .values()
        .filter(|stored| !deleted.contains(&stored.resource.meta.id))
    {
        if let Some(owner) = crate::resource::managed_owner(&stored.resource)? {
            services.insert(owner);
        }
    }
    for record in &plan.create_records {
        if let Some(owner) = crate::resource::managed_owner(record)? {
            services.insert(owner);
        }
    }
    Ok(services)
}

fn report(write: DnsWriteReport, services_with_records: BTreeSet<ServiceId>) -> DnsReport {
    DnsReport {
        created_records: write.created_records,
        replaced_records: write.replaced_records,
        deleted_records: write.deleted_records,
        conflict: write.conflict,
        services_with_records,
    }
}

/// Matchable DNS planning, decoding, and persistence failures.
#[derive(Debug, thiserror::Error)]
pub enum DnsError {
    /// A static or stored resource identifier was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// The pure DNS projection rejected the resource snapshot.
    #[error(transparent)]
    Plan(#[from] DnsPlanError),
    /// The leadership fence or backing store rejected an operation.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// The atomic record writer rejected a planned mutation.
    #[error(transparent)]
    Write(#[from] DnsWriteError),
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
}
