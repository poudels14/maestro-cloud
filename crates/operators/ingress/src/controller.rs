use std::collections::BTreeSet;
use std::sync::Arc;

use kernel_api::{ClusterId, ServiceId, Timestamp};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::Keyspace;

use crate::snapshot::ResourceSnapshot;
use crate::writer::{IngressWriteError, IngressWriteReport, IngressWriter};
use crate::{IngressBackend, IngressBackendError, IngressPlanError, IngressSettings};

/// Result of one finite, globally fenced ingress pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngressReport {
    /// Traffic generations created in Staged phase.
    pub created_generations: usize,
    /// Traffic generation statuses atomically replaced after publication.
    pub updated_generations: usize,
    /// Ingress route generations acknowledged after publication.
    pub updated_routes: usize,
    /// Obsolete traffic generations garbage collected.
    pub deleted_generations: usize,
    /// Per-service backend changes applied before the resource commit.
    pub backend_changes: usize,
    /// Whether the cluster ingress blocklist was published and acknowledged.
    pub blocklist_updated: bool,
    /// Whether concurrent input invalidated the snapshot without a resource commit.
    pub conflict: bool,
    /// Earliest UTC retirement deadline requiring another pass.
    pub requeue_at: Option<Timestamp>,
    services_with_generations: BTreeSet<ServiceId>,
}

impl IngressReport {
    pub(crate) fn has_generations(&self, service_id: &ServiceId) -> bool {
        self.services_with_generations.contains(service_id)
    }
}

/// Store-backed ingress controller with an idempotent publication backend.
pub struct IngressController {
    cluster_id: ClusterId,
    keyspace: Keyspace,
    settings: IngressSettings,
    backend: Arc<dyn IngressBackend>,
    writer: IngressWriter,
}

impl IngressController {
    /// Constructs an ingress controller without reading or mutating cluster state.
    pub fn new(
        cluster_id: ClusterId,
        settings: IngressSettings,
        backend: Arc<dyn IngressBackend>,
    ) -> Result<Self, IngressError> {
        if settings.retirement_grace.is_zero() {
            return Err(IngressPlanError::ZeroRetirementGrace.into());
        }
        Ok(Self {
            writer: IngressWriter::new(&cluster_id)?,
            keyspace: Keyspace::new(&cluster_id),
            cluster_id,
            settings,
            backend,
        })
    }

    /// Projects, preflights, publishes, and atomically acknowledges one ingress step.
    pub async fn reconcile_once(
        &self,
        store: &FencedStore,
        now: Timestamp,
    ) -> Result<IngressReport, IngressError> {
        let snapshot = ResourceSnapshot::load(store, &self.keyspace).await?;
        let plan = crate::plan(snapshot.input(self.cluster_id.clone(), now, self.settings))?;
        let services_with_generations = services_with_generations_after(&snapshot, &plan);
        if !self.writer.preflight(store, &snapshot).await? {
            return Ok(IngressReport {
                conflict: true,
                requeue_at: plan.requeue_at,
                services_with_generations,
                ..Default::default()
            });
        }
        for change in &plan.backend_changes {
            self.backend.apply(change).await?;
        }
        if let Some(change) = plan.blocklist_change.as_ref() {
            self.backend.apply_blocklist(change).await?;
        }
        let write = self.writer.apply(store, &snapshot, &plan).await?;
        Ok(report(
            write,
            plan.backend_changes.len(),
            plan.requeue_at,
            services_with_generations,
        ))
    }
}

fn services_with_generations_after(
    snapshot: &ResourceSnapshot,
    plan: &crate::IngressPlan,
) -> BTreeSet<ServiceId> {
    let deleted = plan.delete_generations.iter().collect::<BTreeSet<_>>();
    snapshot
        .generations
        .values()
        .filter(|stored| !deleted.contains(&stored.resource.meta.id))
        .map(|stored| stored.resource.spec.service_id.clone())
        .chain(
            plan.create_generations
                .iter()
                .map(|generation| generation.spec.service_id.clone()),
        )
        .collect()
}

fn report(
    write: IngressWriteReport,
    backend_changes: usize,
    requeue_at: Option<Timestamp>,
    services_with_generations: BTreeSet<ServiceId>,
) -> IngressReport {
    IngressReport {
        created_generations: write.created_generations,
        updated_generations: write.updated_generations,
        updated_routes: write.updated_routes,
        deleted_generations: write.deleted_generations,
        backend_changes,
        blocklist_updated: write.updated_blocklists > 0,
        conflict: write.conflict,
        requeue_at,
        services_with_generations,
    }
}

/// Matchable ingress construction, planning, backend, and mutation failures.
#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    /// A static or stored resource identifier was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// The pure blue/green state machine rejected the projected resources.
    #[error(transparent)]
    Plan(#[from] IngressPlanError),
    /// The leadership fence or backing store rejected an operation.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// The atomic traffic writer rejected a planned mutation.
    #[error(transparent)]
    Write(#[from] IngressWriteError),
    /// The ingress publication backend failed before status acknowledgement.
    #[error(transparent)]
    Backend(#[from] IngressBackendError),
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
