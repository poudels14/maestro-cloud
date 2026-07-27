use std::collections::BTreeSet;

use kernel_api::{ClusterId, ServiceId, Timestamp};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::Keyspace;

use crate::snapshot::ResourceSnapshot;
use crate::writer::{DeploymentWriteError, DeploymentWriteReport, DeploymentWriter};
use crate::{DeploymentPlanError, LifecycleSettings};

/// Result of one finite, globally fenced deployment lifecycle pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeploymentReport {
    /// Deployment children created for new Service generations.
    pub created_deployments: usize,
    /// Build children created for build-backed Deployments.
    pub created_builds: usize,
    /// Deployment statuses atomically replaced.
    pub updated_deployments: usize,
    /// Service statuses atomically replaced.
    pub updated_services: usize,
    /// Removed Deployment histories garbage collected during Service finalization.
    pub deleted_deployments: usize,
    /// Build children garbage collected during Service finalization.
    pub deleted_builds: usize,
    /// Replica observations garbage collected after their assignments disappear.
    pub deleted_replicas: usize,
    /// Whether concurrent input invalidated the snapshot without committing mutations.
    pub conflict: bool,
    services_with_children: BTreeSet<ServiceId>,
}

impl DeploymentReport {
    pub(crate) fn has_children(&self, service_id: &ServiceId) -> bool {
        self.services_with_children.contains(service_id)
    }
}

/// Store-backed deployment lifecycle controller.
pub struct DeploymentController {
    cluster_id: ClusterId,
    keyspace: Keyspace,
    settings: LifecycleSettings,
    writer: DeploymentWriter,
}

impl DeploymentController {
    /// Constructs a controller without reading or mutating cluster state.
    pub fn new(
        cluster_id: ClusterId,
        settings: LifecycleSettings,
    ) -> Result<Self, DeploymentError> {
        if settings.drain_grace.is_zero() {
            return Err(DeploymentPlanError::ZeroDrainGrace.into());
        }
        Ok(Self {
            writer: DeploymentWriter::new(&cluster_id)?,
            keyspace: Keyspace::new(&cluster_id),
            cluster_id,
            settings,
        })
    }

    /// Projects one Service-scoped resource snapshot, plans, and commits one step.
    pub async fn reconcile_service(
        &self,
        store: &FencedStore,
        service_id: &ServiceId,
        now: Timestamp,
    ) -> Result<DeploymentReport, DeploymentError> {
        let snapshot = ResourceSnapshot::load_service(store, &self.keyspace, service_id).await?;
        let plan = crate::plan(snapshot.input(self.cluster_id.clone(), now, self.settings))?;
        let children = services_with_children_after(&snapshot, &plan);
        let write = self.writer.apply(store, &snapshot, &plan).await?;
        Ok(report(write, children))
    }
}

fn services_with_children_after(
    snapshot: &ResourceSnapshot,
    plan: &crate::DeploymentPlan,
) -> BTreeSet<ServiceId> {
    let deleted_deployments = plan.delete_deployments.iter().collect::<BTreeSet<_>>();
    let deleted_builds = plan.delete_builds.iter().collect::<BTreeSet<_>>();
    let deleted_replicas = plan.delete_replicas.iter().collect::<BTreeSet<_>>();
    snapshot
        .deployments
        .values()
        .filter(|stored| !deleted_deployments.contains(&stored.resource.meta.id))
        .map(|stored| stored.resource.spec.service_id.clone())
        .chain(
            plan.create_deployments
                .iter()
                .map(|deployment| deployment.spec.service_id.clone()),
        )
        .chain(
            snapshot
                .builds
                .values()
                .filter(|stored| !deleted_builds.contains(&stored.resource.meta.id))
                .map(|stored| stored.resource.spec.service_id.clone()),
        )
        .chain(
            plan.create_builds
                .iter()
                .map(|build| build.spec.service_id.clone()),
        )
        .chain(
            snapshot
                .replicas
                .values()
                .filter(|stored| !deleted_replicas.contains(&stored.resource.meta.id))
                .map(|stored| stored.resource.spec.service_id.clone()),
        )
        .collect()
}

fn report(
    write: DeploymentWriteReport,
    services_with_children: BTreeSet<ServiceId>,
) -> DeploymentReport {
    DeploymentReport {
        created_deployments: write.created_deployments,
        created_builds: write.created_builds,
        updated_deployments: write.updated_deployments,
        updated_services: write.updated_services,
        deleted_deployments: write.deleted_deployments,
        deleted_builds: write.deleted_builds,
        deleted_replicas: write.deleted_replicas,
        conflict: write.conflict,
        services_with_children,
    }
}

/// Matchable deployment construction, snapshot, planning, and mutation failures.
#[derive(Debug, thiserror::Error)]
pub enum DeploymentError {
    /// A static or stored resource identifier was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// The pure lifecycle state machine rejected the projected resources.
    #[error(transparent)]
    Plan(#[from] DeploymentPlanError),
    /// The leadership fence or backing store rejected an operation.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// The atomic lifecycle writer rejected a planned mutation.
    #[error(transparent)]
    Write(#[from] DeploymentWriteError),
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
