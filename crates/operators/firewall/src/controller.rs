use std::sync::Arc;

use kernel_api::ClusterId;
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::Keyspace;

use crate::snapshot::ResourceSnapshot;
use crate::writer::{FirewallWriteError, FirewallWriter};
use crate::{FirewallBackend, FirewallBackendError, FirewallPlanError, FirewallSettings};

/// Result of one finite, globally fenced firewall convergence pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FirewallReport {
    /// Number of complete per-node rulesets handed to the backend.
    pub applied_rulesets: usize,
    /// Policy statuses atomically acknowledged after backend success.
    pub updated_policies: usize,
    /// Digest of the complete planned bundle, including an empty bundle.
    pub bundle_digest: String,
    /// Whether concurrent input invalidated the snapshot without acknowledgement.
    pub conflict: bool,
}

/// Store-backed firewall controller with an idempotent whole-bundle backend.
pub struct FirewallController {
    keyspace: Keyspace,
    settings: FirewallSettings,
    backend: Arc<dyn FirewallBackend>,
    reconcile_gate: tokio::sync::Mutex<()>,
}

impl FirewallController {
    /// Constructs a firewall controller without reading or mutating cluster state.
    pub fn new(
        cluster_id: ClusterId,
        settings: FirewallSettings,
        backend: Arc<dyn FirewallBackend>,
    ) -> Self {
        Self {
            keyspace: Keyspace::new(&cluster_id),
            settings,
            backend,
            reconcile_gate: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    /// Projects, preflights, applies, and atomically acknowledges one exact snapshot.
    pub async fn reconcile_once(
        &self,
        store: &FencedStore,
    ) -> Result<FirewallReport, FirewallError> {
        let _guard = self.reconcile_gate.lock().await;
        let snapshot = ResourceSnapshot::load(store, &self.keyspace).await?;
        let plan = crate::plan(snapshot.input(self.settings.clone()))?;
        if !FirewallWriter::preflight(store, &snapshot).await? {
            return Ok(FirewallReport {
                bundle_digest: plan.bundle_digest,
                conflict: true,
                ..Default::default()
            });
        }
        let bundle = plan.bundle();
        self.backend.apply(&bundle).await?;
        let write = FirewallWriter::apply(store, &snapshot, &plan).await?;
        Ok(FirewallReport {
            applied_rulesets: bundle.rulesets.len(),
            updated_policies: write.updated_policies,
            bundle_digest: bundle.digest,
            conflict: write.conflict,
        })
    }
}

/// Matchable firewall construction, planning, backend, and mutation failures.
#[derive(Debug, thiserror::Error)]
pub enum FirewallError {
    /// A static or stored resource identifier was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// The pure firewall compiler rejected the projected resources.
    #[error(transparent)]
    Plan(#[from] FirewallPlanError),
    /// The leadership fence or backing store rejected an operation.
    #[error(transparent)]
    Controller(#[from] ControllerError),
    /// The atomic policy-status writer rejected a planned mutation.
    #[error(transparent)]
    Write(#[from] FirewallWriteError),
    /// The firewall backend failed before status acknowledgement.
    #[error(transparent)]
    Backend(#[from] FirewallBackendError),
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
