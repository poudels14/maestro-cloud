use kernel_api::ClusterId;
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::Keyspace;

use crate::snapshot::ResourceSnapshot;
use crate::writer::{FirewallWriteError, FirewallWriter};
use crate::{FirewallPlanError, FirewallSettings};

/// Result of one finite, globally fenced firewall convergence pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FirewallReport {
    /// Number of per-node desired rulesets created or replaced in the store.
    pub published_rulesets: usize,
    /// Number of desired rulesets still awaiting exact node acknowledgement.
    pub pending_rulesets: usize,
    /// Whether this pass changed the desired per-node resource set.
    pub desired_state_changed: bool,
    /// Policy statuses atomically acknowledged after backend success.
    pub updated_policies: usize,
    /// Digest of the complete planned bundle, including an empty bundle.
    pub bundle_digest: String,
    /// Whether concurrent input invalidated the snapshot without acknowledgement.
    pub conflict: bool,
}

/// Store-backed firewall controller publishing desired state for node-local application.
pub struct FirewallController {
    keyspace: Keyspace,
    settings: FirewallSettings,
    reconcile_gate: tokio::sync::Mutex<()>,
}

impl FirewallController {
    /// Constructs a firewall controller without reading or mutating cluster state.
    pub fn new(cluster_id: ClusterId, settings: FirewallSettings) -> Self {
        Self {
            keyspace: Keyspace::new(&cluster_id),
            settings,
            reconcile_gate: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    /// Publishes per-node desired state and acknowledges policies only after every node applies it.
    pub async fn reconcile_once(
        &self,
        store: &FencedStore,
    ) -> Result<FirewallReport, FirewallError> {
        let _guard = self.reconcile_gate.lock().await;
        let snapshot = ResourceSnapshot::load(store, &self.keyspace).await?;
        let plan = crate::plan(snapshot.input(self.settings.clone()))?;
        let write = FirewallWriter::apply(store, &self.keyspace, &snapshot, &plan).await?;
        Ok(FirewallReport {
            published_rulesets: write.published_rulesets,
            pending_rulesets: write.pending_rulesets,
            desired_state_changed: write.desired_state_changed,
            updated_policies: write.updated_policies,
            bundle_digest: plan.bundle_digest,
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
