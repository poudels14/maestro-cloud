use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::Object;
use kernel_store::{Clock, MonotonicTime, Version};

use crate::FencedStore;

/// Scheduling decision returned by one level-triggered reconcile invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Desired and observed state currently agree.
    Done,
    /// Reconcile again after a relative delay.
    Requeue(Duration),
    /// Reconcile at an absolute injected-clock deadline.
    RequeueAt(MonotonicTime),
}

/// Matchable operator failure classified for retry and status reporting.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReconcileError {
    /// Controller-kernel failure that must escape the work queue immediately.
    #[error(transparent)]
    Infrastructure(#[from] crate::ControllerError),
    /// Transient failure retried with controller backoff.
    #[error("retryable reconciliation failure: {message}")]
    Retryable {
        /// Operator-facing failure detail.
        message: String,
    },
    /// Invalid desired state that requires an external change.
    #[error("terminal reconciliation failure ({reason}): {message}")]
    Terminal {
        /// Stable condition reason used by API clients.
        reason: String,
        /// Operator-facing validation or policy detail.
        message: String,
    },
}

/// Per-invocation kernel capabilities available to one reconciler.
#[derive(Clone)]
pub struct ReconcileContext {
    fenced_store: Arc<FencedStore>,
    clock: Arc<dyn Clock>,
    observed_version: Version,
    attempt: u32,
}

impl ReconcileContext {
    /// Creates context for one invocation; controller runtimes normally own this call.
    pub fn new(
        fenced_store: Arc<FencedStore>,
        clock: Arc<dyn Clock>,
        observed_version: Version,
        attempt: u32,
    ) -> Self {
        Self {
            fenced_store,
            clock,
            observed_version,
            attempt,
        }
    }

    /// Returns the only mutation facade available to the reconciler.
    pub fn store(&self) -> &FencedStore {
        &self.fenced_store
    }

    /// Returns the injected deterministic clock.
    pub fn clock(&self) -> &dyn Clock {
        self.clock.as_ref()
    }

    /// Returns the exact store version observed for optimistic mutations.
    pub fn observed_version(&self) -> Version {
        self.observed_version
    }

    /// Returns the zero-based retry attempt for this resource revision.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// Level-triggered desired-state reconciler for one typed resource kind.
#[async_trait]
pub trait Reconciler: Send + Sync {
    /// Kind-specific resource identity.
    type Id: Clone + Debug + Send + Sync + 'static;
    /// Desired-state type.
    type Spec: Clone + Debug + Send + Sync + 'static;
    /// Observed-state type.
    type Status: Clone + Debug + Send + Sync + 'static;

    /// Stable resource kind registered by this reconciler.
    const KIND: &'static str;

    /// Optional cleanup barrier installed on active resources by the runtime.
    const FINALIZER: Option<&'static str> = None;

    /// Converges one observed resource and returns its next scheduling action.
    ///
    /// Reconciliation is at-least-once. Canceling this future may leave any
    /// completed fenced transaction committed; a later invocation must
    /// converge from every interruption point.
    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError>;

    /// Cleans external state after deletion is requested.
    ///
    /// The runtime removes this controller's finalizer only after this method
    /// returns [`Action::Done`]. The default reuses level-triggered reconcile
    /// logic for controllers whose cleanup is part of the same state machine.
    async fn finalize(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.reconcile(resource, context).await
    }
}
