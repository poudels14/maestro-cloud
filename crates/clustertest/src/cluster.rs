use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;

use crate::{ClusterSnapshot, FixtureName, ReplicaOverride, ServiceFixture};

/// Drives one isolated Maestro topology through behavior-level operations.
///
/// Implementations must apply operations through the same entry points used
/// by that system's API. `await_converged` must wait until reconciliation has
/// produced no state changes for the implementation's quiet-period contract.
#[async_trait]
pub trait AcceptanceCluster: Send {
    /// The implementation's deployment identifier.
    type DeploymentId: Clone + Debug + Eq + Send + Sync;

    /// A matchable error returned by driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Applies a declarative service fixture and returns its new deployment.
    async fn rollout(&mut self, service: ServiceFixture)
    -> Result<Self::DeploymentId, Self::Error>;

    /// Cancels a deployment that may be queued or in progress.
    async fn cancel(&mut self, deployment_id: &Self::DeploymentId) -> Result<(), Self::Error>;

    /// Sets or clears the service's replica override.
    async fn set_replicas(
        &mut self,
        service: &FixtureName,
        replica_override: ReplicaOverride,
    ) -> Result<(), Self::Error>;

    /// Advances injected logical time without waiting on wall-clock time.
    async fn advance(&mut self, duration: Duration) -> Result<(), Self::Error>;

    /// Waits for a quiet reconciliation window and returns the resulting state.
    async fn await_converged(&mut self)
    -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error>;
}
