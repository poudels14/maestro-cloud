use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;

use crate::{
    ClusterSnapshot, FixtureName, ReplicaCount, ReplicaIndex, ReplicaOverride,
    ReplicaRecordDisposition, RolloutFailure, ServiceFixture,
};

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

    /// Runs exactly one reconciliation turn without fabricating readiness.
    async fn reconcile_once(&mut self) -> Result<(), Self::Error>;

    /// Returns current normalized state without driving reconciliation.
    async fn snapshot_now(&mut self) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error>;

    /// Waits for a quiet reconciliation window and returns the resulting state.
    async fn await_converged(&mut self)
    -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error>;
}

/// Injects failures at production trait seams for resilience scenarios.
///
/// Implementations must inject the failure before returning. The subsequent
/// `await_converged` call observes how ordinary reconciliation recovers.
#[async_trait]
pub trait FaultInjectableCluster: AcceptanceCluster {
    /// Makes the selected rollout fail during preparation or artifact build.
    async fn inject_rollout_failure(
        &mut self,
        deployment_id: &Self::DeploymentId,
        failure: RolloutFailure,
    ) -> Result<(), Self::Error>;

    /// Reports one replica as crashed through the implementation's runtime seam.
    async fn inject_replica_crash(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
    ) -> Result<(), Self::Error>;
}

/// Adds runtime-level crash and pending-readiness controls to lifecycle scenarios.
#[async_trait]
pub trait LifecycleFaultCluster: FaultInjectableCluster {
    /// Runs the bounded post-sequence reconciliation window with readiness enabled.
    async fn settle_operation_sequence(
        &mut self,
    ) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error>;

    /// Waits until the selected deployment has started the expected workloads.
    async fn await_started(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replicas: ReplicaCount,
    ) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error>;

    /// Settles reconciliation without reporting pending replicas ready.
    async fn await_stable_without_readiness(
        &mut self,
    ) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error>;

    /// Marks a replica crashed after it has consumed its full restart budget.
    async fn inject_exhausted_replica(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
    ) -> Result<(), Self::Error>;

    /// Terminates a runtime workload independently of health reporting.
    async fn inject_workload_termination(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
        record: ReplicaRecordDisposition,
    ) -> Result<(), Self::Error>;
}
