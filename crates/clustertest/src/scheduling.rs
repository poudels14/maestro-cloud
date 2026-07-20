use std::fmt::Debug;

use async_trait::async_trait;

use crate::{ReplicaCount, SchedulingSnapshot};

/// Drives replica scaling through a scheduler and workload reconciler.
#[async_trait]
pub trait SchedulingCluster: Send {
    /// The implementation's assignment identifier.
    type AssignmentId: Clone + Debug + Eq + Send + Sync;

    /// A matchable error returned by scheduling driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Reconciles the requested replica count and returns externally verified placement.
    async fn scale(
        &mut self,
        replicas: ReplicaCount,
    ) -> Result<SchedulingSnapshot<Self::AssignmentId>, Self::Error>;
}
