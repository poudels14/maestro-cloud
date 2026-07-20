use std::fmt::Debug;

use async_trait::async_trait;

use crate::{
    AssignmentManifestSnapshot, AssignmentWriteOutcome, FencedWriteOutcome, FixtureControllerName,
    FixtureMutationName, FixtureVersion, LeadershipSnapshot,
};

/// Drives leader election, fenced mutations, and assignment CAS writes.
#[async_trait]
pub trait ElectionCluster: Send {
    /// The implementation's opaque fencing token.
    type LeadershipToken: Clone + Debug + Eq + Send + Sync;

    /// A matchable error returned by election driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Waits for one active controller to hold leadership.
    async fn await_leader(
        &mut self,
    ) -> Result<LeadershipSnapshot<Self::LeadershipToken>, Self::Error>;

    /// Attempts one privileged mutation using the supplied fencing token.
    async fn fenced_write(
        &mut self,
        token: &Self::LeadershipToken,
        mutation: FixtureMutationName,
    ) -> Result<FencedWriteOutcome, Self::Error>;

    /// Replaces the workload-node assignment manifest with generation CAS.
    async fn replace_assignment(
        &mut self,
        token: &Self::LeadershipToken,
        expected_generation: u64,
        version: FixtureVersion,
    ) -> Result<AssignmentWriteOutcome, Self::Error>;

    /// Reads the current workload-node assignment manifest linearly.
    async fn assignment(&mut self) -> Result<Option<AssignmentManifestSnapshot>, Self::Error>;

    /// Stops one controller while leaving the control-plane store available.
    async fn stop_controller(
        &mut self,
        controller: &FixtureControllerName,
    ) -> Result<(), Self::Error>;

    /// Removes enough store members to prevent quorum writes.
    async fn lose_quorum(&mut self) -> Result<(), Self::Error>;
}
