use std::fmt::Debug;

use async_trait::async_trait;

use crate::{AffinityObservation, AffinitySession, FixtureNodeName};

/// Drives public requests through node and workload affinity layers.
#[async_trait]
pub trait AffinityCluster: Send {
    /// The implementation's opaque cookie-bearing client session.
    type Session: Debug + Send + Sync;

    /// A matchable error returned by affinity driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Returns every routable node in deterministic order.
    fn nodes(&self) -> Vec<FixtureNodeName>;

    /// Establishes affinity through an ordinary request without a hint.
    async fn establish_affinity(&mut self) -> Result<AffinitySession<Self::Session>, Self::Error>;

    /// Replays the established cookies without an explicit affinity hint.
    async fn replay_affinity(
        &mut self,
        session: &Self::Session,
    ) -> Result<AffinityObservation, Self::Error>;

    /// Replays cookies while explicitly selecting another node.
    async fn override_affinity(
        &mut self,
        session: &Self::Session,
        node: &FixtureNodeName,
    ) -> Result<AffinityObservation, Self::Error>;
}
