use std::fmt::Debug;

use async_trait::async_trait;

use crate::{ControlPlaneReadiness, FixtureMarker, FixtureNodeName, ReadinessProbe};

/// Drives a full control-plane outage and persisted-quorum recovery.
#[async_trait]
pub trait QuorumRecoveryCluster: Send {
    /// A matchable error returned by quorum-recovery driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Returns every persisted voter in deterministic order.
    fn nodes(&self) -> Vec<FixtureNodeName>;

    /// Persists a marker before the outage begins.
    async fn write_marker(&mut self, marker: FixtureMarker) -> Result<(), Self::Error>;

    /// Stops every persisted voter.
    async fn stop_all_nodes(&mut self) -> Result<(), Self::Error>;

    /// Starts one persisted voter without altering membership.
    async fn start_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error>;

    /// Probes the production readiness gate for a bounded or convergence window.
    async fn probe_readiness(
        &mut self,
        probe: ReadinessProbe,
    ) -> Result<ControlPlaneReadiness, Self::Error>;

    /// Reads the marker after quorum recovery.
    async fn read_marker(&mut self) -> Result<Option<FixtureMarker>, Self::Error>;
}
