use async_trait::async_trait;

use crate::{FixtureNodeName, ResourceAvailability, RoutingCluster};

/// Drives coordinated node restarts across the control and data planes.
#[async_trait]
pub trait RestartCluster: RoutingCluster {
    /// Stops or starts every Maestro component assigned to one logical node.
    async fn set_node_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<(), Self::Error>;

    /// Verifies that the remaining control-plane members accept a write.
    async fn verify_quorum_write(
        &mut self,
        unavailable_node: &FixtureNodeName,
    ) -> Result<(), Self::Error>;

    /// Waits until every restarted control-plane member is healthy.
    async fn await_control_plane_ready(&mut self) -> Result<(), Self::Error>;
}
