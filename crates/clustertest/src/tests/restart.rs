use async_trait::async_trait;

use crate::{
    FixtureNodeName, ResourceAvailability, RestartCluster,
    scenarios::serial_node_restarts_preserve_quorum_and_routing,
};

use super::routing::{RoutingWorld, RoutingWorldError};

#[async_trait]
impl RestartCluster for RoutingWorld {
    async fn set_node_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<(), Self::Error> {
        let workload = self
            .workloads
            .get_mut(node)
            .ok_or_else(|| RoutingWorldError::MissingNode(node.as_str().to_string()))?;
        *workload = availability;
        let gateway = self
            .gateways
            .get_mut(node)
            .ok_or_else(|| RoutingWorldError::MissingNode(node.as_str().to_string()))?;
        *gateway = availability;
        Ok(())
    }

    async fn verify_quorum_write(
        &mut self,
        _unavailable_node: &FixtureNodeName,
    ) -> Result<(), Self::Error> {
        let unavailable = self
            .gateways
            .values()
            .filter(|availability| **availability == ResourceAvailability::Unavailable)
            .count();
        if unavailable <= 1 {
            Ok(())
        } else {
            Err(RoutingWorldError::NoQuorum)
        }
    }

    async fn await_control_plane_ready(&mut self) -> Result<(), Self::Error> {
        if self
            .gateways
            .values()
            .all(|availability| *availability == ResourceAvailability::Available)
        {
            Ok(())
        } else {
            Err(RoutingWorldError::ControlPlaneNotReady)
        }
    }
}

#[tokio::test]
async fn serial_restart_scenario_preserves_quorum_and_routing() {
    let mut cluster = RoutingWorld::new();

    serial_node_restarts_preserve_quorum_and_routing(&mut cluster)
        .await
        .expect("serial restart scenario");
}
