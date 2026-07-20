use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::{
    FixtureNodeName, ResourceAvailability, RoutingCluster,
    scenarios::routing_survives_workload_and_gateway_failures,
};

pub(super) struct RoutingWorld {
    pub(super) nodes: Vec<FixtureNodeName>,
    pub(super) workloads: BTreeMap<FixtureNodeName, ResourceAvailability>,
    pub(super) gateways: BTreeMap<FixtureNodeName, ResourceAvailability>,
}

impl RoutingWorld {
    pub(super) fn new() -> Self {
        let nodes = (1..=3)
            .map(|node_number| FixtureNodeName::new(format!("node-{node_number}")))
            .collect::<Vec<_>>();
        Self {
            workloads: nodes
                .iter()
                .cloned()
                .map(|node| (node, ResourceAvailability::Available))
                .collect(),
            gateways: nodes
                .iter()
                .cloned()
                .map(|node| (node, ResourceAvailability::Available))
                .collect(),
            nodes,
        }
    }

    pub(super) fn public_routes(&self) -> BTreeSet<FixtureNodeName> {
        self.nodes
            .iter()
            .filter(|node| {
                self.workloads.get(*node) == Some(&ResourceAvailability::Available)
                    && self.gateways.get(*node) == Some(&ResourceAvailability::Available)
            })
            .cloned()
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum RoutingWorldError {
    #[error("node `{0}` does not exist")]
    MissingNode(String),
    #[error("public ingress is still available")]
    StillAvailable,
    #[error("control plane cannot reach quorum")]
    NoQuorum,
    #[error("control plane is not fully ready")]
    ControlPlaneNotReady,
}

#[async_trait]
impl RoutingCluster for RoutingWorld {
    type Error = RoutingWorldError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.nodes.clone()
    }

    async fn set_workload_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<(), Self::Error> {
        let current = self
            .workloads
            .get_mut(node)
            .ok_or_else(|| RoutingWorldError::MissingNode(node.as_str().to_string()))?;
        *current = availability;
        Ok(())
    }

    async fn set_gateway_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<(), Self::Error> {
        let current = self
            .gateways
            .get_mut(node)
            .ok_or_else(|| RoutingWorldError::MissingNode(node.as_str().to_string()))?;
        *current = availability;
        Ok(())
    }

    async fn await_public_routes(
        &mut self,
        _expected: &BTreeSet<FixtureNodeName>,
    ) -> Result<BTreeSet<FixtureNodeName>, Self::Error> {
        Ok(self.public_routes())
    }

    async fn await_public_unavailable(&mut self) -> Result<(), Self::Error> {
        if self.public_routes().is_empty() {
            Ok(())
        } else {
            Err(RoutingWorldError::StillAvailable)
        }
    }
}

#[tokio::test]
async fn routing_scenario_recovers_from_workload_and_gateway_failures() {
    let mut cluster = RoutingWorld::new();

    routing_survives_workload_and_gateway_failures(&mut cluster)
        .await
        .expect("routing recovery scenario");
}
