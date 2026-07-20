use std::collections::BTreeSet;
use std::fmt::Debug;

use async_trait::async_trait;

use crate::{FixtureNodeName, ResourceAvailability};

/// Drives externally visible routing across an isolated cluster topology.
#[async_trait]
pub trait RoutingCluster: Send {
    /// A matchable error returned by routing driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Returns every logical node in deterministic order.
    fn nodes(&self) -> Vec<FixtureNodeName>;

    /// Makes the workload on one node available or unavailable.
    async fn set_workload_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<(), Self::Error>;

    /// Makes the node gateway available or unavailable.
    async fn set_gateway_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<(), Self::Error>;

    /// Waits for public ingress to route to exactly the expected nodes.
    async fn await_public_routes(
        &mut self,
        expected: &BTreeSet<FixtureNodeName>,
    ) -> Result<BTreeSet<FixtureNodeName>, Self::Error>;

    /// Waits for public ingress to become unavailable.
    async fn await_public_unavailable(&mut self) -> Result<(), Self::Error>;
}
