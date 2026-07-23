use async_trait::async_trait;

use crate::{AcceptanceCluster, FixtureName, FixtureNodeName};

/// Desired admission state for new service deployment generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceFreezeState {
    /// Hold new generations in the queue.
    Frozen,
    /// Admit new generations normally.
    Active,
}

/// Desired workload scheduling state for a node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeDrainState {
    /// Stop admitting new workloads and relocate existing workloads when possible.
    Draining,
    /// Admit workloads normally.
    Available,
}

/// Drives explicit service and placement lifecycle commands through public mutation seams.
#[async_trait]
pub trait ServiceLifecycleCluster: AcceptanceCluster {
    /// Returns the stable logical node names in this topology.
    fn topology_nodes(&self) -> Vec<FixtureNodeName>;

    /// Recycles every workload while preserving the deployment identity.
    async fn restart_deployment(
        &mut self,
        deployment_id: &Self::DeploymentId,
    ) -> Result<(), Self::Error>;

    /// Drains and removes one deployment while retaining its history record.
    async fn remove_deployment(
        &mut self,
        deployment_id: &Self::DeploymentId,
    ) -> Result<(), Self::Error>;

    /// Starts finalizer-driven deletion of a service and all owned resources.
    async fn delete_service(&mut self, service: &FixtureName) -> Result<(), Self::Error>;

    /// Freezes or unfreezes admission of new deployments for one service.
    async fn set_service_frozen(
        &mut self,
        service: &FixtureName,
        state: ServiceFreezeState,
    ) -> Result<(), Self::Error>;

    /// Drains or restores workload placement on one logical node.
    async fn set_node_draining(
        &mut self,
        node: &FixtureNodeName,
        state: NodeDrainState,
    ) -> Result<(), Self::Error>;

    /// Pins a service's next deployment to one logical node.
    async fn set_service_node_affinity(
        &mut self,
        service: &FixtureName,
        node: &FixtureNodeName,
    ) -> Result<(), Self::Error>;
}
