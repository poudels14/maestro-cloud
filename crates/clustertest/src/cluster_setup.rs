use std::collections::BTreeSet;
use std::fmt::Debug;

use async_trait::async_trait;

use crate::FixtureNodeName;

/// Drives real cluster formation through daemon process boundaries.
#[async_trait]
pub trait ClusterSetupCluster: Send {
    /// A matchable error returned by setup driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Returns the configured topology in deterministic order, seed first.
    fn nodes(&self) -> Vec<FixtureNodeName>;

    /// Starts the designated seed as a new one-member cluster.
    async fn bootstrap_seed(&mut self) -> Result<(), Self::Error>;

    /// Authenticates, stages, starts, and activates one declared member.
    async fn join_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error>;

    /// Waits until every expected node has applied the same mesh membership.
    async fn await_mesh(
        &mut self,
        expected: &BTreeSet<FixtureNodeName>,
    ) -> Result<BTreeSet<FixtureNodeName>, Self::Error>;

    /// Sends workload-subnet traffic from one node to another through the mesh.
    async fn ping_workload(
        &mut self,
        source: &FixtureNodeName,
        target: &FixtureNodeName,
    ) -> Result<(), Self::Error>;

    /// Stops one daemon and all child processes it owns.
    async fn stop_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error>;

    /// Restarts one persisted member without changing its membership.
    async fn restart_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error>;

    /// Commits and reads one linearizable value through the surviving quorum.
    async fn verify_store_write(&mut self) -> Result<(), Self::Error>;
}
