//! Runtime-independent acceptance scenarios for Maestro clusters.
//!
//! This crate defines behavior-level fixtures and a driver contract shared by
//! the old control plane and the rewrite. It must not depend on any Maestro
//! production crate so each implementation is tested through the same seam.

mod cluster;
mod error;
mod model;
mod restart;
mod routing;
pub mod scenarios;
mod scheduling;

pub use cluster::{AcceptanceCluster, FaultInjectableCluster};
pub use error::ScenarioError;
pub use model::{
    ClusterSnapshot, DeploymentPhase, DeploymentSnapshot, FixtureName, FixtureNodeName,
    FixtureVersion, IngressFixture, ReplicaCount, ReplicaIndex, ReplicaOverride, ReplicaSnapshot,
    ResourceAvailability, RolloutFailure, ScheduledAssignment, SchedulingSnapshot, ServiceFixture,
    ServiceSnapshot,
};
pub use restart::RestartCluster;
pub use routing::RoutingCluster;
pub use scheduling::SchedulingCluster;

#[cfg(test)]
mod tests;
