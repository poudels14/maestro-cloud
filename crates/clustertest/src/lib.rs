//! Runtime-independent acceptance scenarios for Maestro clusters.
//!
//! This crate defines behavior-level fixtures and a driver contract shared by
//! the old control plane and the rewrite. It must not depend on any Maestro
//! production crate so each implementation is tested through the same seam.

mod cluster;
mod election;
mod error;
mod model;
mod quorum;
mod restart;
mod routing;
pub mod scenarios;
mod scheduling;

pub use cluster::{AcceptanceCluster, FaultInjectableCluster};
pub use election::ElectionCluster;
pub use error::ScenarioError;
pub use model::{
    AssignmentManifestSnapshot, AssignmentWriteOutcome, ClusterSnapshot, ControlPlaneReadiness,
    DeploymentPhase, DeploymentSnapshot, FencedWriteOutcome, FixtureControllerName, FixtureMarker,
    FixtureMutationName, FixtureName, FixtureNodeName, FixtureVersion, IngressFixture,
    LeadershipSnapshot, ReadinessProbe, ReplicaCount, ReplicaIndex, ReplicaOverride,
    ReplicaSnapshot, ResourceAvailability, RolloutFailure, ScheduledAssignment, SchedulingSnapshot,
    ServiceFixture, ServiceSnapshot,
};
pub use quorum::QuorumRecoveryCluster;
pub use restart::RestartCluster;
pub use routing::RoutingCluster;
pub use scheduling::SchedulingCluster;

#[cfg(test)]
mod tests;
