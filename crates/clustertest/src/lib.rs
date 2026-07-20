//! Runtime-independent acceptance scenarios for Maestro clusters.
//!
//! This crate defines behavior-level fixtures and a driver contract shared by
//! the old control plane and the rewrite. It must not depend on any Maestro
//! production crate so each implementation is tested through the same seam.

mod cluster;
mod error;
mod model;
pub mod scenarios;

pub use cluster::AcceptanceCluster;
pub use error::ScenarioError;
pub use model::{
    ClusterSnapshot, DeploymentPhase, DeploymentSnapshot, FixtureName, FixtureVersion,
    IngressFixture, ReplicaCount, ReplicaOverride, ReplicaSnapshot, ServiceFixture,
    ServiceSnapshot,
};
