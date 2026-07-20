use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    DeploymentPhase, FixtureName, LifecycleFaultCluster, ReplicaIndex, ResourceAvailability,
};

/// A runtime artifact identity produced by a deployment build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureArtifact(String);

impl FixtureArtifact {
    /// Creates an observed artifact identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the artifact identity as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Deterministic artifact behavior applied before reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactBehavior {
    /// Complete the build with the selected artifact identity.
    CompleteWith(FixtureArtifact),
    /// Keep the build pending until the injected clock exceeds its timeout.
    NeverCompletes,
}

/// Observable progress of one artifact pipeline stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactStageState {
    /// The stage has not completed.
    Pending,
    /// The stage completed successfully.
    Complete,
}

/// Preparation, build, and persistence evidence for one deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactObservation {
    /// Source preparation state.
    pub preparation: ArtifactStageState,
    /// Artifact build state.
    pub build: ArtifactStageState,
    /// Artifact identity persisted in the deployment record.
    pub persisted_artifact: Option<FixtureArtifact>,
}

/// A health verdict reported through the production monitor seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaHealth {
    /// The replica answered its health probe.
    Healthy,
    /// The replica failed its health probe.
    Unhealthy,
}

/// Replica health state and the cumulative store writes used to reach it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthObservation {
    /// Persisted replica lifecycle phase.
    pub phase: DeploymentPhase,
    /// Consecutive probe failures.
    pub failures: u32,
    /// Runtime workload availability.
    pub workload: ResourceAvailability,
    /// Cumulative health-state writes performed by the driver.
    pub store_writes: u64,
}

/// Adds artifact, freeze, and health-monitor controls to lifecycle scenarios.
#[async_trait]
pub trait LifecycleControlCluster: LifecycleFaultCluster {
    /// Freezes new deploy admission after a deployment is already queued.
    async fn freeze_service(&mut self, service: &FixtureName) -> Result<(), Self::Error>;

    /// Configures deterministic artifact behavior for a queued deployment.
    async fn configure_artifact(
        &mut self,
        deployment_id: &Self::DeploymentId,
        behavior: ArtifactBehavior,
    ) -> Result<(), Self::Error>;

    /// Returns preparation, build, and persisted artifact state.
    async fn artifact_observation(
        &mut self,
        deployment_id: &Self::DeploymentId,
    ) -> Result<ArtifactObservation, Self::Error>;

    /// Returns the unhealthy verdict count that triggers a restart.
    fn health_failure_threshold(&self) -> u32;

    /// Seeds a persisted health state before reporting a verdict.
    async fn seed_replica_health(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
        phase: DeploymentPhase,
        failures: u32,
    ) -> Result<(), Self::Error>;

    /// Observes the current persisted health state without changing it.
    async fn health_observation(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
    ) -> Result<HealthObservation, Self::Error>;

    /// Reports one health verdict and returns the resulting persisted state.
    async fn report_health(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
        health: ReplicaHealth,
    ) -> Result<HealthObservation, Self::Error>;
}
