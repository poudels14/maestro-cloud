use serde::{Deserialize, Serialize};

/// A stable scenario-local service name, mapped to a real `ServiceId` by a driver.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FixtureName(String);

impl FixtureName {
    /// Creates a scenario-local service name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the fixture name as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A stable scenario-local service version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureVersion(String);

impl FixtureVersion {
    /// Creates a scenario-local service version.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the version as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A desired or observed number of replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaCount(u32);

impl ReplicaCount {
    /// Creates a replica count.
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the numeric replica count.
    pub fn get(self) -> u32 {
        self.0
    }
}

/// A stable replica index within one deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaIndex(u32);

impl ReplicaIndex {
    /// Creates a replica index.
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the numeric replica index.
    pub fn get(self) -> u32 {
        self.0
    }
}

/// Whether a fixture exposes ingress during its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngressFixture {
    /// The service has no ingress route.
    Disabled,
    /// The service has an ingress route at the given host.
    Host(String),
}

/// A service definition applied by a shared acceptance scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceFixture {
    /// The stable name used to find the service in snapshots.
    pub name: FixtureName,
    /// The version expected on the resulting deployment.
    pub version: FixtureVersion,
    /// The configured replica floor.
    pub replicas: ReplicaCount,
    /// The fixture's ingress shape.
    pub ingress: IngressFixture,
}

impl ServiceFixture {
    /// Creates a service fixture without ingress.
    pub fn new(name: FixtureName, version: FixtureVersion, replicas: ReplicaCount) -> Self {
        Self {
            name,
            version,
            replicas,
            ingress: IngressFixture::Disabled,
        }
    }

    /// Adds an ingress host to the fixture.
    pub fn with_ingress(mut self, host: impl Into<String>) -> Self {
        self.ingress = IngressFixture::Host(host.into());
        self
    }
}

/// The requested replica-override operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaOverride {
    /// Sets an explicit replica target.
    Set(ReplicaCount),
    /// Clears the override and restores the service configuration.
    Clear,
}

/// A failure injected into a rollout before reconciliation completes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RolloutFailure {
    /// Artifact preparation fails with the supplied diagnostic.
    Prepare(String),
    /// Artifact construction fails with the supplied diagnostic.
    Build(String),
}

/// A deployment phase normalized across Maestro implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentPhase {
    /// Accepted but not yet building.
    Queued,
    /// Preparing or building the workload artifact.
    Building,
    /// Running but not yet ready for traffic.
    PendingReady,
    /// Ready for traffic.
    Ready,
    /// Failed terminally.
    Crashed,
    /// Stopped before removal.
    Terminated,
    /// Fully removed.
    Removed,
    /// Leaving traffic and waiting for finalization.
    Draining,
    /// Canceled by an operator.
    Canceled,
}

/// One replica in a normalized cluster snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaSnapshot {
    /// The stable index within its deployment.
    pub index: u32,
    /// The replica's observed lifecycle phase.
    pub phase: DeploymentPhase,
    /// The restart attempts recorded for this replica.
    pub restart_attempts: u32,
}

/// One deployment in a normalized cluster snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentSnapshot<DeploymentId> {
    /// The implementation-specific deployment identity.
    pub id: DeploymentId,
    /// The version applied by the scenario.
    pub version: FixtureVersion,
    /// The deployment's observed lifecycle phase.
    pub phase: DeploymentPhase,
    /// The deployment's observed replicas, ordered by index.
    pub replicas: Vec<ReplicaSnapshot>,
}

/// One service in a normalized cluster snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceSnapshot<DeploymentId> {
    /// The scenario-local service name.
    pub name: FixtureName,
    /// The replica count from the applied service config.
    pub configured_replicas: ReplicaCount,
    /// The active override, if one is set.
    pub replica_override: Option<ReplicaCount>,
    /// Deployment history ordered from oldest to newest.
    pub deployments: Vec<DeploymentSnapshot<DeploymentId>>,
}

impl<DeploymentId: Eq> ServiceSnapshot<DeploymentId> {
    /// Finds a deployment by its implementation-specific identity.
    pub fn deployment(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<&DeploymentSnapshot<DeploymentId>> {
        self.deployments
            .iter()
            .find(|deployment| deployment.id == *deployment_id)
    }
}

/// A deterministic, behavior-level view of the resources relevant to M0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterSnapshot<DeploymentId> {
    /// Services ordered by fixture name.
    pub services: Vec<ServiceSnapshot<DeploymentId>>,
}

impl<DeploymentId> ClusterSnapshot<DeploymentId> {
    /// Finds a service by its scenario-local name.
    pub fn service(&self, name: &FixtureName) -> Option<&ServiceSnapshot<DeploymentId>> {
        self.services.iter().find(|service| service.name == *name)
    }
}
