use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::lifecycle::FixtureArtifact;

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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

/// A stable scenario-local node name, mapped to a real `NodeId` by a driver.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FixtureNodeName(String);

impl FixtureNodeName {
    /// Creates a scenario-local node name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the fixture node name as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A persisted value used to prove state survives a full control-plane outage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureMarker(String);

impl FixtureMarker {
    /// Creates a persisted acceptance marker.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the marker as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A stable scenario-local controller name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FixtureControllerName(String);

impl FixtureControllerName {
    /// Creates a scenario-local controller name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the controller name as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A unique mutation name used to prove fencing behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureMutationName(String);

impl FixtureMutationName {
    /// Creates a scenario-local mutation name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the mutation name as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An opaque affinity token returned by public ingress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureAffinityToken(String);

impl FixtureAffinityToken {
    /// Creates an observed affinity token.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the token text for opacity assertions and driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Whether both required affinity cookies were returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AffinityCookieSet {
    /// Both node-level and workload-level affinity cookies are present.
    Complete,
    /// One or both affinity cookies are missing.
    Incomplete,
}

/// The readiness shape applied when starting a candidate deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateReadiness {
    /// Every candidate replica starts ready.
    AllReady,
    /// Exactly one candidate replica remains unready until explicitly released.
    OneDelayed,
}

/// Whether the old workload respected an in-flight request during drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrainBehavior {
    /// Shutdown remained pending until the in-flight response completed.
    WaitedForInflight,
    /// Shutdown completed before the in-flight response completed.
    ExitedEarly,
}

/// The externally observed result of an ingress cutover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CutoverObservation {
    /// Deployment versions served while continuous traffic crossed the cutover.
    pub traffic_versions: BTreeSet<FixtureVersion>,
    /// Public requests that failed during cutover.
    pub public_failures: usize,
    /// Version that completed an in-flight request after cutover began.
    pub in_flight_version: FixtureVersion,
    /// Whether workload shutdown honored the in-flight request.
    pub drain_behavior: DrainBehavior,
    /// Deployment versions routed after cutover and old-generation cleanup.
    pub final_routes: BTreeSet<FixtureVersion>,
}

/// The affinity result observed from one public request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AffinityObservation {
    /// The logical node that served the response.
    pub node: FixtureNodeName,
    /// The opaque affinity token returned by ingress.
    pub token: FixtureAffinityToken,
    /// The required cookie set returned with the response.
    pub cookies: AffinityCookieSet,
}

/// An established client session and its first affinity observation.
#[derive(Debug)]
pub struct AffinitySession<Session> {
    /// The implementation-specific replay handle.
    pub session: Session,
    /// The response that established the session.
    pub initial: AffinityObservation,
}

/// The current leader and its opaque fencing token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipSnapshot<LeadershipToken> {
    /// The controller holding leadership.
    pub controller: FixtureControllerName,
    /// The token that must fence privileged writes.
    pub token: LeadershipToken,
}

/// The result of a privileged write attempted through a fencing token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FencedWriteOutcome {
    /// The current leader committed the mutation.
    Applied,
    /// The store rejected or could not commit the mutation.
    Rejected,
}

/// The result of an assignment-manifest generation CAS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssignmentWriteOutcome {
    /// The current leader committed the new generation.
    Applied,
    /// The expected generation did not match the stored generation.
    GenerationConflict,
    /// The fencing token no longer belongs to the leader.
    LeadershipLost,
}

/// The persisted assignment state relevant to election failover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentManifestSnapshot {
    /// The CAS generation stored for the workload node.
    pub generation: u64,
    /// The deployment version referenced by its assignment.
    pub version: FixtureVersion,
}

/// The observation window used when probing control-plane readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadinessProbe {
    /// A short probe used to prove readiness remains blocked.
    Brief,
    /// A convergence probe used after enough voters have returned.
    UntilReady,
}

/// The result of probing the control-plane readiness gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlPlaneReadiness {
    /// Persisted quorum is available and accepts linearizable operations.
    Ready,
    /// Persisted quorum is unavailable within the requested probe window.
    Unavailable,
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

/// The desired availability of an injected test resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceAvailability {
    /// The resource is running and should serve traffic.
    Available,
    /// The resource is stopped and must not receive traffic.
    Unavailable,
}

/// Whether a runtime termination leaves its persisted replica record behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaRecordDisposition {
    /// Preserve the last observed replica record.
    Retained,
    /// Remove the record to model a workload lost before any probe verdict.
    Missing,
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
    /// Publishing the workload artifact to assigned nodes.
    Publishing,
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
    /// Consecutive healthcheck failures recorded for this replica.
    pub healthcheck_failures: u32,
    /// Whether the runtime workload currently exists.
    pub workload: ResourceAvailability,
    /// Logical node currently hosting this replica, when assigned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<FixtureNodeName>,
    /// Opaque runtime workload identity used to prove instance replacement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_instance: Option<String>,
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
    /// Observed phase transitions in write order.
    pub phase_history: Vec<DeploymentPhase>,
    /// Artifact identity persisted after a successful build.
    pub artifact: Option<FixtureArtifact>,
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
    /// Deployment currently selected for service traffic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deployment_id: Option<DeploymentId>,
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

/// One scheduler assignment in a normalized acceptance snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledAssignment<AssignmentId> {
    /// The implementation-specific assignment identity.
    pub id: AssignmentId,
    /// The replica slot owned by this assignment.
    pub replica_index: ReplicaIndex,
    /// The node selected by the scheduler.
    pub node: FixtureNodeName,
}

/// The scheduling and workload state observed after one scale operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulingSnapshot<AssignmentId> {
    /// Assignments ordered by replica index.
    pub assignments: Vec<ScheduledAssignment<AssignmentId>>,
    /// Replica slots the scheduler could not place.
    pub unschedulable_replicas: Vec<ReplicaIndex>,
    /// Runtime workloads left behind without an assignment.
    pub orphaned_workloads: usize,
}
