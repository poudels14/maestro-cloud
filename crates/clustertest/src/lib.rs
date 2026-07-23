//! Runtime-independent acceptance scenarios for Maestro clusters.
//!
//! This crate defines behavior-level fixtures and a driver contract shared by
//! the old control plane and the rewrite. It must not depend on any Maestro
//! production crate so each implementation is tested through the same seam.

mod affinity;
mod build;
mod cluster;
mod cluster_setup;
mod egress;
mod election;
mod error;
mod formation;
mod lifecycle;
mod model;
mod preview;
mod quorum;
mod restart;
mod rollout;
mod routing;
pub mod scenarios;
mod scheduling;
mod service_lifecycle;
mod smoke;
mod upgrade;

pub use affinity::AffinityCluster;
pub use build::{
    ArtifactBuildSnapshot, BuildCluster, BuildCompletion, BuildRolloutSnapshot,
    BuildServiceFixture, BuildSnapshot, FixtureSourceRevision,
};
pub use cluster::{AcceptanceCluster, FaultInjectableCluster, LifecycleFaultCluster};
pub use cluster_setup::ClusterSetupCluster;
pub use egress::{EgressCluster, EgressPolicyFixture, EgressRulesetSnapshot, EgressSnapshot};
pub use election::ElectionCluster;
pub use error::ScenarioError;
pub use formation::{
    BootstrapDecision, FormationCluster, FormationMemberRole, FormationSnapshot, JoinObservation,
    LeadershipAgreement, MembershipAgreement, NodePorts, RegistrationCleanup,
    RegistrationObservation, ReservationState,
};
pub use lifecycle::{
    ArtifactBehavior, ArtifactObservation, ArtifactStageState, FixtureArtifact, HealthObservation,
    LifecycleControlCluster, LifecycleOperation, ReplicaHealth,
};
pub use model::{
    AffinityCookieSet, AffinityObservation, AffinitySession, AssignmentManifestSnapshot,
    AssignmentWriteOutcome, CandidateReadiness, ClusterSnapshot, ControlPlaneReadiness,
    CutoverObservation, DeploymentPhase, DeploymentSnapshot, DrainBehavior, FencedWriteOutcome,
    FixtureAffinityToken, FixtureControllerName, FixtureMarker, FixtureMutationName, FixtureName,
    FixtureNodeName, FixtureVersion, IngressFixture, LeadershipSnapshot, ReadinessProbe,
    ReplicaCount, ReplicaIndex, ReplicaOverride, ReplicaRecordDisposition, ReplicaSnapshot,
    ResourceAvailability, RolloutFailure, ScheduledAssignment, SchedulingSnapshot, ServiceFixture,
    ServiceSnapshot,
};
pub use preview::{
    PreviewCluster, PreviewCompletion, PreviewRolloutSnapshot, PreviewServiceFixture,
};
pub use quorum::QuorumRecoveryCluster;
pub use restart::RestartCluster;
pub use rollout::CutoverCluster;
pub use routing::RoutingCluster;
pub use scheduling::SchedulingCluster;
pub use service_lifecycle::{NodeDrainState, ServiceFreezeState, ServiceLifecycleCluster};
pub use smoke::{
    IngressConfigurationState, IngressStartupCluster, IngressStartupObservation, PeerStoreCluster,
    PeerStoreObservation, SecurityRestartState, SeedControlRole, SeedSecurityCluster,
    SeedSecurityObservation,
};
pub use upgrade::{
    FixtureInstanceId, MaintenanceAttempt, MaintenanceCompletion, MaintenanceFreeze,
    MaintenanceNodeRole, MaintenanceNodeSnapshot, MaintenanceTopology, SchedulingEligibility,
    SelectedRestartObservation, TargetRetention, UpgradeCluster, UpgradeFault, UpgradeObservation,
};

#[cfg(test)]
mod tests;
