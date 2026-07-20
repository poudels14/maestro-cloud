use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{FixtureNodeName, ResourceAvailability};

/// A persisted bootstrap decision observed by a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootstrapDecision {
    /// Start the designated seed as a new one-member cluster.
    BootstrapSeed,
    /// Resume an interrupted designated-seed start.
    ResumeSeed,
    /// Restart from existing membership.
    Restart,
    /// Join using persisted admission information.
    JoinExisting,
}

/// A member's role in the consensus group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FormationMemberRole {
    /// The member is still a non-voting learner.
    Learner,
    /// The member is a promoted voter.
    Voter,
}

/// Whether every member observes the same non-empty leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeadershipAgreement {
    /// Every member observes the same leader.
    Consistent,
    /// Members lack a leader or disagree about its identity.
    Divergent,
}

/// Whether every member observes the same consensus membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipAgreement {
    /// Every member observes the same member set and roles.
    Consistent,
    /// Members disagree about the member set or roles.
    Divergent,
}

/// The control and consensus ports advertised by a registered node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePorts {
    /// Control API port.
    pub api: u16,
    /// Public gateway port.
    pub gateway: u16,
    /// Consensus client port.
    pub consensus_client: u16,
    /// Consensus peer port.
    pub consensus_peer: u16,
}

/// The lifecycle state of a control endpoint reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationState {
    /// The reservation is active and owns its advertised endpoints.
    Active,
    /// The reservation is not active.
    Inactive,
}

/// Evidence captured while one learner joins and becomes a voter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinObservation {
    /// Logical node that joined.
    pub node: FixtureNodeName,
    /// Bootstrap decision before admission information is persisted.
    pub decision_before_join: BootstrapDecision,
    /// Bootstrap decision after admission information is persisted.
    pub decision_after_join: BootstrapDecision,
    /// Members encoded into the admitted node's initial cluster.
    pub initial_members: BTreeSet<FixtureNodeName>,
    /// Final consensus role after catch-up.
    pub final_role: FormationMemberRole,
}

/// A normalized view of the fully formed consensus group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormationSnapshot {
    /// Consensus members and their roles.
    pub members: BTreeMap<FixtureNodeName, FormationMemberRole>,
    /// Agreement about the member set and roles.
    pub membership: MembershipAgreement,
    /// Agreement about the elected leader.
    pub leadership: LeadershipAgreement,
    /// Whether the formed group accepts writes.
    pub writes: ResourceAvailability,
}

/// Registry and reservation evidence for the designated seed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationObservation {
    /// Nodes visible in the live registry.
    pub registered_nodes: BTreeSet<FixtureNodeName>,
    /// Nodes advertising the acceptance image.
    pub image_holders: BTreeSet<FixtureNodeName>,
    /// Node named by the control reservation.
    pub reservation_node: FixtureNodeName,
    /// State recorded by the control reservation.
    pub reservation_state: ReservationState,
    /// Ports advertised by the registered node.
    pub advertised_ports: NodePorts,
    /// Ports stored in the control reservation.
    pub reserved_ports: NodePorts,
}

/// Registry state after the seed revokes its registration lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationCleanup {
    /// Nodes that remain visible in the live registry.
    pub registered_nodes: BTreeSet<FixtureNodeName>,
    /// Nodes that remain as acceptance-image holders.
    pub image_holders: BTreeSet<FixtureNodeName>,
}

/// Drives designated-seed formation and registration through a shared scenario.
#[async_trait]
pub trait FormationCluster: Send {
    /// A matchable error returned by formation driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Returns the configured voters in deterministic order, seed first.
    fn nodes(&self) -> Vec<FixtureNodeName>;

    /// Observes the designated seed's persisted bootstrap decision.
    async fn seed_decision(&mut self) -> Result<BootstrapDecision, Self::Error>;

    /// Persists that the designated seed began starting.
    async fn mark_seed_starting(&mut self) -> Result<(), Self::Error>;

    /// Persists that the designated seed joined its cluster.
    async fn mark_seed_joined(&mut self) -> Result<(), Self::Error>;

    /// Waits until the designated seed is serving consensus requests.
    async fn await_seed(&mut self) -> Result<(), Self::Error>;

    /// Admits, starts, and promotes one configured voter.
    async fn join_and_promote(
        &mut self,
        node: &FixtureNodeName,
    ) -> Result<JoinObservation, Self::Error>;

    /// Returns the converged consensus membership and availability.
    async fn formation_snapshot(&mut self) -> Result<FormationSnapshot, Self::Error>;

    /// Registers the seed and publishes the acceptance image.
    async fn register_seed(&mut self) -> Result<RegistrationObservation, Self::Error>;

    /// Revokes the seed registration and returns the remaining leased records.
    async fn deregister_seed(&mut self) -> Result<RegistrationCleanup, Self::Error>;
}
