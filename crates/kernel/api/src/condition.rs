use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Generation, Timestamp};

/// Stable machine-readable name of a status condition.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConditionType {
    /// A controller completed its primary reconciliation.
    Ready,
    /// A node is explicitly excluded from scheduling.
    Schedulable,
    /// A node is being drained of workloads.
    Draining,
    /// A node is reserved for maintenance.
    Maintenance,
    /// Artifacts retained by a draining node have safe peer copies.
    ArtifactReplicationReady,
    /// The node mesh network is configured and available.
    MeshReady,
    /// The node health probe is passing.
    HealthReady,
    /// An assignment runtime is ready.
    RuntimeReady,
    /// An assignment runtime restart has completed.
    RuntimeRestart,
    /// The node firewall generation is applied.
    FirewallReady,
    /// Legacy data-plane readiness retained during migration.
    LegacyDataPlaneReady,
}

/// Stable machine-readable reason for a condition transition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ConditionReason(pub String);

/// Three-valued state of a status condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConditionState {
    /// The condition currently holds.
    True,
    /// The condition currently does not hold.
    False,
    /// The controller cannot currently determine the condition.
    Unknown,
}

/// Generic status evidence rendered consistently by APIs and the panel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Stable condition name.
    #[serde(rename = "type")]
    pub condition_type: ConditionType,
    /// Current three-valued state.
    #[serde(rename = "status")]
    pub state: ConditionState,
    /// Stable reason for the most recent transition.
    pub reason: ConditionReason,
    /// Human-readable context that must not be used for machine decisions.
    pub message: String,
    /// Desired generation evaluated to produce this condition.
    pub observed_generation: Generation,
    /// Time the state last changed; message-only updates do not change it.
    pub last_transition_time: Timestamp,
}
