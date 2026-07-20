use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Generation, Timestamp};

/// Stable machine-readable name of a status condition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ConditionType(pub String);

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
