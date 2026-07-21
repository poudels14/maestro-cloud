use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    DeploymentGoal, DeploymentId, Generation, ResourceRevision, RolloutState, ServiceId, Timestamp,
};

/// Optimistic lifecycle command targeting one exact resource revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandRequest {
    /// Revision the operator observed before choosing the mutation.
    pub expected_revision: ResourceRevision,
}

/// Result of an accepted service lifecycle command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceCommandResponse {
    /// Mutated service identity.
    pub service_id: ServiceId,
    /// Desired generation after the command.
    pub generation: Generation,
    /// Rollout gate after the command.
    pub rollout: RolloutState,
    /// Effective temporary replica override, when configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_override: Option<u32>,
    /// Deletion request time, when deletion was accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<Timestamp>,
}

/// Result of an accepted immutable-deployment lifecycle command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentCommandResponse {
    /// Mutated deployment identity.
    pub deployment_id: DeploymentId,
    /// Desired deployment generation after the command.
    pub generation: Generation,
    /// Workload restart generation after the command.
    pub restart_generation: Generation,
    /// Desired lifecycle goal after the command.
    pub goal: DeploymentGoal,
}
