use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    DeploymentGoal, DeploymentId, FirewallPolicySpec, Generation, IngressRouteSpec,
    ResourceRevision, RolloutState, ServiceId, ServiceSpec, Timestamp,
};

/// Optimistic lifecycle command targeting one exact resource revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandRequest {
    /// Revision the operator observed before choosing the mutation.
    pub expected_revision: ResourceRevision,
}

/// Optimistic desired-state replacement for one service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceWriteRequest {
    /// Revision observed by the caller, or absence when creating the service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<ResourceRevision>,
    /// Complete desired service state.
    pub spec: ServiceSpec,
}

/// Accepted service desired-state generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceWriteResponse {
    /// Service whose desired state was accepted.
    pub service_id: ServiceId,
    /// Desired generation after the write.
    pub generation: Generation,
}

/// Read-only desired-state comparison for one service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceDiffRequest {
    /// Complete desired service state to compare without persisting it.
    pub spec: ServiceSpec,
}

/// Classification of one declarative service comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ServiceDiffStatus {
    /// The service does not exist yet.
    New,
    /// Desired state is equivalent after typed decoding.
    Unchanged,
    /// At least one desired field differs.
    Changed,
}

/// One masked, operator-facing field change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDiffChange {
    /// Stable dotted path within the service spec.
    pub field: String,
    /// Previous display value, absent when adding a field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Desired display value, absent when removing a field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
}

/// Masked comparison result and exact revision safe to submit on apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDiffResponse {
    /// Compared service identity.
    pub service_id: ServiceId,
    /// Current revision, or absence when the service is new.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<ResourceRevision>,
    /// Overall comparison classification.
    pub status: ServiceDiffStatus,
    /// Ordered masked field changes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<ServiceDiffChange>,
}

/// Complete resource set managed by one declarative service document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceRolloutSpec {
    /// Service workload and artifact desired state.
    pub service: ServiceSpec,
    /// Stable ingress route, or absence to remove the managed route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingress: Option<IngressRouteSpec>,
    /// Stable service egress policy, or absence to remove the managed policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub egress: Option<FirewallPolicySpec>,
}

/// Exact revisions observed for every managed rollout resource.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRolloutRevisions {
    /// Current service revision, or absence when missing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<ResourceRevision>,
    /// Current managed ingress route revision, or absence when missing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingress: Option<ResourceRevision>,
    /// Current managed egress policy revision, or absence when missing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub egress: Option<ResourceRevision>,
}

/// Read-only comparison of a complete declarative service resource set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceRolloutDiffRequest {
    /// Desired resource set to compare.
    pub desired: ServiceRolloutSpec,
}

/// Masked complete-resource comparison safe to submit on apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRolloutDiffResponse {
    /// Compared service identity.
    pub service_id: ServiceId,
    /// Exact revisions observed during the comparison.
    pub expected_revisions: ServiceRolloutRevisions,
    /// Overall comparison classification.
    pub status: ServiceDiffStatus,
    /// Ordered masked field changes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<ServiceDiffChange>,
}

/// Optimistic atomic apply of a declarative service resource set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceRolloutRequest {
    /// Revisions returned by the immediately preceding diff.
    pub expected_revisions: ServiceRolloutRevisions,
    /// Complete desired resource set.
    pub desired: ServiceRolloutSpec,
}

/// Accepted generations for an atomic declarative service apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRolloutResponse {
    /// Service whose resource set was accepted.
    pub service_id: ServiceId,
    /// Service generation after the apply.
    pub service_generation: Generation,
    /// Managed ingress generation, absent when the route was removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingress_generation: Option<Generation>,
    /// Managed egress generation, absent when the policy was removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub egress_generation: Option<Generation>,
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
