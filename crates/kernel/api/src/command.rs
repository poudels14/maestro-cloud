use std::borrow::Cow;

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactArchiveId, DeploymentGoal, DeploymentId, FirewallPolicySpec, Generation,
    IngressRouteSpec, NodeId, RequestId, ResourceRevision, RolloutState, SecretValue, ServiceId,
    ServiceSpec, Timestamp, UpgradePhase, UpgradeRun, UpgradeRunId, UpgradeRunSpec,
};

/// Maximum compressed bytes accepted for one uploaded build context archive.
pub const MAXIMUM_ARTIFACT_ARCHIVE_BYTES: usize = 64 * 1_024 * 1_024;

/// Optimistic lifecycle command targeting one exact resource revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandRequest {
    /// Revision the operator observed before choosing the mutation.
    pub expected_revision: ResourceRevision,
}

/// Accepted node scheduling-state command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeCommandResponse {
    /// Node whose scheduling state was accepted.
    pub node_id: NodeId,
    /// Whether new workload placement is disabled for the node.
    pub draining: bool,
}

/// Observable phase of one permanent node-removal request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NodeRemovalState {
    /// Scheduling is disabled while retained artifacts and assignments leave the node.
    Draining,
    /// Membership and active node resources were removed behind a durable tombstone.
    Removed,
}

/// Explicit identity confirmation for one irreversible node removal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeRemovalRequest {
    /// Stable node identity repeated from the request path to prevent targeting mistakes.
    pub node_id: NodeId,
}

/// Progress receipt for one permanent node-removal request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeRemovalResponse {
    /// Node whose identity is being permanently retired.
    pub node_id: NodeId,
    /// Durable removal phase reached by this request.
    pub state: NodeRemovalState,
}

/// Secret-free state used to make an optimistic Tailscale auth-key rotation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TailscaleAuthKeyStatus {
    /// Revision of the live override, or absence while launch configuration supplies the key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_revision: Option<ResourceRevision>,
}

/// Optimistic replacement for the managed Tailscale gateway authentication key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TailscaleAuthKeyRotationRequest {
    /// Override revision observed by the caller, or absence when creating the first override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<ResourceRevision>,
    /// Replacement key used only by gateway replicas with fresh managed state.
    pub auth_key: SecretValue,
}

/// Durable receipt for an accepted Tailscale auth-key rotation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TailscaleAuthKeyRotationResponse {
    /// Idempotency identity atomically committed with the secret override.
    pub request_id: RequestId,
}

/// Desired identity and behavior for a new cluster upgrade run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpgradeCreateRequest {
    /// Stable run identity used to observe and retry this maintenance operation.
    pub upgrade_run_id: UpgradeRunId,
    /// Operation, target version, node selection, and batching strategy.
    pub spec: UpgradeRunSpec,
    /// Atomically cancel non-terminal maintenance before creating this run.
    #[serde(default, skip_serializing_if = "is_false")]
    pub force: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

/// Selection for canceling cluster maintenance without copying an opaque run identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpgradeCancelRequest {
    /// Cancel every non-terminal maintenance run instead of requiring exactly one.
    #[serde(default)]
    pub all: bool,
}

/// Accepted cancellation of one or more cluster maintenance runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeCancelResponse {
    /// Runs atomically marked for cancellation in stable identity order.
    pub upgrade_run_ids: Vec<UpgradeRunId>,
}

/// Accepted cluster upgrade mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeCommandResponse {
    /// Upgrade run that accepted the mutation.
    pub upgrade_run_id: UpgradeRunId,
    /// Desired generation after the mutation.
    pub generation: Generation,
    /// Upgrade phase observed while accepting the mutation.
    pub phase: UpgradePhase,
    /// Cancellation timestamp, when cancellation was requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<Timestamp>,
}

impl From<&UpgradeRun> for UpgradeCommandResponse {
    fn from(run: &UpgradeRun) -> Self {
        Self {
            upgrade_run_id: run.meta.id.clone(),
            generation: run.meta.generation,
            phase: run.status.phase,
            deletion_timestamp: run.meta.deletion_timestamp,
        }
    }
}

/// Optimistic temporary replica override for one service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceReplicaOverrideRequest {
    /// Revision the operator observed before choosing the mutation.
    pub expected_revision: ResourceRevision,
    /// Temporary replica count, or null to clear the override.
    #[serde(deserialize_with = "required_nullable_replicas")]
    #[schemars(with = "RequiredNullableU32")]
    pub replicas: Option<u32>,
}

/// Result of accepting one content-addressed build context archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactArchiveUploadResponse {
    /// Content address assigned to the archive bytes.
    pub archive_id: ArtifactArchiveId,
    /// Compressed bytes accepted by the archive store.
    pub size_bytes: u64,
}

struct RequiredNullableU32;

impl JsonSchema for RequiredNullableU32 {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "RequiredNullableU32".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        Option::<u32>::json_schema(generator)
    }
}

fn required_nullable_replicas<'de, Deserializer>(
    deserializer: Deserializer,
) -> Result<Option<u32>, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
{
    Option::<u32>::deserialize(deserializer)
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
    /// Allow this service generation to begin even when rollout is frozen.
    #[serde(default)]
    #[schemars(default)]
    pub force: bool,
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
