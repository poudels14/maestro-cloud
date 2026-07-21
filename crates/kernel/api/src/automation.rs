use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    Condition, NodeId, NodeInstanceId, Object, PreviewId, SecretValue, ServiceId, Timestamp,
    UpgradeRunId, WebhookId,
};

/// Desired pull-request preview derivation and teardown policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreviewSpec {
    /// Base service copied into the isolated preview service.
    pub base_service_id: ServiceId,
    /// Source repository in `owner/name` form.
    pub repository: String,
    /// Pull-request number within the repository.
    pub pull_request_number: u64,
    /// Current pull-request head revision.
    pub head_revision: String,
    /// Derived service identity owned by this preview.
    pub service_id: ServiceId,
    /// Grace period between pull-request close and teardown.
    pub close_grace_period_secs: u64,
    /// Absolute preview expiry time.
    pub expires_at: Timestamp,
}

/// Persisted lifecycle of a pull-request preview.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PreviewPhase {
    /// Waiting for source inspection or derived resource creation.
    Pending,
    /// Derived resources exist and the preview can receive traffic.
    Active,
    /// Pull request closed and the grace-period finalizer is waiting.
    Closing,
    /// Derived resources were deleted after close or expiry.
    Expired,
    /// Preview reconciliation ended with a terminal configuration error.
    Failed,
    /// Preview creation was canceled before becoming active.
    Canceled,
}

impl PreviewPhase {
    /// Whether preview lifecycle semantics permit a phase transition.
    pub fn can_transition_to(self, target: Self) -> bool {
        self == target
            || matches!(
                (self, target),
                (
                    Self::Pending,
                    Self::Active | Self::Closing | Self::Expired | Self::Failed | Self::Canceled
                ) | (Self::Active, Self::Closing | Self::Expired | Self::Failed)
                    | (
                        Self::Closing,
                        Self::Pending | Self::Active | Self::Expired | Self::Failed
                    )
                    | (Self::Failed, Self::Pending)
            )
    }
}

/// Observed derived resource and teardown state of a preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreviewStatus {
    /// Current preview phase.
    pub phase: PreviewPhase,
    /// Time teardown may proceed after a close event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub teardown_at: Option<Timestamp>,
    /// Generic source, quota, rollout, and teardown evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A pull-request preview resource.
pub type Preview = Object<PreviewId, PreviewSpec, PreviewStatus>;

/// Node batching strategy for one cluster upgrade run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum UpgradeMode {
    /// Upgrade one node at a time while preserving workload and store availability.
    Rolling,
    /// Drain all nodes, then apply one coordinated batch.
    AllNodes,
}

/// Persisted lifecycle shared by rolling and all-node upgrade modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum UpgradePhase {
    /// Waiting for the upgrade operator.
    Pending,
    /// Selected nodes are becoming unschedulable and draining workloads.
    Draining,
    /// The upgrade source is being staged or applied.
    Applying,
    /// A node restart was requested and has not yet been observed.
    Restarting,
    /// Running version and node health are being verified.
    Verifying,
    /// Every selected node reached the target version and was restored.
    Completed,
    /// A terminal or retry-exhausted failure occurred.
    Failed,
    /// The run was canceled and owned drains were restored.
    Canceled,
}

impl UpgradePhase {
    /// Whether the unified upgrade state machine permits a phase transition.
    pub fn can_transition_to(self, target: Self) -> bool {
        self == target
            || matches!(
                (self, target),
                (Self::Pending, Self::Draining | Self::Canceled)
                    | (
                        Self::Draining,
                        Self::Applying | Self::Failed | Self::Canceled
                    )
                    | (
                        Self::Applying,
                        Self::Restarting | Self::Verifying | Self::Failed
                    )
                    | (Self::Restarting, Self::Verifying | Self::Failed)
                    | (
                        Self::Verifying,
                        Self::Draining | Self::Completed | Self::Failed
                    )
                    | (Self::Failed, Self::Pending | Self::Canceled)
            )
    }
}

/// Desired target and batching for one cluster upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeRunSpec {
    /// Minimum semantic version every selected node must reach.
    pub target_version: String,
    /// Node batching strategy.
    pub mode: UpgradeMode,
    /// Explicit node selection, or every eligible node when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_ids: Vec<NodeId>,
}

/// Per-node progress retained across leader changes and daemon restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeUpgradeStatus {
    /// Node being upgraded.
    pub node_id: NodeId,
    /// Current phase for this node.
    pub phase: UpgradePhase,
    /// Process identity observed before restart.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_instance_id: Option<NodeInstanceId>,
    /// Version last reported by the node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_version: Option<String>,
    /// Attempts consumed by retryable upgrade failures.
    pub attempts: u32,
    /// Earliest time another idempotent upgrade dispatch may be attempted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<Timestamp>,
}

/// Observed aggregate and per-node progress of an upgrade run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeRunStatus {
    /// Aggregate run phase.
    pub phase: UpgradePhase,
    /// Per-node progress in deterministic maintenance order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<NodeUpgradeStatus>,
    /// Generic leadership, retry, and restoration evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A persisted cluster upgrade run resource.
pub type UpgradeRun = Object<UpgradeRunId, UpgradeRunSpec, UpgradeRunStatus>;

/// Built-in event that may be delivered to an outbound webhook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WebhookEvent {
    /// A deployment changed lifecycle phase.
    DeploymentTransition,
    /// A node became unavailable or recovered.
    NodeAvailability,
    /// A preview changed lifecycle phase.
    PreviewTransition,
    /// An upgrade run changed lifecycle phase.
    UpgradeTransition,
}

/// Desired endpoint, event selection, and signing material for a webhook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebhookSpec {
    /// HTTPS endpoint receiving event deliveries.
    pub endpoint: String,
    /// Event classes delivered to the endpoint.
    pub events: Vec<WebhookEvent>,
    /// Secret used to sign delivery payloads.
    pub signing_secret: SecretValue,
}

/// Observed delivery health of a webhook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebhookStatus {
    /// Time of the most recent successful delivery.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<Timestamp>,
    /// Consecutive delivery failures since the last success.
    pub consecutive_failures: u32,
    /// Generic validation and delivery evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// An outbound webhook resource.
pub type Webhook = Object<WebhookId, WebhookSpec, WebhookStatus>;
