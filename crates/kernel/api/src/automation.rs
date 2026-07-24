use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    Condition, DeploymentPhase, Generation, NodeId, NodeInstanceId, Object, PreviewId,
    ResourceName, ResourceRevision, SecretValue, ServiceId, Timestamp, UpgradeRunId, WebhookId,
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

/// Host mutation performed by the shared cluster-maintenance state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum UpgradeOperation {
    /// Stage a new boot generation before restarting each selected node.
    Upgrade,
    /// Preserve the installed generation and only restart each selected node.
    Restart,
}

/// Target-version sentinel carried by restart runs that do not stage an upgrade.
pub const RESTART_TARGET_VERSION: &str = "0.0.0";

/// Node batching strategy for one cluster maintenance run.
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

/// Desired operation, target, and batching for one cluster maintenance run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeRunSpec {
    /// Whether nodes stage an upgrade or only restart their installed generation.
    pub operation: UpgradeOperation,
    /// Minimum semantic version for upgrades; restart runs carry `0.0.0`.
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

/// Wire representation used by an outbound webhook endpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WebhookFormat {
    /// Signed Maestro transition document with deterministic delivery identity.
    #[default]
    Maestro,
    /// Slack incoming-webhook document containing only the human-readable text.
    Slack,
}

/// Notification severity selected independently from event classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WebhookCategory {
    /// Normal lifecycle progress and recovery.
    Info,
    /// Failed deployments, unavailable nodes, and failed automation.
    Error,
}

fn default_webhook_categories() -> Vec<WebhookCategory> {
    vec![WebhookCategory::Info, WebhookCategory::Error]
}

const fn webhook_enabled() -> bool {
    true
}

/// Desired endpoint, event selection, and signing material for a webhook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebhookSpec {
    /// Operator-facing display name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// HTTPS endpoint receiving event deliveries, potentially including credentials.
    pub endpoint: SecretValue,
    /// Event classes delivered to the endpoint.
    pub events: Vec<WebhookEvent>,
    /// Notification severities delivered within the selected event classes.
    #[serde(default = "default_webhook_categories")]
    #[schemars(default = "default_webhook_categories")]
    pub categories: Vec<WebhookCategory>,
    /// Whether transitions are delivered or only baselined.
    #[serde(default = "webhook_enabled")]
    #[schemars(default = "webhook_enabled")]
    pub enabled: bool,
    /// Endpoint-specific wire representation.
    #[serde(default)]
    pub format: WebhookFormat,
    /// Secret used to sign native Maestro payloads; Slack does not use it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signing_secret: Option<SecretValue>,
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
    /// Earliest wall-clock time a failed transition may be retried.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<Timestamp>,
    /// Webhook generation whose subscribed resources were last baselined.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<Generation>,
    /// Last successfully acknowledged state for each subscribed resource.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observations: Vec<WebhookObservation>,
    /// Generic validation and delivery evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Availability state derived from a node's session-bound liveness key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WebhookNodeAvailability {
    /// The node owns an active liveness session.
    Available,
    /// The node's liveness session is absent or expired.
    Unavailable,
}

/// Typed state carried by one webhook transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "event", content = "state", rename_all = "camelCase")]
pub enum WebhookObservedState {
    /// Deployment lifecycle phase.
    DeploymentTransition(DeploymentPhase),
    /// Node session availability.
    NodeAvailability(WebhookNodeAvailability),
    /// Pull-request preview lifecycle phase.
    PreviewTransition(PreviewPhase),
    /// Cluster upgrade lifecycle phase.
    UpgradeTransition(UpgradePhase),
}

impl WebhookObservedState {
    /// Returns the subscription class represented by this state.
    pub const fn event(self) -> WebhookEvent {
        match self {
            Self::DeploymentTransition(_) => WebhookEvent::DeploymentTransition,
            Self::NodeAvailability(_) => WebhookEvent::NodeAvailability,
            Self::PreviewTransition(_) => WebhookEvent::PreviewTransition,
            Self::UpgradeTransition(_) => WebhookEvent::UpgradeTransition,
        }
    }
}

/// Last state durably acknowledged for one subscribed resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebhookObservation {
    /// Built-in resource identity within its event class.
    pub resource_id: ResourceName,
    /// Exact typed state acknowledged by the endpoint.
    pub state: WebhookObservedState,
    /// Source store revision used to derive the delivery identity.
    pub resource_revision: ResourceRevision,
}

/// An outbound webhook resource.
pub type Webhook = Object<WebhookId, WebhookSpec, WebhookStatus>;
