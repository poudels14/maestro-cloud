use std::collections::BTreeMap;
use std::net::IpAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactArchiveId, AssignmentId, BuildId, Condition, DeploymentId, Generation, NodeId, Object,
    ReplicaStateId, SecretValue, ServiceId, Timestamp, WorkloadId,
};

/// Runtime artifact selected for a service deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ArtifactTemplate {
    /// Pull an existing immutable or tag-addressed image.
    Image {
        /// Registry image reference.
        reference: String,
    },
    /// Build an image from a source repository or uploaded archive.
    Build {
        /// Build template flattened beside the artifact discriminator.
        #[serde(flatten)]
        template: BuildTemplate,
    },
}

/// Source and build environment applied to each generated build resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuildTemplate {
    /// Source material to build.
    pub source: BuildSource,
    /// Path to the container build definition within the source.
    pub dockerfile: String,
    /// Non-secret build variables.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    /// Secret build variables that are redacted from debug output.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, SecretValue>,
}

/// Material used as the input to an artifact build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BuildSource {
    /// A source-control repository pinned to a revision.
    Git {
        /// Repository URL.
        repository: String,
        /// Branch, tag, or commit selected for the build.
        revision: String,
    },
    /// A previously uploaded source archive.
    Tarball {
        /// Stable archive handle assigned by the upload API.
        archive_id: ArtifactArchiveId,
    },
}

/// Executable and argument vector passed without shell reinterpretation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommandSpec {
    /// Executable or container entrypoint.
    pub executable: String,
    /// Argument vector passed verbatim to the executable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<String>,
}

/// Health probe performed against a running workload address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "protocol", rename_all = "camelCase")]
pub enum HealthProbe {
    /// HTTP GET probe requiring a successful response.
    Http {
        /// Workload port to probe.
        port: u16,
        /// Absolute request path.
        path: String,
    },
    /// TCP connection probe.
    Tcp {
        /// Workload port to probe.
        port: u16,
    },
}

/// Timing and failure policy for a workload health probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheckSpec {
    /// Probe to perform.
    pub probe: HealthProbe,
    /// Interval between scheduled probes.
    pub interval_secs: u32,
    /// Consecutive failures required before a replica is unhealthy.
    pub unhealthy_threshold: u32,
}

/// Secret values rendered into one private, read-only workload file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretMountSpec {
    /// Absolute workload-visible file path.
    pub mount_path: String,
    /// Dotenv keys and plaintext values encrypted by the store boundary.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub items: BTreeMap<String, SecretValue>,
}

/// Whether API-initiated interactive execution is available to a workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ExecPolicy {
    /// Authenticated exec sessions are allowed.
    Allowed,
    /// Exec sessions are rejected.
    Denied,
}

/// Workload access to the private node API mounted at `/run/maestro`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NodeApiAccess {
    /// No node API credentials or socket are mounted.
    #[default]
    Disabled,
    /// Downward identity and authenticated OTLP ingest are available.
    IdentityAndTelemetry,
    /// Identity, telemetry, and privileged Control mutations are available.
    Privileged,
}

impl NodeApiAccess {
    /// Returns whether the workload receives a node API mount.
    pub fn is_enabled(self) -> bool {
        self != Self::Disabled
    }

    /// Returns whether privileged Control mutations are allowed.
    pub fn allows_control(self) -> bool {
        self == Self::Privileged
    }
}

/// Numeric runtime identity used for process launch and Unix peer authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadUserSpec {
    /// User identity inside the workload and on the node when user namespaces are absent.
    pub user_id: u32,
    /// Primary group identity inside the workload.
    pub group_id: u32,
}

/// Read/write policy for a mounted volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum VolumeAccess {
    /// The workload can read and write the mount.
    ReadWrite,
    /// The workload can only read the mount.
    ReadOnly,
}

/// Storage source mounted into a workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum VolumeSource {
    /// A path on one specific node, which pins scheduling to that node.
    HostPath {
        /// Absolute path on the node.
        path: String,
        /// Node that owns the path.
        node_id: NodeId,
    },
    /// A runtime-managed named volume.
    Managed {
        /// Stable runtime volume name.
        name: String,
    },
}

/// One filesystem mount in a service workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMountSpec {
    /// Storage source.
    pub source: VolumeSource,
    /// Absolute path visible inside the workload.
    pub target: String,
    /// Workload access mode.
    pub access: VolumeAccess,
}

/// Hard node identity and label constraints used by the scheduler.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlacementConstraint {
    /// Required node identity, when placement is pinned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    /// Labels every eligible node must match.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// Desired service configuration used to create immutable deployments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    /// Operator-facing service name.
    pub name: String,
    /// Operator-supplied version used in rollout history.
    pub version: String,
    /// Artifact selection or build template.
    pub artifact: ArtifactTemplate,
    /// Optional command override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandSpec>,
    /// Configured replica floor before a temporary override.
    pub replicas: u32,
    /// Runtime ports exposed to other cluster workloads.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exposed_ports: Vec<u16>,
    /// Optional workload health policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_check: Option<HealthCheckSpec>,
    /// Maximum restarts per assignment, or unlimited when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,
    /// Non-secret runtime environment.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    /// Explicit numeric process identity, required when the node API is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<WorkloadUserSpec>,
    /// Private downward API, OTLP ingest, and optional privileged Control access.
    #[serde(default)]
    pub node_api: NodeApiAccess,
    /// Secret values delivered through a private read-only file mount.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secrets: Option<SecretMountSpec>,
    /// Filesystem mounts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<VolumeMountSpec>,
    /// Hard scheduling constraints.
    #[serde(default)]
    pub placement: PlacementConstraint,
    /// Interactive exec policy.
    pub exec: ExecPolicy,
}

/// Whether new service deployments may begin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RolloutState {
    /// New deployments may proceed.
    Active,
    /// New deployments remain queued until explicitly unfrozen.
    Frozen,
}

/// Observed rollout state and active deployment of a service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    /// Deployment currently receiving service traffic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deployment_id: Option<DeploymentId>,
    /// Temporary replica count override, or the configured floor when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_override: Option<u32>,
    /// Whether new rollouts may begin.
    pub rollout: RolloutState,
    /// Generic readiness and rollout evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A deployable service resource.
pub type Service = Object<ServiceId, ServiceSpec, ServiceStatus>;

/// Returns the stable DNS-safe hostname assigned to one service replica slot.
pub fn workload_hostname(service_id: &ServiceId, replica_index: u32) -> String {
    let suffix = format!("-{replica_index}");
    let mut service = service_id.as_str().replace(['_', '.'], "-");
    service.truncate(63_usize.saturating_sub(suffix.len()));
    format!("{service}{suffix}")
}

/// Persisted phase of an immutable deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeploymentPhase {
    /// Waiting for the deployment operator.
    Queued,
    /// Preparing or resolving the deployment artifact.
    Building,
    /// Workloads started but have not passed readiness checks.
    PendingReady,
    /// Workloads are healthy and may receive traffic.
    Ready,
    /// The deployment exhausted a terminal failure path.
    Crashed,
    /// Runtime workloads have stopped.
    Terminated,
    /// Persistent deployment state is eligible for deletion.
    Removed,
    /// Traffic is leaving the deployment while requests drain.
    Draining,
    /// The queued or building deployment was canceled.
    Canceled,
}

impl DeploymentPhase {
    /// Whether the old-system lifecycle permits this phase transition.
    pub fn can_transition_to(self, target: Self) -> bool {
        match target {
            Self::PendingReady => matches!(self, Self::Building),
            Self::Ready => matches!(self, Self::Building | Self::PendingReady),
            Self::Crashed => !matches!(self, Self::Crashed | Self::Canceled | Self::Terminated),
            Self::Draining => matches!(self, Self::Ready | Self::PendingReady | Self::Building),
            Self::Terminated => !matches!(self, Self::Terminated),
            Self::Queued | Self::Building | Self::Removed | Self::Canceled => true,
        }
    }
}

/// Desired lifecycle outcome for one deployment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DeploymentGoal {
    /// Continue normal build, readiness, and serving reconciliation.
    #[default]
    Run,
    /// Cancel a deployment that has not begun serving.
    Cancel,
    /// Drain workloads and retain the terminal deployment in history.
    Remove,
}

/// Captured service snapshot and desired lifecycle for one deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentSpec {
    /// Service that owns this deployment.
    pub service_id: ServiceId,
    /// Service generation captured when the deployment was queued.
    pub service_generation: Generation,
    /// Desired workload generation, incremented to restart this deployment in place.
    #[serde(default = "initial_restart_generation")]
    #[schemars(default = "initial_restart_generation")]
    pub restart_generation: Generation,
    /// Immutable service configuration used for every replica.
    pub service: ServiceSpec,
    /// User-requested lifecycle outcome reconciled by the deployment operator.
    #[serde(default)]
    pub goal: DeploymentGoal,
    /// Build generated for this deployment, when the artifact needs building.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_id: Option<BuildId>,
}

/// Observed lifecycle, artifact, and timing of one deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentStatus {
    /// Current lifecycle phase.
    pub phase: DeploymentPhase,
    /// Time the deployment was created.
    pub created_at: Timestamp,
    /// Time the deployment first became ready.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<Timestamp>,
    /// Time draining began.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub draining_at: Option<Timestamp>,
    /// Immutable image digest selected or produced for workloads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_digest: Option<String>,
    /// Generic lifecycle and availability evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A service deployment with an immutable workload snapshot and mutable lifecycle goal.
pub type Deployment = Object<DeploymentId, DeploymentSpec, DeploymentStatus>;

/// Desired placement of one deployment replica on one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentSpec {
    /// Service being placed.
    pub service_id: ServiceId,
    /// Immutable deployment being placed.
    pub deployment_id: DeploymentId,
    /// Deployment workload generation this assignment realizes.
    #[serde(default = "initial_restart_generation")]
    #[schemars(default = "initial_restart_generation")]
    pub restart_generation: Generation,
    /// Zero-based replica slot within the deployment.
    pub replica_index: u32,
    /// Node selected by the scheduler.
    pub node_id: NodeId,
    /// Monotonic epoch incremented when a slot moves to another node.
    pub placement_epoch: u64,
    /// Cluster-routable address reserved before the workload starts.
    pub workload_address: IpAddr,
    /// Assignment superseded by this placement, when one is draining.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaces_assignment_id: Option<AssignmentId>,
}

/// Runtime lifecycle of an assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AssignmentPhase {
    /// Waiting for the node agent to create a workload.
    Pending,
    /// The assigned workload is running.
    Running,
    /// The workload is stopping without accepting new traffic.
    Draining,
    /// The workload stopped and no longer owns runtime state.
    Stopped,
    /// The node agent could not converge the assignment.
    Failed,
}

/// Observed workload identity and lifecycle for an assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentStatus {
    /// Current runtime phase.
    pub phase: AssignmentPhase,
    /// Runtime workload identity created for this assignment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_id: Option<WorkloadId>,
    /// Generic runtime and drain evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A scheduled workload assignment resource.
pub type Assignment = Object<AssignmentId, AssignmentSpec, AssignmentStatus>;

fn initial_restart_generation() -> Generation {
    Generation(1)
}

/// Desired identity of one observable deployment replica slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStateSpec {
    /// Service owning the slot.
    pub service_id: ServiceId,
    /// Deployment owning the slot.
    pub deployment_id: DeploymentId,
    /// Current assignment for the slot.
    pub assignment_id: AssignmentId,
    /// Zero-based replica slot within the deployment.
    pub replica_index: u32,
}

/// Health and restart evidence observed for one replica slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStateStatus {
    /// Deployment lifecycle phase observed for the replica.
    pub phase: DeploymentPhase,
    /// Current node when assigned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    /// Current runtime workload identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_id: Option<WorkloadId>,
    /// Consecutive failed health probes.
    pub healthcheck_failures: u32,
    /// Restart attempts consumed by this assignment.
    pub restart_attempts: u32,
    /// Attempt durably reserved before a runtime restart and cleared after it is observed running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_pending_attempt: Option<u32>,
    /// Earliest UTC time at which the pending restart may be attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_not_before: Option<Timestamp>,
    /// Generic health and exhaustion evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// An observed deployment replica resource.
pub type ReplicaState = Object<ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus>;

/// Persisted phase of an artifact build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum BuildPhase {
    /// Waiting for a build worker.
    Queued,
    /// Fetching and preparing source material.
    Preparing,
    /// Running the configured build backend.
    Building,
    /// The immutable artifact is available.
    Succeeded,
    /// The build ended with a terminal error.
    Failed,
    /// The build was canceled before completion.
    Canceled,
}

impl BuildPhase {
    /// Whether the build state machine permits a transition.
    pub fn can_transition_to(self, target: Self) -> bool {
        self == target
            || matches!(
                (self, target),
                (Self::Queued, Self::Preparing | Self::Canceled)
                    | (
                        Self::Preparing,
                        Self::Building | Self::Failed | Self::Canceled
                    )
                    | (
                        Self::Building,
                        Self::Succeeded | Self::Failed | Self::Canceled
                    )
            )
    }
}

/// Desired build source and service association.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuildSpec {
    /// Service requesting the build.
    pub service_id: ServiceId,
    /// Deployment that will consume the build.
    pub deployment_id: DeploymentId,
    /// Immutable build template copied from the service.
    pub template: BuildTemplate,
}

/// Observed build phase and immutable artifact identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuildStatus {
    /// Current build phase.
    pub phase: BuildPhase,
    /// Immutable image digest produced by a successful build.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_digest: Option<String>,
    /// Source revision resolved by the build backend.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    /// Generic progress and failure evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// An artifact build resource.
pub type Build = Object<BuildId, BuildSpec, BuildStatus>;
