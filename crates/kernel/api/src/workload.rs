use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactArchiveId, BuildId, Condition, DeploymentId, Generation, NodeId, Object, SecretValue,
    ServiceId, Timestamp,
};

mod build;
mod placement;

pub use build::{Build, BuildPhase, BuildSpec, BuildStatus};
pub use placement::{
    Assignment, AssignmentPhase, AssignmentSpec, AssignmentStatus, PlacementHistory,
    PlacementHistorySpec, PlacementHistoryStatus, ReplicaState, ReplicaStateSpec,
    ReplicaStateStatus, assignment_workload_address,
};

/// Service annotation containing the immutable Git revision desired by build-watch.
pub const BUILD_WATCH_REVISION_ANNOTATION: &str = "build.maestro.dev/revision";

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
    /// Poll the configured Git ref and roll out newly resolved commits.
    #[serde(default, skip_serializing_if = "is_false")]
    pub watch: bool,
    /// Optional registry prefix receiving a deployment-unique immutable build.
    ///
    /// Absence keeps the artifact registry-free and enables Maestro peer
    /// replication between workload nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry: Option<String>,
    /// Non-secret build variables.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    /// Secret build variables that are redacted from debug output.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, SecretValue>,
}

fn is_false(value: &bool) -> bool {
    !value
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

/// Secret values rendered into one private, read-only workload mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "format",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum SecretMountSpec {
    /// One dotenv-compatible file containing environment-style keys.
    Dotenv {
        /// Absolute workload-visible file path.
        mount_path: String,
        /// Dotenv keys and plaintext values encrypted by the store boundary.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        items: BTreeMap<String, SecretValue>,
    },
    /// One directory containing named files with exact secret bytes.
    Files {
        /// Absolute workload-visible directory path.
        mount_path: String,
        /// Single-component file names and plaintext contents encrypted by the store boundary.
        files: BTreeMap<String, SecretValue>,
    },
}

impl SecretMountSpec {
    /// Returns the absolute path receiving the private mount.
    pub fn mount_path(&self) -> &str {
        match self {
            Self::Dotenv { mount_path, .. } | Self::Files { mount_path, .. } => mount_path,
        }
    }

    /// Returns the secret-bearing values independent of their on-disk representation.
    pub fn values(&self) -> &BTreeMap<String, SecretValue> {
        match self {
            Self::Dotenv { items, .. } => items,
            Self::Files { files, .. } => files,
        }
    }

    /// Returns mutable secret-bearing values for response redaction.
    pub fn values_mut(&mut self) -> &mut BTreeMap<String, SecretValue> {
        match self {
            Self::Dotenv { items, .. } => items,
            Self::Files { files, .. } => files,
        }
    }
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
    /// A runtime-managed volume isolated to one active rollout replica.
    ReplicaManaged {
        /// Stable volume name within the rollout replica.
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

/// Declarative pull-request preview policy for a base service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreviewPolicy {
    /// Grace period retained after a pull request closes.
    pub close_grace_period_secs: u64,
    /// Maximum lifetime measured from pull-request creation.
    pub lifetime_secs: u64,
    /// Replica count assigned to each derived preview service.
    pub replicas: u32,
    /// Runtime environment overlaid on the derived service.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
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
    /// Pull-request preview policy; absence disables preview discovery.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<PreviewPolicy>,
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
    /// Service generation whose next deployment may bypass a rollout freeze once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout_bypass_generation: Option<Generation>,
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
    /// Whether this immutable deployment may begin while its service is frozen.
    #[serde(default)]
    #[schemars(default)]
    pub bypass_rollout_freeze: bool,
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

fn initial_restart_generation() -> Generation {
    Generation(1)
}
