use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    AssignmentId, ClusterId, CommandSpec, DeploymentId, NodeId, ServiceId, WorkloadId,
};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactReference, Capabilities, ExecRequest, ExecSession, LogRequest, LogStream, RuntimeError,
    WorkloadStatsReading,
};

/// Workload metadata label carrying the configured HTTP healthcheck path.
pub const HEALTHCHECK_PATH_LABEL: &str = "maestro.healthcheck-path";

/// Ownership labels persisted on backend objects so a restarted agent can adopt them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadMetadata {
    /// Cluster that owns the backend object.
    pub cluster_id: ClusterId,
    /// Node agent responsible for reconciliation.
    pub node_id: NodeId,
    /// Service whose desired state owns this workload.
    pub service_id: ServiceId,
    /// Immutable deployment whose configuration created this workload.
    pub deployment_id: DeploymentId,
    /// Assignment that requested the workload.
    pub assignment_id: AssignmentId,
    /// Stable identity of this runtime instance.
    pub workload_id: WorkloadId,
    /// Operator-supplied labels copied into downward identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// Runtime user configured for one workload process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadUser {
    /// Host or namespace user identity.
    pub user_id: u32,
    /// Host or namespace group identity.
    pub group_id: u32,
}

/// Host-side source mounted into a workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum MountSource {
    /// Existing absolute node path bind-mounted by the backend.
    HostPath(PathBuf),
    /// Runtime-managed persistent volume name.
    ManagedVolume(String),
}

/// Workload access to one mount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MountAccess {
    /// Workload can read but cannot mutate the mount.
    ReadOnly,
    /// Workload can read and mutate the mount.
    ReadWrite,
}

/// One backend mount with explicit source, target, and access semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadMount {
    /// Host or runtime-managed mount source.
    pub source: MountSource,
    /// Absolute workload-visible target path.
    pub target: PathBuf,
    /// Workload access mode.
    pub access: MountAccess,
}

/// Fields common to every workload kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadConfiguration {
    /// Durable ownership metadata used for adoption and garbage collection.
    pub metadata: WorkloadMetadata,
    /// Workload-visible hostname.
    pub hostname: String,
    /// Non-secret environment passed to the primary process.
    pub environment: BTreeMap<String, String>,
    /// Filesystem mounts applied before the workload starts.
    pub mounts: Vec<WorkloadMount>,
    /// Cluster-routable address allocated by host-owned IPAM.
    pub workload_address: Option<IpAddr>,
    /// Runtime-visible DNS server, normally the node workload-bridge gateway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dns_server: Option<IpAddr>,
    /// Runtime user, or the backend's isolated default when absent.
    pub user: Option<WorkloadUser>,
}

/// Container-specific immutable workload fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerWorkload {
    /// Common workload configuration.
    pub configuration: WorkloadConfiguration,
    /// Immutable image reference or digest.
    pub image: ArtifactReference,
    /// Entrypoint override, or the image default when absent.
    pub command: Option<CommandSpec>,
}

/// Host-process-specific immutable workload fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessWorkload {
    /// Common workload configuration.
    pub configuration: WorkloadConfiguration,
    /// Executable and argument vector passed without shell reinterpretation.
    pub command: CommandSpec,
}

/// Reserved virtual-machine workload fields kept out of container-shaped APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VmWorkload {
    /// Common workload configuration.
    pub configuration: WorkloadConfiguration,
    /// Bootable image path understood by a future VM backend.
    pub image: PathBuf,
    /// Guest memory allocation in mebibytes.
    pub memory_mib: u32,
    /// Virtual CPU allocation.
    pub virtual_cpu_count: u16,
}

/// Typed desired workload without fake image fields on process workloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "spec", rename_all = "camelCase")]
pub enum WorkloadSpec {
    /// OCI container workload.
    Container(ContainerWorkload),
    /// Detached host process managed through the process backend.
    Process(ProcessWorkload),
    /// Reserved virtual-machine workload.
    Vm(VmWorkload),
}

impl WorkloadSpec {
    /// Returns common configuration independent of workload kind.
    pub fn configuration(&self) -> &WorkloadConfiguration {
        match self {
            Self::Container(workload) => &workload.configuration,
            Self::Process(workload) => &workload.configuration,
            Self::Vm(workload) => &workload.configuration,
        }
    }
}

/// Stable Maestro identity plus backend-native object identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadHandle {
    workload_id: WorkloadId,
    backend_id: String,
}

impl WorkloadHandle {
    /// Constructs a backend handle, rejecting an empty native identity.
    pub fn new(
        workload_id: WorkloadId,
        backend_id: impl Into<String>,
    ) -> Result<Self, RuntimeError> {
        let backend_id = backend_id.into();
        if backend_id.is_empty() {
            Err(RuntimeError::InvalidSpec {
                message: "backend workload identity cannot be empty".to_owned(),
            })
        } else {
            Ok(Self {
                workload_id,
                backend_id,
            })
        }
    }

    /// Returns the stable Maestro workload identity.
    pub fn workload_id(&self) -> &WorkloadId {
        &self.workload_id
    }

    /// Returns the backend-native identity used only by the selected adapter.
    pub fn backend_id(&self) -> &str {
        &self.backend_id
    }
}

/// Runtime lifecycle observed for one backend object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkloadState {
    /// Object exists but its primary process has not started.
    Created,
    /// Primary workload process is running.
    Running,
    /// Workload is paused without being stopped.
    Paused,
    /// Primary workload process exited or was stopped.
    Stopped,
    /// Backend reports an unrecoverable workload failure.
    Failed,
}

/// Current backend state of one workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadStatus {
    /// Runtime lifecycle state.
    pub state: WorkloadState,
    /// Last primary-process exit code when the backend reports one.
    pub exit_code: Option<i32>,
    /// Backend detail safe to surface in assignment status.
    pub detail: Option<String>,
}

/// Workload discovered by durable Maestro ownership metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedWorkload {
    /// Runtime handle for lifecycle operations.
    pub handle: WorkloadHandle,
    /// Durable ownership labels read from the backend object.
    pub metadata: WorkloadMetadata,
    /// Current lifecycle state captured with the listing.
    pub status: WorkloadStatus,
}

/// Opaque backend cursor used to resume lifecycle events after reconnecting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCursor(String);

impl EventCursor {
    /// Wraps an opaque backend cursor without interpreting its ordering format.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the opaque backend cursor.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Event subscription scoped to one cluster node's managed workloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRequest {
    /// Cluster owning the backend objects.
    pub cluster_id: ClusterId,
    /// Node agent consuming events.
    pub node_id: NodeId,
    /// Last event durably observed, when the backend supports cursor resume.
    pub after: Option<EventCursor>,
}

/// Backend lifecycle transition kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeEventKind {
    /// Backend object was created.
    Created,
    /// Primary process started.
    Started,
    /// Primary process exited.
    Exited,
    /// Backend object was removed.
    Removed,
}

/// One at-least-once runtime lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEvent {
    /// Backend cursor to commit after the reconciler persists the resulting state.
    pub cursor: EventCursor,
    /// Workload affected by this event.
    pub workload_id: WorkloadId,
    /// Lifecycle transition kind.
    pub kind: RuntimeEventKind,
    /// Exit code attached to an exit event when available.
    pub exit_code: Option<i32>,
}

/// Pull-based, reconnectable lifecycle event stream.
#[async_trait]
pub trait RuntimeEventStream: Send {
    /// Waits for the next at-least-once event. `None` means the backend ended cleanly; callers must
    /// still re-list on every reconnect because no backend guarantees gap-free delivery.
    async fn next(&mut self) -> Result<Option<RuntimeEvent>, RuntimeError>;
}

/// Graceful stop deadline for one workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownRequest {
    /// Maximum time the backend may wait before the caller chooses an explicit kill.
    pub timeout: Duration,
}

/// Backend-neutral workload lifecycle and streaming API.
#[async_trait]
pub trait WorkloadRuntime: Send + Sync {
    /// Returns immutable optional behavior supported by this backend instance.
    fn capabilities(&self) -> Capabilities;

    /// Creates backend state without starting the primary process. Repeating the same stable
    /// workload identity and equivalent specification must return the existing handle; conflicting
    /// specifications return `RuntimeError::Conflict`.
    async fn create(&self, spec: &WorkloadSpec) -> Result<WorkloadHandle, RuntimeError>;

    /// Starts a created or stopped workload idempotently.
    async fn start(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError>;

    /// Requests graceful shutdown and returns once the primary process has exited or the deadline
    /// elapses. Canceling the future does not imply that the backend canceled its stop request.
    async fn stop(
        &self,
        handle: &WorkloadHandle,
        request: ShutdownRequest,
    ) -> Result<(), RuntimeError>;

    /// Forces the primary process to exit idempotently.
    async fn kill(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError>;

    /// Removes stopped backend state idempotently.
    async fn remove(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError>;

    /// Reads current backend state without relying on event delivery.
    async fn status(&self, handle: &WorkloadHandle) -> Result<WorkloadStatus, RuntimeError>;

    /// Lists every backend object labeled for this cluster and node so an agent can adopt or
    /// garbage-collect it after restart.
    async fn list(
        &self,
        cluster_id: &ClusterId,
        node_id: &NodeId,
    ) -> Result<Vec<ObservedWorkload>, RuntimeError>;

    /// Opens an at-least-once lifecycle stream. Consumers must re-list after opening and on every
    /// disconnect because cursor support cannot prove gap-free delivery across backend restarts.
    async fn events(
        &self,
        request: EventRequest,
    ) -> Result<Box<dyn RuntimeEventStream>, RuntimeError>;

    /// Opens runtime-native stdout/stderr delivery without spawning a CLI process.
    async fn logs(
        &self,
        handle: &WorkloadHandle,
        request: LogRequest,
    ) -> Result<Box<dyn LogStream>, RuntimeError>;

    /// Starts an exec session when `RuntimeCapability::Exec` is advertised.
    async fn exec(
        &self,
        handle: &WorkloadHandle,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, RuntimeError>;

    /// Returns either an exact cgroup-v2 path for direct host collection or a runtime-native
    /// snapshot when the daemon owns the workload's kernel isolation.
    async fn stats(&self, handle: &WorkloadHandle) -> Result<WorkloadStatsReading, RuntimeError>;
}
