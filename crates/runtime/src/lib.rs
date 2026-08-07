//! Backend-neutral workload, artifact, and network runtime contracts.
//!
//! This crate translates desired workload state into explicit runtime operations. It may depend
//! on kernel API types, but never on operators, cluster formation, node agents, observability, or
//! application composition roots.

mod artifact;
mod artifact_output;
#[cfg(feature = "build-context")]
pub mod build_context;
mod capabilities;
#[cfg(target_os = "linux")]
mod cgroup;
mod clock;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_artifact;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_artifact_stream;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_artifact_support;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_build;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_config;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_event;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_exec;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_exec_io;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_identity;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_image;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_io;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_network;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_network_linux;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_resolver;
mod containerd_settings;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_support;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_task;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_user_namespace;
#[cfg(all(feature = "containerd", target_os = "linux"))]
mod containerd_volume;
#[cfg(all(feature = "docker", unix))]
mod docker;
#[cfg(all(feature = "docker", unix))]
mod docker_artifact;
#[cfg(all(feature = "docker", unix))]
mod docker_artifact_context;
#[cfg(all(feature = "docker", unix))]
mod docker_artifact_support;
#[cfg(all(feature = "docker", unix))]
mod docker_config;
#[cfg(all(feature = "docker", unix))]
mod docker_network;
#[cfg(all(feature = "docker", unix))]
mod docker_stats;
#[cfg(all(feature = "docker", unix))]
mod docker_stream;
#[cfg(all(feature = "docker", unix))]
mod docker_support;
mod error;
mod execution;
#[cfg(any(test, feature = "test-util"))]
mod fake;
#[cfg(any(test, feature = "test-util"))]
mod fake_network;
#[cfg(any(test, feature = "test-util"))]
mod fake_state;
#[cfg(any(test, feature = "test-util"))]
mod fake_stream;
#[cfg(target_os = "linux")]
mod file_log;
#[cfg(any(
    test,
    all(feature = "containerd", target_os = "linux"),
    all(feature = "docker", unix)
))]
mod managed_volume;
mod network;
#[cfg(target_os = "linux")]
mod process;
#[cfg(target_os = "linux")]
mod process_manifest;
#[cfg(target_os = "linux")]
mod process_settings;
#[cfg(target_os = "linux")]
mod process_stream;
#[cfg(target_os = "linux")]
mod process_support;
mod stats;
mod value_source;
mod workload;

pub use artifact::{
    ArtifactBuildRequest, ArtifactByteStream, ArtifactDigest, ArtifactPrunePolicy,
    ArtifactPruneReport, ArtifactReference, ArtifactSource, ArtifactStore, ArtifactStoreError,
    RegistryCredential,
};
pub use artifact_output::{
    ArtifactBuildOutputSink, ArtifactBuildOutputStream, DiscardArtifactBuildOutput,
    forward_artifact_build_output,
};
pub use capabilities::{Capabilities, RuntimeCapability};
pub use clock::{MonotonicTime, RuntimeClock, TokioRuntimeClock};
#[cfg(all(feature = "containerd", target_os = "linux"))]
pub use containerd::ContainerdRuntime;
pub use containerd_settings::ContainerdRuntimeSettings;
#[cfg(all(feature = "docker", unix))]
pub use docker::DockerRuntime;
pub use error::{CgroupPathError, RuntimeError};
pub use execution::{
    ExecInput, ExecMode, ExecOutput, ExecRequest, ExecSession, ExecSessionKiller, LogCursor,
    LogFrame, LogMode, LogRequest, LogSource, LogStream,
};
#[cfg(any(test, feature = "test-util"))]
pub use fake::{FakeRuntime, FakeRuntimeCall, FakeRuntimeOperation};
#[cfg(any(test, feature = "test-util"))]
pub use fake_network::FakeNetworkProvider;
pub use network::{
    AddressLease, AddressRequest, AddressReservation, NetworkAddressing, NetworkAttachment,
    NetworkCidr, NetworkHandle, NetworkProvider, NetworkProviderError, NetworkSpec,
    WorkloadNetworkStatus,
};
#[cfg(target_os = "linux")]
pub use process::ProcessRuntime;
#[cfg(target_os = "linux")]
pub use process_settings::ProcessRuntimeSettings;
pub use stats::{
    CgroupPath, WorkloadCpuStats, WorkloadIoStats, WorkloadMemoryEvents, WorkloadMemoryStats,
    WorkloadNetworkStats, WorkloadProcessStats, WorkloadResourceStats, WorkloadStatsReading,
    WorkloadStatsSnapshot,
};
pub use value_source::{ValueSourceError, ValueSourceResolver};
pub use workload::{
    ContainerWorkload, EventCursor, EventRequest, HEALTHCHECK_PATH_LABEL, HostPortPublication,
    MountAccess, MountSource, ObservedWorkload, PortProtocol, ProcessWorkload, RuntimeEvent,
    RuntimeEventKind, RuntimeEventStream, ShutdownRequest, VmWorkload, WorkloadCapability,
    WorkloadConfiguration, WorkloadHandle, WorkloadIdMapping, WorkloadMetadata, WorkloadMount,
    WorkloadRuntime, WorkloadSpec, WorkloadState, WorkloadStatus, WorkloadUser,
    WorkloadUserNamespace,
};

/// Reusable backend-neutral conformance batteries.
#[cfg(any(test, feature = "test-util"))]
pub mod conformance;

#[cfg(test)]
mod tests;
