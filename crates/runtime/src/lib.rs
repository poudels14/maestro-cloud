//! Backend-neutral workload, artifact, and network runtime contracts.
//!
//! This crate translates desired workload state into explicit runtime operations. It may depend
//! on kernel API types, but never on operators, cluster formation, node agents, observability, or
//! application composition roots.

mod artifact;
mod capabilities;
mod error;
mod execution;
#[cfg(any(test, feature = "test-util"))]
mod fake;
#[cfg(any(test, feature = "test-util"))]
mod fake_state;
#[cfg(any(test, feature = "test-util"))]
mod fake_stream;
mod network;
mod workload;

pub use artifact::{
    ArtifactBuildRequest, ArtifactByteStream, ArtifactDigest, ArtifactPrunePolicy,
    ArtifactPruneReport, ArtifactReference, ArtifactSource, ArtifactStore, ArtifactStoreError,
};
pub use capabilities::{Capabilities, RuntimeCapability};
pub use error::{CgroupPathError, RuntimeError};
pub use execution::{
    ExecInput, ExecMode, ExecOutput, ExecRequest, ExecSession, LogCursor, LogFrame, LogMode,
    LogRequest, LogSource, LogStream,
};
#[cfg(any(test, feature = "test-util"))]
pub use fake::{FakeRuntime, FakeRuntimeCall, FakeRuntimeOperation};
pub use network::{
    AddressLease, AddressRequest, NetworkAttachment, NetworkCidr, NetworkHandle, NetworkProvider,
    NetworkProviderError, NetworkSpec, WorkloadNetworkStatus,
};
pub use workload::{
    CgroupPath, ContainerWorkload, EventCursor, EventRequest, MountAccess, MountSource,
    ObservedWorkload, ProcessWorkload, RuntimeEvent, RuntimeEventKind, RuntimeEventStream,
    ShutdownRequest, VmWorkload, WorkloadConfiguration, WorkloadHandle, WorkloadMetadata,
    WorkloadMount, WorkloadRuntime, WorkloadSpec, WorkloadState, WorkloadStatus, WorkloadUser,
};

/// Reusable backend-neutral conformance batteries.
#[cfg(any(test, feature = "test-util"))]
pub mod conformance;

#[cfg(test)]
mod tests;
