use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// One optional behavior a runtime backend can perform faithfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RuntimeCapability {
    /// Run an additional command inside an existing workload.
    Exec,
    /// Allocate a terminal and resize it during an exec session.
    InteractiveExec,
    /// Attach a workload to a network after creation.
    DynamicNetwork,
    /// Pause and resume an existing workload without stopping it.
    Pause,
    /// Build an immutable artifact from source.
    BuildArtifact,
    /// Push an immutable artifact to a remote registry.
    PushArtifact,
    /// Export and import artifact bytes without buffering the complete archive.
    TransferArtifact,
    /// Run a virtual-machine workload.
    VirtualMachine,
}

/// Immutable set of optional operations supported by one backend instance.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Capabilities(BTreeSet<RuntimeCapability>);

impl Capabilities {
    /// Constructs a deduplicated capability set.
    pub fn new(capabilities: impl IntoIterator<Item = RuntimeCapability>) -> Self {
        Self(capabilities.into_iter().collect())
    }

    /// Returns an empty capability set.
    pub fn none() -> Self {
        Self::default()
    }

    /// Returns whether this backend supports the requested operation.
    pub fn supports(&self, capability: RuntimeCapability) -> bool {
        self.0.contains(&capability)
    }

    /// Iterates supported capabilities in stable order.
    pub fn iter(&self) -> impl Iterator<Item = RuntimeCapability> + '_ {
        self.0.iter().copied()
    }
}
