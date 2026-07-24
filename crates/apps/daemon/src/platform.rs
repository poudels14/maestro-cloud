use async_trait::async_trait;
use kernel_api::NodeFirewallSpec;
use node_agent::{
    FirewallBackend, FirewallBackendError, MeshBackend, MeshBackendError, MeshConfiguration,
    WorkloadBridge, WorkloadBridgeBackend, WorkloadBridgeBackendError, WorkloadNetworkStats,
    WorkloadNetworkStatsError, WorkloadNetworkStatsReader,
};
use runtime::WorkloadHandle;

/// Type marker for host-network capabilities intentionally absent on macOS.
pub(crate) struct AbsentHostNetworkBackend;

#[async_trait]
impl MeshBackend for AbsentHostNetworkBackend {
    async fn apply(&self, _desired: &MeshConfiguration) -> Result<(), MeshBackendError> {
        Err(MeshBackendError::new(
            "mesh networking is unavailable in the macOS development profile",
        ))
    }
}

#[async_trait]
impl FirewallBackend for AbsentHostNetworkBackend {
    async fn apply(&self, _desired: &NodeFirewallSpec) -> Result<(), FirewallBackendError> {
        Err(FirewallBackendError::new(
            "host firewall management is unavailable in the macOS development profile",
        ))
    }
}

#[async_trait]
impl WorkloadBridgeBackend for AbsentHostNetworkBackend {
    async fn apply(&self, _desired: &WorkloadBridge) -> Result<(), WorkloadBridgeBackendError> {
        Err(WorkloadBridgeBackendError::new(
            "host bridge management is unavailable in the macOS development profile",
        ))
    }
}

/// Network counters are already included in Docker's runtime-native stats snapshot.
pub(crate) struct RuntimeDelegatedNetworkStatsReader;

#[async_trait]
impl WorkloadNetworkStatsReader for RuntimeDelegatedNetworkStatsReader {
    async fn read(
        &self,
        _workload: &WorkloadHandle,
    ) -> Result<Option<WorkloadNetworkStats>, WorkloadNetworkStatsError> {
        Ok(None)
    }
}
