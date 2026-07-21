use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_store::Clock;
use tokio::sync::watch;

/// Stable Linux interface owned by Maestro's workload network.
pub const WORKLOAD_BRIDGE_NAME: &str = "maestro0";

/// Complete desired state for the node-local workload bridge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadBridge {
    /// Exact interface name shared with firewall policy compilation.
    pub name: String,
    /// Node-local gateway used by workloads and the authoritative DNS listener.
    pub gateway: Ipv4Addr,
    /// Prefix length of the node's workload subnet.
    pub prefix_length: u8,
    /// Link MTU kept equal to the WireGuard mesh MTU.
    pub mtu_bytes: u16,
}

impl WorkloadBridge {
    /// Validates one immutable node-local bridge allocation.
    pub fn new(
        gateway: Ipv4Addr,
        prefix_length: u8,
        mtu_bytes: u16,
    ) -> Result<Self, WorkloadBridgeError> {
        if prefix_length == 0 || prefix_length > 30 {
            return Err(WorkloadBridgeError::InvalidPrefixLength { prefix_length });
        }
        if gateway.is_unspecified()
            || gateway.is_loopback()
            || gateway.is_multicast()
            || gateway == Ipv4Addr::BROADCAST
        {
            return Err(WorkloadBridgeError::InvalidGateway { gateway });
        }
        if mtu_bytes == 0 {
            return Err(WorkloadBridgeError::ZeroMtu);
        }
        Ok(Self {
            name: WORKLOAD_BRIDGE_NAME.to_string(),
            gateway,
            prefix_length,
            mtu_bytes,
        })
    }
}

/// Idempotent host boundary for one Maestro-owned workload bridge.
#[async_trait]
pub trait WorkloadBridgeBackend: Send + Sync {
    /// Creates or repairs the complete bridge, address, MTU, and link state.
    async fn apply(&self, desired: &WorkloadBridge) -> Result<(), WorkloadBridgeBackendError>;
}

/// Periodically repairs the node-local workload bridge until shutdown.
pub struct WorkloadBridgeAgent<Backend> {
    desired: WorkloadBridge,
    backend: Backend,
    clock: Arc<dyn Clock>,
    resync_interval: Duration,
}

impl<Backend> WorkloadBridgeAgent<Backend>
where
    Backend: WorkloadBridgeBackend,
{
    /// Binds an exact desired bridge to a host backend and injected clock.
    pub fn new(
        desired: WorkloadBridge,
        backend: Backend,
        clock: Arc<dyn Clock>,
        resync_interval: Duration,
    ) -> Result<Self, WorkloadBridgeError> {
        if resync_interval.is_zero() {
            return Err(WorkloadBridgeError::ZeroResyncInterval);
        }
        Ok(Self {
            desired,
            backend,
            clock,
            resync_interval,
        })
    }

    /// Returns the desired bridge for DNS and firewall composition.
    pub fn desired(&self) -> &WorkloadBridge {
        &self.desired
    }

    /// Repairs the complete bridge once.
    pub async fn reconcile_once(&self) -> Result<(), WorkloadBridgeError> {
        self.backend.apply(&self.desired).await.map_err(Into::into)
    }

    /// Runs level-triggered repair on every bounded resync interval.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), WorkloadBridgeError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.reconcile_once().await?;
            let next = self.clock.now().saturating_add(self.resync_interval);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = self.clock.sleep_until(next) => {}
            }
        }
    }
}

/// Matchable Linux bridge application failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("workload bridge backend failed: {message}")]
pub struct WorkloadBridgeBackendError {
    message: String,
}

impl WorkloadBridgeBackendError {
    /// Creates an adapter-neutral backend failure.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Returns operator-facing backend detail.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Invalid desired state or failure to repair the local workload bridge.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadBridgeError {
    /// Workload networks require space for network, gateway, and workload addresses.
    #[error("workload bridge prefix length {prefix_length} must be between 1 and 30")]
    InvalidPrefixLength { prefix_length: u8 },
    /// The bridge gateway must be a concrete unicast address.
    #[error("workload bridge gateway `{gateway}` is not a usable unicast address")]
    InvalidGateway { gateway: Ipv4Addr },
    /// A zero MTU cannot carry workload traffic.
    #[error("workload bridge MTU must be greater than zero")]
    ZeroMtu,
    /// A zero interval would create an unbounded hot repair loop.
    #[error("workload bridge resync interval must be greater than zero")]
    ZeroResyncInterval,
    /// The host backend could not converge the bridge.
    #[error(transparent)]
    Backend(#[from] WorkloadBridgeBackendError),
}
