use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use async_trait::async_trait;
use kernel_api::WorkloadId;

use crate::WorkloadHandle;

/// Canonical IPv4 or IPv6 network range owned by a runtime network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkCidr {
    network_address: IpAddr,
    prefix_length: u8,
}

impl NetworkCidr {
    /// Constructs a canonical network range with all host bits cleared.
    pub fn new(network_address: IpAddr, prefix_length: u8) -> Result<Self, NetworkProviderError> {
        let valid_prefix = match network_address {
            IpAddr::V4(_) => prefix_length <= 32,
            IpAddr::V6(_) => prefix_length <= 128,
        };
        if !valid_prefix {
            return Err(NetworkProviderError::InvalidRange {
                message: format!(
                    "prefix length {prefix_length} is invalid for address `{network_address}`"
                ),
            });
        }
        if canonical_address(network_address, prefix_length) != network_address {
            Err(NetworkProviderError::InvalidRange {
                message: format!("address `{network_address}` has host bits set"),
            })
        } else {
            Ok(Self {
                network_address,
                prefix_length,
            })
        }
    }

    /// Returns the first address in the canonical network.
    pub fn network_address(self) -> IpAddr {
        self.network_address
    }

    /// Returns the CIDR prefix length in bits.
    pub fn prefix_length(self) -> u8 {
        self.prefix_length
    }

    /// Returns whether an address belongs to this network range.
    pub fn contains(self, address: IpAddr) -> bool {
        canonical_address(address, self.prefix_length) == self.network_address
    }
}

/// Desired runtime network configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkSpec {
    /// Stable backend network name.
    pub name: String,
    /// Workload address range.
    pub range: NetworkCidr,
    /// Host-side gateway inside the range.
    pub gateway: IpAddr,
}

/// Stable handle returned after ensuring a runtime network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkHandle {
    name: String,
}

impl NetworkHandle {
    /// Constructs a non-empty backend network handle.
    pub fn new(name: impl Into<String>) -> Result<Self, NetworkProviderError> {
        let name = name.into();
        if name.is_empty() {
            Err(NetworkProviderError::InvalidRange {
                message: "network name cannot be empty".to_owned(),
            })
        } else {
            Ok(Self { name })
        }
    }

    /// Returns the stable backend network name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Host-IPAM address selection for one workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressRequest {
    /// Allocate any currently free address from the network.
    Any,
    /// Reserve this exact preselected assignment address.
    Exact(IpAddr),
}

/// Address reservation owned by one workload identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressLease {
    /// Workload that owns the reservation.
    pub workload_id: WorkloadId,
    /// Allocated cluster-routable address.
    pub address: IpAddr,
}

/// Current network attachment returned by backend inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkAttachment {
    /// Runtime network name.
    pub network: NetworkHandle,
    /// Workload address observed by the backend.
    pub address: IpAddr,
    /// Backend interface name when one is externally meaningful.
    pub interface_name: Option<String>,
}

/// Complete network state observed for one workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadNetworkStatus {
    /// Attachments in stable network-name order.
    pub attachments: Vec<NetworkAttachment>,
}

/// Matchable runtime network backend failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetworkProviderError {
    /// Network range, gateway, or name is invalid.
    #[error("invalid runtime network: {message}")]
    InvalidRange {
        /// Stable explanation safe to surface.
        message: String,
    },
    /// Requested network does not exist.
    #[error("runtime network `{name}` does not exist")]
    NetworkNotFound {
        /// Missing backend network name.
        name: String,
    },
    /// Requested address is already owned by another workload.
    #[error("runtime address `{address}` is already allocated")]
    AddressConflict {
        /// Conflicting workload address.
        address: IpAddr,
    },
    /// Backend is temporarily unavailable and reconciliation should retry.
    #[error("runtime network backend is unavailable: {message}")]
    Unavailable {
        /// Backend detail safe to log.
        message: String,
    },
    /// Backend rejected a request that retrying cannot repair.
    #[error("runtime network backend rejected the operation: {message}")]
    Rejected {
        /// Backend detail safe to surface.
        message: String,
    },
}

/// Runtime-native network lifecycle and host-owned address allocation.
#[async_trait]
pub trait NetworkProvider: Send + Sync {
    /// Creates or idempotently validates a runtime network.
    async fn ensure_network(
        &self,
        spec: &NetworkSpec,
    ) -> Result<NetworkHandle, NetworkProviderError>;

    /// Reserves one address for a stable workload identity.
    async fn allocate_address(
        &self,
        network: &NetworkHandle,
        workload_id: &WorkloadId,
        request: AddressRequest,
    ) -> Result<AddressLease, NetworkProviderError>;

    /// Attaches one workload to an allocated address idempotently.
    async fn attach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<NetworkAttachment, NetworkProviderError>;

    /// Detaches one workload without releasing its address reservation.
    async fn detach(
        &self,
        workload: &WorkloadHandle,
        network: &NetworkHandle,
    ) -> Result<(), NetworkProviderError>;

    /// Reads all current backend attachments for one workload.
    async fn inspect(
        &self,
        workload: &WorkloadHandle,
    ) -> Result<WorkloadNetworkStatus, NetworkProviderError>;

    /// Releases one address only when the stable workload owner matches.
    async fn release_address(
        &self,
        network: &NetworkHandle,
        lease: &AddressLease,
    ) -> Result<(), NetworkProviderError>;
}

fn canonical_address(address: IpAddr, prefix_length: u8) -> IpAddr {
    match address {
        IpAddr::V4(address) => {
            let host_bits = u32::BITS.saturating_sub(u32::from(prefix_length));
            let mask = u32::MAX.checked_shl(host_bits).unwrap_or(0);
            IpAddr::V4(Ipv4Addr::from(u32::from(address) & mask))
        }
        IpAddr::V6(address) => {
            let host_bits = u128::BITS.saturating_sub(u32::from(prefix_length));
            let mask = u128::MAX.checked_shl(host_bits).unwrap_or(0);
            IpAddr::V6(Ipv6Addr::from(u128::from(address) & mask))
        }
    }
}
