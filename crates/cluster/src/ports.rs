use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Default UDP port for the cluster WireGuard mesh.
pub const DEFAULT_WIREGUARD_PORT: u16 = 51_820;

/// Ports selected during cluster initialization and persisted for every node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterPorts {
    format_version: u8,
    /// Cluster gateway listener port.
    pub gateway: u16,
    /// Internal store client port.
    pub store_client: u16,
    /// Internal store membership port.
    pub store_peer: u16,
    /// WireGuard mesh UDP port.
    pub wireguard: u16,
}

impl ClusterPorts {
    /// Current persisted format version.
    pub const FORMAT_VERSION: u8 = 1;

    /// Creates and validates the cluster port allocation.
    pub fn new(
        gateway: u16,
        store_client: u16,
        store_peer: u16,
        wireguard: u16,
    ) -> Result<Self, ClusterPortsError> {
        let ports = Self {
            format_version: Self::FORMAT_VERSION,
            gateway,
            store_client,
            store_peer,
            wireguard,
        };
        ports.validate()?;
        Ok(ports)
    }

    /// Returns the persisted format version.
    pub fn format_version(self) -> u8 {
        self.format_version
    }

    /// Validates the persisted version and all port values.
    pub fn validate(self) -> Result<(), ClusterPortsError> {
        if self.format_version != Self::FORMAT_VERSION {
            return Err(ClusterPortsError::UnsupportedFormat {
                observed: self.format_version,
                supported: Self::FORMAT_VERSION,
            });
        }

        let values = [
            self.gateway,
            self.store_client,
            self.store_peer,
            self.wireguard,
        ];
        if values.contains(&0) {
            return Err(ClusterPortsError::ZeroPort);
        }
        if values.iter().copied().collect::<BTreeSet<_>>().len() != values.len() {
            return Err(ClusterPortsError::DuplicatePort);
        }
        Ok(())
    }

    /// Ensures a node's public API port does not collide with a cluster service.
    pub fn validate_api_port(self, api_port: u16) -> Result<(), ClusterPortsError> {
        self.validate()?;
        if api_port == 0 {
            return Err(ClusterPortsError::ZeroPort);
        }
        if [
            self.gateway,
            self.store_client,
            self.store_peer,
            self.wireguard,
        ]
        .contains(&api_port)
        {
            return Err(ClusterPortsError::ApiPortConflict { port: api_port });
        }
        Ok(())
    }
}

/// Why a persisted cluster port allocation was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClusterPortsError {
    /// The persisted shape was written by an unsupported implementation.
    #[error("unsupported cluster port format {observed}; this binary supports {supported}")]
    UnsupportedFormat { observed: u8, supported: u8 },
    /// Port zero cannot be persisted because it requests dynamic allocation.
    #[error("cluster ports must be non-zero")]
    ZeroPort,
    /// Every cluster service needs an unambiguous port assignment.
    #[error("cluster ports must be distinct")]
    DuplicatePort,
    /// The public API cannot share a port with an internal cluster service.
    #[error("node API port {port} conflicts with a cluster service port")]
    ApiPortConflict { port: u16 },
}
