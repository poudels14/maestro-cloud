use kernel_api::SecretValue;
use serde::{Deserialize, Serialize};

/// Default number of highly available Cloudflare Tunnel connectors.
pub const DEFAULT_CLOUDFLARE_TUNNEL_REPLICAS: u32 = 2;
/// Bound retained from the legacy deployment contract.
pub const MAX_CLOUDFLARE_TUNNEL_REPLICAS: u32 = 25;
const MAXIMUM_TUNNEL_TOKEN_BYTES: usize = 16 * 1_024;

/// Cluster-wide remotely managed Cloudflare Tunnel connector policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloudflareTunnelConfig {
    /// Connector token issued for one remotely managed tunnel.
    pub token: SecretValue,
    /// Desired number of connector workloads.
    pub replicas: u32,
}

impl CloudflareTunnelConfig {
    /// Rejects unusable secrets and unbounded connector fleets.
    pub fn validate(&self) -> Result<(), CloudflareTunnelConfigError> {
        let token = self.token.expose();
        if token.trim().is_empty() || token.contains('\0') {
            return Err(CloudflareTunnelConfigError::InvalidToken);
        }
        if token.len() > MAXIMUM_TUNNEL_TOKEN_BYTES {
            return Err(CloudflareTunnelConfigError::TokenTooLong);
        }
        if self.replicas == 0 {
            return Err(CloudflareTunnelConfigError::ZeroReplicas);
        }
        if self.replicas > MAX_CLOUDFLARE_TUNNEL_REPLICAS {
            return Err(CloudflareTunnelConfigError::TooManyReplicas {
                replicas: self.replicas,
                maximum: MAX_CLOUDFLARE_TUNNEL_REPLICAS,
            });
        }
        Ok(())
    }
}

/// Invalid remotely managed Cloudflare Tunnel configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CloudflareTunnelConfigError {
    /// Empty and NUL-bearing credentials are never passed to a workload.
    #[error("tunnel token must not be empty or contain NUL bytes")]
    InvalidToken,
    /// Secret material is bounded before it enters launch documents or the store.
    #[error("tunnel token exceeds {MAXIMUM_TUNNEL_TOKEN_BYTES} bytes")]
    TokenTooLong,
    /// An enabled tunnel requires at least one connector.
    #[error("tunnel replicas must be greater than zero")]
    ZeroReplicas,
    /// Connector fleets remain within the reviewed operational bound.
    #[error("tunnel replicas {replicas} exceed the maximum {maximum}")]
    TooManyReplicas { replicas: u32, maximum: u32 },
}
