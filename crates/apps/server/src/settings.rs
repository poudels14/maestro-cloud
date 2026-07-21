use std::net::SocketAddr;

use kernel_api::SecretValue;

/// Listener and operator authentication policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerSettings {
    /// Exact host address and port claimed by this node API.
    pub bind_address: SocketAddr,
    /// HS256 operator key; loopback-only development may omit it.
    pub jwt_secret_key: Option<SecretValue>,
}

impl ServerSettings {
    /// Constructs a listener with authentication disabled only on loopback.
    pub fn new(bind_address: SocketAddr, jwt_secret_key: Option<SecretValue>) -> Self {
        Self {
            bind_address,
            jwt_secret_key,
        }
    }

    /// Rejects an exposed listener without authentication or a weak configured key.
    pub fn validate(self) -> Result<Self, ServerSettingsError> {
        if !self.bind_address.ip().is_loopback() && self.jwt_secret_key.is_none() {
            return Err(ServerSettingsError::UnauthenticatedNonLoopback {
                address: self.bind_address,
            });
        }
        if self
            .jwt_secret_key
            .as_ref()
            .is_some_and(|secret| secret.expose().len() < 32)
        {
            return Err(ServerSettingsError::WeakJwtSecret);
        }
        Ok(self)
    }
}

/// Unsafe API listener or authentication policy.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServerSettingsError {
    /// Network-reachable APIs must never silently bypass operator authentication.
    #[error("non-loopback API listener `{address}` requires a JWT secret key")]
    UnauthenticatedNonLoopback { address: SocketAddr },
    /// Short symmetric keys do not provide the expected HS256 security margin.
    #[error("JWT secret key must contain at least 32 bytes")]
    WeakJwtSecret,
}
