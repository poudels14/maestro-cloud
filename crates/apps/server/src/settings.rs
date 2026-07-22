use std::net::SocketAddr;

use kernel_api::SecretValue;

/// PEM identity presented by an HTTPS listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsIdentity {
    /// PEM-encoded leaf certificate and any required intermediates.
    pub certificate_pem: String,
    /// PEM-encoded private key protected from debug output.
    pub private_key_pem: SecretValue,
}

impl TlsIdentity {
    /// Creates an in-memory server identity without persisting another key copy.
    pub fn new(certificate_pem: impl Into<String>, private_key_pem: SecretValue) -> Self {
        Self {
            certificate_pem: certificate_pem.into(),
            private_key_pem,
        }
    }
}

/// Listener and operator authentication policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerSettings {
    /// Exact host address and port claimed by this node API.
    pub bind_address: SocketAddr,
    /// HS256 operator key; loopback-only development may omit it.
    pub jwt_secret_key: Option<SecretValue>,
    /// HTTPS identity; plaintext is allowed only on loopback.
    pub tls_identity: Option<TlsIdentity>,
    /// Cluster CA used by this node's internal mutual-TLS clients.
    pub cluster_trust_root_pem: Option<String>,
    /// Node identity presented only by internal mutual-TLS clients.
    pub cluster_client_identity: Option<TlsIdentity>,
}

impl ServerSettings {
    /// Constructs a listener with authentication disabled only on loopback.
    pub fn new(bind_address: SocketAddr, jwt_secret_key: Option<SecretValue>) -> Self {
        Self {
            bind_address,
            jwt_secret_key,
            tls_identity: None,
            cluster_trust_root_pem: None,
            cluster_client_identity: None,
        }
    }

    /// Requires the listener to present the supplied HTTPS identity.
    pub fn with_tls_identity(mut self, identity: TlsIdentity) -> Self {
        self.tls_identity = Some(identity);
        self
    }

    /// Makes the cluster trust root available to internal node clients.
    pub fn with_cluster_trust_root(mut self, trust_root_pem: impl Into<String>) -> Self {
        self.cluster_trust_root_pem = Some(trust_root_pem.into());
        self
    }

    /// Configures the node certificate presented to cluster peers.
    pub fn with_cluster_client_identity(mut self, identity: TlsIdentity) -> Self {
        self.cluster_client_identity = Some(identity);
        self
    }

    /// Rejects an exposed listener without authentication or a weak configured key.
    pub fn validate(self) -> Result<Self, ServerSettingsError> {
        if !self.bind_address.ip().is_loopback() && self.jwt_secret_key.is_none() {
            return Err(ServerSettingsError::UnauthenticatedNonLoopback {
                address: self.bind_address,
            });
        }
        if !self.bind_address.ip().is_loopback() && self.tls_identity.is_none() {
            return Err(ServerSettingsError::PlaintextNonLoopback {
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
        if self
            .cluster_trust_root_pem
            .as_ref()
            .is_some_and(|trust_root| trust_root.trim().is_empty())
        {
            return Err(ServerSettingsError::EmptyClusterTrustRoot);
        }
        Ok(self)
    }

    pub(crate) fn requires_node_client_certificate(&self) -> bool {
        self.tls_identity.is_some() && self.cluster_trust_root_pem.is_some()
    }
}

/// Unsafe API listener or authentication policy.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServerSettingsError {
    /// Network-reachable APIs must never silently bypass operator authentication.
    #[error("non-loopback API listener `{address}` requires a JWT secret key")]
    UnauthenticatedNonLoopback { address: SocketAddr },
    /// Network-reachable operator credentials must never cross plaintext HTTP.
    #[error("non-loopback API listener `{address}` requires a TLS identity")]
    PlaintextNonLoopback { address: SocketAddr },
    /// Short symmetric keys do not provide the expected HS256 security margin.
    #[error("JWT secret key must contain at least 32 bytes")]
    WeakJwtSecret,
    /// A present trust root must contain certificate material.
    #[error("cluster trust root cannot be empty")]
    EmptyClusterTrustRoot,
}
