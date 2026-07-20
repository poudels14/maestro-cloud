use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::ResourceAvailability;

/// Whether a production ingress process accepted its generated configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngressConfigurationState {
    /// The process accepted the configuration and remained running.
    Accepted,
    /// The process rejected the configuration or exited during startup.
    Rejected,
}

/// Evidence from starting the pinned ingress runtime with production flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressStartupObservation {
    /// Whether the runtime accepted the generated access-log configuration.
    pub configuration: IngressConfigurationState,
    /// Whether the published ingress endpoint accepted a connection.
    pub endpoint: ResourceAvailability,
}

/// Evidence that an embedded-store endpoint works outside its own process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStoreObservation {
    /// Whether an isolated peer can reach the advertised endpoint.
    pub endpoint: ResourceAvailability,
}

/// Whether repeating security initialization preserved the initialized state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecurityRestartState {
    /// Reinitialization was idempotent through an authenticated client.
    Preserved,
    /// Reinitialization failed or changed the initialized security state.
    Diverged,
}

/// The control-plane capability assigned to the designated seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeedControlRole {
    /// The seed participates in consensus and runs control-plane services.
    VotingControlPlane,
    /// The seed lacks the voting control-plane capability required for bootstrap.
    Unsupported,
}

/// Evidence from bootstrapping a designated seed while configured peers are offline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedSecurityObservation {
    /// The configured role of the designated seed.
    pub seed_role: SeedControlRole,
    /// Number of voters in the static topology, including the seed.
    pub configured_voters: usize,
    /// Number of configured remote peers that remained unreachable.
    pub unreachable_peers: usize,
    /// Whether the seed's store remained available after authentication was enabled.
    pub store: ResourceAvailability,
    /// Whether repeating initialization after a client restart was idempotent.
    pub security_restart: SecurityRestartState,
    /// Whether the seed's gateway-root record was initialized.
    pub gateway_root: ResourceAvailability,
}

/// Starts production ingress configuration through a reusable smoke scenario.
#[async_trait]
pub trait IngressStartupCluster: Send {
    /// A matchable error returned by the system-specific driver.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Starts the pinned ingress runtime with production access-log settings.
    async fn start_ingress(&mut self) -> Result<IngressStartupObservation, Self::Error>;
}

/// Probes an embedded store from a peer runtime through a reusable smoke scenario.
#[async_trait]
pub trait PeerStoreCluster: Send {
    /// A matchable error returned by the system-specific driver.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Starts one store member and probes its advertised endpoint from an isolated peer.
    async fn probe_store_from_peer(&mut self) -> Result<PeerStoreObservation, Self::Error>;
}

/// Bootstraps seed security while configured remote voters remain offline.
#[async_trait]
pub trait SeedSecurityCluster: Send {
    /// A matchable error returned by the system-specific driver.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Initializes authentication and repeats it through a restarted authenticated client.
    async fn bootstrap_seed_security(&mut self) -> Result<SeedSecurityObservation, Self::Error>;
}
