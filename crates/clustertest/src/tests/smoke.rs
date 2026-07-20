use async_trait::async_trait;

use crate::{
    IngressConfigurationState, IngressStartupCluster, IngressStartupObservation, PeerStoreCluster,
    PeerStoreObservation, ResourceAvailability, SecurityRestartState, SeedControlRole,
    SeedSecurityCluster, SeedSecurityObservation,
    scenarios::{
        isolated_seed_security_restart_is_idempotent,
        production_ingress_access_log_configuration_starts,
        single_node_store_endpoint_is_peer_reachable,
    },
};

#[derive(Debug, thiserror::Error)]
#[error("startup smoke world failed")]
struct SmokeWorldError;

struct IngressWorld;

#[async_trait]
impl IngressStartupCluster for IngressWorld {
    type Error = SmokeWorldError;

    async fn start_ingress(&mut self) -> Result<IngressStartupObservation, Self::Error> {
        Ok(IngressStartupObservation {
            configuration: IngressConfigurationState::Accepted,
            endpoint: ResourceAvailability::Available,
        })
    }
}

struct PeerStoreWorld;

#[async_trait]
impl PeerStoreCluster for PeerStoreWorld {
    type Error = SmokeWorldError;

    async fn probe_store_from_peer(&mut self) -> Result<PeerStoreObservation, Self::Error> {
        Ok(PeerStoreObservation {
            endpoint: ResourceAvailability::Available,
        })
    }
}

struct SeedSecurityWorld;

#[async_trait]
impl SeedSecurityCluster for SeedSecurityWorld {
    type Error = SmokeWorldError;

    async fn bootstrap_seed_security(&mut self) -> Result<SeedSecurityObservation, Self::Error> {
        Ok(SeedSecurityObservation {
            seed_role: SeedControlRole::VotingControlPlane,
            configured_voters: 3,
            unreachable_peers: 2,
            store: ResourceAvailability::Available,
            security_restart: SecurityRestartState::Preserved,
            gateway_root: ResourceAvailability::Available,
        })
    }
}

#[tokio::test]
async fn production_ingress_configuration_starts() {
    production_ingress_access_log_configuration_starts(&mut IngressWorld)
        .await
        .expect("production ingress smoke scenario");
}

#[tokio::test]
async fn single_node_store_is_reachable_from_peer() {
    single_node_store_endpoint_is_peer_reachable(&mut PeerStoreWorld)
        .await
        .expect("peer store smoke scenario");
}

#[tokio::test]
async fn isolated_seed_repeats_security_initialization() {
    isolated_seed_security_restart_is_idempotent(&mut SeedSecurityWorld)
        .await
        .expect("isolated seed security scenario");
}
