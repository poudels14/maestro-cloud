use crate::{
    IngressConfigurationState, IngressStartupCluster, PeerStoreCluster, ResourceAvailability,
    ScenarioError, SecurityRestartState, SeedControlRole, SeedSecurityCluster,
};

/// Proves production ingress access-log settings parse and expose ingress.
pub async fn production_ingress_access_log_configuration_starts<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: IngressStartupCluster,
{
    let observation = cluster
        .start_ingress()
        .await
        .map_err(|error| driver_error("start production ingress configuration", error))?;
    if observation.configuration == IngressConfigurationState::Accepted
        && observation.endpoint == ResourceAvailability::Available
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "production ingress did not start cleanly: {observation:?}"
        )))
    }
}

/// Proves a one-member embedded store advertises an endpoint usable by peers.
pub async fn single_node_store_endpoint_is_peer_reachable<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: PeerStoreCluster,
{
    let observation = cluster
        .probe_store_from_peer()
        .await
        .map_err(|error| driver_error("probe single-node store from peer", error))?;
    if observation.endpoint == ResourceAvailability::Available {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "single-node store was not peer reachable: {observation:?}"
        )))
    }
}

/// Proves an isolated designated seed can initialize and repeat security setup.
pub async fn isolated_seed_security_restart_is_idempotent<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: SeedSecurityCluster,
{
    let observation = cluster
        .bootstrap_seed_security()
        .await
        .map_err(|error| driver_error("bootstrap isolated seed security", error))?;
    if observation.seed_role == SeedControlRole::VotingControlPlane
        && observation.configured_voters == 3
        && observation.unreachable_peers == 2
        && observation.store == ResourceAvailability::Available
        && observation.security_restart == SecurityRestartState::Preserved
        && observation.gateway_root == ResourceAvailability::Available
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "isolated seed security did not converge: {observation:?}"
        )))
    }
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
