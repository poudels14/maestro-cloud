use std::collections::BTreeSet;

use crate::{FixtureNodeName, ResourceAvailability, RestartCluster, ScenarioError};

/// Proves that serial node restarts preserve quorum writes and public routing.
pub async fn serial_node_restarts_preserve_quorum_and_routing<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: RestartCluster,
{
    let nodes = cluster.nodes();
    if nodes.len() != 3 {
        Err(ScenarioError::Assertion(format!(
            "serial restart requires exactly three nodes, observed {}",
            nodes.len()
        )))
    } else {
        let all_nodes = nodes.iter().cloned().collect::<BTreeSet<_>>();
        assert_routes(cluster, &all_nodes, "initial restart routing convergence").await?;

        for restarting_node in &nodes {
            cluster
                .set_node_availability(restarting_node, ResourceAvailability::Unavailable)
                .await
                .map_err(|error| driver_error("stop node for serial restart", error))?;
            cluster
                .verify_quorum_write(restarting_node)
                .await
                .map_err(|error| driver_error("verify quorum write during restart", error))?;
            let remaining_nodes = all_nodes
                .iter()
                .filter(|node| *node != restarting_node)
                .cloned()
                .collect::<BTreeSet<_>>();
            assert_routes(
                cluster,
                &remaining_nodes,
                "routing convergence during node restart",
            )
            .await?;

            cluster
                .set_node_availability(restarting_node, ResourceAvailability::Available)
                .await
                .map_err(|error| driver_error("restore node after serial restart", error))?;
            cluster
                .await_control_plane_ready()
                .await
                .map_err(|error| driver_error("await control-plane recovery", error))?;
            assert_routes(
                cluster,
                &all_nodes,
                "routing convergence after node restart",
            )
            .await?;
        }
        Ok(())
    }
}

async fn assert_routes<Cluster>(
    cluster: &mut Cluster,
    expected: &BTreeSet<FixtureNodeName>,
    operation: &'static str,
) -> Result<(), ScenarioError>
where
    Cluster: RestartCluster,
{
    let actual = cluster
        .await_public_routes(expected)
        .await
        .map_err(|error| driver_error(operation, error))?;
    if actual == *expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "public routing converged to {actual:?}, expected {expected:?}"
        )))
    }
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
