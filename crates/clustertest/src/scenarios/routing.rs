use std::collections::BTreeSet;

use crate::{FixtureNodeName, ResourceAvailability, RoutingCluster, ScenarioError};

/// Proves that ingress removes and restores failed workloads and gateways.
pub async fn routing_survives_workload_and_gateway_failures<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: RoutingCluster,
{
    let nodes = cluster.nodes();
    let [node_one, node_two, node_three] = nodes.as_slice() else {
        return Err(ScenarioError::Assertion(format!(
            "routing recovery requires exactly three nodes, observed {}",
            nodes.len()
        )));
    };
    let all_nodes = node_set([node_one, node_two, node_three]);
    assert_routes(cluster, &all_nodes, "initial routing convergence").await?;

    cluster
        .set_workload_availability(node_two, ResourceAvailability::Unavailable)
        .await
        .map_err(|error| driver_error("stop node-two workload", error))?;
    let without_node_two = node_set([node_one, node_three]);
    assert_routes(cluster, &without_node_two, "workload failure convergence").await?;

    cluster
        .set_workload_availability(node_two, ResourceAvailability::Available)
        .await
        .map_err(|error| driver_error("restore node-two workload", error))?;
    assert_routes(cluster, &all_nodes, "workload recovery convergence").await?;

    cluster
        .set_gateway_availability(node_one, ResourceAvailability::Unavailable)
        .await
        .map_err(|error| driver_error("stop node-one gateway", error))?;
    let without_node_one = node_set([node_two, node_three]);
    assert_routes(cluster, &without_node_one, "gateway failure convergence").await?;

    cluster
        .set_gateway_availability(node_two, ResourceAvailability::Unavailable)
        .await
        .map_err(|error| driver_error("stop node-two gateway", error))?;
    cluster
        .set_gateway_availability(node_three, ResourceAvailability::Unavailable)
        .await
        .map_err(|error| driver_error("stop node-three gateway", error))?;
    cluster
        .await_public_unavailable()
        .await
        .map_err(|error| driver_error("await total ingress loss", error))?;

    cluster
        .set_gateway_availability(node_two, ResourceAvailability::Available)
        .await
        .map_err(|error| driver_error("restore node-two gateway", error))?;
    cluster
        .set_gateway_availability(node_three, ResourceAvailability::Available)
        .await
        .map_err(|error| driver_error("restore node-three gateway", error))?;
    assert_routes(
        cluster,
        &without_node_one,
        "partial gateway recovery convergence",
    )
    .await?;

    cluster
        .set_gateway_availability(node_one, ResourceAvailability::Available)
        .await
        .map_err(|error| driver_error("restore node-one gateway", error))?;
    assert_routes(cluster, &all_nodes, "full gateway recovery convergence").await
}

async fn assert_routes<Cluster>(
    cluster: &mut Cluster,
    expected: &BTreeSet<FixtureNodeName>,
    operation: &'static str,
) -> Result<(), ScenarioError>
where
    Cluster: RoutingCluster,
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

fn node_set<const NODE_COUNT: usize>(
    nodes: [&FixtureNodeName; NODE_COUNT],
) -> BTreeSet<FixtureNodeName> {
    nodes.into_iter().cloned().collect()
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
