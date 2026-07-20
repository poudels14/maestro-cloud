use std::collections::BTreeSet;

use crate::{ClusterSetupCluster, FixtureNodeName, ScenarioError};

/// Proves one- and three-node bootstrap, mesh, loss, and persisted restart behavior.
pub async fn cluster_bootstraps_joins_meshes_and_recovers<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: ClusterSetupCluster,
{
    let nodes = cluster.nodes();
    if !matches!(nodes.len(), 1 | 3) {
        return Err(ScenarioError::Assertion(format!(
            "cluster setup requires one or three control-plane nodes, found {}",
            nodes.len()
        )));
    }
    cluster
        .bootstrap_seed()
        .await
        .map_err(|error| driver_error("bootstrap designated seed", error))?;
    for node in nodes.iter().skip(1) {
        cluster
            .join_node(node)
            .await
            .map_err(|error| driver_error("join and activate cluster node", error))?;
    }

    let expected = nodes.iter().cloned().collect::<BTreeSet<_>>();
    assert_mesh(cluster, &expected, "initial mesh convergence").await?;
    cluster
        .verify_store_write()
        .await
        .map_err(|error| driver_error("verify formed-cluster store write", error))?;

    if let [source, failed, _survivor] = nodes.as_slice() {
        cluster
            .ping_workload(source, failed)
            .await
            .map_err(|error| driver_error("ping remote workload over WireGuard", error))?;
        cluster
            .stop_node(failed)
            .await
            .map_err(|error| driver_error("stop one formed-cluster node", error))?;
        cluster
            .verify_store_write()
            .await
            .map_err(|error| driver_error("verify quorum write after node loss", error))?;
        cluster
            .restart_node(failed)
            .await
            .map_err(|error| driver_error("restart persisted cluster node", error))?;
        assert_mesh(cluster, &expected, "mesh convergence after restart").await?;
        cluster
            .ping_workload(source, failed)
            .await
            .map_err(|error| driver_error("ping workload after node restart", error))?;
    }
    Ok(())
}

async fn assert_mesh<Cluster>(
    cluster: &mut Cluster,
    expected: &BTreeSet<FixtureNodeName>,
    operation: &'static str,
) -> Result<(), ScenarioError>
where
    Cluster: ClusterSetupCluster,
{
    let actual = cluster
        .await_mesh(expected)
        .await
        .map_err(|error| driver_error(operation, error))?;
    if actual == *expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "mesh converged to {actual:?}, expected {expected:?}"
        )))
    }
}

fn driver_error(operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation,
        message: error.to_string(),
    }
}
