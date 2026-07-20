use crate::{
    ControlPlaneReadiness, FixtureMarker, QuorumRecoveryCluster, ReadinessProbe, ScenarioError,
};

/// Proves that one voter cannot pass readiness and restored quorum preserves state.
pub async fn all_voter_restart_waits_for_quorum_and_preserves_state<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: QuorumRecoveryCluster,
{
    let nodes = cluster.nodes();
    let [first_node, remaining_nodes @ ..] = nodes.as_slice() else {
        return Err(ScenarioError::Assertion(
            "quorum recovery requires at least one persisted voter".to_string(),
        ));
    };
    if remaining_nodes.is_empty() {
        Err(ScenarioError::Assertion(
            "quorum recovery requires more than one persisted voter".to_string(),
        ))
    } else {
        let marker = FixtureMarker::new("before-restart");
        cluster
            .write_marker(marker.clone())
            .await
            .map_err(|error| driver_error("write pre-restart marker", error))?;
        cluster
            .stop_all_nodes()
            .await
            .map_err(|error| driver_error("stop every persisted voter", error))?;
        cluster
            .start_node(first_node)
            .await
            .map_err(|error| driver_error("restart first persisted voter", error))?;
        let early = cluster
            .probe_readiness(ReadinessProbe::Brief)
            .await
            .map_err(|error| driver_error("probe readiness without quorum", error))?;
        if early == ControlPlaneReadiness::Ready {
            Err(ScenarioError::Assertion(
                "control-plane readiness passed with only one persisted voter".to_string(),
            ))
        } else {
            for node in remaining_nodes {
                cluster
                    .start_node(node)
                    .await
                    .map_err(|error| driver_error("restart remaining persisted voter", error))?;
            }
            let restored = cluster
                .probe_readiness(ReadinessProbe::UntilReady)
                .await
                .map_err(|error| driver_error("await restored persisted quorum", error))?;
            if restored != ControlPlaneReadiness::Ready {
                Err(ScenarioError::Assertion(
                    "control-plane readiness remained blocked after voters returned".to_string(),
                ))
            } else {
                let preserved = cluster
                    .read_marker()
                    .await
                    .map_err(|error| driver_error("read post-restart marker", error))?;
                if preserved == Some(marker) {
                    Ok(())
                } else {
                    Err(ScenarioError::Assertion(format!(
                        "persisted marker changed across full restart: {preserved:?}"
                    )))
                }
            }
        }
    }
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
