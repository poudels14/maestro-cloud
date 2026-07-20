use crate::{
    AffinityCluster, AffinityCookieSet, AffinityObservation, FixtureNodeName, ScenarioError,
};

/// Proves affinity tokens are opaque, cookie replay is sticky, and hints override.
pub async fn affinity_is_opaque_sticky_and_overridable<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AffinityCluster,
{
    let nodes = cluster.nodes();
    let session = cluster
        .establish_affinity()
        .await
        .map_err(|error| driver_error("establish affinity", error))?;
    assert_complete_and_opaque(&session.initial)?;

    for _ in 0..8 {
        let replay = cluster
            .replay_affinity(&session.session)
            .await
            .map_err(|error| driver_error("replay affinity cookies", error))?;
        if replay.node != session.initial.node || replay.token != session.initial.token {
            return Err(ScenarioError::Assertion(format!(
                "affinity replay moved from {:?} to {:?}",
                session.initial, replay
            )));
        }
    }

    let target = nodes
        .iter()
        .find(|node| **node != session.initial.node)
        .ok_or_else(|| {
            ScenarioError::Assertion(
                "affinity override requires at least two routable nodes".to_string(),
            )
        })?;
    let first_override = cluster
        .override_affinity(&session.session, target)
        .await
        .map_err(|error| driver_error("override affinity target", error))?;
    assert_target(target, &first_override)?;
    for _ in 1..4 {
        let replay = cluster
            .override_affinity(&session.session, target)
            .await
            .map_err(|error| driver_error("replay affinity override", error))?;
        if replay.node != first_override.node || replay.token != first_override.token {
            return Err(ScenarioError::Assertion(format!(
                "affinity override was unstable: first {first_override:?}, replay {replay:?}"
            )));
        }
    }
    Ok(())
}

fn assert_complete_and_opaque(observation: &AffinityObservation) -> Result<(), ScenarioError> {
    if observation.cookies != AffinityCookieSet::Complete {
        Err(ScenarioError::Assertion(
            "initial affinity response omitted a required cookie".to_string(),
        ))
    } else if observation
        .token
        .as_str()
        .contains(observation.node.as_str())
    {
        Err(ScenarioError::Assertion(format!(
            "affinity token exposed node identity `{}`",
            observation.node.as_str()
        )))
    } else {
        Ok(())
    }
}

fn assert_target(
    target: &FixtureNodeName,
    observation: &AffinityObservation,
) -> Result<(), ScenarioError> {
    if observation.node != *target {
        Err(ScenarioError::Assertion(format!(
            "affinity override selected {:?}, expected {target:?}",
            observation.node
        )))
    } else if observation.token.as_str().contains(target.as_str()) {
        Err(ScenarioError::Assertion(format!(
            "override affinity token exposed node identity `{}`",
            target.as_str()
        )))
    } else {
        Ok(())
    }
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
