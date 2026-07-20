use std::collections::BTreeSet;

use crate::{CandidateReadiness, CutoverCluster, DrainBehavior, FixtureVersion, ScenarioError};

/// Proves readiness gating, uninterrupted cutover, and graceful in-flight drain.
pub async fn readiness_gated_cutover_preserves_traffic_and_inflight_requests<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: CutoverCluster,
{
    let previous = FixtureVersion::new("v1");
    let candidate = FixtureVersion::new("v2");
    cluster
        .deploy_initial(previous.clone())
        .await
        .map_err(|error| driver_error("deploy initial traffic generation", error))?;
    assert_routes(cluster, version_set([&previous]), "initial routes").await?;

    cluster
        .deploy_candidate(candidate.clone(), CandidateReadiness::OneDelayed)
        .await
        .map_err(|error| driver_error("deploy readiness-gated candidate", error))?;
    assert_routes(
        cluster,
        version_set([&previous]),
        "routes while candidate is unready",
    )
    .await?;
    cluster
        .make_candidate_ready(&candidate)
        .await
        .map_err(|error| driver_error("release delayed candidate replica", error))?;
    assert_routes(
        cluster,
        version_set([&previous]),
        "routes before explicit cutover",
    )
    .await?;

    let observation = cluster
        .cutover_with_inflight_request(&previous, &candidate)
        .await
        .map_err(|error| driver_error("cut over with live traffic", error))?;
    let expected_traffic = version_set([&previous, &candidate]);
    let expected_final = version_set([&candidate]);
    if observation.public_failures != 0 {
        Err(ScenarioError::Assertion(format!(
            "{} public request(s) failed during cutover",
            observation.public_failures
        )))
    } else if observation.traffic_versions != expected_traffic {
        Err(ScenarioError::Assertion(format!(
            "cutover traffic observed {:?}, expected {:?}",
            observation.traffic_versions, expected_traffic
        )))
    } else if observation.in_flight_version != previous {
        Err(ScenarioError::Assertion(format!(
            "in-flight request completed on {:?}, expected {:?}",
            observation.in_flight_version, previous
        )))
    } else if observation.drain_behavior != DrainBehavior::WaitedForInflight {
        Err(ScenarioError::Assertion(
            "old workload exited before its in-flight request completed".to_string(),
        ))
    } else if observation.final_routes != expected_final {
        Err(ScenarioError::Assertion(format!(
            "final routes were {:?}, expected {:?}",
            observation.final_routes, expected_final
        )))
    } else {
        Ok(())
    }
}

async fn assert_routes<Cluster>(
    cluster: &mut Cluster,
    expected: BTreeSet<FixtureVersion>,
    operation: &'static str,
) -> Result<(), ScenarioError>
where
    Cluster: CutoverCluster,
{
    let actual = cluster
        .await_routed_versions()
        .await
        .map_err(|error| driver_error(operation, error))?;
    if actual == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "routed versions were {actual:?}, expected {expected:?}"
        )))
    }
}

fn version_set<const VERSION_COUNT: usize>(
    versions: [&FixtureVersion; VERSION_COUNT],
) -> BTreeSet<FixtureVersion> {
    versions.into_iter().cloned().collect()
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
