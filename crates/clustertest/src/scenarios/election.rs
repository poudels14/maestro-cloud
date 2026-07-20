use crate::{
    AssignmentManifestSnapshot, AssignmentWriteOutcome, ElectionCluster, FencedWriteOutcome,
    FixtureMutationName, FixtureVersion, ScenarioError,
};

/// Proves leader failover fences stale writes and preserves assignment CAS state.
pub async fn leader_failover_fences_stale_writes<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: ElectionCluster,
{
    let first_leader = cluster
        .await_leader()
        .await
        .map_err(|error| driver_error("await initial leader", error))?;
    assert_fenced_write(
        cluster,
        &first_leader.token,
        "before-failover",
        FencedWriteOutcome::Applied,
    )
    .await?;
    assert_assignment_write(
        cluster,
        &first_leader.token,
        0,
        FixtureVersion::new("v1"),
        AssignmentWriteOutcome::Applied,
    )
    .await?;

    cluster
        .stop_controller(&first_leader.controller)
        .await
        .map_err(|error| driver_error("stop initial leader", error))?;
    let successor = cluster
        .await_leader()
        .await
        .map_err(|error| driver_error("await successor leader", error))?;
    if successor.controller == first_leader.controller {
        Err(ScenarioError::Assertion(
            "leadership did not move to a surviving controller".to_string(),
        ))
    } else {
        assert_fenced_write(
            cluster,
            &first_leader.token,
            "stale-after-failover",
            FencedWriteOutcome::Rejected,
        )
        .await?;
        assert_assignment_state(cluster, 1, FixtureVersion::new("v1")).await?;
        assert_assignment_write(
            cluster,
            &first_leader.token,
            1,
            FixtureVersion::new("v2"),
            AssignmentWriteOutcome::LeadershipLost,
        )
        .await?;
        assert_assignment_write(
            cluster,
            &successor.token,
            1,
            FixtureVersion::new("v2"),
            AssignmentWriteOutcome::Applied,
        )
        .await?;
        assert_assignment_state(cluster, 2, FixtureVersion::new("v2")).await?;
        assert_fenced_write(
            cluster,
            &successor.token,
            "live-after-failover",
            FencedWriteOutcome::Applied,
        )
        .await?;

        cluster
            .lose_quorum()
            .await
            .map_err(|error| driver_error("remove store quorum", error))?;
        assert_fenced_write(
            cluster,
            &successor.token,
            "without-quorum",
            FencedWriteOutcome::Rejected,
        )
        .await
    }
}

async fn assert_fenced_write<Cluster>(
    cluster: &mut Cluster,
    token: &Cluster::LeadershipToken,
    mutation: &'static str,
    expected: FencedWriteOutcome,
) -> Result<(), ScenarioError>
where
    Cluster: ElectionCluster,
{
    let actual = cluster
        .fenced_write(token, FixtureMutationName::new(mutation))
        .await
        .map_err(|error| driver_error("attempt fenced mutation", error))?;
    if actual == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "fenced mutation `{mutation}` returned {actual:?}, expected {expected:?}"
        )))
    }
}

async fn assert_assignment_write<Cluster>(
    cluster: &mut Cluster,
    token: &Cluster::LeadershipToken,
    expected_generation: u64,
    version: FixtureVersion,
    expected: AssignmentWriteOutcome,
) -> Result<(), ScenarioError>
where
    Cluster: ElectionCluster,
{
    let actual = cluster
        .replace_assignment(token, expected_generation, version)
        .await
        .map_err(|error| driver_error("replace assignment manifest", error))?;
    if actual == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "assignment replacement returned {actual:?}, expected {expected:?}"
        )))
    }
}

async fn assert_assignment_state<Cluster>(
    cluster: &mut Cluster,
    generation: u64,
    version: FixtureVersion,
) -> Result<(), ScenarioError>
where
    Cluster: ElectionCluster,
{
    let actual = cluster
        .assignment()
        .await
        .map_err(|error| driver_error("read assignment manifest", error))?;
    let expected = Some(AssignmentManifestSnapshot {
        generation,
        version,
    });
    if actual == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "assignment state was {actual:?}, expected {expected:?}"
        )))
    }
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
