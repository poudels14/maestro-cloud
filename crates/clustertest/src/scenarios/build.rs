use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BuildCluster, BuildCompletion, BuildRolloutSnapshot, BuildServiceFixture, FixtureName,
    FixtureSourceRevision, ScenarioError,
};

const INITIAL_REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const UPDATED_REVISION: &str = "89abcdef0123456789abcdef0123456789abcdef";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GitWatchMode {
    Disabled,
    Enabled,
}

/// Proves Git source pinning, secret delivery, and deployment of the built artifact.
pub async fn git_build_rolls_out_an_immutable_artifact<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: BuildCluster,
{
    cluster
        .set_remote_revision(FixtureSourceRevision::new(INITIAL_REVISION))
        .await
        .map_err(|error| driver_error("set initial Git revision", error))?;
    cluster
        .apply_build_service(build_service(GitWatchMode::Disabled))
        .await
        .map_err(|error| driver_error("apply Git build service", error))?;
    let snapshot = cluster
        .await_build_converged()
        .await
        .map_err(|error| driver_error("await Git build rollout", error))?;

    assert_active_revision(&snapshot, INITIAL_REVISION)?;
    if snapshot.watched_revision.is_some()
        || snapshot.builds.len() != 1
        || snapshot.artifact_builds.len() != 1
    {
        return Err(ScenarioError::Assertion(format!(
            "non-watched Git build did not produce exactly one artifact: {snapshot:?}"
        )));
    }
    let build = snapshot
        .builds
        .first()
        .ok_or_else(|| ScenarioError::Assertion("build snapshot was empty".to_string()))?;
    if build.completion != BuildCompletion::Succeeded || build.image_digest.is_none() {
        return Err(ScenarioError::Assertion(format!(
            "Git build did not persist an immutable artifact: {build:?}"
        )));
    }
    let request = snapshot.artifact_builds.first().ok_or_else(|| {
        ScenarioError::Assertion("artifact request snapshot was empty".to_string())
    })?;
    if request.arguments != build_arguments() || request.secret_names != build_secrets() {
        return Err(ScenarioError::Assertion(format!(
            "artifact backend did not receive the declared build inputs: {request:?}"
        )));
    }
    Ok(())
}

/// Proves watched commits cause one new rollout and unchanged polling is write-free.
pub async fn watched_git_revision_rolls_out_once_per_change<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: BuildCluster,
{
    cluster
        .set_remote_revision(FixtureSourceRevision::new(INITIAL_REVISION))
        .await
        .map_err(|error| driver_error("set initial watched revision", error))?;
    cluster
        .apply_build_service(build_service(GitWatchMode::Enabled))
        .await
        .map_err(|error| driver_error("apply watched Git service", error))?;
    let initial = cluster
        .await_build_converged()
        .await
        .map_err(|error| driver_error("await initial watched rollout", error))?;
    assert_active_revision(&initial, INITIAL_REVISION)?;
    if initial.watched_revision.is_some() {
        return Err(ScenarioError::Assertion(format!(
            "initial build redundantly persisted a watched revision: {initial:?}"
        )));
    }
    let initial_artifacts = initial.artifact_builds.len();

    cluster
        .set_remote_revision(FixtureSourceRevision::new(UPDATED_REVISION))
        .await
        .map_err(|error| driver_error("advance watched revision", error))?;
    let updated = cluster
        .await_build_converged()
        .await
        .map_err(|error| driver_error("await watched redeploy", error))?;
    assert_watched_revision(&updated, UPDATED_REVISION)?;
    assert_active_revision(&updated, UPDATED_REVISION)?;
    if updated.artifact_builds.len() != initial_artifacts.saturating_add(1)
        || count_revision(&updated, UPDATED_REVISION) != 1
    {
        return Err(ScenarioError::Assertion(format!(
            "one Git change did not produce exactly one new build: {updated:?}"
        )));
    }

    let unchanged = cluster
        .await_build_converged()
        .await
        .map_err(|error| driver_error("reconcile unchanged watched revision", error))?;
    if unchanged != updated {
        return Err(ScenarioError::Assertion(format!(
            "unchanged Git polling mutated build state: before={updated:?}, after={unchanged:?}"
        )));
    }
    Ok(())
}

fn build_service(watch_mode: GitWatchMode) -> BuildServiceFixture {
    BuildServiceFixture {
        name: FixtureName::new("build-api"),
        repository: "https://github.com/maestro-tests/build-api.git".to_string(),
        branch: "main".to_string(),
        dockerfile: "Dockerfile".to_string(),
        watch: watch_mode == GitWatchMode::Enabled,
        arguments: build_arguments(),
        secret_names: build_secrets(),
    }
}

fn build_arguments() -> BTreeMap<String, String> {
    BTreeMap::from([("PROFILE".to_string(), "release".to_string())])
}

fn build_secrets() -> BTreeSet<String> {
    BTreeSet::from(["GH_TOKEN".to_string(), "PACKAGE_TOKEN".to_string()])
}

fn assert_watched_revision(
    snapshot: &BuildRolloutSnapshot,
    expected: &str,
) -> Result<(), ScenarioError> {
    if snapshot
        .watched_revision
        .as_ref()
        .is_some_and(|revision| revision.as_str() == expected)
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "Git watcher did not persist revision `{expected}`: {snapshot:?}"
        )))
    }
}

fn assert_active_revision(
    snapshot: &BuildRolloutSnapshot,
    expected: &str,
) -> Result<(), ScenarioError> {
    if snapshot
        .active_revision
        .as_ref()
        .is_some_and(|revision| revision.as_str() == expected)
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "active deployment did not consume revision `{expected}`: {snapshot:?}"
        )))
    }
}

fn count_revision(snapshot: &BuildRolloutSnapshot, expected: &str) -> usize {
    snapshot
        .builds
        .iter()
        .filter(|build| {
            build
                .source_revision
                .as_ref()
                .is_some_and(|revision| revision.as_str() == expected)
        })
        .count()
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
