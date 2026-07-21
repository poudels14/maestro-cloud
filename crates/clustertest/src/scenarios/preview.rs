use crate::{
    FixtureName, FixtureSourceRevision, PreviewCluster, PreviewCompletion, PreviewRolloutSnapshot,
    PreviewServiceFixture, ScenarioError,
};

const INITIAL_REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const UPDATED_REVISION: &str = "89abcdef0123456789abcdef0123456789abcdef";

/// Proves open, push, close-grace, and expiry behavior for a pull-request preview.
pub async fn pull_request_preview_completes_full_lifecycle<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: PreviewCluster,
{
    cluster
        .apply_preview_service(preview_service())
        .await
        .map_err(|error| driver_error("apply preview-enabled service", error))?;
    cluster
        .open_pull_request(FixtureSourceRevision::new(INITIAL_REVISION))
        .await
        .map_err(|error| driver_error("open pull request", error))?;
    let opened = cluster
        .await_preview_converged()
        .await
        .map_err(|error| driver_error("await opened preview", error))?;
    assert_active(&opened, INITIAL_REVISION)?;
    let identity = stable_identity(&opened)?;
    let initial_generation = opened.service_generation.ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "opened preview has no derived service generation: {opened:?}"
        ))
    })?;

    cluster
        .push_pull_request(FixtureSourceRevision::new(UPDATED_REVISION))
        .await
        .map_err(|error| driver_error("push pull-request revision", error))?;
    let pushed = cluster
        .await_preview_converged()
        .await
        .map_err(|error| driver_error("await pushed preview", error))?;
    assert_active(&pushed, UPDATED_REVISION)?;
    assert_identity(&pushed, &identity)?;
    if pushed
        .service_generation
        .is_none_or(|generation| generation <= initial_generation)
    {
        return Err(ScenarioError::Assertion(format!(
            "pull-request push did not advance the derived service generation: \
             before={opened:?}, after={pushed:?}"
        )));
    }

    cluster
        .close_pull_request()
        .await
        .map_err(|error| driver_error("close pull request", error))?;
    let closing = cluster
        .await_preview_converged()
        .await
        .map_err(|error| driver_error("await preview close grace", error))?;
    assert_identity(&closing, &identity)?;
    if closing.completion != Some(PreviewCompletion::Closing)
        || closing
            .active_revision
            .as_ref()
            .map(FixtureSourceRevision::as_str)
            != Some(UPDATED_REVISION)
    {
        return Err(ScenarioError::Assertion(format!(
            "closed preview was not retained during its grace period: {closing:?}"
        )));
    }

    cluster
        .elapse_close_grace()
        .await
        .map_err(|error| driver_error("elapse preview close grace", error))?;
    let expired = cluster
        .await_preview_converged()
        .await
        .map_err(|error| driver_error("await preview expiry", error))?;
    if expired.preview_id.is_some()
        || expired.service_id.is_some()
        || expired.public_host.is_some()
        || expired.completion.is_some()
        || expired.desired_revision.is_some()
        || expired.active_revision.is_some()
    {
        return Err(ScenarioError::Assertion(format!(
            "expired preview retained owned resources: {expired:?}"
        )));
    }
    Ok(())
}

fn preview_service() -> PreviewServiceFixture {
    PreviewServiceFixture {
        name: FixtureName::new("preview-api"),
        repository: "https://github.com/maestro-tests/preview-api.git".to_string(),
        branch: "main".to_string(),
        pull_request_number: 42,
        close_grace_period_secs: 10,
    }
}

fn assert_active(snapshot: &PreviewRolloutSnapshot, revision: &str) -> Result<(), ScenarioError> {
    if snapshot.completion == Some(PreviewCompletion::Active)
        && snapshot
            .desired_revision
            .as_ref()
            .is_some_and(|observed| observed.as_str() == revision)
        && snapshot
            .active_revision
            .as_ref()
            .is_some_and(|observed| observed.as_str() == revision)
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "preview did not serve revision `{revision}`: {snapshot:?}"
        )))
    }
}

fn stable_identity(
    snapshot: &PreviewRolloutSnapshot,
) -> Result<(FixtureName, FixtureName, String), ScenarioError> {
    match (
        snapshot.preview_id.clone(),
        snapshot.service_id.clone(),
        snapshot.public_host.clone(),
    ) {
        (Some(preview_id), Some(service_id), Some(public_host)) => {
            Ok((preview_id, service_id, public_host))
        }
        _ => Err(ScenarioError::Assertion(format!(
            "active preview did not expose stable identity and routing: {snapshot:?}"
        ))),
    }
}

fn assert_identity(
    snapshot: &PreviewRolloutSnapshot,
    expected: &(FixtureName, FixtureName, String),
) -> Result<(), ScenarioError> {
    if snapshot.preview_id.as_ref() == Some(&expected.0)
        && snapshot.service_id.as_ref() == Some(&expected.1)
        && snapshot.public_host.as_ref() == Some(&expected.2)
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "preview identity or public host changed: expected={expected:?}, observed={snapshot:?}"
        )))
    }
}

fn driver_error(operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation,
        message: error.to_string(),
    }
}
