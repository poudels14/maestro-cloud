use std::sync::Arc;

use kernel_api::{Build, BuildPhase, Condition, ConditionState};
use runtime::ArtifactSource;

use super::support::{
    RecordingArtifacts, RecordingSource, TestResult, TestWorld, prepared, queued_build,
};
use crate::BuildSourceError;

#[tokio::test]
async fn queued_build_pins_source_and_persists_immutable_digest() -> TestResult {
    let world = TestWorld::new().await?;
    world
        .seed(&queued_build("containers/api.Dockerfile")?)
        .await?;
    let source = Arc::new(RecordingSource::successful("commit-abc"));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller = world.runtime(source.clone(), artifacts.clone())?;

    assert_eq!(controller.reconcile_snapshot().await?, 0);
    assert_eq!(controller.reconcile_snapshot().await?, 1);
    assert_eq!(world.build().await?.status.phase, BuildPhase::Preparing);
    assert_eq!(controller.reconcile_snapshot().await?, 1);
    let building = world.build().await?;
    assert_eq!(building.status.phase, BuildPhase::Building);
    assert_eq!(
        building.status.source_revision.as_deref(),
        Some("commit-abc")
    );
    assert_eq!(controller.reconcile_snapshot().await?, 1);

    let succeeded = world.build().await?;
    assert_eq!(succeeded.status.phase, BuildPhase::Succeeded);
    assert_eq!(
        succeeded.status.image_digest.as_deref(),
        Some("sha256:abc123")
    );
    let condition = only_condition(&succeeded)?;
    assert_eq!(condition.state, ConditionState::True);
    assert_eq!(condition.reason.0, "BuildSucceeded");
    assert_eq!(condition.last_transition_time.0, 12_345);
    assert_eq!(source.calls(), [None, Some("commit-abc".to_string())]);
    assert_eq!(
        source.github_tokens(),
        [
            Some("github-secret".to_owned()),
            Some("github-secret".to_owned())
        ]
    );

    let requests = artifacts.calls();
    assert_eq!(requests.len(), 1);
    let request = requests.first().ok_or("artifact request missing")?;
    assert_eq!(
        request.arguments.get("PROFILE").map(String::as_str),
        Some("release")
    );
    assert_eq!(
        request.secrets.get("TOKEN").map(|secret| secret.expose()),
        Some("secret-value")
    );
    assert_eq!(
        request
            .secrets
            .get("GH_TOKEN")
            .map(|secret| secret.expose()),
        Some("github-secret")
    );
    assert!(request.tags.is_empty());
    assert_eq!(
        request.source,
        ArtifactSource::Directory {
            root: "/var/lib/maestro/builds/build-1".into(),
            definition: "containers/api.Dockerfile".into(),
        }
    );
    Ok(())
}

#[tokio::test]
async fn permanent_source_rejection_is_recorded_as_failed() -> TestResult {
    let world = TestWorld::new().await?;
    world.seed(&queued_build("Dockerfile")?).await?;
    let source = Arc::new(RecordingSource::new(vec![Err(BuildSourceError::rejected(
        "revision does not exist",
    ))]));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller = world.runtime(source, artifacts.clone())?;

    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;

    let failed = world.build().await?;
    assert_eq!(failed.status.phase, BuildPhase::Failed);
    let condition = only_condition(&failed)?;
    assert_eq!(condition.reason.0, "SourceRejected");
    assert_eq!(condition.message, "revision does not exist");
    assert!(artifacts.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn transient_source_failure_keeps_preparing_phase_for_retry() -> TestResult {
    let world = TestWorld::new().await?;
    world.seed(&queued_build("Dockerfile")?).await?;
    let source = Arc::new(RecordingSource::new(vec![
        Err(BuildSourceError::unavailable("git host timed out")),
        Ok(prepared("commit-after-retry")),
    ]));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller = world.runtime(source, artifacts)?;

    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    assert_eq!(world.build().await?.status.phase, BuildPhase::Preparing);

    controller.reconcile_snapshot().await?;
    let building = world.build().await?;
    assert_eq!(building.status.phase, BuildPhase::Building);
    assert_eq!(
        building.status.source_revision.as_deref(),
        Some("commit-after-retry")
    );
    Ok(())
}

#[tokio::test]
async fn invalid_definition_fails_before_source_is_materialized() -> TestResult {
    let world = TestWorld::new().await?;
    world.seed(&queued_build("../Dockerfile")?).await?;
    let source = Arc::new(RecordingSource::successful("commit-abc"));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller = world.runtime(source.clone(), artifacts)?;

    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;

    let failed = world.build().await?;
    assert_eq!(failed.status.phase, BuildPhase::Failed);
    assert_eq!(only_condition(&failed)?.reason.0, "InvalidBuildDefinition");
    assert!(source.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn concurrent_cancellation_wins_after_artifact_side_effect() -> TestResult {
    let world = TestWorld::new().await?;
    world.seed(&queued_build("Dockerfile")?).await?;
    let source = Arc::new(RecordingSource::successful("commit-abc"));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller = world.runtime(source, artifacts.clone())?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    artifacts.race_with_cancellation(world.store.clone(), world.build_key()?);

    controller.reconcile_snapshot().await?;

    let canceled = world.build().await?;
    assert_eq!(canceled.status.phase, BuildPhase::Canceled);
    assert!(canceled.status.image_digest.is_none());
    assert_eq!(artifacts.calls().len(), 1);
    Ok(())
}

#[tokio::test]
async fn source_revision_drift_fails_without_building() -> TestResult {
    let world = TestWorld::new().await?;
    world.seed(&queued_build("Dockerfile")?).await?;
    let source = Arc::new(RecordingSource::new(vec![
        Ok(prepared("commit-abc")),
        Ok(prepared("commit-new")),
    ]));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller = world.runtime(source, artifacts.clone())?;

    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;

    let failed = world.build().await?;
    assert_eq!(failed.status.phase, BuildPhase::Failed);
    assert_eq!(only_condition(&failed)?.reason.0, "SourceRevisionChanged");
    assert!(artifacts.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn deletion_finalizer_cleans_source_before_removing_build() -> TestResult {
    let world = TestWorld::new().await?;
    world.seed(&queued_build("Dockerfile")?).await?;
    let source = Arc::new(RecordingSource::successful("commit-abc"));
    let controller = world.runtime(source.clone(), Arc::new(RecordingArtifacts::successful()?))?;
    controller.reconcile_snapshot().await?;
    world.mark_deleting().await?;

    assert_eq!(controller.reconcile_snapshot().await?, 1);

    assert!(!world.build_exists().await?);
    assert_eq!(
        source.cleanup_calls(),
        [kernel_api::BuildId::new("build-1")?]
    );
    Ok(())
}

fn only_condition(build: &Build) -> TestResult<&Condition> {
    if build.status.conditions.len() != 1 {
        return Err(format!(
            "expected one Build condition, found {}",
            build.status.conditions.len()
        )
        .into());
    }
    build
        .status
        .conditions
        .first()
        .ok_or_else(|| "Build condition missing".into())
}
