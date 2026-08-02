use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use kernel_api::{Build, BuildPhase, Condition, ConditionState, DepotBuildConfig, SecretValue};
use runtime::{
    ArtifactBuildRequest, ArtifactDigest, ArtifactSource, ArtifactStoreError, ValueSourceError,
    ValueSourceResolver,
};

use super::support::{
    RecordingArtifacts, RecordingSource, TestResult, TestWorld, prepared, queued_build,
};
use crate::{BuildSourceError, DepotBuildBackend};

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
        request.arguments.get("PROFILE").map(SecretValue::expose),
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
async fn external_build_values_are_resolved_for_execution_without_persisting_plaintext()
-> TestResult {
    let world = TestWorld::new().await?;
    let mut build = queued_build("Dockerfile")?;
    build.spec.template.environment.clear();
    build.spec.template.environment_source = Some("aws-secret://build-environment".to_owned());
    build.spec.template.secrets.clear();
    build.spec.template.secrets_source = Some("aws-secret://build-secrets".to_owned());
    world.seed(&build).await?;
    let resolver = Arc::new(FixedValueSources {
        values: BTreeMap::from([
            (
                "aws-secret://build-environment".to_owned(),
                BTreeMap::from([("PROFILE".to_owned(), SecretValue::new("external-profile"))]),
            ),
            (
                "aws-secret://build-secrets".to_owned(),
                BTreeMap::from([
                    (
                        "GH_TOKEN".to_owned(),
                        SecretValue::new("external-github-token"),
                    ),
                    ("TOKEN".to_owned(), SecretValue::new("external-build-token")),
                ]),
            ),
        ]),
        calls: AtomicUsize::new(0),
    });
    let source = Arc::new(RecordingSource::successful("commit-abc"));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller =
        world.runtime_with_value_sources(source.clone(), artifacts.clone(), resolver.clone())?;

    for _ in 0..4 {
        controller.reconcile_snapshot().await?;
    }

    assert_eq!(resolver.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        source.github_tokens(),
        [
            Some("external-github-token".to_owned()),
            Some("external-github-token".to_owned())
        ]
    );
    let requests = artifacts.calls();
    let request = requests.first().ok_or("artifact request missing")?;
    assert_eq!(
        request.arguments.get("PROFILE").map(SecretValue::expose),
        Some("external-profile")
    );
    assert_eq!(
        request.secrets.get("TOKEN").map(|value| value.expose()),
        Some("external-build-token")
    );

    let stored = world.build().await?;
    assert!(stored.spec.template.environment.is_empty());
    assert!(stored.spec.template.secrets.is_empty());
    assert_eq!(
        stored.spec.template.environment_source.as_deref(),
        Some("aws-secret://build-environment")
    );
    assert_eq!(
        stored.spec.template.secrets_source.as_deref(),
        Some("aws-secret://build-secrets")
    );
    let encoded = serde_json::to_string(&stored)?;
    assert!(!encoded.contains("external-build-token"));
    Ok(())
}

struct FixedValueSources {
    values: BTreeMap<String, BTreeMap<String, SecretValue>>,
    calls: AtomicUsize,
}

#[async_trait]
impl ValueSourceResolver for FixedValueSources {
    async fn resolve(
        &self,
        source: &str,
    ) -> Result<BTreeMap<String, SecretValue>, ValueSourceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.values
            .get(source)
            .cloned()
            .ok_or_else(|| ValueSourceError::Rejected {
                message: format!("missing fake source `{source}`"),
            })
    }
}

#[tokio::test]
async fn registry_build_publishes_a_deployment_unique_immutable_reference() -> TestResult {
    let world = TestWorld::new().await?;
    let mut build = queued_build("Dockerfile")?;
    build.spec.template.registry = Some("registry.example/team".to_owned());
    world.seed(&build).await?;
    let source = Arc::new(RecordingSource::successful("commit-abc"));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller = world.runtime(source, artifacts.clone())?;

    assert_eq!(controller.reconcile_snapshot().await?, 0);
    assert_eq!(controller.reconcile_snapshot().await?, 1);
    assert_eq!(controller.reconcile_snapshot().await?, 1);
    assert_eq!(controller.reconcile_snapshot().await?, 1);

    let succeeded = world.build().await?;
    assert_eq!(succeeded.status.phase, BuildPhase::Succeeded);
    assert_eq!(
        succeeded.status.image_digest.as_deref(),
        Some("registry.example/team/api@sha256:abc123")
    );
    assert_eq!(
        artifacts.publishes(),
        [(
            runtime::ArtifactDigest::new("sha256:abc123")?,
            runtime::ArtifactReference::new("registry.example/team/api:deployment-1")?,
        )]
    );
    Ok(())
}

#[tokio::test]
async fn depot_build_uses_remote_backend_before_registry_publication() -> TestResult {
    let world = TestWorld::new().await?;
    let mut build = queued_build("Dockerfile")?;
    build.spec.template.depot = Some(DepotBuildConfig {
        project: "project-123".to_owned(),
    });
    build.spec.template.registry = Some("registry.example/team".to_owned());
    world.seed(&build).await?;
    let source = Arc::new(RecordingSource::successful("commit-abc"));
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let depot = Arc::new(RecordingDepot::new()?);
    let controller = world.runtime_with_depot(source, artifacts.clone(), Some(depot.clone()))?;

    assert_eq!(controller.reconcile_snapshot().await?, 0);
    assert_eq!(controller.reconcile_snapshot().await?, 1);
    assert_eq!(controller.reconcile_snapshot().await?, 1);
    assert_eq!(controller.reconcile_snapshot().await?, 1);

    assert!(artifacts.calls().is_empty());
    assert_eq!(depot.projects(), ["project-123".to_owned()]);
    assert_eq!(depot.requests().len(), 1);
    assert_eq!(
        world.build().await?.status.image_digest.as_deref(),
        Some("registry.example/team/api@sha256:depot")
    );
    assert_eq!(
        artifacts.publishes(),
        [(
            ArtifactDigest::new("sha256:depot")?,
            runtime::ArtifactReference::new("registry.example/team/api:deployment-1")?,
        )]
    );
    Ok(())
}

#[tokio::test]
async fn depot_build_fails_cleanly_when_cluster_has_no_token() -> TestResult {
    let world = TestWorld::new().await?;
    let mut build = queued_build("Dockerfile")?;
    build.spec.template.depot = Some(DepotBuildConfig {
        project: "project-123".to_owned(),
    });
    world.seed(&build).await?;
    let artifacts = Arc::new(RecordingArtifacts::successful()?);
    let controller = world.runtime(
        Arc::new(RecordingSource::successful("commit-abc")),
        artifacts.clone(),
    )?;

    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;
    controller.reconcile_snapshot().await?;

    let failed = world.build().await?;
    assert_eq!(failed.status.phase, BuildPhase::Failed);
    assert_eq!(only_condition(&failed)?.reason.0, "ArtifactBuildRejected");
    assert!(
        only_condition(&failed)?
            .message
            .contains("cluster has no Depot token")
    );
    assert!(artifacts.calls().is_empty());
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

struct RecordingDepot {
    digest: ArtifactDigest,
    projects: Mutex<Vec<String>>,
    requests: Mutex<Vec<ArtifactBuildRequest>>,
}

impl RecordingDepot {
    fn new() -> TestResult<Self> {
        Ok(Self {
            digest: ArtifactDigest::new("sha256:depot")?,
            projects: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        })
    }

    fn projects(&self) -> Vec<String> {
        lock(&self.projects).clone()
    }

    fn requests(&self) -> Vec<ArtifactBuildRequest> {
        lock(&self.requests).clone()
    }
}

#[async_trait]
impl DepotBuildBackend for RecordingDepot {
    async fn build(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        lock(&self.requests).push(request.clone());
        lock(&self.projects).push(project.to_owned());
        Ok(self.digest.clone())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
