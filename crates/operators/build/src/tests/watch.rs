use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    AnnotationKey, BUILD_WATCH_REVISION_ANNOTATION, BuildSource, DeploymentPhase, RolloutState,
    Service,
};
use kernel_store::{CasOutcome, ExpectedVersion, PutRequest, Store};

use super::watch_support::{FakeRevisionResolver, WatchTestResult, WatchWorld, fixture};
use crate::BuildSourceError;

const REVISION_A: &str = "0123456789abcdef0123456789abcdef01234567";
const REVISION_B: &str = "1123456789abcdef0123456789abcdef01234567";
const REVISION_C: &str = "2123456789abcdef0123456789abcdef01234567";

#[test]
fn watcher_rejects_a_zero_poll_interval() -> WatchTestResult {
    let result = crate::BuildWatchReconciler::new(
        kernel_api::ClusterId::new("cluster-1")?,
        Arc::new(FakeRevisionResolver::fixed(REVISION_A)),
        crate::BuildWatchSettings {
            poll_interval: Duration::ZERO,
        },
    );

    assert!(matches!(
        result,
        Err(crate::BuildWatchError::ZeroPollInterval)
    ));
    Ok(())
}

#[tokio::test]
async fn watcher_persists_a_new_remote_revision() -> WatchTestResult {
    let world = WatchWorld::new().await?;
    let (service, deployment, build) = fixture(DeploymentPhase::Ready, Some(REVISION_A), "main")?;
    world.seed(&service, &deployment, &build).await?;
    let resolver = Arc::new(FakeRevisionResolver::fixed(REVISION_B));
    let runtime = world.runtime(resolver.clone())?;

    assert_eq!(runtime.reconcile_snapshot().await?, 1);

    assert_eq!(desired_revision(&world.service().await?), Some(REVISION_B));
    assert_eq!(resolver.calls().len(), 1);
    assert_eq!(
        resolver.github_tokens(),
        [Some("github-watch-secret".to_owned())]
    );
    Ok(())
}

#[tokio::test]
async fn watcher_does_not_rewrite_an_unchanged_revision() -> WatchTestResult {
    let world = WatchWorld::new().await?;
    let (service, deployment, build) = fixture(DeploymentPhase::Ready, Some(REVISION_A), "main")?;
    world.seed(&service, &deployment, &build).await?;
    let before = world.stored_service().await?.version;
    let resolver = Arc::new(FakeRevisionResolver::fixed(REVISION_A));

    world.runtime(resolver)?.reconcile_snapshot().await?;

    assert_eq!(world.stored_service().await?.version, before);
    assert!(desired_revision(&world.service().await?).is_none());
    Ok(())
}

#[tokio::test]
async fn malformed_dependency_is_quarantined_until_repaired() -> WatchTestResult {
    let world = WatchWorld::new().await?;
    let (service, deployment, build) = fixture(DeploymentPhase::Ready, Some(REVISION_A), "main")?;
    world.seed(&service, &deployment, &build).await?;
    let deployment_key = world.deployment_key()?;
    let resolver = Arc::new(FakeRevisionResolver::fixed(REVISION_B));
    let runtime = world.runtime(resolver.clone())?;

    let stored = world
        .store
        .get(&deployment_key)
        .await?
        .ok_or("deployment missing")?;
    assert!(matches!(
        world
            .store
            .put_cas(PutRequest {
                key: deployment_key.clone(),
                value: b"not-json".to_vec(),
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert!(resolver.calls().is_empty());

    let stored = world
        .store
        .get(&deployment_key)
        .await?
        .ok_or("malformed deployment missing")?;
    let mut misidentified = deployment.clone();
    misidentified.meta.id = kernel_api::DeploymentId::new("different-deployment")?;
    assert!(matches!(
        world
            .store
            .put_cas(PutRequest {
                key: deployment_key.clone(),
                value: serde_json::to_vec(&misidentified)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert!(resolver.calls().is_empty());

    let stored = world
        .store
        .get(&deployment_key)
        .await?
        .ok_or("misidentified deployment missing")?;
    assert!(matches!(
        world
            .store
            .put_cas(PutRequest {
                key: deployment_key,
                value: serde_json::to_vec(&deployment)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(desired_revision(&world.service().await?), Some(REVISION_B));
    assert_eq!(resolver.calls().len(), 1);
    Ok(())
}

#[tokio::test]
async fn frozen_or_inflight_rollout_suppresses_remote_polling() -> WatchTestResult {
    for frozen in [false, true] {
        let world = WatchWorld::new().await?;
        let (mut service, deployment, build) = fixture(
            if frozen {
                DeploymentPhase::Ready
            } else {
                DeploymentPhase::Building
            },
            Some(REVISION_A),
            "main",
        )?;
        if frozen {
            service.status.rollout = RolloutState::Frozen;
        }
        world.seed(&service, &deployment, &build).await?;
        let resolver = Arc::new(FakeRevisionResolver::fixed(REVISION_B));

        world
            .runtime(resolver.clone())?
            .reconcile_snapshot()
            .await?;

        assert!(resolver.calls().is_empty());
        assert!(desired_revision(&world.service().await?).is_none());
    }
    Ok(())
}

#[tokio::test]
async fn pending_desired_revision_is_not_skipped_by_a_newer_remote_head() -> WatchTestResult {
    let world = WatchWorld::new().await?;
    let (mut service, deployment, build) =
        fixture(DeploymentPhase::Ready, Some(REVISION_A), "main")?;
    service.meta.annotations.insert(
        AnnotationKey(BUILD_WATCH_REVISION_ANNOTATION.to_string()),
        REVISION_B.to_string(),
    );
    world.seed(&service, &deployment, &build).await?;
    let resolver = Arc::new(FakeRevisionResolver::fixed(REVISION_C));

    world
        .runtime(resolver.clone())?
        .reconcile_snapshot()
        .await?;

    assert!(resolver.calls().is_empty());
    assert_eq!(desired_revision(&world.service().await?), Some(REVISION_B));
    Ok(())
}

#[tokio::test]
async fn transient_remote_failure_retries_without_advancing_revision() -> WatchTestResult {
    let world = WatchWorld::new().await?;
    let (service, deployment, build) = fixture(DeploymentPhase::Ready, Some(REVISION_A), "main")?;
    world.seed(&service, &deployment, &build).await?;
    let resolver = Arc::new(FakeRevisionResolver::new(vec![
        Err(BuildSourceError::unavailable("remote timed out")),
        Ok(Some(REVISION_B.to_string())),
    ]));
    let runtime = world.runtime(resolver.clone())?;

    runtime.reconcile_snapshot().await?;
    assert!(desired_revision(&world.service().await?).is_none());
    runtime.reconcile_snapshot().await?;

    assert_eq!(desired_revision(&world.service().await?), Some(REVISION_B));
    assert_eq!(resolver.calls().len(), 2);
    Ok(())
}

#[tokio::test]
async fn failed_build_without_a_resolved_source_does_not_loop() -> WatchTestResult {
    let world = WatchWorld::new().await?;
    let (service, deployment, build) = fixture(DeploymentPhase::Crashed, None, "main")?;
    world.seed(&service, &deployment, &build).await?;
    let resolver = Arc::new(FakeRevisionResolver::fixed(REVISION_B));

    world
        .runtime(resolver.clone())?
        .reconcile_snapshot()
        .await?;

    assert!(resolver.calls().is_empty());
    assert!(desired_revision(&world.service().await?).is_none());
    Ok(())
}

#[tokio::test]
async fn concurrent_service_update_wins_remote_poll_cas() -> WatchTestResult {
    let world = WatchWorld::new().await?;
    let (service, deployment, build) = fixture(DeploymentPhase::Ready, Some(REVISION_A), "main")?;
    world.seed(&service, &deployment, &build).await?;
    let resolver = Arc::new(RacingResolver {
        store: world.store.clone(),
        key: world.service_key()?,
    });

    world.runtime(resolver)?.reconcile_snapshot().await?;

    assert_eq!(desired_revision(&world.service().await?), Some(REVISION_C));
    Ok(())
}

fn desired_revision(service: &kernel_api::Service) -> Option<&str> {
    service
        .meta
        .annotations
        .get(&AnnotationKey(BUILD_WATCH_REVISION_ANNOTATION.to_string()))
        .map(String::as_str)
}

struct RacingResolver {
    store: Arc<kernel_store::InMemoryStore>,
    key: kernel_store::StoreKey,
}

#[async_trait]
impl crate::BuildRevisionResolver for RacingResolver {
    async fn resolve_revision(
        &self,
        _source: &BuildSource,
        _github_token: Option<&kernel_api::SecretValue>,
    ) -> Result<Option<String>, BuildSourceError> {
        let stored = self
            .store
            .get(&self.key)
            .await
            .map_err(|error| BuildSourceError::unavailable(error.to_string()))?
            .ok_or_else(|| BuildSourceError::rejected("racing Service disappeared"))?;
        let mut service: Service = serde_json::from_slice(&stored.value)
            .map_err(|error| BuildSourceError::rejected(error.to_string()))?;
        service.meta.annotations.insert(
            AnnotationKey(BUILD_WATCH_REVISION_ANNOTATION.to_string()),
            REVISION_C.to_string(),
        );
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self.key.clone(),
                value: serde_json::to_vec(&service)
                    .map_err(|error| BuildSourceError::rejected(error.to_string()))?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await
            .map_err(|error| BuildSourceError::unavailable(error.to_string()))?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(Some(REVISION_B.to_string()))
        } else {
            Err(BuildSourceError::unavailable(
                "injected Service update conflicted",
            ))
        }
    }
}
