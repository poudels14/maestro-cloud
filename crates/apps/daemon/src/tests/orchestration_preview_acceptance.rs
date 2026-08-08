use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use clustertest::{
    FixtureName, FixtureSourceRevision, PreviewCluster, PreviewCompletion, PreviewRolloutSnapshot,
    PreviewServiceFixture, scenarios,
};
use kernel_api::{
    ArtifactTemplate, Build, BuildSource, BuildTemplate, Deployment, IngressRoute, IngressRouteId,
    Preview, PreviewPhase, PreviewPolicy, RolloutState, Service, ServiceId, Timestamp,
};
use preview::{
    PullRequest, PullRequestApi, PullRequestApiError, PullRequestDeployment, PullRequestReadiness,
};

use super::orchestration::{RolloutWorld, put};
use super::orchestration_fixture::{route, service};

struct PreviewAcceptanceWorld {
    inner: RolloutWorld,
    pull_requests: Arc<FakePullRequests>,
    node_count: u8,
    fixture: Option<PreviewServiceFixture>,
}

impl PreviewAcceptanceWorld {
    async fn new(node_count: u8) -> Result<Self, PreviewAcceptanceError> {
        let pull_requests = Arc::new(FakePullRequests::default());
        let inner = RolloutWorld::new_empty_with_previews(node_count, pull_requests.clone())
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        Ok(Self {
            inner,
            pull_requests,
            node_count,
            fixture: None,
        })
    }

    async fn snapshot(&self) -> Result<PreviewRolloutSnapshot, PreviewAcceptanceError> {
        let fixture = self
            .fixture
            .as_ref()
            .ok_or_else(|| PreviewAcceptanceError::new("preview service was not applied"))?;
        let expected_id = format!(
            "{}-pr-{}",
            fixture.name.as_str(),
            fixture.pull_request_number
        );
        let previews = self
            .inner
            .list::<Preview>("Preview")
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        let preview = previews
            .iter()
            .find(|preview| preview.meta.id.as_str() == expected_id);
        let services = self
            .inner
            .list::<Service>("Service")
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        let derived = services
            .iter()
            .find(|service| service.meta.id.as_str() == expected_id);
        let routes = self
            .inner
            .list::<IngressRoute>("IngressRoute")
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        let public_host = derived.and_then(|service| {
            routes
                .iter()
                .find(|route| route.spec.service_id == service.meta.id)
                .and_then(|route| route.spec.hosts.first().cloned())
        });
        let builds = self
            .inner
            .list::<Build>("Build")
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        let deployments = self
            .inner
            .list::<Deployment>("Deployment")
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        let active_revision = derived
            .and_then(|service| service.status.active_deployment_id.as_ref())
            .and_then(|active_id| {
                deployments
                    .iter()
                    .find(|deployment| &deployment.meta.id == active_id)
            })
            .and_then(|deployment| deployment.spec.build_id.as_ref())
            .and_then(|build_id| builds.iter().find(|build| &build.meta.id == build_id))
            .and_then(|build| build.status.source_revision.as_deref())
            .map(FixtureSourceRevision::new);
        let desired_revision = derived
            .and_then(|service| match &service.spec.artifact {
                ArtifactTemplate::Build { template } => match &template.source {
                    BuildSource::Git { revision, .. } => Some(revision.as_str()),
                    BuildSource::Tarball { .. } => None,
                },
                ArtifactTemplate::Image { .. } => None,
            })
            .map(FixtureSourceRevision::new);
        Ok(PreviewRolloutSnapshot {
            preview_id: preview.map(|preview| FixtureName::new(preview.meta.id.to_string())),
            completion: preview
                .map(|preview| project_phase(preview.status.phase))
                .transpose()?,
            service_id: derived.map(|service| FixtureName::new(service.meta.id.to_string())),
            service_generation: derived.map(|service| service.meta.generation.0),
            desired_revision,
            active_revision,
            public_host,
        })
    }

    fn fixture(&self) -> Result<&PreviewServiceFixture, PreviewAcceptanceError> {
        self.fixture
            .as_ref()
            .ok_or_else(|| PreviewAcceptanceError::new("preview service was not applied"))
    }
}

#[async_trait]
impl PreviewCluster for PreviewAcceptanceWorld {
    type Error = PreviewAcceptanceError;

    async fn apply_preview_service(
        &mut self,
        fixture: PreviewServiceFixture,
    ) -> Result<(), Self::Error> {
        let service_id =
            ServiceId::new(fixture.name.as_str()).map_err(PreviewAcceptanceError::from_driver)?;
        let mut base =
            service(u32::from(self.node_count)).map_err(PreviewAcceptanceError::from_driver)?;
        base.meta.id = service_id.clone();
        base.spec.name = fixture.name.as_str().to_string();
        base.spec.artifact = ArtifactTemplate::Build {
            template: BuildTemplate {
                source: BuildSource::Git {
                    repository: fixture.repository.clone(),
                    revision: fixture.branch.clone(),
                },
                dockerfile: "Dockerfile".to_string(),
                watch: false,
                registry: None,
                registry_repository: None,
                depot: None,
                environment: BTreeMap::new(),
                environment_source: None,
                secrets: BTreeMap::new(),
                secrets_source: None,
            },
        };
        base.spec.preview = Some(PreviewPolicy {
            close_grace_period_secs: fixture.close_grace_period_secs,
            lifetime_secs: 3_600,
            replicas: 1,
            environment: BTreeMap::from([("MAESTRO_PREVIEW".to_string(), "true".to_string())]),
            environment_source: None,
        });
        base.status.rollout = RolloutState::Active;
        let mut base_route = route().map_err(PreviewAcceptanceError::from_driver)?;
        base_route.meta.id = IngressRouteId::new(format!("{}-route", fixture.name.as_str()))
            .map_err(PreviewAcceptanceError::from_driver)?;
        base_route.spec.service_id = service_id;
        base_route.spec.hosts = vec![format!("{}.example.test", fixture.name.as_str())];
        put(&self.inner.store, &self.inner.keys, "Service", &base)
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        put(
            &self.inner.store,
            &self.inner.keys,
            "IngressRoute",
            &base_route,
        )
        .await
        .map_err(PreviewAcceptanceError::from_driver)?;
        self.fixture = Some(fixture);
        Ok(())
    }

    async fn open_pull_request(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error> {
        self.inner.build_backend.set_revision(revision.as_str());
        let fixture = self.fixture()?;
        self.pull_requests.set_open(PullRequest {
            number: fixture.pull_request_number,
            title: "Acceptance preview".to_string(),
            author: "maestro-test".to_string(),
            readiness: PullRequestReadiness::Ready,
            created_at: Timestamp(10_000),
            head_reference: "feature/acceptance".to_string(),
            head_revision: revision.as_str().to_string(),
            head_repository: Some("maestro-tests/preview-api".to_string()),
        });
        Ok(())
    }

    async fn push_pull_request(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error> {
        self.pull_requests.set_revision(revision.as_str())?;
        self.inner.build_backend.set_revision(revision.as_str());
        Ok(())
    }

    async fn close_pull_request(&mut self) -> Result<(), Self::Error> {
        self.pull_requests.close();
        Ok(())
    }

    async fn elapse_close_grace(&mut self) -> Result<(), Self::Error> {
        let grace_millis = i64::try_from(
            self.fixture()?
                .close_grace_period_secs
                .saturating_mul(1_000),
        )
        .unwrap_or(i64::MAX);
        self.inner.set_time(10_001_i64.saturating_add(grace_millis));
        self.inner
            .converge()
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        self.inner.set_time(20_001_i64.saturating_add(grace_millis));
        Ok(())
    }

    async fn await_preview_converged(&mut self) -> Result<PreviewRolloutSnapshot, Self::Error> {
        self.inner
            .converge()
            .await
            .map_err(PreviewAcceptanceError::from_driver)?;
        self.snapshot().await
    }
}

#[derive(Default)]
struct FakePullRequests {
    open: Mutex<Option<PullRequest>>,
}

impl FakePullRequests {
    fn set_open(&self, pull_request: PullRequest) {
        *lock(&self.open) = Some(pull_request);
    }

    fn set_revision(&self, revision: &str) -> Result<(), PreviewAcceptanceError> {
        let mut open = lock(&self.open);
        let pull_request = open
            .as_mut()
            .ok_or_else(|| PreviewAcceptanceError::new("pull request is not open"))?;
        pull_request.head_revision = revision.to_string();
        Ok(())
    }

    fn close(&self) {
        *lock(&self.open) = None;
    }
}

#[async_trait]
impl PullRequestApi for FakePullRequests {
    async fn list_open(
        &self,
        owner: &str,
        repository: &str,
    ) -> Result<Vec<PullRequest>, PullRequestApiError> {
        if owner != "maestro-tests" || repository != "preview-api" {
            return Err(PullRequestApiError::Rejected {
                message: format!("unexpected repository `{owner}/{repository}`"),
            });
        }
        Ok(lock(&self.open).iter().cloned().collect())
    }

    async fn publish_deployment(
        &self,
        owner: &str,
        repository: &str,
        _deployment: &PullRequestDeployment,
    ) -> Result<(), PullRequestApiError> {
        if owner == "maestro-tests" && repository == "preview-api" {
            Ok(())
        } else {
            Err(PullRequestApiError::Rejected {
                message: format!("unexpected repository `{owner}/{repository}`"),
            })
        }
    }
}

fn project_phase(phase: PreviewPhase) -> Result<PreviewCompletion, PreviewAcceptanceError> {
    match phase {
        PreviewPhase::Pending => Ok(PreviewCompletion::Pending),
        PreviewPhase::Active => Ok(PreviewCompletion::Active),
        PreviewPhase::Closing => Ok(PreviewCompletion::Closing),
        PreviewPhase::Expired => Ok(PreviewCompletion::Expired),
        PreviewPhase::Failed | PreviewPhase::Canceled => Err(PreviewAcceptanceError::new(format!(
            "preview reached unexpected terminal phase {phase:?}"
        ))),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[derive(Debug)]
struct PreviewAcceptanceError(String);

impl PreviewAcceptanceError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    fn from_driver(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

impl Display for PreviewAcceptanceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PreviewAcceptanceError {}

#[tokio::test]
async fn shared_preview_scenario_drives_composed_operators()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        scenarios::pull_request_preview_completes_full_lifecycle(
            &mut PreviewAcceptanceWorld::new(node_count).await?,
        )
        .await?;
    }
    Ok(())
}
