use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use clustertest::{
    ArtifactBuildSnapshot, BuildCluster, BuildCompletion, BuildRolloutSnapshot,
    BuildServiceFixture, BuildSnapshot, FixtureSourceRevision, scenarios,
};
use kernel_api::{
    ArtifactTemplate, BUILD_WATCH_REVISION_ANNOTATION, Build, BuildPhase, BuildSource,
    BuildTemplate, Deployment, SecretValue, Service, ServiceId,
};

use super::orchestration::{RolloutWorld, put};
use super::orchestration_fixture::service;

struct BuildAcceptanceWorld {
    inner: RolloutWorld,
    node_count: u8,
    service_id: Option<ServiceId>,
}

impl BuildAcceptanceWorld {
    async fn new(node_count: u8) -> Result<Self, BuildAcceptanceError> {
        Ok(Self {
            inner: RolloutWorld::new_empty(node_count)
                .await
                .map_err(BuildAcceptanceError::from_driver)?,
            node_count,
            service_id: None,
        })
    }

    async fn snapshot(&self) -> Result<BuildRolloutSnapshot, BuildAcceptanceError> {
        let service_id = self
            .service_id
            .as_ref()
            .ok_or_else(|| BuildAcceptanceError::new("build service was not applied"))?;
        let service = self
            .inner
            .list::<Service>("Service")
            .await
            .map_err(BuildAcceptanceError::from_driver)?
            .into_iter()
            .find(|service| &service.meta.id == service_id)
            .ok_or_else(|| BuildAcceptanceError::new("build service is missing"))?;
        let deployments = self
            .inner
            .list::<Deployment>("Deployment")
            .await
            .map_err(BuildAcceptanceError::from_driver)?;
        let mut builds = self
            .inner
            .list::<Build>("Build")
            .await
            .map_err(BuildAcceptanceError::from_driver)?;
        builds.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
        let active_revision = service
            .status
            .active_deployment_id
            .as_ref()
            .and_then(|active_id| {
                deployments
                    .iter()
                    .find(|deployment| &deployment.meta.id == active_id)
            })
            .and_then(|deployment| deployment.spec.build_id.as_ref())
            .and_then(|build_id| builds.iter().find(|build| &build.meta.id == build_id))
            .and_then(|build| build.status.source_revision.as_deref())
            .map(FixtureSourceRevision::new);
        let watched_revision = service
            .meta
            .annotations
            .get(&kernel_api::AnnotationKey(
                BUILD_WATCH_REVISION_ANNOTATION.to_string(),
            ))
            .cloned()
            .map(FixtureSourceRevision::new);
        let builds = builds.into_iter().map(project_build).collect();
        let artifact_builds = self
            .inner
            .build_backend
            .builds()
            .into_iter()
            .map(|request| ArtifactBuildSnapshot {
                arguments: request
                    .arguments
                    .into_iter()
                    .map(|(name, value)| (name, value.expose().to_owned()))
                    .collect(),
                secret_names: request.secrets.into_keys().collect(),
            })
            .collect();
        Ok(BuildRolloutSnapshot {
            watched_revision,
            active_revision,
            builds,
            artifact_builds,
        })
    }
}

#[async_trait]
impl BuildCluster for BuildAcceptanceWorld {
    type Error = BuildAcceptanceError;

    async fn set_remote_revision(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error> {
        self.inner.build_backend.set_revision(revision.as_str());
        Ok(())
    }

    async fn apply_build_service(
        &mut self,
        fixture: BuildServiceFixture,
    ) -> Result<(), Self::Error> {
        let service_id =
            ServiceId::new(fixture.name.as_str()).map_err(BuildAcceptanceError::from_driver)?;
        let mut resource =
            service(u32::from(self.node_count)).map_err(BuildAcceptanceError::from_driver)?;
        resource.meta.id = service_id.clone();
        resource.spec.name = fixture.name.as_str().to_string();
        resource.spec.artifact = ArtifactTemplate::Build {
            template: BuildTemplate {
                source: BuildSource::Git {
                    repository: fixture.repository,
                    revision: fixture.branch,
                },
                dockerfile: fixture.dockerfile,
                watch: fixture.watch,
                registry: None,
                depot: None,
                environment: fixture.arguments,
                environment_source: None,
                secrets: fixture
                    .secret_names
                    .into_iter()
                    .map(|name| {
                        let value = SecretValue::new(format!("acceptance-{name}"));
                        (name, value)
                    })
                    .collect::<BTreeMap<_, _>>(),
                secrets_source: None,
            },
        };
        put(&self.inner.store, &self.inner.keys, "Service", &resource)
            .await
            .map_err(BuildAcceptanceError::from_driver)?;
        self.service_id = Some(service_id);
        Ok(())
    }

    async fn await_build_converged(&mut self) -> Result<BuildRolloutSnapshot, Self::Error> {
        self.inner
            .converge()
            .await
            .map_err(BuildAcceptanceError::from_driver)?;
        self.snapshot().await
    }
}

fn project_build(build: Build) -> BuildSnapshot {
    let completion = match build.status.phase {
        BuildPhase::Queued | BuildPhase::Preparing | BuildPhase::Building => {
            BuildCompletion::Running
        }
        BuildPhase::Succeeded => BuildCompletion::Succeeded,
        BuildPhase::Failed => BuildCompletion::Failed,
        BuildPhase::Canceled => BuildCompletion::Canceled,
    };
    BuildSnapshot {
        source_revision: build.status.source_revision.map(FixtureSourceRevision::new),
        completion,
        image_digest: build.status.image_digest,
    }
}

#[derive(Debug)]
struct BuildAcceptanceError(String);

impl BuildAcceptanceError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    fn from_driver(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

impl Display for BuildAcceptanceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for BuildAcceptanceError {}

#[tokio::test]
async fn shared_build_scenarios_drive_composed_operators()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        scenarios::git_build_rolls_out_an_immutable_artifact(
            &mut BuildAcceptanceWorld::new(node_count).await?,
        )
        .await?;
        scenarios::watched_git_revision_rolls_out_once_per_change(
            &mut BuildAcceptanceWorld::new(node_count).await?,
        )
        .await?;
    }
    Ok(())
}
