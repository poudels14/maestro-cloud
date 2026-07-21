use async_trait::async_trait;

use crate::{
    ArtifactBuildSnapshot, BuildCluster, BuildCompletion, BuildRolloutSnapshot,
    BuildServiceFixture, BuildSnapshot, FixtureSourceRevision,
    scenarios::{
        git_build_rolls_out_an_immutable_artifact, watched_git_revision_rolls_out_once_per_change,
    },
};

struct BuildWorld {
    remote_revision: FixtureSourceRevision,
    applied: Option<BuildServiceFixture>,
    built_revision: Option<FixtureSourceRevision>,
    watched_revision: Option<FixtureSourceRevision>,
    builds: Vec<BuildSnapshot>,
    artifact_builds: Vec<ArtifactBuildSnapshot>,
}

impl BuildWorld {
    fn new() -> Self {
        Self {
            remote_revision: FixtureSourceRevision::new("unresolved"),
            applied: None,
            built_revision: None,
            watched_revision: None,
            builds: Vec::new(),
            artifact_builds: Vec::new(),
        }
    }

    fn converge(&mut self) {
        let Some(fixture) = &self.applied else {
            return;
        };
        if self.built_revision.as_ref() == Some(&self.remote_revision) {
            return;
        }
        if fixture.watch && self.built_revision.is_some() {
            self.watched_revision = Some(self.remote_revision.clone());
        }
        self.built_revision = Some(self.remote_revision.clone());
        self.builds.push(BuildSnapshot {
            source_revision: Some(self.remote_revision.clone()),
            completion: BuildCompletion::Succeeded,
            image_digest: Some(format!("fixture@sha256:{}", self.builds.len() + 1)),
        });
        self.artifact_builds.push(ArtifactBuildSnapshot {
            arguments: fixture.arguments.clone(),
            secret_names: fixture.secret_names.clone(),
        });
    }
}

#[derive(Debug, thiserror::Error)]
#[error("build world failed")]
struct BuildWorldError;

#[async_trait]
impl BuildCluster for BuildWorld {
    type Error = BuildWorldError;

    async fn set_remote_revision(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error> {
        self.remote_revision = revision;
        Ok(())
    }

    async fn apply_build_service(
        &mut self,
        fixture: BuildServiceFixture,
    ) -> Result<(), Self::Error> {
        self.applied = Some(fixture);
        Ok(())
    }

    async fn await_build_converged(&mut self) -> Result<BuildRolloutSnapshot, Self::Error> {
        self.converge();
        Ok(BuildRolloutSnapshot {
            watched_revision: self.watched_revision.clone(),
            active_revision: self.built_revision.clone(),
            builds: self.builds.clone(),
            artifact_builds: self.artifact_builds.clone(),
        })
    }
}

#[tokio::test]
async fn build_scenarios_pass_fast_fake() {
    git_build_rolls_out_an_immutable_artifact(&mut BuildWorld::new())
        .await
        .expect("immutable Git build scenario");
    watched_git_revision_rolls_out_once_per_change(&mut BuildWorld::new())
        .await
        .expect("watched Git build scenario");
}
