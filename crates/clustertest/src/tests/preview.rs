use async_trait::async_trait;

use crate::{
    FixtureName, FixtureSourceRevision, PreviewCluster, PreviewCompletion, PreviewRolloutSnapshot,
    PreviewServiceFixture, scenarios::pull_request_preview_completes_full_lifecycle,
};

#[derive(Default)]
struct PreviewWorld {
    fixture: Option<PreviewServiceFixture>,
    revision: Option<FixtureSourceRevision>,
    open: bool,
    grace_elapsed: bool,
    generation: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("preview world failed")]
struct PreviewWorldError;

#[async_trait]
impl PreviewCluster for PreviewWorld {
    type Error = PreviewWorldError;

    async fn apply_preview_service(
        &mut self,
        fixture: PreviewServiceFixture,
    ) -> Result<(), Self::Error> {
        self.fixture = Some(fixture);
        Ok(())
    }

    async fn open_pull_request(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error> {
        self.revision = Some(revision);
        self.open = true;
        self.generation = 1;
        Ok(())
    }

    async fn push_pull_request(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error> {
        self.revision = Some(revision);
        self.generation = self.generation.saturating_add(1);
        Ok(())
    }

    async fn close_pull_request(&mut self) -> Result<(), Self::Error> {
        self.open = false;
        Ok(())
    }

    async fn elapse_close_grace(&mut self) -> Result<(), Self::Error> {
        self.grace_elapsed = true;
        Ok(())
    }

    async fn await_preview_converged(&mut self) -> Result<PreviewRolloutSnapshot, Self::Error> {
        let Some(fixture) = &self.fixture else {
            return Ok(empty_snapshot());
        };
        if !self.open && self.grace_elapsed {
            return Ok(empty_snapshot());
        }
        let identity = FixtureName::new(format!(
            "{}-pr-{}",
            fixture.name.as_str(),
            fixture.pull_request_number
        ));
        Ok(PreviewRolloutSnapshot {
            preview_id: Some(identity.clone()),
            completion: Some(if self.open {
                PreviewCompletion::Active
            } else {
                PreviewCompletion::Closing
            }),
            service_id: Some(identity.clone()),
            service_generation: Some(self.generation),
            desired_revision: self.revision.clone(),
            active_revision: self.revision.clone(),
            public_host: Some(format!("{}.preview.example.test", identity.as_str())),
        })
    }
}

fn empty_snapshot() -> PreviewRolloutSnapshot {
    PreviewRolloutSnapshot {
        preview_id: None,
        completion: None,
        service_id: None,
        service_generation: None,
        desired_revision: None,
        active_revision: None,
        public_host: None,
    }
}

#[tokio::test]
async fn preview_scenario_passes_fast_fake() {
    pull_request_preview_completes_full_lifecycle(&mut PreviewWorld::default())
        .await
        .expect("full preview lifecycle scenario");
}
