use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::{
    AssignmentManifestSnapshot, AssignmentWriteOutcome, ElectionCluster, FencedWriteOutcome,
    FixtureControllerName, FixtureMutationName, FixtureVersion, LeadershipSnapshot,
    scenarios::leader_failover_fences_stale_writes,
};

struct ElectionWorld {
    active: BTreeSet<FixtureControllerName>,
    leader: FixtureControllerName,
    token: u64,
    assignment: Option<AssignmentManifestSnapshot>,
    has_quorum: bool,
}

impl ElectionWorld {
    fn new() -> Self {
        let leader = FixtureControllerName::new("controller-a");
        Self {
            active: [leader.clone(), FixtureControllerName::new("controller-b")]
                .into_iter()
                .collect(),
            leader,
            token: 1,
            assignment: None,
            has_quorum: true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ElectionWorldError {
    #[error("controller `{0}` is not active")]
    MissingController(String),
    #[error("no controller is available for leadership")]
    NoLeader,
}

#[async_trait]
impl ElectionCluster for ElectionWorld {
    type LeadershipToken = u64;
    type Error = ElectionWorldError;

    async fn await_leader(
        &mut self,
    ) -> Result<LeadershipSnapshot<Self::LeadershipToken>, Self::Error> {
        Ok(LeadershipSnapshot {
            controller: self.leader.clone(),
            token: self.token,
        })
    }

    async fn fenced_write(
        &mut self,
        token: &Self::LeadershipToken,
        _mutation: FixtureMutationName,
    ) -> Result<FencedWriteOutcome, Self::Error> {
        if *token == self.token && self.has_quorum {
            Ok(FencedWriteOutcome::Applied)
        } else {
            Ok(FencedWriteOutcome::Rejected)
        }
    }

    async fn replace_assignment(
        &mut self,
        token: &Self::LeadershipToken,
        expected_generation: u64,
        version: FixtureVersion,
    ) -> Result<AssignmentWriteOutcome, Self::Error> {
        let generation = self
            .assignment
            .as_ref()
            .map(|assignment| assignment.generation)
            .unwrap_or(0);
        if *token != self.token {
            Ok(AssignmentWriteOutcome::LeadershipLost)
        } else if generation != expected_generation {
            Ok(AssignmentWriteOutcome::GenerationConflict)
        } else {
            self.assignment = Some(AssignmentManifestSnapshot {
                generation: generation.saturating_add(1),
                version,
            });
            Ok(AssignmentWriteOutcome::Applied)
        }
    }

    async fn assignment(&mut self) -> Result<Option<AssignmentManifestSnapshot>, Self::Error> {
        Ok(self.assignment.clone())
    }

    async fn stop_controller(
        &mut self,
        controller: &FixtureControllerName,
    ) -> Result<(), Self::Error> {
        if self.active.remove(controller) {
            self.leader = self
                .active
                .iter()
                .next()
                .cloned()
                .ok_or(ElectionWorldError::NoLeader)?;
            self.token = self.token.saturating_add(1);
            Ok(())
        } else {
            Err(ElectionWorldError::MissingController(
                controller.as_str().to_string(),
            ))
        }
    }

    async fn lose_quorum(&mut self) -> Result<(), Self::Error> {
        self.has_quorum = false;
        Ok(())
    }
}

#[tokio::test]
async fn election_scenario_fences_stale_writes_after_failover() {
    let mut cluster = ElectionWorld::new();

    leader_failover_fences_stale_writes(&mut cluster)
        .await
        .expect("election fencing scenario");
}
