use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::{
    CandidateReadiness, CutoverCluster, CutoverObservation, DrainBehavior, FixtureVersion,
    scenarios::readiness_gated_cutover_preserves_traffic_and_inflight_requests,
};

struct CutoverWorld {
    routed: BTreeSet<FixtureVersion>,
    candidate: Option<FixtureVersion>,
}

impl CutoverWorld {
    fn new() -> Self {
        Self {
            routed: BTreeSet::new(),
            candidate: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("cutover world failed")]
struct CutoverWorldError;

#[async_trait]
impl CutoverCluster for CutoverWorld {
    type Error = CutoverWorldError;

    async fn deploy_initial(&mut self, version: FixtureVersion) -> Result<(), Self::Error> {
        self.routed = [version].into_iter().collect();
        Ok(())
    }

    async fn deploy_candidate(
        &mut self,
        version: FixtureVersion,
        _readiness: CandidateReadiness,
    ) -> Result<(), Self::Error> {
        self.candidate = Some(version);
        Ok(())
    }

    async fn await_routed_versions(&mut self) -> Result<BTreeSet<FixtureVersion>, Self::Error> {
        Ok(self.routed.clone())
    }

    async fn make_candidate_ready(&mut self, _version: &FixtureVersion) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn cutover_with_inflight_request(
        &mut self,
        previous: &FixtureVersion,
        candidate: &FixtureVersion,
    ) -> Result<CutoverObservation, Self::Error> {
        self.routed = [candidate.clone()].into_iter().collect();
        Ok(CutoverObservation {
            traffic_versions: [previous.clone(), candidate.clone()].into_iter().collect(),
            public_failures: 0,
            in_flight_version: previous.clone(),
            drain_behavior: DrainBehavior::WaitedForInflight,
            final_routes: self.routed.clone(),
        })
    }
}

#[tokio::test]
async fn rollout_scenario_preserves_traffic_and_inflight_requests() {
    let mut cluster = CutoverWorld::new();

    readiness_gated_cutover_preserves_traffic_and_inflight_requests(&mut cluster)
        .await
        .expect("readiness-gated rollout scenario");
}
