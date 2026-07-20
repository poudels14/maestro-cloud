use std::collections::BTreeSet;
use std::fmt::Debug;

use async_trait::async_trait;

use crate::{CandidateReadiness, CutoverObservation, FixtureVersion};

/// Drives readiness-gated ingress cutover with live and in-flight traffic.
#[async_trait]
pub trait CutoverCluster: Send {
    /// A matchable error returned by cutover driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Starts the initial ready deployment and routes traffic to it.
    async fn deploy_initial(&mut self, version: FixtureVersion) -> Result<(), Self::Error>;

    /// Starts a candidate deployment without changing the current routes.
    async fn deploy_candidate(
        &mut self,
        version: FixtureVersion,
        readiness: CandidateReadiness,
    ) -> Result<(), Self::Error>;

    /// Waits for and returns the deployment versions currently receiving traffic.
    async fn await_routed_versions(&mut self) -> Result<BTreeSet<FixtureVersion>, Self::Error>;

    /// Makes every candidate replica ready while preserving the current routes.
    async fn make_candidate_ready(&mut self, version: &FixtureVersion) -> Result<(), Self::Error>;

    /// Cuts traffic to the candidate while draining an in-flight old-version request.
    async fn cutover_with_inflight_request(
        &mut self,
        previous: &FixtureVersion,
        candidate: &FixtureVersion,
    ) -> Result<CutoverObservation, Self::Error>;
}
