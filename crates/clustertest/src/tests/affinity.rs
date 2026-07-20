use async_trait::async_trait;

use crate::{
    AffinityCluster, AffinityCookieSet, AffinityObservation, AffinitySession, FixtureAffinityToken,
    FixtureNodeName, scenarios::affinity_is_opaque_sticky_and_overridable,
};

struct AffinityWorld {
    nodes: Vec<FixtureNodeName>,
}

impl AffinityWorld {
    fn new() -> Self {
        Self {
            nodes: (1..=3)
                .map(|node_number| FixtureNodeName::new(format!("node-{node_number}")))
                .collect(),
        }
    }

    fn observation(&self, node: FixtureNodeName) -> AffinityObservation {
        let node_number = self
            .nodes
            .iter()
            .position(|candidate| *candidate == node)
            .unwrap_or_default()
            .saturating_add(1);
        AffinityObservation {
            node,
            token: FixtureAffinityToken::new(format!("opaque-token-{node_number}")),
            cookies: AffinityCookieSet::Complete,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("affinity world failed")]
struct AffinityWorldError;

#[async_trait]
impl AffinityCluster for AffinityWorld {
    type Session = FixtureNodeName;
    type Error = AffinityWorldError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.nodes.clone()
    }

    async fn establish_affinity(&mut self) -> Result<AffinitySession<Self::Session>, Self::Error> {
        let node = self.nodes.first().cloned().ok_or(AffinityWorldError)?;
        Ok(AffinitySession {
            session: node.clone(),
            initial: self.observation(node),
        })
    }

    async fn replay_affinity(
        &mut self,
        session: &Self::Session,
    ) -> Result<AffinityObservation, Self::Error> {
        Ok(self.observation(session.clone()))
    }

    async fn override_affinity(
        &mut self,
        _session: &Self::Session,
        node: &FixtureNodeName,
    ) -> Result<AffinityObservation, Self::Error> {
        Ok(self.observation(node.clone()))
    }
}

#[tokio::test]
async fn affinity_scenario_is_opaque_sticky_and_overridable() {
    let mut cluster = AffinityWorld::new();

    affinity_is_opaque_sticky_and_overridable(&mut cluster)
        .await
        .expect("affinity scenario");
}
