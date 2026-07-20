use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::{
    ControlPlaneReadiness, FixtureMarker, FixtureNodeName, QuorumRecoveryCluster, ReadinessProbe,
    scenarios::all_voter_restart_waits_for_quorum_and_preserves_state,
};

struct QuorumWorld {
    nodes: Vec<FixtureNodeName>,
    running: BTreeSet<FixtureNodeName>,
    values: BTreeMap<String, FixtureMarker>,
}

impl QuorumWorld {
    fn new() -> Self {
        let nodes = (1..=3)
            .map(|node_number| FixtureNodeName::new(format!("node-{node_number}")))
            .collect::<Vec<_>>();
        Self {
            running: nodes.iter().cloned().collect(),
            nodes,
            values: BTreeMap::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum QuorumWorldError {
    #[error("node `{0}` does not exist")]
    MissingNode(String),
}

#[async_trait]
impl QuorumRecoveryCluster for QuorumWorld {
    type Error = QuorumWorldError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.nodes.clone()
    }

    async fn write_marker(&mut self, marker: FixtureMarker) -> Result<(), Self::Error> {
        self.values.insert("marker".to_string(), marker);
        Ok(())
    }

    async fn stop_all_nodes(&mut self) -> Result<(), Self::Error> {
        self.running.clear();
        Ok(())
    }

    async fn start_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        if self.nodes.contains(node) {
            self.running.insert(node.clone());
            Ok(())
        } else {
            Err(QuorumWorldError::MissingNode(node.as_str().to_string()))
        }
    }

    async fn probe_readiness(
        &mut self,
        _probe: ReadinessProbe,
    ) -> Result<ControlPlaneReadiness, Self::Error> {
        if self.running.len() > self.nodes.len() / 2 {
            Ok(ControlPlaneReadiness::Ready)
        } else {
            Ok(ControlPlaneReadiness::Unavailable)
        }
    }

    async fn read_marker(&mut self) -> Result<Option<FixtureMarker>, Self::Error> {
        Ok(self.values.get("marker").cloned())
    }
}

#[tokio::test]
async fn all_voter_restart_scenario_waits_for_quorum_and_preserves_state() {
    let mut cluster = QuorumWorld::new();

    all_voter_restart_waits_for_quorum_and_preserves_state(&mut cluster)
        .await
        .expect("all-voter restart scenario");
}
