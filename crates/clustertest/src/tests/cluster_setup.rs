use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::{
    ClusterSetupCluster, FixtureNodeName, scenarios::cluster_bootstraps_joins_meshes_and_recovers,
};

#[tokio::test]
async fn setup_scenario_covers_one_and_three_node_topologies()
-> Result<(), Box<dyn std::error::Error>> {
    let mut one = SetupWorld::new(1);
    cluster_bootstraps_joins_meshes_and_recovers(&mut one).await?;
    assert_eq!(
        one.actions,
        vec!["bootstrap:node-1", "mesh:node-1", "write",]
    );

    let mut three = SetupWorld::new(3);
    cluster_bootstraps_joins_meshes_and_recovers(&mut three).await?;
    assert_eq!(
        three.actions,
        vec![
            "bootstrap:node-1",
            "join:node-2",
            "join:node-3",
            "mesh:node-1,node-2,node-3",
            "write",
            "ping:node-1:node-2",
            "stop:node-2",
            "write",
            "restart:node-2",
            "mesh:node-1,node-2,node-3",
            "ping:node-1:node-2",
        ]
    );
    Ok(())
}

struct SetupWorld {
    nodes: Vec<FixtureNodeName>,
    running: BTreeSet<FixtureNodeName>,
    actions: Vec<String>,
}

impl SetupWorld {
    fn new(count: usize) -> Self {
        Self {
            nodes: (1..=count)
                .map(|index| FixtureNodeName::new(format!("node-{index}")))
                .collect(),
            running: BTreeSet::new(),
            actions: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("setup world failed: {0}")]
struct SetupWorldError(String);

#[async_trait]
impl ClusterSetupCluster for SetupWorld {
    type Error = SetupWorldError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.nodes.clone()
    }

    async fn bootstrap_seed(&mut self) -> Result<(), Self::Error> {
        let seed = self
            .nodes
            .first()
            .cloned()
            .ok_or_else(|| SetupWorldError("missing seed".to_owned()))?;
        self.actions.push(format!("bootstrap:{}", seed.as_str()));
        self.running.insert(seed);
        Ok(())
    }

    async fn join_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        self.actions.push(format!("join:{}", node.as_str()));
        self.running.insert(node.clone());
        Ok(())
    }

    async fn await_mesh(
        &mut self,
        expected: &BTreeSet<FixtureNodeName>,
    ) -> Result<BTreeSet<FixtureNodeName>, Self::Error> {
        self.actions.push(format!(
            "mesh:{}",
            expected
                .iter()
                .map(FixtureNodeName::as_str)
                .collect::<Vec<_>>()
                .join(",")
        ));
        Ok(self.running.clone())
    }

    async fn ping_workload(
        &mut self,
        source: &FixtureNodeName,
        target: &FixtureNodeName,
    ) -> Result<(), Self::Error> {
        if !self.running.contains(source) || !self.running.contains(target) {
            return Err(SetupWorldError("ping endpoint is down".to_owned()));
        }
        self.actions
            .push(format!("ping:{}:{}", source.as_str(), target.as_str()));
        Ok(())
    }

    async fn stop_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        self.actions.push(format!("stop:{}", node.as_str()));
        self.running.remove(node);
        Ok(())
    }

    async fn restart_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        self.actions.push(format!("restart:{}", node.as_str()));
        self.running.insert(node.clone());
        Ok(())
    }

    async fn verify_store_write(&mut self) -> Result<(), Self::Error> {
        let quorum = self.nodes.len() / 2 + 1;
        if self.running.len() < quorum {
            return Err(SetupWorldError("quorum unavailable".to_owned()));
        }
        self.actions.push("write".to_owned());
        Ok(())
    }
}
