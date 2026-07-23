use async_trait::async_trait;

use crate::{
    EgressCluster, EgressPolicyFixture, EgressRulesetSnapshot, EgressSnapshot, FixtureNodeName,
    scenarios::egress_policy_applies_and_deletes_atomically,
};

struct EgressWorld {
    nodes: Vec<FixtureNodeName>,
    policy: Option<EgressPolicyFixture>,
    bundle_revision: u64,
}

impl EgressWorld {
    fn new(node_count: usize) -> Self {
        Self {
            nodes: (1..=node_count)
                .map(|index| FixtureNodeName::new(format!("node-{index}")))
                .collect(),
            policy: None,
            bundle_revision: 0,
        }
    }

    fn snapshot(&self, policy_acknowledged: bool) -> EgressSnapshot {
        EgressSnapshot {
            policy_present: self.policy.is_some(),
            policy_acknowledged,
            bundle_digest: self
                .policy
                .as_ref()
                .map(|_| format!("fixture-bundle-{}", self.bundle_revision)),
            rulesets: self
                .nodes
                .iter()
                .cloned()
                .map(|node| EgressRulesetSnapshot {
                    script: self.render_ruleset(&node),
                    node,
                })
                .collect(),
        }
    }

    fn render_ruleset(&self, node: &FixtureNodeName) -> String {
        let policy = self.policy.as_ref().map_or_else(
            || "    # no service egress policy\n".to_owned(),
            |fixture| {
                format!(
                    "    ip daddr {} tcp dport {} accept\n    reject\n",
                    fixture.cidr, fixture.port
                )
            },
        );
        format!(
            "# complete ruleset for {}\ntable inet maestro_firewall {{\n{policy}}}\n",
            node.as_str()
        )
    }
}

#[derive(Debug, thiserror::Error)]
#[error("egress world requires at least one node")]
struct EgressWorldError;

#[async_trait]
impl EgressCluster for EgressWorld {
    type Error = EgressWorldError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.nodes.clone()
    }

    async fn apply_egress_policy(
        &mut self,
        fixture: EgressPolicyFixture,
    ) -> Result<EgressSnapshot, Self::Error> {
        if self.nodes.is_empty() {
            return Err(EgressWorldError);
        }
        self.policy = Some(fixture);
        self.bundle_revision += 1;
        Ok(self.snapshot(true))
    }

    async fn delete_egress_policy(&mut self) -> Result<EgressSnapshot, Self::Error> {
        if self.nodes.is_empty() {
            return Err(EgressWorldError);
        }
        self.policy = None;
        self.bundle_revision += 1;
        Ok(self.snapshot(false))
    }
}

#[tokio::test]
async fn egress_policy_scenario_covers_one_and_three_node_topologies()
-> Result<(), Box<dyn std::error::Error>> {
    for node_count in [1, 3] {
        let mut world = EgressWorld::new(node_count);

        egress_policy_applies_and_deletes_atomically(&mut world).await?;

        assert_eq!(world.nodes.len(), node_count);
        assert!(world.policy.is_none());
        assert_eq!(world.bundle_revision, 2);
    }
    Ok(())
}
