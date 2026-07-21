use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use clustertest::{
    EgressCluster, EgressPolicyFixture, EgressRulesetSnapshot, EgressSnapshot, FixtureNodeName,
    scenarios,
};
use kernel_api::{FirewallPolicy, PortRange, ResourceKind, ResourceName, Timestamp};
use kernel_store::{CasOutcome, ExpectedVersion, PutRequest, Store};

use super::orchestration::{RolloutWorld, put};
use super::orchestration_fixture::egress_policy;

struct EgressWorld {
    inner: RolloutWorld,
    node_count: u8,
}

impl EgressWorld {
    async fn new(node_count: u8) -> Result<Self, EgressError> {
        let inner = RolloutWorld::new(node_count)
            .await
            .map_err(EgressError::from_driver)?;
        inner.converge().await.map_err(EgressError::from_driver)?;
        Ok(Self { inner, node_count })
    }

    async fn snapshot(&self) -> Result<EgressSnapshot, EgressError> {
        let policies = self
            .inner
            .list::<FirewallPolicy>("FirewallPolicy")
            .await
            .map_err(EgressError::from_driver)?;
        let bundle = self
            .inner
            .latest_firewall_bundle()
            .map_err(EgressError::from_driver)?;
        let policy = policies.first();
        let policy_acknowledged = policy.is_some_and(|policy| {
            policy.status.applied_generation == policy.meta.generation
                && policy.status.ruleset_digest.as_deref() == Some(bundle.digest.as_str())
        });
        let rulesets = bundle
            .rulesets
            .into_iter()
            .map(|ruleset| EgressRulesetSnapshot {
                node: FixtureNodeName::new(ruleset.node_id.as_str()),
                script: ruleset.script,
            })
            .collect();
        Ok(EgressSnapshot {
            policy_present: policy.is_some(),
            policy_acknowledged,
            bundle_digest: policy_acknowledged.then_some(bundle.digest),
            rulesets,
        })
    }
}

#[async_trait]
impl EgressCluster for EgressWorld {
    type Error = EgressError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        (1..=self.node_count)
            .map(|index| FixtureNodeName::new(format!("node-{index}")))
            .collect()
    }

    async fn apply_egress_policy(
        &mut self,
        fixture: EgressPolicyFixture,
    ) -> Result<EgressSnapshot, Self::Error> {
        let mut policy = egress_policy().map_err(EgressError::from_driver)?;
        let rule = policy
            .spec
            .rules
            .first_mut()
            .ok_or_else(|| EgressError::new("egress fixture has no rule"))?;
        rule.cidr = fixture.cidr;
        rule.ports = vec![PortRange {
            start: fixture.port,
            end: fixture.port,
        }];
        put(
            &self.inner.store,
            &self.inner.keys,
            "FirewallPolicy",
            &policy,
        )
        .await
        .map_err(EgressError::from_driver)?;
        self.inner
            .converge()
            .await
            .map_err(EgressError::from_driver)?;
        self.snapshot().await
    }

    async fn delete_egress_policy(&mut self) -> Result<EgressSnapshot, Self::Error> {
        let policy = self
            .inner
            .list::<FirewallPolicy>("FirewallPolicy")
            .await
            .map_err(EgressError::from_driver)?
            .into_iter()
            .next()
            .ok_or_else(|| EgressError::new("egress policy is absent"))?;
        let key = self.inner.keys.resource(
            &ResourceKind::new("FirewallPolicy").map_err(EgressError::from_driver)?,
            &ResourceName::from(policy.meta.id),
        );
        let stored = self
            .inner
            .store
            .get(&key)
            .await
            .map_err(EgressError::from_driver)?
            .ok_or_else(|| EgressError::new("egress policy disappeared"))?;
        let mut policy: FirewallPolicy =
            serde_json::from_slice(&stored.value).map_err(EgressError::from_driver)?;
        policy.meta.deletion_timestamp = Some(Timestamp(10_000));
        let outcome = self
            .inner
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&policy).map_err(EgressError::from_driver)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await
            .map_err(EgressError::from_driver)?;
        if !matches!(outcome, CasOutcome::Applied(_)) {
            return Err(EgressError::new("egress policy deletion conflicted"));
        }
        self.inner
            .converge()
            .await
            .map_err(EgressError::from_driver)?;
        self.snapshot().await
    }
}

#[derive(Debug)]
struct EgressError(String);

impl EgressError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    fn from_driver(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

impl Display for EgressError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for EgressError {}

#[tokio::test]
async fn shared_egress_scenario_drives_composed_firewall_operator()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        scenarios::egress_policy_applies_and_deletes_atomically(
            &mut EgressWorld::new(node_count).await?,
        )
        .await?;
    }
    Ok(())
}
