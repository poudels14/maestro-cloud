use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::FixtureNodeName;

/// One service-scoped egress rule used by the shared acceptance scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressPolicyFixture {
    /// Destination network allowed by the fixture.
    pub cidr: String,
    /// TCP destination port allowed inside that network.
    pub port: u16,
}

/// Exact normalized firewall artifact for one logical node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressRulesetSnapshot {
    /// Node that owns this complete ruleset.
    pub node: FixtureNodeName,
    /// Exact nftables input handed to the backend.
    pub script: String,
}

/// Policy acknowledgement and exact per-node artifacts after convergence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressSnapshot {
    /// Whether the fixture policy remains persisted.
    pub policy_present: bool,
    /// Whether persisted policy status acknowledges the applied bundle.
    pub policy_acknowledged: bool,
    /// Digest shared by the policy status and applied bundle.
    pub bundle_digest: Option<String>,
    /// Complete rulesets ordered by logical node.
    pub rulesets: Vec<EgressRulesetSnapshot>,
}

/// Drives service egress policy creation and finalizer-backed deletion.
#[async_trait]
pub trait EgressCluster: Send {
    /// A matchable error returned by egress driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Returns the stable logical node names in this topology.
    fn nodes(&self) -> Vec<FixtureNodeName>;

    /// Applies one policy through the public resource mutation seam and converges it.
    async fn apply_egress_policy(
        &mut self,
        fixture: EgressPolicyFixture,
    ) -> Result<EgressSnapshot, Self::Error>;

    /// Deletes the fixture policy through its finalizer path and converges it.
    async fn delete_egress_policy(&mut self) -> Result<EgressSnapshot, Self::Error>;
}
