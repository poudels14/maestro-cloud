use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{FixtureNodeName, FixtureVersion};

/// A runtime instance identity used to prove a node restarted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureInstanceId(String);

impl FixtureInstanceId {
    /// Creates an observed runtime instance identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the identity as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A node's maintenance-relevant cluster role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceNodeRole {
    /// A data-plane worker with no consensus vote.
    Worker,
    /// A control-plane consensus voter.
    Voter,
}

/// Whether the scheduler may place work on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulingEligibility {
    /// The node accepts workload placement.
    Eligible,
    /// The node remains drained or otherwise unschedulable.
    Ineligible,
}

/// The observable maintenance state of one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceNodeSnapshot {
    /// Cluster role that determines safe maintenance order.
    pub role: MaintenanceNodeRole,
    /// Controller version reported by the node.
    pub version: FixtureVersion,
    /// Runtime identity changed by a successful restart.
    pub instance_id: FixtureInstanceId,
    /// Scheduling state after maintenance.
    pub scheduling: SchedulingEligibility,
}

/// The live topology used to plan a maintenance operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceTopology {
    /// Live nodes keyed by stable logical identity.
    pub nodes: BTreeMap<FixtureNodeName, MaintenanceNodeSnapshot>,
    /// Current leader, which must be upgraded after the other voters.
    pub leader: FixtureNodeName,
}

/// A deterministic failure applied to a rolling upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpgradeFault {
    /// The selected node rejects its first upgrade request.
    FailFirstAttempt { node: FixtureNodeName },
}

/// One request issued to a node during a maintenance run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceAttempt {
    /// Node receiving the request.
    pub node: FixtureNodeName,
    /// Nodes observed as drained when the request arrived.
    pub drained_nodes: BTreeSet<FixtureNodeName>,
}

/// Terminal result of a maintenance run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceCompletion {
    /// Every selected node completed maintenance.
    Succeeded,
    /// The run terminated without completing every selected node.
    Failed,
}

/// Whether the durable maintenance target survived until terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetRetention {
    /// The target remained present throughout the non-terminal run.
    RetainedUntilCompletion,
    /// The target disappeared before the run became terminal.
    ClearedEarly,
}

/// Whether a cluster-wide maintenance freeze remains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceFreeze {
    /// No freeze remains after the run.
    Cleared,
    /// The cluster is still frozen.
    Present,
}

/// Evidence produced by a rolling upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollingUpgradeObservation {
    /// Planned node order before execution began.
    pub planned_nodes: Vec<FixtureNodeName>,
    /// Upgrade requests in issue order, including retries.
    pub attempts: Vec<MaintenanceAttempt>,
    /// Terminal run result.
    pub completion: MaintenanceCompletion,
    /// Retention of the durable target while work remained.
    pub target_retention: TargetRetention,
    /// Freeze state after terminal cleanup.
    pub final_freeze: MaintenanceFreeze,
    /// Live node state after terminal cleanup.
    pub final_nodes: BTreeMap<FixtureNodeName, MaintenanceNodeSnapshot>,
}

/// Evidence produced by a selected-node restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedRestartObservation {
    /// Nodes persisted into the restart plan.
    pub planned_nodes: Vec<FixtureNodeName>,
    /// Nodes that received a restart request.
    pub requested_nodes: Vec<FixtureNodeName>,
    /// Terminal run result.
    pub completion: MaintenanceCompletion,
    /// Freeze state after terminal cleanup.
    pub final_freeze: MaintenanceFreeze,
}

/// Drives coordinated upgrades and restarts through shared acceptance scenarios.
#[async_trait]
pub trait UpgradeCluster: Send {
    /// A matchable error returned by maintenance driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Returns the live topology before maintenance begins.
    async fn topology(&mut self) -> Result<MaintenanceTopology, Self::Error>;

    /// Runs a rolling upgrade with deterministic fault injection.
    async fn rolling_upgrade(
        &mut self,
        target: FixtureVersion,
        fault: UpgradeFault,
    ) -> Result<RollingUpgradeObservation, Self::Error>;

    /// Restarts exactly one selected node through the coordinated state machine.
    async fn restart_node(
        &mut self,
        node: &FixtureNodeName,
    ) -> Result<SelectedRestartObservation, Self::Error>;
}
