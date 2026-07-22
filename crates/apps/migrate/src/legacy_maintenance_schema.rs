use kernel_api::{NodeId, UpgradeRunId};
use serde::{Deserialize, Serialize};

use crate::legacy_node_schema::LegacyNodeRole;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LegacyMaintenanceKind {
    #[default]
    Upgrade,
    Restart,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LegacyUpgradeBatch {
    #[default]
    Rolling,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LegacyUpgradePhase {
    Draining,
    AwaitingLeadershipTransfer,
    UpgradeRequested,
    SelfRestartPending,
    Verifying,
    Restoring,
    Succeeded,
    Failed,
}

impl LegacyUpgradePhase {
    pub(crate) const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LegacyUpgradeNodeStatus {
    Pending,
    Draining,
    Upgrading,
    Verifying,
    Restoring,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LegacySystemUpgradeStage {
    UpdatingSource,
    ValidatingSource,
    RebuildingSystem,
    PrebuildingImages,
    Restarting,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyUpgradeNodeStep {
    pub(crate) node_id: NodeId,
    pub(crate) hostname: String,
    pub(crate) role: LegacyNodeRole,
    pub(crate) from_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) from_instance_id: Option<String>,
    pub(crate) status: LegacyUpgradeNodeStatus,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) completed_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) upgrade_started_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) last_upgrade_request_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) upgrade_stage: Option<LegacySystemUpgradeStage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) restart_started_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retry_not_before_ms: Option<i64>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyUpgradeEvent {
    pub(crate) at_ms: i64,
    pub(crate) phase: LegacyUpgradePhase,
    pub(crate) node_id: Option<NodeId>,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyUpgradeRun {
    pub(crate) run_id: UpgradeRunId,
    #[serde(default)]
    pub(crate) kind: LegacyMaintenanceKind,
    #[serde(default)]
    pub(crate) batch: LegacyUpgradeBatch,
    pub(crate) target_version: String,
    pub(crate) requested_at_ms: i64,
    pub(crate) updated_at_ms: i64,
    pub(crate) requested_by_node_id: NodeId,
    pub(crate) phase: LegacyUpgradePhase,
    pub(crate) phase_started_at_ms: i64,
    pub(crate) current_node_index: usize,
    pub(crate) nodes: Vec<LegacyUpgradeNodeStep>,
    pub(crate) history: Vec<LegacyUpgradeEvent>,
    pub(crate) failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacySystemUpgradeProgress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<UpgradeRunId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) attempt_id: Option<String>,
    pub(crate) target_version: String,
    pub(crate) stage: LegacySystemUpgradeStage,
    pub(crate) updated_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}
