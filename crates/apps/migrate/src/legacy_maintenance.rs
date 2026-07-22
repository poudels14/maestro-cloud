use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{AnnotationKey, BuiltinResource, NodeId, NodeRole};
use serde::Serialize;

use crate::LegacyEntry;
use crate::legacy_convert::LegacyPlanError;
use crate::legacy_maintenance_schema::{
    LegacyMaintenanceKind, LegacySystemUpgradeProgress, LegacySystemUpgradeStage,
    LegacyUpgradeNodeStatus, LegacyUpgradePhase, LegacyUpgradeRun,
};
use crate::legacy_nodes::LegacyNodeCatalog;

const UPGRADE_REQUEST: &str = "/maetro/system/upgrade-request";
const UPGRADE_PROGRESS: &str = "/maetro/system/upgrade-progress";
const RESTART_REQUEST: &str = "/maetro/system/restart-request";
const CLUSTER_FREEZE: &str = "/maetro/system/cluster-freeze";
const CLUSTER_UPGRADE: &str = "/maetro/cluster/upgrade/current";
const ARCHIVE_ANNOTATION: &str = "migration.maestro.dev/legacy-maintenance-state";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyMaintenanceCatalog {
    archive: LegacyMaintenanceArchive,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyMaintenanceCatalog {
    pub(crate) fn decode(
        entries: &[LegacyEntry],
        nodes: &LegacyNodeCatalog,
    ) -> Result<Self, LegacyMaintenanceError> {
        let mut run = None;
        let mut progress = BTreeMap::new();
        let mut unclaimed = Vec::new();
        for entry in entries {
            match classify_key(entry.key())? {
                Some(MaintenanceKey::UpgradeRequest | MaintenanceKey::RestartRequest) => {
                    return Err(active(entry.key(), "a node action is still requested"));
                }
                Some(MaintenanceKey::Freeze) => {
                    return Err(active(entry.key(), "the cluster is still frozen"));
                }
                Some(MaintenanceKey::Run) => {
                    let decoded: LegacyUpgradeRun = decode_json(entry)?;
                    validate_run(entry.key(), &decoded)?;
                    if !decoded.phase.is_terminal() {
                        return Err(active(
                            entry.key(),
                            format!("maintenance run `{}` is not terminal", decoded.run_id),
                        ));
                    }
                    run = Some(decoded);
                }
                Some(MaintenanceKey::Progress(scope)) => {
                    let decoded: LegacySystemUpgradeProgress = decode_json(entry)?;
                    validate_progress(entry.key(), &decoded)?;
                    progress.insert(scope, decoded);
                }
                None => unclaimed.push(entry.clone()),
            }
        }
        if let Some(node_id) = nodes.maintenance_drain() {
            return Err(active(
                node_id.to_string(),
                "a node still has an upgrade- or restart-owned drain",
            ));
        }
        Ok(Self {
            archive: LegacyMaintenanceArchive { run, progress },
            unclaimed,
        })
    }

    pub(crate) fn annotate_master(
        &self,
        resources: &mut [BuiltinResource],
    ) -> Result<(), LegacyPlanError> {
        if self.archive.run.is_none() && self.archive.progress.is_empty() {
            return Ok(());
        }
        let master = resources.iter_mut().find_map(|resource| match resource {
            BuiltinResource::Node(node) if node.spec.role == NodeRole::Master => Some(node),
            _ => None,
        });
        let master = master.ok_or_else(|| LegacyPlanError::InvalidClusterState {
            resource_id: "legacy-maintenance".to_owned(),
            message: "converted master node is missing".to_owned(),
        })?;
        let value = serde_json::to_string(&self.archive).map_err(|error| {
            LegacyPlanError::InvalidClusterState {
                resource_id: master.meta.id.to_string(),
                message: format!("could not preserve legacy maintenance state: {error}"),
            }
        })?;
        master
            .meta
            .annotations
            .insert(AnnotationKey(ARCHIVE_ANNOTATION.to_owned()), value);
        Ok(())
    }
}

fn validate_run(key: &str, run: &LegacyUpgradeRun) -> Result<(), LegacyMaintenanceError> {
    if run.nodes.is_empty()
        || run.current_node_index > run.nodes.len()
        || run.requested_at_ms < 0
        || run.phase_started_at_ms < run.requested_at_ms
        || run.updated_at_ms < run.phase_started_at_ms
    {
        return Err(invalid(key, "run shape or timestamps are inconsistent"));
    }
    match run.kind {
        LegacyMaintenanceKind::Upgrade if !valid_text(&run.target_version, 256) => {
            return Err(invalid(key, "upgrade target version is invalid"));
        }
        LegacyMaintenanceKind::Restart if !run.target_version.is_empty() => {
            return Err(invalid(
                key,
                "restart run unexpectedly has a target version",
            ));
        }
        _ => {}
    }
    if (run.phase == LegacyUpgradePhase::Succeeded && run.failure.is_some())
        || (run.phase == LegacyUpgradePhase::Failed
            && run
                .failure
                .as_deref()
                .is_none_or(|failure| !valid_text(failure, 4_096)))
    {
        return Err(invalid(key, "terminal phase and failure details disagree"));
    }
    if run.phase == LegacyUpgradePhase::Succeeded
        && run
            .nodes
            .iter()
            .any(|node| node.status != LegacyUpgradeNodeStatus::Succeeded)
    {
        return Err(invalid(
            key,
            "successful run contains a node that did not succeed",
        ));
    }
    let mut node_ids = BTreeSet::new();
    for node in &run.nodes {
        if !node_ids.insert(node.node_id.clone())
            || !valid_text(&node.hostname, 256)
            || !valid_text(&node.from_version, 256)
            || invalid_times([
                node.started_at_ms,
                node.completed_at_ms,
                node.upgrade_started_at_ms,
                node.last_upgrade_request_at_ms,
                node.restart_started_at_ms,
                node.retry_not_before_ms,
            ])
            || node
                .error
                .as_deref()
                .is_some_and(|error| !valid_text(error, 4_096))
        {
            return Err(invalid(
                key,
                "run contains an invalid or duplicate node step",
            ));
        }
        if let (Some(started), Some(completed)) = (node.started_at_ms, node.completed_at_ms)
            && completed < started
        {
            return Err(invalid(key, "node completion predates its start"));
        }
    }
    validate_history(key, run)
}

fn validate_history(key: &str, run: &LegacyUpgradeRun) -> Result<(), LegacyMaintenanceError> {
    let mut previous = run.requested_at_ms;
    for event in &run.history {
        if event.at_ms < previous
            || event.at_ms > run.updated_at_ms
            || !valid_text(&event.message, 4_096)
        {
            return Err(invalid(
                key,
                "maintenance history is malformed or unordered",
            ));
        }
        previous = event.at_ms;
    }
    if run.history.last().map(|event| event.phase) != Some(run.phase) {
        return Err(invalid(
            key,
            "maintenance history does not end at the recorded phase",
        ));
    }
    Ok(())
}

fn validate_progress(
    key: &str,
    progress: &LegacySystemUpgradeProgress,
) -> Result<(), LegacyMaintenanceError> {
    let invalid_error = match progress.stage {
        LegacySystemUpgradeStage::Failed => progress
            .error
            .as_deref()
            .is_none_or(|error| !valid_text(error, 4_096)),
        _ => progress.error.is_some(),
    };
    if !valid_text(&progress.target_version, 256)
        || progress.updated_at_ms < 0
        || progress
            .attempt_id
            .as_deref()
            .is_some_and(|attempt| !valid_token(attempt, 256))
        || invalid_error
    {
        return Err(invalid(key, "upgrade progress is malformed"));
    }
    Ok(())
}

fn classify_key(key: &str) -> Result<Option<MaintenanceKey>, LegacyMaintenanceError> {
    if key == CLUSTER_FREEZE {
        return Ok(Some(MaintenanceKey::Freeze));
    }
    if key == CLUSTER_UPGRADE {
        return Ok(Some(MaintenanceKey::Run));
    }
    for (base, maintenance_key) in [
        (UPGRADE_REQUEST, MaintenanceKey::UpgradeRequest),
        (RESTART_REQUEST, MaintenanceKey::RestartRequest),
    ] {
        if key == base || key.starts_with(&format!("{base}/")) {
            validate_scope(key, base)?;
            return Ok(Some(maintenance_key));
        }
    }
    if key == UPGRADE_PROGRESS || key.starts_with(&format!("{UPGRADE_PROGRESS}/")) {
        return validate_scope(key, UPGRADE_PROGRESS).map(|scope| {
            Some(MaintenanceKey::Progress(
                scope.unwrap_or_else(|| "$cluster".to_owned()),
            ))
        });
    }
    Ok(None)
}

fn validate_scope(key: &str, base: &str) -> Result<Option<String>, LegacyMaintenanceError> {
    if key == base {
        return Ok(None);
    }
    let scope = key
        .strip_prefix(&format!("{base}/"))
        .ok_or_else(|| invalid(key, "maintenance key is malformed"))?;
    if scope.is_empty() || scope.contains('/') {
        return Err(invalid(key, "maintenance key has an invalid node scope"));
    }
    NodeId::new(scope)
        .map(Some)
        .map(|scope| scope.map(|scope| scope.to_string()))
        .map_err(|error| invalid(key, format!("maintenance node scope is invalid: {error}")))
}

fn invalid_times<const LENGTH: usize>(values: [Option<i64>; LENGTH]) -> bool {
    values.into_iter().flatten().any(|value| value < 0)
}

fn valid_text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.trim() == value && value.len() <= max
}

fn valid_token(value: &str, max: usize) -> bool {
    valid_text(value, max)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyMaintenanceError> {
    serde_json::from_slice(entry.value())
        .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))
}

fn active(key: impl Into<String>, message: impl Into<String>) -> LegacyMaintenanceError {
    LegacyMaintenanceError::ActiveOperation {
        key: key.into(),
        message: message.into(),
    }
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyMaintenanceError {
    LegacyMaintenanceError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyMaintenanceError {
    #[error("legacy maintenance state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
    #[error("legacy maintenance is not quiescent at `{key}`: {message}")]
    ActiveOperation { key: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyMaintenanceArchive {
    #[serde(skip_serializing_if = "Option::is_none")]
    run: Option<LegacyUpgradeRun>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    progress: BTreeMap<String, LegacySystemUpgradeProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MaintenanceKey {
    UpgradeRequest,
    Progress(String),
    RestartRequest,
    Freeze,
    Run,
}
