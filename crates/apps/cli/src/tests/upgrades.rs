use std::sync::Mutex;

use kernel_api::{
    CommandRequest, Generation, NodeId, RequestId, Timestamp, UpgradeCommandResponse,
    UpgradeCreateRequest, UpgradeMode, UpgradePhase, UpgradeRun, UpgradeRunId,
};
use serde_json::json;

use crate::CliError;
use crate::upgrades::{UpgradeApi, cancel, list, start};

struct RecordingUpgradeApi {
    runs: Vec<UpgradeRun>,
    starts: Mutex<Vec<(RequestId, UpgradeCreateRequest)>>,
    cancels: Mutex<Vec<(UpgradeRunId, RequestId, CommandRequest)>>,
}

impl UpgradeApi for RecordingUpgradeApi {
    async fn list_upgrades(&self) -> Result<Vec<UpgradeRun>, CliError> {
        Ok(self.runs.clone())
    }

    async fn get_upgrade(&self, upgrade_run_id: &UpgradeRunId) -> Result<UpgradeRun, CliError> {
        self.runs
            .iter()
            .find(|run| &run.meta.id == upgrade_run_id)
            .cloned()
            .ok_or_else(|| CliError::not_found(format!("upgrade `{upgrade_run_id}`")))
    }

    async fn start_upgrade(
        &self,
        request_id: &RequestId,
        request: UpgradeCreateRequest,
    ) -> Result<UpgradeCommandResponse, CliError> {
        let response = UpgradeCommandResponse {
            upgrade_run_id: request.upgrade_run_id.clone(),
            generation: Generation(1),
            phase: UpgradePhase::Pending,
            deletion_timestamp: None,
        };
        self.starts
            .lock()
            .map_err(|_| poisoned())?
            .push((request_id.clone(), request));
        Ok(response)
    }

    async fn cancel_upgrade(
        &self,
        upgrade_run_id: &UpgradeRunId,
        request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<UpgradeCommandResponse, CliError> {
        self.cancels.lock().map_err(|_| poisoned())?.push((
            upgrade_run_id.clone(),
            request_id.clone(),
            request,
        ));
        Ok(UpgradeCommandResponse {
            upgrade_run_id: upgrade_run_id.clone(),
            generation: Generation(1),
            phase: UpgradePhase::Failed,
            deletion_timestamp: Some(Timestamp(100)),
        })
    }
}

#[tokio::test]
async fn upgrade_commands_preserve_selection_identity_and_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let api = api()?;
    let mut output = Vec::new();
    list(&api, &mut output).await?;
    start(
        &api,
        " 2.0.0 ".to_string(),
        UpgradeMode::AllNodes,
        vec!["node-a".to_string()],
        Some("upgrade-new".to_string()),
        RequestId::new("start-upgrade-1")?,
        &mut output,
    )
    .await?;
    cancel(
        &api,
        "upgrade-a".to_string(),
        RequestId::new("cancel-upgrade-1")?,
        &mut output,
    )
    .await?;

    let starts = api.starts.lock().map_err(|_| "start lock poisoned")?;
    let (_, request) = starts.first().ok_or("upgrade was not started")?;
    assert_eq!(request.upgrade_run_id.as_str(), "upgrade-new");
    assert_eq!(request.spec.target_version, "2.0.0");
    assert_eq!(request.spec.mode, UpgradeMode::AllNodes);
    assert_eq!(
        request.spec.node_ids.first().map(NodeId::as_str),
        Some("node-a")
    );
    let cancels = api.cancels.lock().map_err(|_| "cancel lock poisoned")?;
    let (_, request_id, request) = cancels.first().ok_or("upgrade was not canceled")?;
    assert_eq!(request_id.as_str(), "cancel-upgrade-1");
    assert_eq!(request.expected_revision.0, 9);
    let output = String::from_utf8(output)?;
    assert!(
        output
            .find("upgrade-a")
            .is_some_and(|first| output.find("upgrade-z").is_some_and(|last| first < last))
    );
    assert!(output.contains("--upgrade-run-id upgrade-new"));
    assert!(output.contains("--idempotency-key start-upgrade-1"));
    assert!(output.contains("upgrade `upgrade-a` cancellation accepted"));
    Ok(())
}

#[tokio::test]
async fn upgrade_rejects_duplicate_nodes_before_mutation() -> Result<(), Box<dyn std::error::Error>>
{
    let api = api()?;
    let error = start(
        &api,
        "2.0.0".to_string(),
        UpgradeMode::Rolling,
        vec!["node-a".to_string(), "node-a".to_string()],
        Some("upgrade-duplicate".to_string()),
        RequestId::new("upgrade-duplicate-1")?,
        &mut Vec::new(),
    )
    .await
    .expect_err("duplicate nodes must fail");
    assert!(error.to_string().contains("duplicate node"));
    assert!(
        api.starts
            .lock()
            .map_err(|_| "start lock poisoned")?
            .is_empty()
    );
    Ok(())
}

fn api() -> Result<RecordingUpgradeApi, Box<dyn std::error::Error>> {
    Ok(RecordingUpgradeApi {
        runs: vec![
            run("upgrade-z", 3, "3.0.0", "allNodes", "completed")?,
            run("upgrade-a", 9, "1.5.0", "rolling", "failed")?,
        ],
        starts: Mutex::new(Vec::new()),
        cancels: Mutex::new(Vec::new()),
    })
}

fn run(
    id: &str,
    revision: u64,
    target: &str,
    mode: &str,
    phase: &str,
) -> Result<UpgradeRun, serde_json::Error> {
    serde_json::from_value(json!({
        "meta": {"id": id, "revision": revision, "generation": 1},
        "spec": {"targetVersion": target, "mode": mode},
        "status": {"phase": phase}
    }))
}

fn poisoned() -> CliError {
    CliError::invalid_input("test lock poisoned")
}
