use std::collections::BTreeSet;
use std::io::Write;

use kernel_api::{
    CommandRequest, NodeId, RequestId, UpgradeCommandResponse, UpgradeCreateRequest, UpgradeMode,
    UpgradePhase, UpgradeRun, UpgradeRunId, UpgradeRunSpec,
};

use crate::CliError;
use crate::api_client::ApiClient;

pub(crate) async fn list(client: &impl UpgradeApi, output: &mut dyn Write) -> Result<(), CliError> {
    let mut runs = client.list_upgrades().await?;
    runs.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    if runs.is_empty() {
        return writeln!(output, "[maestro]: no cluster upgrades found").map_err(output_error);
    }
    writeln!(
        output,
        "{:<28}  {:<16}  {:<9}  {:<10}  NODES",
        "UPGRADE RUN", "TARGET", "BATCH", "PHASE"
    )
    .map_err(output_error)?;
    for run in runs {
        let nodes = if run.spec.node_ids.is_empty() {
            "all".to_string()
        } else {
            run.spec
                .node_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        };
        writeln!(
            output,
            "{:<28}  {:<16}  {:<9}  {:<10}  {}",
            run.meta.id,
            run.spec.target_version,
            mode_name(run.spec.mode),
            phase_name(run.status.phase),
            nodes,
        )
        .map_err(output_error)?;
    }
    Ok(())
}

pub(crate) async fn start(
    client: &impl UpgradeApi,
    target_version: String,
    mode: UpgradeMode,
    node_ids: Vec<String>,
    upgrade_run_id: Option<String>,
    request_id: RequestId,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let target_version = target_version.trim();
    let target_version = semver::Version::parse(target_version)
        .map_err(|error| CliError::invalid_input(format!("invalid target version: {error}")))?
        .to_string();
    let node_ids = node_ids
        .into_iter()
        .map(|node_id| {
            NodeId::new(node_id).map_err(|error| CliError::invalid_input(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if node_ids.iter().collect::<BTreeSet<_>>().len() != node_ids.len() {
        return Err(CliError::invalid_input(
            "upgrade node selection contains a duplicate node",
        ));
    }
    let upgrade_run_id = upgrade_run_id
        .map(UpgradeRunId::new)
        .transpose()
        .map_err(|error| CliError::invalid_input(error.to_string()))?
        .map_or_else(generated_run_id, Ok)?;
    writeln!(
        output,
        "[maestro]: starting `{upgrade_run_id}`; retry with --upgrade-run-id {upgrade_run_id} --idempotency-key {request_id}"
    )
    .map_err(output_error)?;
    let response = client
        .start_upgrade(
            &request_id,
            UpgradeCreateRequest {
                upgrade_run_id: upgrade_run_id.clone(),
                spec: UpgradeRunSpec {
                    target_version,
                    mode,
                    node_ids,
                },
            },
        )
        .await?;
    if response.upgrade_run_id != upgrade_run_id {
        return Err(CliError::invalid_api_response(
            "upgrade receipt does not match the submitted run",
        ));
    }
    writeln!(
        output,
        "[maestro]: cluster upgrade `{}` accepted in {} phase",
        response.upgrade_run_id,
        phase_name(response.phase),
    )
    .map_err(output_error)
}

pub(crate) async fn cancel(
    client: &impl UpgradeApi,
    upgrade_run_id: String,
    request_id: RequestId,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let upgrade_run_id = UpgradeRunId::new(upgrade_run_id)
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    let run = client.get_upgrade(&upgrade_run_id).await?;
    let response = client
        .cancel_upgrade(
            &upgrade_run_id,
            &request_id,
            CommandRequest {
                expected_revision: run.meta.revision,
            },
        )
        .await?;
    if response.upgrade_run_id != upgrade_run_id || response.deletion_timestamp.is_none() {
        return Err(CliError::invalid_api_response(
            "upgrade cancellation receipt does not match the submitted run",
        ));
    }
    writeln!(
        output,
        "[maestro]: upgrade `{}` cancellation accepted",
        response.upgrade_run_id
    )
    .map_err(output_error)
}

pub(crate) trait UpgradeApi {
    async fn list_upgrades(&self) -> Result<Vec<UpgradeRun>, CliError>;

    async fn get_upgrade(&self, upgrade_run_id: &UpgradeRunId) -> Result<UpgradeRun, CliError>;

    async fn start_upgrade(
        &self,
        request_id: &RequestId,
        request: UpgradeCreateRequest,
    ) -> Result<UpgradeCommandResponse, CliError>;

    async fn cancel_upgrade(
        &self,
        upgrade_run_id: &UpgradeRunId,
        request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<UpgradeCommandResponse, CliError>;
}

impl UpgradeApi for ApiClient {
    async fn list_upgrades(&self) -> Result<Vec<UpgradeRun>, CliError> {
        self.get("/api/cluster/upgrades").await
    }

    async fn get_upgrade(&self, upgrade_run_id: &UpgradeRunId) -> Result<UpgradeRun, CliError> {
        self.get(&format!("/api/cluster/upgrades/{upgrade_run_id}"))
            .await
    }

    async fn start_upgrade(
        &self,
        request_id: &RequestId,
        request: UpgradeCreateRequest,
    ) -> Result<UpgradeCommandResponse, CliError> {
        self.post("/api/cluster/upgrades", request_id, &request)
            .await
    }

    async fn cancel_upgrade(
        &self,
        upgrade_run_id: &UpgradeRunId,
        request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<UpgradeCommandResponse, CliError> {
        self.delete(
            &format!("/api/cluster/upgrades/{upgrade_run_id}"),
            request_id,
            &request,
        )
        .await
    }
}

fn generated_run_id() -> Result<UpgradeRunId, CliError> {
    UpgradeRunId::new(format!("upgrade-{}", uuid::Uuid::new_v4().simple()))
        .map_err(|error| CliError::invalid_input(error.to_string()))
}

fn mode_name(mode: UpgradeMode) -> &'static str {
    match mode {
        UpgradeMode::Rolling => "rolling",
        UpgradeMode::AllNodes => "all",
    }
}

fn phase_name(phase: UpgradePhase) -> &'static str {
    match phase {
        UpgradePhase::Pending => "pending",
        UpgradePhase::Draining => "draining",
        UpgradePhase::Applying => "applying",
        UpgradePhase::Restarting => "restarting",
        UpgradePhase::Verifying => "verifying",
        UpgradePhase::Completed => "completed",
        UpgradePhase::Failed => "failed",
        UpgradePhase::Canceled => "canceled",
    }
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
