use std::io::Write;

use kernel_api::{Deployment, DeploymentGoal, DeploymentPhase, ServiceId};

use crate::CliError;
use crate::services::ServiceApi;

pub(crate) async fn list(
    client: &impl ServiceApi,
    service_id: String,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| CliError::invalid_input(error.to_string()))?;
    let deployments = client.list_deployments(&service_id).await?;
    write_history(&service_id, deployments, output)
}

pub(crate) fn write_history(
    service_id: &ServiceId,
    mut deployments: Vec<Deployment>,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    deployments.sort_by(|left, right| {
        right
            .status
            .created_at
            .cmp(&left.status.created_at)
            .then_with(|| left.meta.id.cmp(&right.meta.id))
    });
    if deployments.is_empty() {
        return writeln!(
            output,
            "[maestro]: no deployments found for service `{service_id}`"
        )
        .map_err(output_error);
    }
    let rows = deployments
        .iter()
        .map(DeploymentRow::from)
        .collect::<Vec<_>>();
    let widths = DeploymentWidths::for_rows(&rows);
    writeln!(
        output,
        "{:<id_width$}  {:<version_width$}  {:<phase_width$}  {:<goal_width$}  {:>generation_width$}  {:>created_width$}  {:>ready_width$}  IMAGE",
        "DEPLOYMENT",
        "VERSION",
        "PHASE",
        "GOAL",
        "GENERATION",
        "CREATED MS",
        "READY MS",
        id_width = widths.id,
        version_width = widths.version,
        phase_width = widths.phase,
        goal_width = widths.goal,
        generation_width = widths.generation,
        created_width = widths.created,
        ready_width = widths.ready,
    )
    .map_err(output_error)?;
    for row in rows {
        writeln!(
            output,
            "{:<id_width$}  {:<version_width$}  {:<phase_width$}  {:<goal_width$}  {:>generation_width$}  {:>created_width$}  {:>ready_width$}  {}",
            row.id,
            row.version,
            row.phase,
            row.goal,
            row.generation,
            row.created,
            row.ready,
            row.image,
            id_width = widths.id,
            version_width = widths.version,
            phase_width = widths.phase,
            goal_width = widths.goal,
            generation_width = widths.generation,
            created_width = widths.created,
            ready_width = widths.ready,
        )
        .map_err(output_error)?;
    }
    Ok(())
}

struct DeploymentRow {
    id: String,
    version: String,
    phase: &'static str,
    goal: &'static str,
    generation: String,
    created: String,
    ready: String,
    image: String,
}

impl From<&Deployment> for DeploymentRow {
    fn from(deployment: &Deployment) -> Self {
        Self {
            id: deployment.meta.id.to_string(),
            version: deployment.spec.service.version.clone(),
            phase: phase_name(deployment.status.phase),
            goal: goal_name(deployment.spec.goal),
            generation: deployment.spec.service_generation.0.to_string(),
            created: deployment.status.created_at.0.to_string(),
            ready: deployment
                .status
                .ready_at
                .map(|timestamp| timestamp.0.to_string())
                .unwrap_or_else(|| "-".to_string()),
            image: deployment
                .status
                .image_digest
                .clone()
                .unwrap_or_else(|| "-".to_string()),
        }
    }
}

fn phase_name(phase: DeploymentPhase) -> &'static str {
    match phase {
        DeploymentPhase::Queued => "queued",
        DeploymentPhase::Preparing => "preparing",
        DeploymentPhase::Building => "building",
        DeploymentPhase::Publishing => "publishing",
        DeploymentPhase::Starting => "starting",
        DeploymentPhase::PendingReady => "pending-ready",
        DeploymentPhase::Retrying => "retrying",
        DeploymentPhase::Ready => "ready",
        DeploymentPhase::Recovering => "recovering",
        DeploymentPhase::Stopping => "stopping",
        DeploymentPhase::Stopped => "stopped",
        DeploymentPhase::Crashed => "crashed",
        DeploymentPhase::Removed => "removed",
        DeploymentPhase::Draining => "draining",
        DeploymentPhase::Canceled => "canceled",
    }
}

fn goal_name(goal: DeploymentGoal) -> &'static str {
    match goal {
        DeploymentGoal::Run => "run",
        DeploymentGoal::Cancel => "cancel",
        DeploymentGoal::Remove => "remove",
    }
}

#[derive(Clone, Copy)]
struct DeploymentWidths {
    id: usize,
    version: usize,
    phase: usize,
    goal: usize,
    generation: usize,
    created: usize,
    ready: usize,
}

impl DeploymentWidths {
    fn for_rows(rows: &[DeploymentRow]) -> Self {
        Self {
            id: width("DEPLOYMENT", rows.iter().map(|row| row.id.as_str())),
            version: width("VERSION", rows.iter().map(|row| row.version.as_str())),
            phase: width("PHASE", rows.iter().map(|row| row.phase)),
            goal: width("GOAL", rows.iter().map(|row| row.goal)),
            generation: width("GENERATION", rows.iter().map(|row| row.generation.as_str())),
            created: width("CREATED MS", rows.iter().map(|row| row.created.as_str())),
            ready: width("READY MS", rows.iter().map(|row| row.ready.as_str())),
        }
    }
}

fn width<'a>(heading: &'a str, values: impl Iterator<Item = &'a str>) -> usize {
    values
        .map(str::len)
        .fold(heading.len(), |current, value| current.max(value))
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
