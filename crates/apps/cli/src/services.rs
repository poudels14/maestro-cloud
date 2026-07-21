use std::io::Write;

use kernel_api::{ArtifactTemplate, RolloutState, Service};

use crate::CliError;
use crate::api_client::ApiClient;

pub(crate) async fn list(client: &ApiClient, output: &mut dyn Write) -> Result<(), CliError> {
    let services = client.get::<Vec<Service>>("/api/services").await?;
    write_services(services, output)
}

pub(crate) fn write_services(
    mut services: Vec<Service>,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    services.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    if services.is_empty() {
        writeln!(output, "[maestro]: no services found")
            .map_err(|source| CliError::io("failed to write command output", source))?;
        return Ok(());
    }
    let rows = services.iter().map(ServiceRow::from).collect::<Vec<_>>();
    let widths = ColumnWidths::for_rows(&rows);
    writeln!(
        output,
        "{:<id_width$}  {:<name_width$}  {:<version_width$}  {:>replicas_width$}  {:<rollout_width$}  {:<active_width$}  ARTIFACT",
        "ID",
        "NAME",
        "VERSION",
        "REPLICAS",
        "ROLLOUT",
        "ACTIVE DEPLOYMENT",
        id_width = widths.id,
        name_width = widths.name,
        version_width = widths.version,
        replicas_width = widths.replicas,
        rollout_width = widths.rollout,
        active_width = widths.active,
    )
    .map_err(|source| CliError::io("failed to write command output", source))?;
    for row in rows {
        writeln!(
            output,
            "{:<id_width$}  {:<name_width$}  {:<version_width$}  {:>replicas_width$}  {:<rollout_width$}  {:<active_width$}  {}",
            row.id,
            row.name,
            row.version,
            row.replicas,
            row.rollout,
            row.active,
            row.artifact,
            id_width = widths.id,
            name_width = widths.name,
            version_width = widths.version,
            replicas_width = widths.replicas,
            rollout_width = widths.rollout,
            active_width = widths.active,
        )
        .map_err(|source| CliError::io("failed to write command output", source))?;
    }
    Ok(())
}

struct ServiceRow {
    id: String,
    name: String,
    version: String,
    replicas: String,
    rollout: &'static str,
    active: String,
    artifact: String,
}

impl From<&Service> for ServiceRow {
    fn from(service: &Service) -> Self {
        let replicas = match service.status.replica_override {
            Some(replicas) => format!("{replicas}*"),
            None => service.spec.replicas.to_string(),
        };
        let rollout = match service.status.rollout {
            RolloutState::Active => "active",
            RolloutState::Frozen => "frozen",
        };
        let active = service
            .status
            .active_deployment_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_string());
        let artifact = match &service.spec.artifact {
            ArtifactTemplate::Image { reference } => reference.clone(),
            ArtifactTemplate::Build { .. } => "build".to_string(),
        };
        Self {
            id: service.meta.id.to_string(),
            name: service.spec.name.clone(),
            version: service.spec.version.clone(),
            replicas,
            rollout,
            active,
            artifact,
        }
    }
}

#[derive(Clone, Copy)]
struct ColumnWidths {
    id: usize,
    name: usize,
    version: usize,
    replicas: usize,
    rollout: usize,
    active: usize,
}

impl ColumnWidths {
    fn for_rows(rows: &[ServiceRow]) -> Self {
        Self {
            id: width("ID", rows.iter().map(|row| row.id.as_str())),
            name: width("NAME", rows.iter().map(|row| row.name.as_str())),
            version: width("VERSION", rows.iter().map(|row| row.version.as_str())),
            replicas: width("REPLICAS", rows.iter().map(|row| row.replicas.as_str())),
            rollout: width("ROLLOUT", rows.iter().map(|row| row.rollout)),
            active: width(
                "ACTIVE DEPLOYMENT",
                rows.iter().map(|row| row.active.as_str()),
            ),
        }
    }
}

fn width<'a>(heading: &'a str, values: impl Iterator<Item = &'a str>) -> usize {
    values
        .map(str::len)
        .fold(heading.len(), |current, value| current.max(value))
}
