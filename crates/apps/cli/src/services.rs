use std::io::Write;

use kernel_api::{
    ArtifactTemplate, CommandRequest, Deployment, DeploymentCommandResponse, DeploymentId,
    RequestId, RolloutState, Service, ServiceCommandResponse, ServiceId,
};

use crate::CliError;
use crate::api_client::ApiClient;

pub(crate) async fn list(client: &impl ServiceApi, output: &mut dyn Write) -> Result<(), CliError> {
    let services = client.list_services().await?;
    write_services(services, output)
}

pub(crate) async fn redeploy(
    client: &impl ServiceApi,
    service_id: String,
    request_id: RequestId,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| CliError::invalid_input(error.to_string()))?;
    let service = client.get_service(&service_id).await?;
    let response = client
        .redeploy_service(
            &service_id,
            &request_id,
            CommandRequest {
                expected_revision: service.meta.revision,
            },
        )
        .await?;
    writeln!(
        output,
        "[maestro]: redeploy accepted for `{}` at generation {}",
        response.service_id, response.generation.0
    )
    .map_err(|source| CliError::io("failed to write command output", source))
}

pub(crate) async fn cancel(
    client: &impl ServiceApi,
    service_id: String,
    deployment_id: String,
    request_id: RequestId,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| CliError::invalid_input(error.to_string()))?;
    let deployment_id = DeploymentId::new(deployment_id)
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    let deployment = client.get_deployment(&service_id, &deployment_id).await?;
    let response = client
        .cancel_deployment(
            &service_id,
            &deployment_id,
            &request_id,
            CommandRequest {
                expected_revision: deployment.meta.revision,
            },
        )
        .await?;
    writeln!(
        output,
        "[maestro]: cancel accepted for deployment `{}`",
        response.deployment_id
    )
    .map_err(|source| CliError::io("failed to write command output", source))
}

pub(crate) trait ServiceApi {
    async fn list_services(&self) -> Result<Vec<Service>, CliError>;

    async fn get_service(&self, service_id: &ServiceId) -> Result<Service, CliError>;

    async fn get_deployment(
        &self,
        service_id: &ServiceId,
        deployment_id: &DeploymentId,
    ) -> Result<Deployment, CliError>;

    async fn redeploy_service(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<ServiceCommandResponse, CliError>;

    async fn cancel_deployment(
        &self,
        service_id: &ServiceId,
        deployment_id: &DeploymentId,
        request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<DeploymentCommandResponse, CliError>;
}

impl ServiceApi for ApiClient {
    async fn list_services(&self) -> Result<Vec<Service>, CliError> {
        self.get("/api/services").await
    }

    async fn get_service(&self, service_id: &ServiceId) -> Result<Service, CliError> {
        self.get(&format!("/api/services/{service_id}")).await
    }

    async fn get_deployment(
        &self,
        service_id: &ServiceId,
        deployment_id: &DeploymentId,
    ) -> Result<Deployment, CliError> {
        self.get(&format!(
            "/api/services/{service_id}/deployments/{deployment_id}"
        ))
        .await
    }

    async fn redeploy_service(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        self.post(
            &format!("/api/services/{service_id}/redeploy"),
            request_id,
            &request,
        )
        .await
    }

    async fn cancel_deployment(
        &self,
        service_id: &ServiceId,
        deployment_id: &DeploymentId,
        request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<DeploymentCommandResponse, CliError> {
        self.post(
            &format!("/api/services/{service_id}/deployments/{deployment_id}/cancel"),
            request_id,
            &request,
        )
        .await
    }
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
