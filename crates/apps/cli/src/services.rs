use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use kernel_api::{
    ArtifactArchiveId, ArtifactArchiveUploadResponse, ArtifactTemplate, BuiltinKind,
    CommandRequest, Deployment, DeploymentCommandResponse, DeploymentId, Ownership, Preview,
    RequestId, RolloutState, Service, ServiceCommandResponse, ServiceId,
    ServiceReplicaOverrideRequest, ServiceRolloutDiffRequest, ServiceRolloutDiffResponse,
    ServiceRolloutRequest, ServiceRolloutResponse,
};

use crate::CliError;
use crate::api_client::ApiClient;

pub(crate) async fn list(client: &impl ServiceApi, output: &mut dyn Write) -> Result<(), CliError> {
    let (services, previews) = tokio::try_join!(client.list_services(), client.list_previews())?;
    write_services(services, previews, output)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceLifecycleAction {
    Redeploy,
    Freeze,
    Unfreeze,
    Delete,
}

impl ServiceLifecycleAction {
    fn verb(self) -> &'static str {
        match self {
            Self::Redeploy => "redeploy",
            Self::Freeze => "freeze",
            Self::Unfreeze => "unfreeze",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeploymentLifecycleAction {
    Restart,
    Cancel,
    Remove,
}

impl DeploymentLifecycleAction {
    fn verb(self) -> &'static str {
        match self {
            Self::Restart => "restart",
            Self::Cancel => "cancel",
            Self::Remove => "remove",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicaOverride {
    Set(u32),
    Clear,
}

pub(crate) async fn service_lifecycle(
    client: &impl ServiceApi,
    service_id: String,
    request_id: RequestId,
    action: ServiceLifecycleAction,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| CliError::invalid_input(error.to_string()))?;
    let service = client.get_service(&service_id).await?;
    let response = client
        .command_service(
            &service_id,
            &request_id,
            action,
            CommandRequest {
                expected_revision: service.meta.revision,
            },
        )
        .await?;
    writeln!(
        output,
        "[maestro]: {} accepted for `{}` at generation {}",
        action.verb(),
        response.service_id,
        response.generation.0
    )
    .map_err(|source| CliError::io("failed to write command output", source))
}

pub(crate) async fn deployment_lifecycle(
    client: &impl ServiceApi,
    service_id: String,
    deployment_id: String,
    request_id: RequestId,
    action: DeploymentLifecycleAction,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| CliError::invalid_input(error.to_string()))?;
    let deployment_id = DeploymentId::new(deployment_id)
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    let deployment = client.get_deployment(&service_id, &deployment_id).await?;
    let response = client
        .command_deployment(
            &service_id,
            &deployment_id,
            &request_id,
            action,
            CommandRequest {
                expected_revision: deployment.meta.revision,
            },
        )
        .await?;
    writeln!(
        output,
        "[maestro]: {} accepted for deployment `{}` at generation {}",
        action.verb(),
        response.deployment_id,
        response.generation.0
    )
    .map_err(|source| CliError::io("failed to write command output", source))
}

pub(crate) async fn set_replicas(
    client: &impl ServiceApi,
    service_id: String,
    request_id: RequestId,
    replicas: ReplicaOverride,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| CliError::invalid_input(error.to_string()))?;
    let service = client.get_service(&service_id).await?;
    let replicas = match replicas {
        ReplicaOverride::Set(replicas) => Some(replicas),
        ReplicaOverride::Clear => None,
    };
    let response = client
        .set_service_replicas(
            &service_id,
            &request_id,
            ServiceReplicaOverrideRequest {
                expected_revision: service.meta.revision,
                replicas,
            },
        )
        .await?;
    let replica_description = response
        .replica_override
        .map(|replicas| replicas.to_string())
        .unwrap_or_else(|| "cleared".to_string());
    writeln!(
        output,
        "[maestro]: replica override accepted for `{}`: {replica_description}",
        response.service_id
    )
    .map_err(|source| CliError::io("failed to write command output", source))
}

pub(crate) trait ServiceApi {
    async fn upload_artifact_archive(
        &self,
        archive_id: &ArtifactArchiveId,
        content: Vec<u8>,
    ) -> Result<ArtifactArchiveUploadResponse, CliError>;

    async fn list_services(&self) -> Result<Vec<Service>, CliError>;

    async fn list_previews(&self) -> Result<Vec<Preview>, CliError>;

    async fn get_service(&self, service_id: &ServiceId) -> Result<Service, CliError>;

    async fn get_deployment(
        &self,
        service_id: &ServiceId,
        deployment_id: &DeploymentId,
    ) -> Result<Deployment, CliError>;

    async fn list_deployments(&self, service_id: &ServiceId) -> Result<Vec<Deployment>, CliError>;

    async fn command_service(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        action: ServiceLifecycleAction,
        request: CommandRequest,
    ) -> Result<ServiceCommandResponse, CliError>;

    async fn command_deployment(
        &self,
        service_id: &ServiceId,
        deployment_id: &DeploymentId,
        request_id: &RequestId,
        action: DeploymentLifecycleAction,
        request: CommandRequest,
    ) -> Result<DeploymentCommandResponse, CliError>;

    async fn set_service_replicas(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        request: ServiceReplicaOverrideRequest,
    ) -> Result<ServiceCommandResponse, CliError>;

    async fn diff_rollout(
        &self,
        service_id: &ServiceId,
        request: ServiceRolloutDiffRequest,
    ) -> Result<ServiceRolloutDiffResponse, CliError>;

    async fn apply_rollout(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        request: ServiceRolloutRequest,
    ) -> Result<ServiceRolloutResponse, CliError>;
}

impl ServiceApi for ApiClient {
    async fn upload_artifact_archive(
        &self,
        archive_id: &ArtifactArchiveId,
        content: Vec<u8>,
    ) -> Result<ArtifactArchiveUploadResponse, CliError> {
        ApiClient::upload_artifact_archive(self, archive_id, content).await
    }

    async fn list_services(&self) -> Result<Vec<Service>, CliError> {
        self.get("/api/services").await
    }

    async fn list_previews(&self) -> Result<Vec<Preview>, CliError> {
        self.get("/api/previews").await
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

    async fn list_deployments(&self, service_id: &ServiceId) -> Result<Vec<Deployment>, CliError> {
        self.get(&format!("/api/services/{service_id}/deployments"))
            .await
    }

    async fn command_service(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        action: ServiceLifecycleAction,
        request: CommandRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        let path = match action {
            ServiceLifecycleAction::Redeploy => format!("/api/services/{service_id}/redeploy"),
            ServiceLifecycleAction::Freeze => format!("/api/services/{service_id}/freeze"),
            ServiceLifecycleAction::Unfreeze => format!("/api/services/{service_id}/unfreeze"),
            ServiceLifecycleAction::Delete => format!("/api/services/{service_id}"),
        };
        match action {
            ServiceLifecycleAction::Delete => self.delete(&path, request_id, &request).await,
            ServiceLifecycleAction::Redeploy
            | ServiceLifecycleAction::Freeze
            | ServiceLifecycleAction::Unfreeze => self.post(&path, request_id, &request).await,
        }
    }

    async fn command_deployment(
        &self,
        service_id: &ServiceId,
        deployment_id: &DeploymentId,
        request_id: &RequestId,
        action: DeploymentLifecycleAction,
        request: CommandRequest,
    ) -> Result<DeploymentCommandResponse, CliError> {
        let action = action.verb();
        self.post(
            &format!("/api/services/{service_id}/deployments/{deployment_id}/{action}"),
            request_id,
            &request,
        )
        .await
    }

    async fn set_service_replicas(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        request: ServiceReplicaOverrideRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        self.put(
            &format!("/api/services/{service_id}/replicas"),
            request_id,
            &request,
        )
        .await
    }

    async fn diff_rollout(
        &self,
        service_id: &ServiceId,
        request: ServiceRolloutDiffRequest,
    ) -> Result<ServiceRolloutDiffResponse, CliError> {
        self.post_query(
            &format!("/api/services/{service_id}/rollout/diff"),
            &request,
        )
        .await
    }

    async fn apply_rollout(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        request: ServiceRolloutRequest,
    ) -> Result<ServiceRolloutResponse, CliError> {
        self.post(
            &format!("/api/services/{service_id}/rollout"),
            request_id,
            &request,
        )
        .await
    }
}

pub(crate) fn write_services(
    mut services: Vec<Service>,
    previews: Vec<Preview>,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    services.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    if services.is_empty() {
        writeln!(output, "[maestro]: no services found")
            .map_err(|source| CliError::io("failed to write command output", source))?;
        return Ok(());
    }
    let service_ids = services
        .iter()
        .map(|service| service.meta.id.clone())
        .collect::<BTreeSet<_>>();
    let mut previews_by_base = BTreeMap::<ServiceId, Vec<ServiceRow>>::new();
    let mut service_rows = Vec::new();
    for service in &services {
        if let Some(base_service_id) = preview_base_service(service, &previews)
            .filter(|base_service_id| service_ids.contains(*base_service_id))
        {
            previews_by_base
                .entry(base_service_id.clone())
                .or_default()
                .push(ServiceRow::from(service).indented(4));
        } else {
            service_rows.push((service.meta.id.clone(), ServiceRow::from(service)));
        }
    }
    let groups = service_rows
        .into_iter()
        .map(|(service_id, service)| ServiceGroup {
            service,
            previews: previews_by_base.remove(&service_id).unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    let rows = groups
        .iter()
        .flat_map(|group| std::iter::once(&group.service).chain(group.previews.iter()))
        .collect::<Vec<_>>();
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
    for group in groups {
        write_service_row(output, &group.service, widths)?;
        if !group.previews.is_empty() {
            writeln!(output, "  PR")
                .map_err(|source| CliError::io("failed to write command output", source))?;
            for row in &group.previews {
                write_service_row(output, row, widths)?;
            }
        }
    }
    Ok(())
}

fn write_service_row(
    output: &mut dyn Write,
    row: &ServiceRow,
    widths: ColumnWidths,
) -> Result<(), CliError> {
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
    .map_err(|source| CliError::io("failed to write command output", source))
}

fn preview_base_service<'a>(service: &Service, previews: &'a [Preview]) -> Option<&'a ServiceId> {
    previews
        .iter()
        .find(|preview| {
            preview.spec.service_id == service.meta.id
                && service.meta.owner_refs.iter().any(|owner| {
                    owner.ownership == Ownership::Controller
                        && owner.resource.kind.as_str() == BuiltinKind::Preview.as_str()
                        && owner.resource.id.as_str() == preview.meta.id.as_str()
                })
        })
        .map(|preview| &preview.spec.base_service_id)
}

struct ServiceGroup {
    service: ServiceRow,
    previews: Vec<ServiceRow>,
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
            ArtifactTemplate::Image { reference } => reference
                .split_once('@')
                .map_or(reference.as_str(), |(reference, _digest)| reference)
                .to_string(),
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

impl ServiceRow {
    fn indented(mut self, width: usize) -> Self {
        self.id.insert_str(0, &" ".repeat(width));
        self
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
    fn for_rows(rows: &[&ServiceRow]) -> Self {
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
