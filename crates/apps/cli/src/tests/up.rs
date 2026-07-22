use std::sync::Mutex;

use kernel_api::{
    ArtifactArchiveId, ArtifactArchiveUploadResponse, ArtifactTemplate, CommandRequest, Deployment,
    DeploymentCommandResponse, DeploymentId, Generation, RequestId, Service,
    ServiceCommandResponse, ServiceDiffStatus, ServiceId, ServiceReplicaOverrideRequest,
    ServiceRolloutDiffRequest, ServiceRolloutDiffResponse, ServiceRolloutRequest,
    ServiceRolloutResponse,
};
use sha2::{Digest, Sha256};

use crate::CliError;
use crate::config_source::ConfigSourceReader;
use crate::services::{DeploymentLifecycleAction, ServiceApi, ServiceLifecycleAction};
use crate::up::run;

struct MemoryReader {
    document: String,
}

impl ConfigSourceReader for MemoryReader {
    async fn read(&self, _source: &str) -> Result<String, CliError> {
        Ok(self.document.clone())
    }
}

#[derive(Default)]
struct RecordingApi {
    archive: Mutex<Option<(ArtifactArchiveId, Vec<u8>)>>,
    desired: Mutex<Option<kernel_api::ServiceRolloutSpec>>,
    writes: Mutex<Vec<(RequestId, ServiceRolloutRequest)>>,
}

impl ServiceApi for RecordingApi {
    async fn upload_artifact_archive(
        &self,
        archive_id: &ArtifactArchiveId,
        content: Vec<u8>,
    ) -> Result<ArtifactArchiveUploadResponse, CliError> {
        self.archive
            .lock()
            .map_err(|_| poisoned())?
            .replace((archive_id.clone(), content.clone()));
        Ok(ArtifactArchiveUploadResponse {
            archive_id: archive_id.clone(),
            size_bytes: content.len() as u64,
        })
    }

    async fn diff_rollout(
        &self,
        service_id: &ServiceId,
        request: ServiceRolloutDiffRequest,
    ) -> Result<ServiceRolloutDiffResponse, CliError> {
        self.desired
            .lock()
            .map_err(|_| poisoned())?
            .replace(request.desired);
        Ok(ServiceRolloutDiffResponse {
            service_id: service_id.clone(),
            expected_revisions: kernel_api::ServiceRolloutRevisions::default(),
            status: ServiceDiffStatus::New,
            changes: Vec::new(),
        })
    }

    async fn apply_rollout(
        &self,
        service_id: &ServiceId,
        request_id: &RequestId,
        request: ServiceRolloutRequest,
    ) -> Result<ServiceRolloutResponse, CliError> {
        self.writes
            .lock()
            .map_err(|_| poisoned())?
            .push((request_id.clone(), request));
        Ok(ServiceRolloutResponse {
            service_id: service_id.clone(),
            service_generation: Generation(1),
            ingress_generation: None,
            egress_generation: None,
        })
    }

    async fn list_services(&self) -> Result<Vec<Service>, CliError> {
        Err(unexpected())
    }

    async fn get_service(&self, _service_id: &ServiceId) -> Result<Service, CliError> {
        Err(unexpected())
    }

    async fn get_deployment(
        &self,
        _service_id: &ServiceId,
        _deployment_id: &DeploymentId,
    ) -> Result<Deployment, CliError> {
        Err(unexpected())
    }

    async fn command_service(
        &self,
        _service_id: &ServiceId,
        _request_id: &RequestId,
        _action: ServiceLifecycleAction,
        _request: CommandRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        Err(unexpected())
    }

    async fn command_deployment(
        &self,
        _service_id: &ServiceId,
        _deployment_id: &DeploymentId,
        _request_id: &RequestId,
        _action: DeploymentLifecycleAction,
        _request: CommandRequest,
    ) -> Result<DeploymentCommandResponse, CliError> {
        Err(unexpected())
    }

    async fn set_service_replicas(
        &self,
        _service_id: &ServiceId,
        _request_id: &RequestId,
        _request: ServiceReplicaOverrideRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        Err(unexpected())
    }
}

#[tokio::test]
async fn up_uploads_a_content_address_then_applies_the_same_tarball_source()
-> Result<(), Box<dyn std::error::Error>> {
    let context = tempfile::tempdir()?;
    std::fs::write(context.path().join("Dockerfile"), b"FROM scratch\n")?;
    let reader = MemoryReader {
        document: r#"{
            services: {
                api: {
                    name: "API",
                    build: {
                        dockerfile: "Dockerfile",
                        secrets: { items: { TOKEN: "private-build-token" } }
                    },
                    deploy: { replicas: 1 }
                }
            }
        }"#
        .to_string(),
    };
    let api = RecordingApi::default();
    let mut output = Vec::new();
    run(
        &api,
        "services.jsonc",
        "api".to_string(),
        context.path(),
        RequestId::new("up-1")?,
        &mut output,
        &reader,
    )
    .await?;

    let archive = api.archive.lock().map_err(|_| "archive lock poisoned")?;
    let (archive_id, bytes) = archive.as_ref().ok_or("archive was not uploaded")?;
    assert!(bytes.starts_with(&[0x1f, 0x8b]));
    assert_eq!(
        archive_id,
        &ArtifactArchiveId::from_sha256(Sha256::digest(bytes).into())
    );
    let desired = api.desired.lock().map_err(|_| "desired lock poisoned")?;
    let desired = desired.as_ref().ok_or("rollout was not diffed")?;
    assert!(matches!(
        &desired.service.artifact,
        ArtifactTemplate::Build { template }
            if matches!(&template.source, kernel_api::BuildSource::Tarball { archive_id: source_id } if source_id == archive_id)
    ));
    let writes = api.writes.lock().map_err(|_| "write lock poisoned")?;
    assert_eq!(writes.len(), 1);
    assert_eq!(
        writes.first().map(|write| &write.0),
        Some(&RequestId::new("up-1")?)
    );
    assert_eq!(writes.first().map(|write| &write.1.desired), Some(desired));
    let output = String::from_utf8(output)?;
    assert!(output.contains("local rollout accepted for `api`"));
    assert!(!output.contains("private-build-token"));
    Ok(())
}

fn poisoned() -> CliError {
    CliError::invalid_input("test lock poisoned")
}

fn unexpected() -> CliError {
    CliError::invalid_input("unexpected API call")
}
