use std::sync::Mutex;

use kernel_api::{
    CommandRequest, Deployment, DeploymentCommandResponse, DeploymentId, Generation, RequestId,
    Service, ServiceCommandResponse, ServiceDiffChange, ServiceDiffStatus, ServiceId,
    ServiceRolloutDiffRequest, ServiceRolloutDiffResponse, ServiceRolloutRequest,
    ServiceRolloutResponse,
};

use crate::CliError;
use crate::config_source::ConfigSourceReader;
use crate::rollout::run;
use crate::services::ServiceApi;

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
    writes: Mutex<Vec<(RequestId, ServiceRolloutRequest)>>,
}

impl ServiceApi for RecordingApi {
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

    async fn redeploy_service(
        &self,
        _service_id: &ServiceId,
        _request_id: &RequestId,
        _request: CommandRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        Err(unexpected())
    }

    async fn cancel_deployment(
        &self,
        _service_id: &ServiceId,
        _deployment_id: &DeploymentId,
        _request_id: &RequestId,
        _request: CommandRequest,
    ) -> Result<DeploymentCommandResponse, CliError> {
        Err(unexpected())
    }

    async fn diff_rollout(
        &self,
        service_id: &ServiceId,
        _request: ServiceRolloutDiffRequest,
    ) -> Result<ServiceRolloutDiffResponse, CliError> {
        Ok(ServiceRolloutDiffResponse {
            service_id: service_id.clone(),
            expected_revisions: kernel_api::ServiceRolloutRevisions {
                service: Some(kernel_api::ResourceRevision(7)),
                ingress: None,
                egress: None,
            },
            status: ServiceDiffStatus::Changed,
            changes: vec![ServiceDiffChange {
                field: "environment.TOKEN".to_string(),
                from: Some("••••-old".to_string()),
                to: Some("••••-new".to_string()),
            }],
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
            .map_err(|_| CliError::invalid_input("write lock was poisoned"))?
            .push((request_id.clone(), request));
        Ok(ServiceRolloutResponse {
            service_id: service_id.clone(),
            service_generation: Generation(8),
            ingress_generation: None,
            egress_generation: None,
        })
    }
}

#[tokio::test]
async fn rollout_previews_masked_changes_then_applies_the_previewed_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let reader = MemoryReader {
        document: r#"{
            futureRoot: true,
            services: {
                api: {
                    name: "API",
                    image: "api:latest",
                    deploy: { replicas: 2, env: { items: { TOKEN: "private-new" } } }
                }
            }
        }"#
        .to_string(),
    };
    let api = RecordingApi::default();
    let mut input = std::io::Cursor::new(Vec::<u8>::new());
    let mut output = Vec::new();
    run(
        &api,
        "services.jsonc",
        &["api".to_string()],
        true,
        true,
        Some("rollout-1".to_string()),
        &mut input,
        &mut output,
        &reader,
    )
    .await?;
    let output = String::from_utf8(output)?;
    assert!(output.contains("warning: ignored field `futureRoot`"));
    assert!(output.contains("environment.TOKEN: ••••-old -> ••••-new"));
    assert!(output.contains("rollout accepted for `api` at generation 8"));
    assert!(!output.contains("private-new"));

    let writes = api.writes.lock().map_err(|_| "write lock was poisoned")?;
    assert_eq!(writes.len(), 1);
    let (request_id, request) = writes.first().ok_or("missing service write")?;
    assert_eq!(request_id, &RequestId::new("rollout-1")?);
    assert_eq!(
        request.expected_revisions.service,
        Some(kernel_api::ResourceRevision(7)),
    );
    assert_eq!(
        request
            .desired
            .service
            .environment
            .get("TOKEN")
            .map(String::as_str),
        Some("private-new")
    );
    Ok(())
}

#[tokio::test]
async fn rollout_dry_run_never_writes_and_prompts_for_apply()
-> Result<(), Box<dyn std::error::Error>> {
    let reader = MemoryReader {
        document: r#"{
            services: {
                api: { name: "API", image: "api:latest", deploy: { replicas: 1 } }
            }
        }"#
        .to_string(),
    };
    let api = RecordingApi::default();
    let mut input = std::io::Cursor::new(Vec::<u8>::new());
    let mut output = Vec::new();
    run(
        &api,
        "services.jsonc",
        &[],
        false,
        false,
        None,
        &mut input,
        &mut output,
        &reader,
    )
    .await?;
    assert!(String::from_utf8(output)?.contains("run with --apply"));
    assert!(
        api.writes
            .lock()
            .map_err(|_| "write lock was poisoned")?
            .is_empty()
    );
    Ok(())
}

fn unexpected() -> CliError {
    CliError::invalid_input("unexpected API call")
}
