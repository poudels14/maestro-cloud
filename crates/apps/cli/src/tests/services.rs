use std::sync::Mutex;

use kernel_api::{
    CommandRequest, Deployment, DeploymentCommandResponse, DeploymentGoal, DeploymentId,
    Generation, RequestId, RolloutState, Service, ServiceCommandResponse, ServiceDiffRequest,
    ServiceDiffResponse, ServiceId, ServiceWriteRequest, ServiceWriteResponse,
};
use serde_json::json;

use crate::CliError;
use crate::services::{ServiceApi, cancel, redeploy, write_services};

#[test]
fn service_listing_is_stable_and_never_prints_secrets() -> Result<(), Box<dyn std::error::Error>> {
    let services = vec![
        service("worker", "Worker", "2", 3, None)?,
        service("api", "API", "1", 2, Some(1))?,
    ];
    let mut output = Vec::new();
    write_services(services, &mut output)?;
    let output = String::from_utf8(output)?;
    assert!(output.contains("ID"));
    assert!(output.contains("api"));
    assert!(output.contains("1*"));
    assert!(output.contains("registry.example.test/api:1"));
    assert!(
        output
            .find("api")
            .is_some_and(|api| { output.find("worker").is_some_and(|worker| api < worker) })
    );
    assert!(!output.contains("database-password"));
    Ok(())
}

#[test]
fn empty_service_listing_is_explicit() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = Vec::new();
    write_services(Vec::new(), &mut output)?;
    assert_eq!(String::from_utf8(output)?, "[maestro]: no services found\n");
    Ok(())
}

#[tokio::test]
async fn lifecycle_commands_submit_the_observed_revision() -> Result<(), Box<dyn std::error::Error>>
{
    let service = service("api", "API", "1", 2, None)?;
    let deployment = deployment(&service)?;
    let api = RecordingServiceApi {
        service,
        deployment,
        requests: Mutex::new(Vec::new()),
    };
    let mut output = Vec::new();
    redeploy(
        &api,
        "api".to_string(),
        RequestId::new("redeploy-1")?,
        &mut output,
    )
    .await?;
    cancel(
        &api,
        "api".to_string(),
        "deployment-1".to_string(),
        RequestId::new("cancel-1")?,
        &mut output,
    )
    .await?;
    let requests = api
        .requests
        .lock()
        .map_err(|_| "request lock was poisoned")?;
    assert_eq!(
        requests.as_slice(),
        [
            CommandRequest {
                expected_revision: kernel_api::ResourceRevision(1)
            },
            CommandRequest {
                expected_revision: kernel_api::ResourceRevision(9)
            }
        ]
    );
    let output = String::from_utf8(output)?;
    assert!(output.contains("redeploy accepted"));
    assert!(output.contains("cancel accepted"));
    Ok(())
}

struct RecordingServiceApi {
    service: Service,
    deployment: Deployment,
    requests: Mutex<Vec<CommandRequest>>,
}

impl ServiceApi for RecordingServiceApi {
    async fn list_services(&self) -> Result<Vec<Service>, CliError> {
        Ok(vec![self.service.clone()])
    }

    async fn get_service(&self, _service_id: &ServiceId) -> Result<Service, CliError> {
        Ok(self.service.clone())
    }

    async fn get_deployment(
        &self,
        _service_id: &ServiceId,
        _deployment_id: &DeploymentId,
    ) -> Result<Deployment, CliError> {
        Ok(self.deployment.clone())
    }

    async fn redeploy_service(
        &self,
        service_id: &ServiceId,
        _request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        self.requests
            .lock()
            .map_err(|_| CliError::invalid_input("request lock was poisoned"))?
            .push(request);
        Ok(ServiceCommandResponse {
            service_id: service_id.clone(),
            generation: Generation(2),
            rollout: RolloutState::Active,
            replica_override: None,
            deletion_timestamp: None,
        })
    }

    async fn cancel_deployment(
        &self,
        _service_id: &ServiceId,
        deployment_id: &DeploymentId,
        _request_id: &RequestId,
        request: CommandRequest,
    ) -> Result<DeploymentCommandResponse, CliError> {
        self.requests
            .lock()
            .map_err(|_| CliError::invalid_input("request lock was poisoned"))?
            .push(request);
        Ok(DeploymentCommandResponse {
            deployment_id: deployment_id.clone(),
            generation: Generation(2),
            restart_generation: Generation(1),
            goal: DeploymentGoal::Cancel,
        })
    }

    async fn diff_service(
        &self,
        _service_id: &ServiceId,
        _request: ServiceDiffRequest,
    ) -> Result<ServiceDiffResponse, CliError> {
        Err(CliError::invalid_input("unexpected diff request"))
    }

    async fn put_service(
        &self,
        _service_id: &ServiceId,
        _request_id: &RequestId,
        _request: ServiceWriteRequest,
    ) -> Result<ServiceWriteResponse, CliError> {
        Err(CliError::invalid_input("unexpected service write"))
    }
}

fn service(
    id: &str,
    name: &str,
    version: &str,
    replicas: u32,
    replica_override: Option<u32>,
) -> Result<Service, serde_json::Error> {
    serde_json::from_value(json!({
        "meta": {
            "id": id,
            "revision": 1,
            "generation": 1
        },
        "spec": {
            "name": name,
            "version": version,
            "artifact": {
                "type": "image",
                "reference": format!("registry.example.test/{id}:{version}")
            },
            "replicas": replicas,
            "environment": {},
            "secrets": {
                "mountPath": "/run/secrets/service.env",
                "items": {"DATABASE_PASSWORD": "database-password"}
            },
            "exec": "allowed"
        },
        "status": {
            "replicaOverride": replica_override,
            "rollout": "active"
        }
    }))
}

fn deployment(service: &Service) -> Result<Deployment, serde_json::Error> {
    serde_json::from_value(json!({
        "meta": {
            "id": "deployment-1",
            "revision": 9,
            "generation": 1
        },
        "spec": {
            "serviceId": service.meta.id,
            "serviceGeneration": service.meta.generation,
            "restartGeneration": 1,
            "service": service.spec,
            "goal": "run"
        },
        "status": {
            "phase": "QUEUED",
            "createdAt": 1
        }
    }))
}
