use std::sync::Mutex;

use kernel_api::{
    ArtifactArchiveId, ArtifactArchiveUploadResponse, ArtifactTemplate, CommandRequest, Deployment,
    DeploymentCommandResponse, DeploymentGoal, DeploymentId, Generation, Preview, RequestId,
    RolloutState, Service, ServiceCommandResponse, ServiceId, ServiceReplicaOverrideRequest,
    ServiceRolloutDiffRequest, ServiceRolloutDiffResponse, ServiceRolloutRequest,
    ServiceRolloutResponse,
};
use serde_json::json;

use crate::CliError;
use crate::services::{
    DeploymentLifecycleAction, ReplicaOverride, ServiceApi, ServiceLifecycleAction,
    deployment_lifecycle, service_lifecycle, set_replicas, write_services,
};

#[test]
fn service_listing_is_stable_and_never_prints_secrets() -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = service("worker", "Worker", "2", 3, None)?;
    let ArtifactTemplate::Image { reference } = &mut worker.spec.artifact else {
        unreachable!("fixture uses an image artifact")
    };
    reference.push_str("@sha256:0123456789abcdef");
    let services = vec![worker, service("api", "API", "1", 2, Some(1))?];
    let mut output = Vec::new();
    write_services(services, Vec::new(), &mut output)?;
    let output = String::from_utf8(output)?;
    assert!(output.contains("ID"));
    assert!(output.contains("api"));
    assert!(output.contains("1*"));
    assert!(output.contains("registry.example.test/api:1"));
    assert!(output.contains("registry.example.test/worker:2"));
    assert!(!output.contains("sha256:"));
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
    write_services(Vec::new(), Vec::new(), &mut output)?;
    assert_eq!(String::from_utf8(output)?, "[maestro]: no services found\n");
    Ok(())
}

#[test]
fn preview_services_are_indented_under_a_pr_group() -> Result<(), Box<dyn std::error::Error>> {
    let mut preview_1025 = service("app-pr-1025", "app-pr-1025", "1", 1, None)?;
    preview_1025.meta.owner_refs = serde_json::from_value(json!([{
        "resource": {"kind": "Preview", "id": "preview-1025"},
        "ownership": "controller"
    }]))?;
    let mut preview_1010 = service("app-pr-1010", "app-pr-1010", "1", 1, None)?;
    preview_1010.meta.owner_refs = serde_json::from_value(json!([{
        "resource": {"kind": "Preview", "id": "preview-1010"},
        "ownership": "controller"
    }]))?;
    let mut output = Vec::new();

    write_services(
        vec![
            preview_1025,
            service("maestro-system-traefik", "Traefik", "3", 3, None)?,
            service("app", "dashboard", "1", 1, None)?,
            preview_1010,
        ],
        vec![
            preview("preview-1010", "app", "app-pr-1010", 1010)?,
            preview("preview-1025", "app", "app-pr-1025", 1025)?,
        ],
        &mut output,
    )?;

    let output = String::from_utf8(output)?;
    let lines = output.lines().collect::<Vec<_>>();
    assert!(lines[1].starts_with("app "));
    assert_eq!(lines[2], "  PR");
    assert!(lines[3].starts_with("    app-pr-1010 "));
    assert!(lines[4].starts_with("    app-pr-1025 "));
    assert!(lines[5].starts_with("maestro-system-traefik "));
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
        service_requests: Mutex::new(Vec::new()),
        deployment_requests: Mutex::new(Vec::new()),
        replica_requests: Mutex::new(Vec::new()),
    };
    let mut output = Vec::new();
    for action in [
        ServiceLifecycleAction::Redeploy,
        ServiceLifecycleAction::Freeze,
        ServiceLifecycleAction::Unfreeze,
        ServiceLifecycleAction::Delete,
    ] {
        service_lifecycle(
            &api,
            "api".to_string(),
            RequestId::new(format!("{action:?}-1"))?,
            action,
            &mut output,
        )
        .await?;
    }
    for action in [
        DeploymentLifecycleAction::Restart,
        DeploymentLifecycleAction::Cancel,
        DeploymentLifecycleAction::Remove,
    ] {
        deployment_lifecycle(
            &api,
            "api".to_string(),
            "deployment-1".to_string(),
            RequestId::new(format!("{action:?}-1"))?,
            action,
            &mut output,
        )
        .await?;
    }
    for (request_id, replicas) in [
        ("replicas-set-1", ReplicaOverride::Set(4)),
        ("replicas-clear-1", ReplicaOverride::Clear),
    ] {
        set_replicas(
            &api,
            "api".to_string(),
            RequestId::new(request_id)?,
            replicas,
            &mut output,
        )
        .await?;
    }

    let service_requests = api
        .service_requests
        .lock()
        .map_err(|_| "request lock was poisoned")?;
    assert_eq!(
        service_requests.as_slice(),
        [
            ServiceLifecycleAction::Redeploy,
            ServiceLifecycleAction::Freeze,
            ServiceLifecycleAction::Unfreeze,
            ServiceLifecycleAction::Delete,
        ]
    );
    let deployment_requests = api
        .deployment_requests
        .lock()
        .map_err(|_| "request lock was poisoned")?;
    assert_eq!(
        deployment_requests.as_slice(),
        [
            DeploymentLifecycleAction::Restart,
            DeploymentLifecycleAction::Cancel,
            DeploymentLifecycleAction::Remove,
        ]
    );
    let replica_requests = api
        .replica_requests
        .lock()
        .map_err(|_| "request lock was poisoned")?;
    assert_eq!(replica_requests.as_slice(), [Some(4), None]);
    let output = String::from_utf8(output)?;
    for verb in [
        "redeploy", "freeze", "unfreeze", "delete", "restart", "cancel", "remove",
    ] {
        assert!(output.contains(&format!("{verb} accepted")));
    }
    assert!(output.contains("replica override accepted for `api`: 4"));
    assert!(output.contains("replica override accepted for `api`: cleared"));
    Ok(())
}

struct RecordingServiceApi {
    service: Service,
    deployment: Deployment,
    service_requests: Mutex<Vec<ServiceLifecycleAction>>,
    deployment_requests: Mutex<Vec<DeploymentLifecycleAction>>,
    replica_requests: Mutex<Vec<Option<u32>>>,
}

impl ServiceApi for RecordingServiceApi {
    async fn upload_artifact_archive(
        &self,
        _archive_id: &ArtifactArchiveId,
        _content: Vec<u8>,
    ) -> Result<ArtifactArchiveUploadResponse, CliError> {
        Err(CliError::invalid_input("unexpected archive upload"))
    }

    async fn list_services(&self) -> Result<Vec<Service>, CliError> {
        Ok(vec![self.service.clone()])
    }

    async fn list_previews(&self) -> Result<Vec<Preview>, CliError> {
        Ok(Vec::new())
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

    async fn list_deployments(&self, _service_id: &ServiceId) -> Result<Vec<Deployment>, CliError> {
        Ok(vec![self.deployment.clone()])
    }

    async fn command_service(
        &self,
        service_id: &ServiceId,
        _request_id: &RequestId,
        action: ServiceLifecycleAction,
        request: CommandRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        assert_eq!(request.expected_revision, kernel_api::ResourceRevision(1));
        self.service_requests
            .lock()
            .map_err(|_| CliError::invalid_input("request lock was poisoned"))?
            .push(action);
        Ok(ServiceCommandResponse {
            service_id: service_id.clone(),
            generation: Generation(2),
            rollout: RolloutState::Active,
            replica_override: None,
            deletion_timestamp: None,
        })
    }

    async fn command_deployment(
        &self,
        _service_id: &ServiceId,
        deployment_id: &DeploymentId,
        _request_id: &RequestId,
        action: DeploymentLifecycleAction,
        request: CommandRequest,
    ) -> Result<DeploymentCommandResponse, CliError> {
        assert_eq!(request.expected_revision, kernel_api::ResourceRevision(9));
        self.deployment_requests
            .lock()
            .map_err(|_| CliError::invalid_input("request lock was poisoned"))?
            .push(action);
        Ok(DeploymentCommandResponse {
            deployment_id: deployment_id.clone(),
            generation: Generation(2),
            restart_generation: Generation(1),
            goal: DeploymentGoal::Cancel,
        })
    }

    async fn set_service_replicas(
        &self,
        service_id: &ServiceId,
        _request_id: &RequestId,
        request: ServiceReplicaOverrideRequest,
    ) -> Result<ServiceCommandResponse, CliError> {
        assert_eq!(request.expected_revision, kernel_api::ResourceRevision(1));
        self.replica_requests
            .lock()
            .map_err(|_| CliError::invalid_input("request lock was poisoned"))?
            .push(request.replicas);
        Ok(ServiceCommandResponse {
            service_id: service_id.clone(),
            generation: Generation(1),
            rollout: RolloutState::Active,
            replica_override: request.replicas,
            deletion_timestamp: None,
        })
    }

    async fn diff_rollout(
        &self,
        _service_id: &ServiceId,
        _request: ServiceRolloutDiffRequest,
    ) -> Result<ServiceRolloutDiffResponse, CliError> {
        Err(CliError::invalid_input("unexpected rollout diff"))
    }

    async fn apply_rollout(
        &self,
        _service_id: &ServiceId,
        _request_id: &RequestId,
        _request: ServiceRolloutRequest,
    ) -> Result<ServiceRolloutResponse, CliError> {
        Err(CliError::invalid_input("unexpected rollout apply"))
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
                "format": "dotenv",
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

fn preview(
    id: &str,
    base_service_id: &str,
    service_id: &str,
    pull_request_number: u64,
) -> Result<Preview, serde_json::Error> {
    serde_json::from_value(json!({
        "meta": {
            "id": id,
            "revision": 1,
            "generation": 1
        },
        "spec": {
            "baseServiceId": base_service_id,
            "repository": "Baton-AI/baton",
            "pullRequestNumber": pull_request_number,
            "title": format!("PR {pull_request_number}"),
            "headReference": format!("feature/{pull_request_number}"),
            "author": "octocat",
            "headRevision": "abc123",
            "serviceId": service_id,
            "closeGracePeriodSecs": 60,
            "expiresAt": 10_000
        },
        "status": {
            "phase": "active"
        }
    }))
}
