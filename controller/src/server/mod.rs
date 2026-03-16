use std::{path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{delete, get, patch, post},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;

use self::types::{
    CancelDeploymentResponse, RemoveDeploymentResponse, RolloutServiceRequest,
    RolloutServiceResponse, ServiceListItem,
};
use crate::deployment::store::{ClusterStore, UpsertServiceOutcome};
use crate::deployment::types::{
    CancelDeploymentOutcome, Deployment, ServiceBuildConfig, ServiceConfig, ServiceDeployConfig,
    ServiceDeployment, ServiceProvider,
};
use crate::signal::ShutdownEvent;
use crate::supervisor::logs;

mod types;

const DEFAULT_LOG_LIMIT: usize = 1000;

const SYSTEM_SERVICES: &[(&str, &str, &str)] = &[
    ("maestro-etcd", "etcd", "quay.io/coreos/etcd:v3.6.8"),
    ("maestro-ingress", "Ingress", "traefik:v3.6"),
    ("maestro-probe", "Probe", "maestro-probe"),
];

#[derive(Clone)]
struct AppState {
    store: Arc<dyn ClusterStore>,
    logs_dir: PathBuf,
}

pub(crate) struct Server {
    state: AppState,
}

impl Server {
    pub(crate) fn new(store: Arc<dyn ClusterStore>, logs_dir: PathBuf) -> Self {
        Self {
            state: AppState { store, logs_dir },
        }
    }

    fn app(&self) -> Router {
        Router::new()
            .route("/_healthy", get(Self::healthy))
            .route("/api/services", get(Self::list_services))
            .route("/api/services/rollout", post(Self::rollout_service))
            .route(
                "/api/services/{serviceId}/deployments",
                get(Self::list_deployments),
            )
            .route(
                "/api/services/{serviceId}/deployments/{deploymentId}/cancel",
                patch(Self::cancel_deployment),
            )
            .route(
                "/api/services/{serviceId}/deployments/{deploymentId}/remove",
                patch(Self::remove_deployment),
            )
            .route(
                "/api/services/{serviceId}/redeploy",
                post(Self::redeploy_service),
            )
            .route("/api/services/{serviceId}", delete(Self::delete_service))
            .route(
                "/api/services/{serviceId}/deployments/{deploymentId}/logs",
                get(Self::get_deployment_logs),
            )
            .route("/api/system/{name}/logs", get(Self::get_system_logs))
            .with_state(self.state.clone())
            .layer(middleware::from_fn(log_http))
    }

    pub(crate) async fn serve(
        self,
        bind_addr: &str,
        mut shutdown_rx: broadcast::Receiver<ShutdownEvent>,
    ) -> crate::error::Result<()> {
        let app = self.app();
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|err| format!("failed to bind {bind_addr}: {err}"))?;
        println!(
            "[probe] server listening on http://{bind_addr} [pid={}]",
            std::process::id()
        );
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                loop {
                    match shutdown_rx.recv().await {
                        Ok(ShutdownEvent::Graceful) | Ok(ShutdownEvent::Force) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            })
            .await
            .map_err(|err| format!("server error: {err}").into())
    }

    async fn healthy() -> &'static str {
        "ok"
    }

    async fn rollout_service(
        State(state): State<AppState>,
        Json(request): Json<RolloutServiceRequest>,
    ) -> Result<Json<RolloutServiceResponse>, (StatusCode, String)> {
        let service_config = build_service_config(request).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid rollout request payload: {err}"),
            )
        })?;

        let outcome = upsert_config_and_maybe_queue(state.store.as_ref(), service_config)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let response = match outcome {
            UpsertServiceOutcome::Queued {
                deployment_index,
                deployment,
            } => RolloutServiceResponse {
                queued: true,
                replicas: None,
                deployment_id: Some(deployment.id.clone()),
                deployment_index: Some(deployment_index),
                service_id: deployment.config.id.clone(),
                status: Some(deployment.status),
                version: deployment.config.version.clone(),
            },
            UpsertServiceOutcome::Unchanged {
                service_id,
                version,
            } => RolloutServiceResponse {
                queued: false,
                replicas: None,
                deployment_id: None,
                deployment_index: None,
                service_id,
                status: None,
                version,
            },
            UpsertServiceOutcome::Scaled {
                service_id,
                version,
                replicas,
            } => RolloutServiceResponse {
                queued: false,
                replicas: Some(replicas),
                deployment_id: None,
                deployment_index: None,
                service_id,
                status: None,
                version,
            },
        };

        Ok(Json(response))
    }

    async fn list_services(
        State(state): State<AppState>,
    ) -> Result<Json<Vec<ServiceListItem>>, (StatusCode, String)> {
        let infos = state
            .store
            .list_service_infos()
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let mut items: Vec<ServiceListItem> = Vec::with_capacity(infos.len());
        for info in infos {
            let status = state
                .store
                .get_service_status(&info.config.id)
                .await
                .unwrap_or(None);
            items.push(ServiceListItem {
                service: info.config,
                status,
                system: false,
            });
        }

        for (id, name, image) in SYSTEM_SERVICES {
            items.push(ServiceListItem {
                service: ServiceConfig {
                    id: id.to_string(),
                    name: name.to_string(),
                    version: String::new(),
                    provider: ServiceProvider::Docker,
                    build: None,
                    image: Some(image.to_string()),
                    deploy: ServiceDeployConfig {
                        flags: vec![],
                        expose_ports: vec![],
                        command: None,
                        healthcheck_path: None,
                        replicas: 1,
                    },
                    ingress: None,
                },
                status: Some(crate::deployment::types::DeploymentStatus::Ready),
                system: true,
            });
        }

        Ok(Json(items))
    }

    async fn list_deployments(
        Path(service_id): Path<String>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::deployment::types::DeploymentWithReplicas>>, (StatusCode, String)>
    {
        let service_id = service_id.trim();
        validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        let mut deployments = state
            .store
            .list_service_deployments_with_replicas(service_id)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        deployments.sort_by(|a, b| {
            b.deployment
                .created_at
                .cmp(&a.deployment.created_at)
                .then_with(|| b.deployment.id.cmp(&a.deployment.id))
        });

        Ok(Json(deployments))
    }

    async fn cancel_deployment(
        Path((service_id, deployment_id)): Path<(String, String)>,
        State(state): State<AppState>,
    ) -> Result<Json<CancelDeploymentResponse>, (StatusCode, String)> {
        let service_id = service_id.trim().to_string();
        let deployment_id = deployment_id.trim().to_string();
        validate_service_id(&service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        if deployment_id.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "deploymentId cannot be empty".to_string(),
            ));
        }

        let deployment = Deployment {
            service_id: service_id.clone(),
            id: deployment_id.clone(),
            replica_index: 0,
        };
        let outcome = state
            .store
            .cancel_service_deployment(&deployment)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        match outcome {
            CancelDeploymentOutcome::Canceled(updated) => Ok(Json(CancelDeploymentResponse {
                canceled: true,
                service_id,
                deployment_id,
                status: updated.status,
            })),
            CancelDeploymentOutcome::NotCancelable(existing) => Err((
                StatusCode::CONFLICT,
                format!(
                    "deployment `{deployment_id}` cannot be canceled from status {:?}",
                    existing.status
                ),
            )),
            CancelDeploymentOutcome::NotFound => Err((
                StatusCode::NOT_FOUND,
                format!("deployment `{deployment_id}` was not found for service `{service_id}`"),
            )),
        }
    }

    async fn redeploy_service(
        Path(service_id): Path<String>,
        State(state): State<AppState>,
    ) -> Result<Json<RolloutServiceResponse>, (StatusCode, String)> {
        let service_id = service_id.trim().to_string();
        validate_service_id(&service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        let info = state
            .store
            .read_service_info(&service_id)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let Some(info) = info else {
            return Err((
                StatusCode::NOT_FOUND,
                format!("service `{service_id}` not found"),
            ));
        };

        let deployment = ServiceDeployment::new(info.config)
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let outcome = state
            .store
            .queue_deployment(deployment)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(Json(RolloutServiceResponse {
            queued: true,
            replicas: None,
            deployment_id: Some(outcome.deployment.id.clone()),
            deployment_index: Some(outcome.deployment_index),
            service_id: outcome.deployment.config.id.clone(),
            status: Some(outcome.deployment.status),
            version: outcome.deployment.config.version.clone(),
        }))
    }

    async fn delete_service(
        Path(service_id): Path<String>,
        State(state): State<AppState>,
    ) -> Result<StatusCode, (StatusCode, String)> {
        let service_id = service_id.trim();
        validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        state
            .store
            .delete_service(service_id)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(StatusCode::NO_CONTENT)
    }

    async fn remove_deployment(
        Path((service_id, deployment_id)): Path<(String, String)>,
        State(state): State<AppState>,
    ) -> Result<Json<RemoveDeploymentResponse>, (StatusCode, String)> {
        let service_id = service_id.trim().to_string();
        let deployment_id = deployment_id.trim().to_string();
        validate_service_id(&service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        if deployment_id.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "deploymentId cannot be empty".to_string(),
            ));
        }

        let deployment = Deployment {
            service_id: service_id.clone(),
            id: deployment_id.clone(),
            replica_index: 0,
        };
        let outcome = state
            .store
            .stop_service_deployment(&deployment)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        match outcome {
            Some(updated)
                if updated.status == crate::deployment::types::DeploymentStatus::Removed =>
            {
                Ok(Json(RemoveDeploymentResponse {
                    removed: true,
                    service_id,
                    deployment_id,
                    status: updated.status,
                }))
            }
            Some(existing) => Err((
                StatusCode::CONFLICT,
                format!(
                    "deployment `{deployment_id}` cannot be removed from status {:?}",
                    existing.status
                ),
            )),
            None => Err((
                StatusCode::NOT_FOUND,
                format!("deployment `{deployment_id}` was not found for service `{service_id}`"),
            )),
        }
    }

    async fn get_deployment_logs(
        Path((service_id, deployment_id)): Path<(String, String)>,
        Query(query): Query<LogsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
        let service_id = service_id.trim();
        let deployment_id = deployment_id.trim();
        validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        if deployment_id.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "deploymentId cannot be empty".to_string(),
            ));
        }

        let deployment_dir = state
            .logs_dir
            .join("services")
            .join(service_id)
            .join("deployments")
            .join(deployment_id);

        let deployment = state
            .store
            .read_service_deployment(&Deployment {
                service_id: service_id.to_string(),
                id: deployment_id.to_string(),
                replica_index: 0,
            })
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let tail = query.tail.unwrap_or(DEFAULT_LOG_LIMIT);
        let replicas = deployment
            .as_ref()
            .map(|d| d.config.deploy.replicas)
            .unwrap_or(1);

        let mut all_entries: Vec<serde_json::Value> = Vec::new();
        for replica_index in 0..replicas {
            let log_path = deployment_dir
                .join(format!("replica{replica_index}"))
                .join("logs.jsonl");
            let hostname = deployment
                .as_ref()
                .map(|d| d.hostname_for_replica(replica_index))
                .unwrap_or_else(|| format!("replica{replica_index}"));
            if let Ok(mut entries) = logs::read_jsonl_logs(&log_path, tail).await {
                for entry in &mut entries {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert(
                            "hostname".to_string(),
                            serde_json::Value::String(hostname.clone()),
                        );
                    }
                }
                all_entries.extend(entries);
            }
        }

        all_entries.sort_by(|a, b| {
            let ts_a = a.get("ts").and_then(|v| v.as_u64()).unwrap_or(0);
            let ts_b = b.get("ts").and_then(|v| v.as_u64()).unwrap_or(0);
            ts_a.cmp(&ts_b)
        });

        let start = all_entries.len().saturating_sub(tail);
        Ok(Json(all_entries[start..].to_vec()))
    }

    async fn get_system_logs(
        Path(name): Path<String>,
        Query(query): Query<LogsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
        let name = name.trim();
        validate_service_id(name, "name").map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        let log_path = state.logs_dir.join("system").join(name).join("logs.jsonl");
        logs::read_jsonl_logs(&log_path, query.tail.unwrap_or(DEFAULT_LOG_LIMIT))
            .await
            .map(Json)
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    }
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    tail: Option<usize>,
}

fn build_service_config(request: RolloutServiceRequest) -> Result<ServiceConfig, String> {
    let RolloutServiceRequest {
        id,
        name,
        provider,
        build,
        image,
        deploy,
        ingress,
    } = request;

    let service_id = id.trim().to_string();
    validate_service_id(&service_id, "id")?;

    let service_name = name.trim().to_string();
    if service_name.is_empty() {
        return Err("name cannot be empty".to_string());
    }
    let (build, image, deploy) = validate_service_provider_config(provider, build, image, deploy)?;

    let version_payload = json!({
        "id": &service_id,
        "name": &service_name,
        "provider": &provider,
        "build": &build,
        "image": &image,
        "deploy": {
            "flags": &deploy.flags,
            "exposePorts": &deploy.expose_ports,
            "command": &deploy.command,
            "healthcheckPath": &deploy.healthcheck_path,
        },
        "ingress": &ingress
    });
    let version_bytes = serde_json::to_vec(&version_payload)
        .map_err(|err| format!("failed to serialize version payload: {err}"))?;
    let hash = Sha256::digest(version_bytes);
    let version = format!("cfg-{}", hex_lower(hash.as_slice()));
    Ok(ServiceConfig {
        id: service_id,
        name: service_name,
        version,
        provider,
        build,
        image,
        deploy,
        ingress,
    })
}

async fn upsert_config_and_maybe_queue(
    store: &dyn ClusterStore,
    service_config: ServiceConfig,
) -> Result<UpsertServiceOutcome, String> {
    let service_id = service_config.id.trim().to_string();
    let service_version = service_config.version.trim().to_string();
    if service_version.is_empty() {
        return Err("service version cannot be empty".to_string());
    }

    let service_status = store
        .get_service_status(&service_id)
        .await
        .map_err(|err| err.to_string())?;
    if let Some(existing) = store
        .read_service_info(&service_id)
        .await
        .map_err(|err| err.to_string())?
        && existing.config.version == service_version
        && is_active_service_status(service_status.as_ref())
    {
        let desired_replicas = service_config.deploy.replicas;
        if existing.config.deploy.replicas != desired_replicas {
            let mut updated_config = existing.config.clone();
            updated_config.deploy.replicas = desired_replicas;
            store
                .update_service_config(&service_id, updated_config)
                .await
                .map_err(|err| err.to_string())?;
            return Ok(UpsertServiceOutcome::Scaled {
                service_id,
                version: service_version,
                replicas: desired_replicas,
            });
        }
        return Ok(UpsertServiceOutcome::Unchanged {
            service_id,
            version: service_version,
        });
    }

    let deployment = ServiceDeployment::new(service_config).map_err(|err| err.to_string())?;
    let outcome = store
        .queue_deployment(deployment)
        .await
        .map_err(|err| err.to_string())?;
    Ok(UpsertServiceOutcome::Queued {
        deployment_index: outcome.deployment_index,
        deployment: outcome.deployment,
    })
}

fn is_active_service_status(status: Option<&crate::deployment::types::DeploymentStatus>) -> bool {
    matches!(
        status,
        Some(
            crate::deployment::types::DeploymentStatus::Queued
                | crate::deployment::types::DeploymentStatus::Building
                | crate::deployment::types::DeploymentStatus::PendingReady
                | crate::deployment::types::DeploymentStatus::Ready
        )
    )
}

fn validate_build_config(
    build: Option<ServiceBuildConfig>,
    image: Option<String>,
) -> Result<(Option<ServiceBuildConfig>, Option<String>), String> {
    match (build, image) {
        (Some(build), None) => {
            let repo = build.repo.trim().to_string();
            if repo.is_empty() {
                return Err("build.repo cannot be empty".to_string());
            }
            let dockerfile_path = build.dockerfile_path.trim().to_string();
            if dockerfile_path.is_empty() {
                return Err("build.dockerfilePath cannot be empty".to_string());
            }
            Ok((
                Some(ServiceBuildConfig {
                    repo,
                    dockerfile_path,
                }),
                None,
            ))
        }
        (None, Some(image)) => {
            let image = image.trim().to_string();
            if image.is_empty() {
                return Err("image cannot be empty".to_string());
            }
            Ok((None, Some(image)))
        }
        (Some(_), Some(_)) => Err("set either `build` or `image`, not both".to_string()),
        (None, None) => Err("set either `build` or `image`".to_string()),
    }
}

fn validate_service_provider_config(
    provider: ServiceProvider,
    build: Option<ServiceBuildConfig>,
    image: Option<String>,
    deploy: ServiceDeployConfig,
) -> Result<
    (
        Option<ServiceBuildConfig>,
        Option<String>,
        ServiceDeployConfig,
    ),
    String,
> {
    match provider {
        ServiceProvider::Docker => {
            let (build, image) = validate_build_config(build, image)?;
            Ok((build, image, deploy))
        }
        ServiceProvider::Shell => {
            if build.is_some() || image.is_some() {
                return Err(
                    "shell provider does not allow `build` or `image`; set deploy.command instead"
                        .to_string(),
                );
            }
            let Some(command) = deploy.command.as_ref() else {
                return Err("shell provider requires deploy.command".to_string());
            };
            if command.command.trim().is_empty() {
                return Err("shell provider requires non-empty deploy.command.command".to_string());
            }
            Ok((None, None, deploy))
        }
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from_digit((byte >> 4) as u32, 16).expect("hex nibble"));
        output.push(char::from_digit((byte & 0x0f) as u32, 16).expect("hex nibble"));
    }
    output
}

fn validate_service_id(service_id: &str, field_name: &str) -> Result<(), String> {
    if service_id.is_empty() {
        return Err(format!("{field_name} cannot be empty"));
    }
    let is_id_url_safe = service_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');
    if !is_id_url_safe {
        return Err(format!(
            "{field_name} must be URL-safe and contain only letters, numbers, '-' or '_'"
        ));
    }
    Ok(())
}

async fn log_http(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    let status = response.status();
    let elapsed_ms = started.elapsed().as_millis();

    println!("{method} {path} -> {status} ({elapsed_ms}ms)");
    response
}

#[cfg(test)]
#[path = "../tests/server/mod.rs"]
mod tests;
