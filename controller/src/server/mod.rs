use std::sync::Arc;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, patch, post},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;

use self::types::{
    CancelDeploymentResponse, PatchServiceRequest, PatchServiceResponse, RemoveDeploymentResponse,
    ServiceListItem,
};
use crate::deployment::store::{CancelDeploymentOutcome, ClusterStore, UpsertServiceOutcome};
use crate::deployment::types::{
    ServiceBuildConfig, ServiceConfig, ServiceDeployConfig, ServiceDeployment, ServiceProvider,
};
use crate::signal::ShutdownEvent;

mod types;

#[derive(Clone)]
struct AppState {
    store: Arc<dyn ClusterStore>,
}

pub(crate) struct Server {
    state: AppState,
}

impl Server {
    pub(crate) fn new(store: Arc<dyn ClusterStore>) -> Self {
        Self {
            state: AppState { store },
        }
    }

    fn app(&self) -> Router {
        Router::new()
            .route("/_healthy", get(Self::healthy))
            .route("/api/services", get(Self::list_services))
            .route("/api/services/patch", patch(Self::patch_service))
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
            "[maestro]: server listening on http://{bind_addr} [pid={}]",
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

    async fn patch_service(
        State(state): State<AppState>,
        Json(request): Json<PatchServiceRequest>,
    ) -> Result<Json<PatchServiceResponse>, (StatusCode, String)> {
        let service_config = build_service_config(request).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid patch request payload: {err}"),
            )
        })?;

        let outcome = upsert_config_and_maybe_queue(state.store.as_ref(), service_config)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let response = match outcome {
            UpsertServiceOutcome::Queued {
                deployment_index,
                deployment,
            } => PatchServiceResponse {
                queued: true,
                deployment_id: Some(deployment.id.clone()),
                deployment_index: Some(deployment_index),
                service_id: deployment.config.id.clone(),
                status: Some(deployment.status),
                version: deployment.config.version.clone(),
            },
            UpsertServiceOutcome::Unchanged {
                service_id,
                version,
            } => PatchServiceResponse {
                queued: false,
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

        let items = infos
            .into_iter()
            .map(|info| ServiceListItem {
                service: info.config,
                status: info.status,
            })
            .collect();

        Ok(Json(items))
    }

    async fn list_deployments(
        Path(service_id): Path<String>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<ServiceDeployment>>, (StatusCode, String)> {
        let service_id = service_id.trim();
        validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        let deployments = state
            .store
            .list_service_deployments(service_id)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

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

        let outcome = state
            .store
            .cancel_service_deployment(&service_id, &deployment_id)
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
    ) -> Result<Json<PatchServiceResponse>, (StatusCode, String)> {
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

        Ok(Json(PatchServiceResponse {
            queued: true,
            deployment_id: Some(outcome.deployment.id.clone()),
            deployment_index: Some(outcome.deployment_index),
            service_id: outcome.deployment.config.id.clone(),
            status: Some(outcome.deployment.status),
            version: outcome.deployment.config.version.clone(),
        }))
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

        let outcome = state
            .store
            .stop_service_deployment(&service_id, &deployment_id)
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
}

fn build_service_config(request: PatchServiceRequest) -> Result<ServiceConfig, String> {
    let PatchServiceRequest {
        id,
        name,
        provider,
        build,
        image,
        deploy,
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
        "deploy": &deploy
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

    if let Some(existing) = store
        .read_service_info(&service_id)
        .await
        .map_err(|err| err.to_string())?
        && existing.config.version == service_version
        && is_active_service_status(existing.status.as_ref())
    {
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

    println!("[http] {method} {path} -> {status} ({elapsed_ms}ms)");
    response
}

#[cfg(test)]
#[path = "../tests/server/mod.rs"]
mod tests;
