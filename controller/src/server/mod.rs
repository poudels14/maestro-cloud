use std::io::Read;
use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, patch, post},
};
use flate2::read::GzDecoder;
use serde::de::DeserializeOwned;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;

use self::types::{
    CancelDeploymentResponse, RemoveDeploymentResponse, RolloutChange, RolloutDiffResponse,
    RolloutDiffStatus, RolloutServiceRequest, RolloutServiceResponse, ServiceListItem,
};
use crate::deployment::store::{ClusterStore, UpsertServiceOutcome};
use crate::deployment::types::{
    CancelDeploymentOutcome, Deployment, SecretsConfig, ServiceConfig, ServiceDeployConfig,
    ServiceDeployment, ServiceProvider,
};
use crate::signal::ShutdownEvent;

mod types;

const DEFAULT_LOG_LIMIT: usize = 1000;

const SYSTEM_SERVICES: &[(&str, &str, &str)] = &[
    ("maestro-etcd", "etcd", "quay.io/coreos/etcd:v3.6.8"),
    ("maestro-ingress", "ingress", "traefik:v3.6"),
    ("maestro-probe", "controller", "maestro-probe"),
    ("maestro-admin", "admin", "maestro-admin"),
    ("maestro-tailscale", "tailscale", "maestro-tailscale"),
];

#[derive(Clone)]
struct AppState {
    store: Arc<dyn ClusterStore>,
    log_store: Option<Arc<crate::logs::LogStore>>,
    jwt_secret: Option<String>,
    system_type: Option<String>,
}

pub(crate) struct Server {
    state: AppState,
}

impl Server {
    pub(crate) fn new(
        store: Arc<dyn ClusterStore>,
        log_store: Option<Arc<crate::logs::LogStore>>,
        jwt_secret: Option<String>,
        system_type: Option<String>,
    ) -> Self {
        Self {
            state: AppState {
                store,
                log_store,
                jwt_secret,
                system_type,
            },
        }
    }

    fn app(&self) -> Router {
        Router::new()
            .route("/_healthy", get(Self::healthy))
            .route("/api/services", get(Self::list_services))
            .route("/api/services/rollout", post(Self::rollout_service))
            .route("/api/services/rollout/diff", post(Self::rollout_diff))
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
            .route(
                "/api/services/{serviceId}/freeze",
                patch(Self::freeze_service),
            )
            .route(
                "/api/services/{serviceId}/deployments/{deploymentId}",
                delete(Self::delete_deployment),
            )
            .route("/api/services/{serviceId}", delete(Self::delete_service))
            .route(
                "/api/services/{serviceId}/deployments/{deploymentId}/logs",
                get(Self::get_deployment_logs),
            )
            .route("/api/system/upgrade", post(Self::upgrade_system))
            .route("/api/system/{name}/logs", get(Self::get_system_logs))
            .route("/api/logs", post(Self::ingest_logs))
            .route("/api/metrics", post(Self::ingest_metrics))
            .route("/api/metrics/node", get(Self::get_node_metrics))
            .route("/api/metrics/cluster", get(Self::get_cluster_metrics))
            .route(
                "/api/services/{serviceId}/metrics",
                get(Self::get_service_metrics),
            )
            .route(
                "/api/services/{serviceId}/metrics/containers",
                get(Self::get_container_metrics),
            )
            .with_state(self.state.clone())
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
        headers: HeaderMap,
        Query(query): Query<ForceQuery>,
        State(state): State<AppState>,
        body: Bytes,
    ) -> Result<Json<RolloutServiceResponse>, (StatusCode, String)> {
        verify_jwt(&state.jwt_secret, &headers)?;
        let request: RolloutServiceRequest = parse_json_body(&headers, body)?;
        let service_config = build_service_config(request).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid rollout request payload: {err}"),
            )
        })?;
        eprintln!(
            "[probe] rollout request service_id={} version={} force={}",
            service_config.id,
            service_config.version,
            query.force.unwrap_or(false)
        );

        if !query.force.unwrap_or(false) {
            let info = state
                .store
                .read_service_info(&service_config.id)
                .await
                .ok()
                .flatten();
            if let Some(info) = info {
                if info.deploy_frozen {
                    return Err((
                        StatusCode::CONFLICT,
                        format!(
                            "deploy is frozen for service `{}`; use ?force=true to override",
                            service_config.id
                        ),
                    ));
                }
            }
        }

        let outcome = upsert_config_and_maybe_queue(state.store.as_ref(), service_config)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let response = match outcome {
            UpsertServiceOutcome::Queued {
                deployment_index,
                deployment,
            } => {
                eprintln!(
                    "[probe] rollout queued service_id={} deployment_id={} index={deployment_index}",
                    deployment.config.id, deployment.id
                );
                RolloutServiceResponse {
                    queued: true,
                    replicas: None,
                    deployment_id: Some(deployment.id.clone()),
                    deployment_index: Some(deployment_index),
                    service_id: deployment.config.id.clone(),
                    status: Some(deployment.status),
                    version: deployment.config.version.clone(),
                }
            }
            UpsertServiceOutcome::Unchanged {
                service_id,
                version,
            } => {
                eprintln!("[probe] rollout unchanged service_id={service_id}");
                RolloutServiceResponse {
                    queued: false,
                    replicas: None,
                    deployment_id: None,
                    deployment_index: None,
                    service_id,
                    status: None,
                    version,
                }
            }
            UpsertServiceOutcome::Scaled {
                service_id,
                version,
                replicas,
            } => {
                eprintln!("[probe] rollout scaled service_id={service_id} replicas={replicas}");
                RolloutServiceResponse {
                    queued: false,
                    replicas: Some(replicas),
                    deployment_id: None,
                    deployment_index: None,
                    service_id,
                    status: None,
                    version,
                }
            }
        };

        Ok(Json(response))
    }

    async fn rollout_diff(
        State(state): State<AppState>,
        Json(request): Json<RolloutServiceRequest>,
    ) -> Result<Json<Vec<RolloutDiffResponse>>, (StatusCode, String)> {
        let service_config = build_service_config(request).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid rollout request payload: {err}"),
            )
        })?;

        let diff = compute_rollout_diff(state.store.as_ref(), &service_config)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(Json(vec![diff]))
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
                deploy_frozen: info.deploy_frozen,
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
                        max_restarts: None,
                        env: Default::default(),
                        secrets: None,
                    },
                    ingress: None,
                },
                status: Some(crate::deployment::types::DeploymentStatus::Ready),
                system: true,
                deploy_frozen: false,
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
        crate::validation::validate_service_id(service_id, "serviceId")
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
        crate::validation::validate_service_id(&service_id, "serviceId")
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

    async fn freeze_service(
        Path(service_id): Path<String>,
        State(state): State<AppState>,
        Json(body): Json<types::FreezeRequest>,
    ) -> Result<Json<types::FreezeResponse>, (StatusCode, String)> {
        let service_id = service_id.trim().to_string();
        crate::validation::validate_service_id(&service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        state
            .store
            .set_deploy_frozen(&service_id, body.frozen)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(Json(types::FreezeResponse {
            service_id,
            deploy_frozen: body.frozen,
        }))
    }

    async fn redeploy_service(
        Path(service_id): Path<String>,
        Query(query): Query<ForceQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<RolloutServiceResponse>, (StatusCode, String)> {
        let service_id = service_id.trim().to_string();
        crate::validation::validate_service_id(&service_id, "serviceId")
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

        if info.deploy_frozen && !query.force.unwrap_or(false) {
            return Err((
                StatusCode::CONFLICT,
                format!("deploy is frozen for service `{service_id}`; use ?force=true to override"),
            ));
        }

        let mut config = info.config;

        let deployments = state
            .store
            .list_service_deployments(&service_id)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        if let Some(prev) = deployments.first() {
            if let Some(secrets) = &mut config.deploy.secrets {
                let items = state
                    .store
                    .read_deployment_secrets(&service_id, &prev.id)
                    .await
                    .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                secrets.items = items;
            }
        }

        let deployment = ServiceDeployment::new(config)
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
        crate::validation::validate_service_id(service_id, "serviceId")
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
        crate::validation::validate_service_id(&service_id, "serviceId")
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

    async fn delete_deployment(
        Path((service_id, deployment_id)): Path<(String, String)>,
        State(state): State<AppState>,
    ) -> Result<StatusCode, (StatusCode, String)> {
        let service_id = service_id.trim().to_string();
        let deployment_id = deployment_id.trim().to_string();
        crate::validation::validate_service_id(&service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        if deployment_id.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "deploymentId cannot be empty".to_string(),
            ));
        }

        let deployment = crate::deployment::types::Deployment {
            service_id: service_id.clone(),
            id: deployment_id.clone(),
            replica_index: 0,
        };
        let result = state
            .store
            .delete_deployment(&deployment)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        if result.is_none() {
            return Err((
                StatusCode::NOT_FOUND,
                format!("deployment `{deployment_id}` was not found for service `{service_id}`"),
            ));
        }

        Ok(StatusCode::NO_CONTENT)
    }

    async fn get_deployment_logs(
        Path((service_id, deployment_id)): Path<(String, String)>,
        Query(query): Query<LogsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
        let service_id = service_id.trim();
        let deployment_id = deployment_id.trim();
        crate::validation::validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        if deployment_id.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "deploymentId cannot be empty".to_string(),
            ));
        }

        let tail = query.tail.unwrap_or(DEFAULT_LOG_LIMIT);

        if let Some(log_store) = &state.log_store {
            let prefix = format!("{service_id}/{deployment_id}/");
            let entries = log_store
                .read_tail_by_prefix(&prefix, tail)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
            let values: Vec<serde_json::Value> = entries
                .into_iter()
                .map(|e| serde_json::to_value(e).unwrap_or_default())
                .collect();
            return Ok(Json(values));
        }

        Ok(Json(Vec::new()))
    }

    async fn get_system_logs(
        Path(name): Path<String>,
        Query(query): Query<LogsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
        let name = name.trim();
        crate::validation::validate_service_id(name, "name")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        let tail = query.tail.unwrap_or(DEFAULT_LOG_LIMIT);

        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let entries = if name == "maestro-probe" {
            log_store
                .read_tail_sources(&["maestro-probe", "maestro-controller"], tail)
                .await
        } else {
            log_store.read_tail(name, tail).await
        }
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let values: Vec<serde_json::Value> = entries
            .into_iter()
            .filter_map(|e| serde_json::to_value(e).ok())
            .collect();
        Ok(Json(values))
    }

    async fn ingest_logs(
        State(state): State<AppState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<&'static str, (StatusCode, String)> {
        let entries: Vec<crate::logs::LogEntry> = parse_json_body(&headers, body)?;
        let Some(log_store) = &state.log_store else {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "log store not configured".to_string(),
            ));
        };
        log_store
            .append(&entries)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok("ok")
    }

    async fn ingest_metrics(
        State(state): State<AppState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<&'static str, (StatusCode, String)> {
        let entries: Vec<crate::metrics::MetricPoint> = parse_json_body(&headers, body)?;
        let Some(log_store) = &state.log_store else {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "log store not configured".to_string(),
            ));
        };
        log_store
            .append_metrics(&entries)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let _ = log_store.cleanup_old_metrics(7 * 24 * 60 * 60 * 1000).await;
        Ok("ok")
    }

    async fn get_node_metrics(
        Query(query): Query<MetricsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::metrics::MetricPoint>>, (StatusCode, String)> {
        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let (from, to) = metrics_time_range(&query);
        let entries = log_store
            .read_metrics("node", from, to)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(entries))
    }

    async fn get_cluster_metrics(
        Query(query): Query<MetricsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::metrics::MetricPoint>>, (StatusCode, String)> {
        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let (from, to) = metrics_time_range(&query);
        let entries = log_store
            .read_metrics("cluster", from, to)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(entries))
    }

    async fn get_service_metrics(
        Path(service_id): Path<String>,
        Query(query): Query<MetricsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::metrics::MetricPoint>>, (StatusCode, String)> {
        let service_id = service_id.trim();
        crate::validation::validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let (from, to) = metrics_time_range(&query);
        let source = format!("service:{service_id}");
        let entries = log_store
            .read_metrics(&source, from, to)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(entries))
    }

    async fn get_container_metrics(
        Path(service_id): Path<String>,
        Query(query): Query<MetricsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::metrics::MetricPoint>>, (StatusCode, String)> {
        let service_id = service_id.trim();
        crate::validation::validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let (from, to) = metrics_time_range(&query);
        let prefix = format!("container:{service_id}-");
        let entries = log_store
            .read_metrics_by_prefix(&prefix, from, to)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(entries))
    }

    async fn upgrade_system(
        headers: HeaderMap,
        State(state): State<AppState>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        verify_jwt(&state.jwt_secret, &headers)?;
        let system_type = state.system_type.as_deref().unwrap_or("controller");
        eprintln!("[probe] upgrade request system={system_type}");
        state
            .store
            .put_system_upgrade_request(system_type)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        eprintln!("[probe] upgrade request accepted system={system_type}");
        Ok(Json(json!({
            "accepted": true,
            "system": system_type,
        })))
    }
}

#[derive(serde::Deserialize)]
struct MetricsQuery {
    from: Option<i64>,
    to: Option<i64>,
}

fn metrics_time_range(query: &MetricsQuery) -> (i64, i64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let from = query.from.unwrap_or(now - 3_600_000);
    let to = query.to.unwrap_or(now);
    (from, to)
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    tail: Option<usize>,
}

#[derive(serde::Deserialize)]
struct ForceQuery {
    force: Option<bool>,
}

fn verify_jwt(
    jwt_secret: &Option<String>,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, String)> {
    let Some(secret) = jwt_secret else {
        return Ok(());
    };
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "missing or invalid Authorization header".to_string(),
            )
        })?;
    let key = jsonwebtoken::DecodingKey::from_secret(secret.as_bytes());
    let validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    jsonwebtoken::decode::<serde_json::Value>(token, &key, &validation).map_err(|err| {
        (
            StatusCode::UNAUTHORIZED,
            format!("invalid auth token: {err}"),
        )
    })?;
    Ok(())
}

fn parse_json_body<T: DeserializeOwned>(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<T, (StatusCode, String)> {
    let is_gzip = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("gzip"))
        })
        .unwrap_or(false);

    let bytes = if is_gzip {
        let mut decoder = GzDecoder::new(body.as_ref());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("failed to decode gzip request body: {err}"),
            )
        })?;
        decompressed
    } else {
        body.to_vec()
    };

    serde_json::from_slice::<T>(&bytes).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid JSON request body: {err}"),
        )
    })
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
    crate::validation::validate_service_id(&service_id, "id")?;

    let service_name = name.trim().to_string();
    if service_name.is_empty() {
        return Err("name cannot be empty".to_string());
    }
    let (build, image, deploy) =
        crate::validation::validate_service_provider_config(provider, &build, &image, &deploy)?;

    let secrets_hash = deploy.secrets.as_ref().map(|s| s.compute_secrets_hash());
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
            "env": &deploy.env,
            "secretsHash": &secrets_hash,
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

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from_digit((byte >> 4) as u32, 16).expect("hex nibble"));
        output.push(char::from_digit((byte & 0x0f) as u32, 16).expect("hex nibble"));
    }
    output
}

async fn compute_rollout_diff(
    store: &dyn ClusterStore,
    new_config: &ServiceConfig,
) -> Result<RolloutDiffResponse, String> {
    let existing = store
        .read_service_info(&new_config.id)
        .await
        .map_err(|err| err.to_string())?;

    let Some(existing) = existing else {
        return Ok(RolloutDiffResponse {
            service_id: new_config.id.clone(),
            status: RolloutDiffStatus::New,
            changes: Vec::new(),
        });
    };

    let old = &existing.config;
    let mut changes = Vec::new();

    if old.image != new_config.image {
        changes.push(RolloutChange {
            field: "image".into(),
            from: old.image.clone(),
            to: new_config.image.clone(),
        });
    }

    if old.build != new_config.build {
        changes.push(RolloutChange {
            field: "build".into(),
            from: old
                .build
                .as_ref()
                .map(|b| format!("{}:{}", b.repo, b.dockerfile_path)),
            to: new_config
                .build
                .as_ref()
                .map(|b| format!("{}:{}", b.repo, b.dockerfile_path)),
        });
    }

    if old.ingress != new_config.ingress {
        changes.push(RolloutChange {
            field: "ingress".into(),
            from: old.ingress.as_ref().map(|i| i.host.clone()),
            to: new_config.ingress.as_ref().map(|i| i.host.clone()),
        });
    }

    if old.deploy.replicas != new_config.deploy.replicas {
        changes.push(RolloutChange {
            field: "replicas".into(),
            from: Some(old.deploy.replicas.to_string()),
            to: Some(new_config.deploy.replicas.to_string()),
        });
    }

    if old.deploy.healthcheck_path != new_config.deploy.healthcheck_path {
        changes.push(RolloutChange {
            field: "healthcheckPath".into(),
            from: old.deploy.healthcheck_path.clone(),
            to: new_config.deploy.healthcheck_path.clone(),
        });
    }

    if old.deploy.command != new_config.deploy.command {
        changes.push(RolloutChange {
            field: "command".into(),
            from: old.deploy.command.as_ref().map(|c| c.command.clone()),
            to: new_config
                .deploy
                .command
                .as_ref()
                .map(|c| c.command.clone()),
        });
    }

    for (key, new_val) in &new_config.deploy.env {
        if let Some(old_val) = old.deploy.env.get(key) {
            if old_val != new_val {
                changes.push(RolloutChange {
                    field: format!("env.{key}"),
                    from: Some(old_val.clone()),
                    to: Some(new_val.clone()),
                });
            }
        } else {
            changes.push(RolloutChange {
                field: format!("env.{key}"),
                from: None,
                to: Some(new_val.clone()),
            });
        }
    }
    for key in old.deploy.env.keys() {
        if !new_config.deploy.env.contains_key(key) {
            changes.push(RolloutChange {
                field: format!("env.{key}"),
                from: Some(old.deploy.env[key].clone()),
                to: None,
            });
        }
    }

    diff_secrets(
        &old.deploy.secrets,
        &new_config.deploy.secrets,
        &mut changes,
    );

    let status = if changes.is_empty() {
        RolloutDiffStatus::Unchanged
    } else {
        RolloutDiffStatus::Changed
    };

    Ok(RolloutDiffResponse {
        service_id: new_config.id.clone(),
        status,
        changes,
    })
}

fn diff_secrets(
    old: &Option<SecretsConfig>,
    new: &Option<SecretsConfig>,
    changes: &mut Vec<RolloutChange>,
) {
    let old_keys = old.as_ref().map(|s| &s.keys);
    let new_items = new.as_ref().map(|s| &s.items);

    match (old_keys, new_items) {
        (None, None) => {}
        (None, Some(new_items)) => {
            for key in new_items.keys() {
                changes.push(RolloutChange {
                    field: format!("secret.{key}"),
                    from: None,
                    to: Some("(set)".into()),
                });
            }
        }
        (Some(old_keys), None) => {
            for key in old_keys.keys() {
                changes.push(RolloutChange {
                    field: format!("secret.{key}"),
                    from: Some("(set)".into()),
                    to: None,
                });
            }
        }
        (Some(old_keys), Some(new_items)) => {
            for (key, new_val) in new_items {
                let new_hash = SecretsConfig::compute_value_hash(new_val);
                if let Some(meta) = old_keys.get(key) {
                    if meta.hash != new_hash {
                        changes.push(RolloutChange {
                            field: format!("secret.{key}"),
                            from: Some("(changed)".into()),
                            to: Some("(changed)".into()),
                        });
                    }
                } else {
                    changes.push(RolloutChange {
                        field: format!("secret.{key}"),
                        from: None,
                        to: Some("(set)".into()),
                    });
                }
            }
            for key in old_keys.keys() {
                if !new_items.contains_key(key) {
                    changes.push(RolloutChange {
                        field: format!("secret.{key}"),
                        from: Some("(set)".into()),
                        to: None,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/server/mod.rs"]
mod tests;
