use std::{collections::HashSet, io::Read, path::Path as FsPath, sync::Arc};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Multipart, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{delete, get, patch, post},
};
use flate2::read::GzDecoder;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;

use self::types::{
    CancelDeploymentResponse, CreateSlackWebhookRequest, RemoveDeploymentResponse,
    ReplicasOverrideRequest, ReplicasResponse, RolloutChange, RolloutDiffResponse,
    RolloutDiffStatus, RolloutServiceRequest, RolloutServiceResponse, ServiceListItem,
    SlackWebhookView, UpdateSlackWebhookRequest, UpgradeSystemRequest, UploadServiceResponse,
};
use crate::deployment::store::{ClusterStore, UpsertServiceOutcome};
use crate::deployment::types::{
    CancelDeploymentOutcome, Deployment, DeploymentBuildInfo, SecretsConfig, ServiceConfig,
    ServiceDeployConfig, ServiceDeployment,
};
use crate::logs::LogEntry;
use crate::logs::store::LogOrigin;
use crate::signal::ShutdownEvent;

mod types;

const DEFAULT_LOG_LIMIT: usize = 1000;
const MAX_LOG_LIMIT: usize = 2000;
const MAX_REPLICAS_OVERRIDE: u32 = 25;
const MAESTRO_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
enum UpgradeVersionError {
    InvalidCurrent(semver::Error),
    InvalidTarget(semver::Error),
    NotNewer {
        current: semver::Version,
        target: semver::Version,
    },
}

fn validate_upgrade_version(
    current: &str,
    target: &str,
) -> Result<(semver::Version, semver::Version), UpgradeVersionError> {
    let current = semver::Version::parse(current).map_err(UpgradeVersionError::InvalidCurrent)?;
    let target = semver::Version::parse(target).map_err(UpgradeVersionError::InvalidTarget)?;
    if target <= current {
        return Err(UpgradeVersionError::NotNewer { current, target });
    }
    Ok((current, target))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiskInfo {
    name: String,
    mount_point: String,
    total_bytes: u64,
    available_bytes: u64,
    file_system: String,
}

#[derive(Clone)]
struct AppState {
    store: Arc<dyn ClusterStore>,
    log_store: Option<Arc<crate::logs::TelemetryStore>>,
    jwt_secret_key: Option<String>,
    system_type: Option<String>,
    cluster_name: String,
    cluster_alias: String,
    masked_config: Option<Arc<crate::config::MaskedConfig>>,
    slack: crate::slack::SlackNotifier,
    allow_cli_deployment: bool,
    upload_dir: std::path::PathBuf,
}

pub(crate) struct Server {
    state: AppState,
}

impl Server {
    pub(crate) fn new(
        store: Arc<dyn ClusterStore>,
        log_store: Option<Arc<crate::logs::TelemetryStore>>,
        jwt_secret_key: Option<String>,
        system_type: Option<String>,
        cluster_name: String,
        cluster_alias: String,
        masked_config: Option<Arc<crate::config::MaskedConfig>>,
        slack: crate::slack::SlackNotifier,
        allow_cli_deployment: bool,
        upload_dir: std::path::PathBuf,
    ) -> Self {
        Self {
            state: AppState {
                store,
                log_store,
                jwt_secret_key,
                system_type,
                cluster_name,
                cluster_alias,
                masked_config,
                slack,
                allow_cli_deployment,
                upload_dir,
            },
        }
    }

    fn app(&self) -> Router {
        let auth = middleware::from_fn_with_state(self.state.clone(), require_jwt);
        let protected = Router::new()
            .route("/api/services/rollout", post(Self::rollout_service))
            .route(
                "/api/services/up",
                post(Self::upload_service).layer(DefaultBodyLimit::disable()),
            )
            .route("/api/system/upgrade", post(Self::upgrade_system))
            .route("/api/system/restart", post(Self::restart_system))
            .route_layer(auth);

        let public = Router::new()
            .route("/_healthy", get(Self::healthy))
            .route("/api/cluster", get(Self::get_cluster_info))
            .route("/api/config", get(Self::get_config))
            .route("/api/services", get(Self::list_services))
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
                "/api/services/{serviceId}/restart",
                post(Self::restart_service),
            )
            .route(
                "/api/services/{serviceId}/freeze",
                patch(Self::freeze_service),
            )
            .route(
                "/api/services/{serviceId}/replicas",
                patch(Self::set_service_replicas).delete(Self::clear_service_replicas),
            )
            .route(
                "/api/webhooks/slack",
                get(Self::list_slack_webhooks).post(Self::create_slack_webhook),
            )
            .route(
                "/api/webhooks/slack/{id}",
                patch(Self::update_slack_webhook).delete(Self::delete_slack_webhook),
            )
            .route(
                "/api/webhooks/slack/{id}/test",
                post(Self::test_slack_webhook),
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
            .route(
                "/api/services/{serviceId}/logs",
                get(Self::get_service_logs),
            )
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
                "/api/services/{serviceId}/traffic",
                get(Self::get_service_traffic),
            )
            .route("/api/disks", get(Self::get_disks))
            .route("/api/ingress/routes", get(Self::list_ingress_routes))
            .route(
                "/api/services/{serviceId}/metrics/containers",
                get(Self::get_container_metrics),
            );

        public.merge(protected).with_state(self.state.clone())
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
            "server listening on http://{bind_addr} [pid={}]",
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

    async fn get_config(
        State(state): State<AppState>,
    ) -> Result<Json<Arc<crate::config::MaskedConfig>>, (StatusCode, String)> {
        match &state.masked_config {
            Some(config) => Ok(Json(config.clone())),
            None => Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "MAESTRO_CONFIG env var not set".to_string(),
            )),
        }
    }

    async fn get_cluster_info(State(state): State<AppState>) -> Json<serde_json::Value> {
        let canonical_domain = format!("{}.maestro.internal", state.cluster_name);
        let alias_domain = format!("{}.maestro.internal", state.cluster_alias);
        let upgrading = state
            .store
            .read_system_upgrade_request()
            .await
            .ok()
            .flatten()
            .is_some();
        Json(serde_json::json!({
            "clusterName": state.cluster_name,
            "clusterAlias": state.cluster_alias,
            "canonicalDomain": canonical_domain,
            "aliasDomain": alias_domain,
            "version": MAESTRO_VERSION,
            "upgrading": upgrading,
        }))
    }

    async fn rollout_service(
        headers: HeaderMap,
        Query(query): Query<ForceQuery>,
        State(state): State<AppState>,
        body: Bytes,
    ) -> Result<Json<RolloutServiceResponse>, (StatusCode, String)> {
        let request: RolloutServiceRequest = parse_json_body(&headers, body)?;
        let service_config = build_service_config(request).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid rollout request payload: {err}"),
            )
        })?;
        if let Some(build) = service_config.build.as_ref()
            && build
                .repo
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err((
                    StatusCode::BAD_REQUEST,
                    "build.repo is required for rollout; use the `maestro services up` command to upload a local context".to_string(),
                ));
        }
        eprintln!(
            "rollout request service_id={} version={} force={}",
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
            if let Some(info) = info
                && info.deploy_frozen
            {
                return Err((
                    StatusCode::CONFLICT,
                    format!(
                        "deploy is frozen for service `{}`; use ?force=true to override",
                        service_config.id
                    ),
                ));
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
                    "rollout queued service_id={} deployment_id={} index={deployment_index}",
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
                eprintln!("rollout unchanged service_id={service_id}");
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
                eprintln!("rollout scaled service_id={service_id} replicas={replicas}");
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
        if let Some(build) = service_config.build.as_ref()
            && build
                .repo
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err((
                    StatusCode::BAD_REQUEST,
                    "build.repo is required for rollout; use the `maestro services up` command to upload a local context".to_string(),
                ));
        }

        let diff = compute_rollout_diff(state.store.as_ref(), &service_config)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(Json(vec![diff]))
    }

    async fn upload_service(
        State(state): State<AppState>,
        mut multipart: Multipart,
    ) -> Result<Json<UploadServiceResponse>, (StatusCode, String)> {
        if !state.allow_cli_deployment {
            return Err((
                StatusCode::FORBIDDEN,
                "cli deployment is disabled on this cluster; set allow-cli-deployment: true in maestro.jsonc to enable".to_string(),
            ));
        }

        std::fs::create_dir_all(&state.upload_dir).map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create upload dir: {err}"),
            )
        })?;
        let temp_filename = format!("upload-{}.tmp", crate::utils::nanoid::unique_id(16));
        let temp_path = state.upload_dir.join(&temp_filename);

        let mut spec_bytes: Option<Bytes> = None;
        let mut archive_hash: Option<String> = None;
        let mut archive_persisted = false;
        while let Some(mut field) = multipart.next_field().await.map_err(|err| {
            let _ = std::fs::remove_file(&temp_path);
            (StatusCode::BAD_REQUEST, format!("invalid multipart: {err}"))
        })? {
            match field.name() {
                Some("spec") => {
                    spec_bytes = Some(field.bytes().await.map_err(|err| {
                        let _ = std::fs::remove_file(&temp_path);
                        (
                            StatusCode::BAD_REQUEST,
                            format!("failed to read spec part: {err}"),
                        )
                    })?);
                }
                Some("context") => {
                    use std::io::Write;
                    let mut file = std::fs::File::create(&temp_path).map_err(|err| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to create upload temp file: {err}"),
                        )
                    })?;
                    let mut hasher = Sha256::new();
                    while let Some(chunk) = field.chunk().await.map_err(|err| {
                        let _ = std::fs::remove_file(&temp_path);
                        (
                            StatusCode::BAD_REQUEST,
                            format!("failed to read context chunk: {err}"),
                        )
                    })? {
                        hasher.update(&chunk);
                        file.write_all(&chunk).map_err(|err| {
                            let _ = std::fs::remove_file(&temp_path);
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("failed to write upload temp file: {err}"),
                            )
                        })?;
                    }
                    archive_hash = Some(hex_lower(hasher.finalize().as_slice()));
                    archive_persisted = true;
                }
                _ => {}
            }
        }

        let cleanup_temp = || {
            if archive_persisted {
                let _ = std::fs::remove_file(&temp_path);
            }
        };

        let spec = spec_bytes.ok_or_else(|| {
            cleanup_temp();
            (
                StatusCode::BAD_REQUEST,
                "missing `spec` part in multipart payload".to_string(),
            )
        })?;
        let archive_hash = archive_hash.ok_or_else(|| {
            cleanup_temp();
            (
                StatusCode::BAD_REQUEST,
                "missing `context` part in multipart payload".to_string(),
            )
        })?;

        let request: RolloutServiceRequest = serde_json::from_slice(&spec).map_err(|err| {
            cleanup_temp();
            (StatusCode::BAD_REQUEST, format!("invalid spec json: {err}"))
        })?;
        let mut service_config = build_service_config(request).map_err(|err| {
            cleanup_temp();
            (
                StatusCode::BAD_REQUEST,
                format!("invalid upload request payload: {err}"),
            )
        })?;

        service_config.name = format!("[up] {}", service_config.name);
        service_config.version = format!("{}-up-{}", service_config.version, &archive_hash[..12]);

        let mut deployment = ServiceDeployment::new(service_config).map_err(|err| {
            cleanup_temp();
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
        })?;
        let archive_filename = format!("{}-{}.tar.gz", deployment.config.id, deployment.id);
        let archive_path = state.upload_dir.join(&archive_filename);
        std::fs::rename(&temp_path, &archive_path).map_err(|err| {
            cleanup_temp();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to finalize upload archive: {err}"),
            )
        })?;
        deployment.upload_archive = Some(archive_filename);

        let queued = match state.store.queue_deployment(deployment).await {
            Ok(queued) => queued,
            Err(err) => {
                let _ = std::fs::remove_file(&archive_path);
                return Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()));
            }
        };

        eprintln!(
            "cli-upload queued service_id={} deployment_id={} index={}",
            queued.deployment.config.id, queued.deployment.id, queued.deployment_index,
        );

        Ok(Json(UploadServiceResponse {
            service_id: queued.deployment.config.id.clone(),
            deployment_id: queued.deployment.id.clone(),
            version: queued.deployment.config.version.clone(),
            name: queued.deployment.config.name.clone(),
        }))
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
                replicas_override: info.replicas_override,
                service: info.config,
                status,
                system: false,
            });
        }

        let cloudflared_replicas = state
            .masked_config
            .as_ref()
            .and_then(|cfg| cfg.cloudflare.as_ref())
            .and_then(|cf| cf.tunnel.replicas)
            .unwrap_or(2)
            .max(1);

        for system_service in crate::deployment::SYSTEM_SERVICES {
            let replicas = match system_service.id {
                "maestro-cloudflared" => cloudflared_replicas,
                _ => 1,
            };
            items.push(ServiceListItem {
                service: ServiceConfig {
                    id: system_service.id.to_string(),
                    name: system_service.name.to_string(),
                    version: String::new(),
                    build: None,
                    image: Some(system_service.image.to_string()),
                    deploy: ServiceDeployConfig {
                        flags: vec![],
                        expose_ports: vec![],
                        command: None,
                        healthcheck_path: None,
                        healthcheck_interval:
                            crate::deployment::types::DEFAULT_HEALTHCHECK_INTERVAL_SECS,
                        replicas,
                        max_restarts: None,
                        env: Default::default(),
                        secrets: None,
                        volumes: vec![],
                    },
                    ingress: None,
                },
                status: Some(crate::deployment::types::DeploymentStatus::Ready),
                system: true,
                deploy_frozen: false,
                replicas_override: None,
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

        for item in &mut deployments {
            item.deployment.config = item.deployment.config.mask_secrets();
        }

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

    async fn set_service_replicas(
        Path(service_id): Path<String>,
        State(state): State<AppState>,
        Json(body): Json<ReplicasOverrideRequest>,
    ) -> Result<Json<ReplicasResponse>, (StatusCode, String)> {
        let service_id = service_id.trim().to_string();
        crate::validation::validate_service_id(&service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        if body.replicas == 0 {
            return Err((StatusCode::BAD_REQUEST, "replicas must be >= 1".to_string()));
        }
        if body.replicas > MAX_REPLICAS_OVERRIDE {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("replicas must be <= {MAX_REPLICAS_OVERRIDE}"),
            ));
        }

        let info = state
            .store
            .read_service_info(&service_id)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    format!("service `{service_id}` not found"),
                )
            })?;

        let configured = info.config.deploy.replicas;
        if body.replicas < configured {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "replicas override ({}) cannot be less than the configured value ({configured})",
                    body.replicas
                ),
            ));
        }
        if body.replicas > 1 && crate::validation::has_writable_volume(&info.config.deploy) {
            return Err((
                StatusCode::BAD_REQUEST,
                "cannot scale above 1 replica while a writable volume is mounted; \
                 mark the volume `readOnly: true` first"
                    .to_string(),
            ));
        }

        let new_override = if body.replicas == configured {
            None
        } else {
            Some(body.replicas)
        };

        state
            .store
            .set_replicas_override(&service_id, new_override)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(Json(ReplicasResponse {
            service_id,
            replicas: body.replicas,
            replicas_override: new_override,
        }))
    }

    async fn clear_service_replicas(
        Path(service_id): Path<String>,
        State(state): State<AppState>,
    ) -> Result<Json<ReplicasResponse>, (StatusCode, String)> {
        let service_id = service_id.trim().to_string();
        crate::validation::validate_service_id(&service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        let info = state
            .store
            .read_service_info(&service_id)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    format!("service `{service_id}` not found"),
                )
            })?;

        state
            .store
            .set_replicas_override(&service_id, None)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(Json(ReplicasResponse {
            service_id,
            replicas: info.config.deploy.replicas,
            replicas_override: None,
        }))
    }

    async fn list_slack_webhooks(
        State(state): State<AppState>,
    ) -> Result<Json<Vec<SlackWebhookView>>, (StatusCode, String)> {
        let webhooks = state
            .store
            .list_slack_webhooks()
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(webhooks.iter().map(slack_webhook_view).collect()))
    }

    async fn create_slack_webhook(
        State(state): State<AppState>,
        Json(body): Json<CreateSlackWebhookRequest>,
    ) -> Result<Json<SlackWebhookView>, (StatusCode, String)> {
        validate_webhook_url(&body.url)?;
        if body.name.trim().is_empty() {
            return Err((StatusCode::BAD_REQUEST, "name cannot be empty".to_string()));
        }
        if body.categories.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "at least one category must be selected".to_string(),
            ));
        }

        let mut webhooks = state
            .store
            .list_slack_webhooks()
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let new_webhook = crate::slack::SlackWebhook {
            id: format!("wh_{}", crate::utils::nanoid::unique_id(10)),
            name: body.name.trim().to_string(),
            url: crate::utils::crypto::SecretString::new(body.url),
            categories: body.categories,
            enabled: body.enabled.unwrap_or(true),
        };
        webhooks.push(new_webhook.clone());

        state
            .store
            .write_slack_webhooks(&webhooks)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(Json(slack_webhook_view(&new_webhook)))
    }

    async fn update_slack_webhook(
        Path(id): Path<String>,
        State(state): State<AppState>,
        Json(body): Json<UpdateSlackWebhookRequest>,
    ) -> Result<Json<SlackWebhookView>, (StatusCode, String)> {
        let mut webhooks = state
            .store
            .list_slack_webhooks()
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let target = webhooks
            .iter_mut()
            .find(|w| w.id == id)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("webhook `{id}` not found")))?;

        if let Some(name) = body.name {
            if name.trim().is_empty() {
                return Err((StatusCode::BAD_REQUEST, "name cannot be empty".to_string()));
            }
            target.name = name.trim().to_string();
        }
        if let Some(url) = body.url {
            validate_webhook_url(&url)?;
            target.url = crate::utils::crypto::SecretString::new(url);
        }
        if let Some(categories) = body.categories {
            if categories.is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "at least one category must be selected".to_string(),
                ));
            }
            target.categories = categories;
        }
        if let Some(enabled) = body.enabled {
            target.enabled = enabled;
        }

        let updated = target.clone();

        state
            .store
            .write_slack_webhooks(&webhooks)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        Ok(Json(slack_webhook_view(&updated)))
    }

    async fn delete_slack_webhook(
        Path(id): Path<String>,
        State(state): State<AppState>,
    ) -> Result<StatusCode, (StatusCode, String)> {
        let mut webhooks = state
            .store
            .list_slack_webhooks()
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let before = webhooks.len();
        webhooks.retain(|w| w.id != id);
        if webhooks.len() == before {
            return Err((StatusCode::NOT_FOUND, format!("webhook `{id}` not found")));
        }
        state
            .store
            .write_slack_webhooks(&webhooks)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(StatusCode::NO_CONTENT)
    }

    async fn test_slack_webhook(
        Path(id): Path<String>,
        State(state): State<AppState>,
    ) -> Result<StatusCode, (StatusCode, String)> {
        let webhooks = state
            .store
            .list_slack_webhooks()
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let webhook = webhooks
            .into_iter()
            .find(|w| w.id == id)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("webhook `{id}` not found")))?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let text = format!(
            ":test_tube: [{}] test message from maestro webhook `{}`",
            state.cluster_name, webhook.name
        );
        let response = client
            .post(webhook.url.as_str())
            .json(&serde_json::json!({ "text": text }))
            .send()
            .await
            .map_err(|err| {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("failed to deliver test message: {err}"),
                )
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err((
                StatusCode::BAD_GATEWAY,
                format!("slack returned {status}: {body}"),
            ));
        }
        Ok(StatusCode::NO_CONTENT)
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
        if let Some(prev) = deployments.first()
            && let Some(secrets) = &mut config.deploy.secrets
        {
            let items = state
                .store
                .read_deployment_secrets(&service_id, &prev.id)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
            secrets.items = items;
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

    async fn restart_service(
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

        let deployments = state
            .store
            .list_service_deployments(&service_id)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let previous = deployments
            .iter()
            .find(|deployment| deployment.build.is_some())
            .ok_or_else(|| {
                (
                    StatusCode::CONFLICT,
                    format!("service `{service_id}` has no built image to restart from"),
                )
            })?;

        let previous_image = previous
            .build
            .as_ref()
            .expect("previous deployment has build info")
            .docker_image_id
            .clone();
        let previous_git_commit = previous.git_commit.clone();

        let mut config = info.config;
        if let Some(secrets) = config.deploy.secrets.as_mut() {
            secrets.items.clear();
        }

        let mut deployment = ServiceDeployment::new(config)
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        deployment.build = Some(DeploymentBuildInfo {
            docker_image_id: previous_image,
        });
        deployment.git_commit = previous_git_commit;

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

        state.slack.notify_service_removed(service_id);

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
    ) -> Result<Json<Vec<LogEntry>>, (StatusCode, String)> {
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

        let tail = query.tail.unwrap_or(DEFAULT_LOG_LIMIT).min(MAX_LOG_LIMIT);
        let origin = parse_phase(query.phase.as_deref())?;

        if let Some(log_store) = &state.log_store {
            let prefix = format!("{service_id}/{deployment_id}/");
            let entries = if let Some(before) = query.before {
                log_store
                    .read_before_by_prefix_origin(&prefix, origin, before, tail)
                    .await
            } else if let Some(after) = query.after {
                log_store
                    .read_after_by_prefix_origin(&prefix, origin, after, tail)
                    .await
            } else {
                log_store
                    .read_tail_by_prefix_origin(&prefix, origin, tail)
                    .await
            }
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
            return Ok(Json(entries));
        }

        Ok(Json(Vec::new()))
    }

    async fn get_service_logs(
        Path(service_id): Path<String>,
        Query(query): Query<LogsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<LogEntry>>, (StatusCode, String)> {
        let service_id = service_id.trim();
        crate::validation::validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        let tail = query.tail.unwrap_or(DEFAULT_LOG_LIMIT).min(MAX_LOG_LIMIT);
        let origin = parse_phase(query.phase.as_deref())?;

        if let Some(log_store) = &state.log_store {
            let prefix = format!("{service_id}/");
            let entries = if let Some(before) = query.before {
                log_store
                    .read_before_by_prefix_origin(&prefix, origin, before, tail)
                    .await
            } else if let Some(after) = query.after {
                log_store
                    .read_after_by_prefix_origin(&prefix, origin, after, tail)
                    .await
            } else {
                log_store
                    .read_tail_by_prefix_origin(&prefix, origin, tail)
                    .await
            }
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
            return Ok(Json(entries));
        }

        Ok(Json(Vec::new()))
    }

    async fn get_system_logs(
        Path(name): Path<String>,
        Query(query): Query<LogsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<LogEntry>>, (StatusCode, String)> {
        let name = name.trim();
        crate::validation::validate_service_id(name, "name")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        let tail = query.tail.unwrap_or(DEFAULT_LOG_LIMIT).min(MAX_LOG_LIMIT);

        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let entries = if name == "maestro-probe" {
            if let Some(before) = query.before {
                log_store
                    .read_before_sources(&["maestro-probe", "maestro-controller"], before, tail)
                    .await
            } else if let Some(after) = query.after {
                log_store
                    .read_after_sources(&["maestro-probe", "maestro-controller"], after, tail)
                    .await
            } else {
                log_store
                    .read_tail_sources(&["maestro-probe", "maestro-controller"], tail)
                    .await
            }
        } else {
            if let Some(before) = query.before {
                log_store.read_before_for_source(name, before, tail).await
            } else if let Some(after) = query.after {
                log_store.read_after_for_source(name, after, tail).await
            } else {
                log_store.read_tail(name, tail).await
            }
        }
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(entries))
    }

    async fn ingest_logs(
        State(state): State<AppState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<&'static str, (StatusCode, String)> {
        // New shippers include idempotency metadata; both fields are optional
        // so old controllers can continue shipping during a rolling upgrade.
        let entries: Vec<crate::logs::IngestLogEntry> = parse_json_body(&headers, body)?;
        let Some(log_store) = &state.log_store else {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "log store not configured".to_string(),
            ));
        };
        log_store
            .append_ingest(&entries)
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

    async fn list_ingress_routes(
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::deployment::types::IngressRouting>>, (StatusCode, String)> {
        let routes = state
            .store
            .list_ingress_routes()
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(routes))
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

    async fn get_service_traffic(
        Path(service_id): Path<String>,
        Query(query): Query<MetricsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::metrics::TrafficPoint>>, (StatusCode, String)> {
        let service_id = service_id.trim();
        crate::validation::validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let (from, to) = metrics_time_range(&query);
        let entries = log_store
            .read_traffic_metrics(service_id, from, to)
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
        body: Bytes,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let request: UpgradeSystemRequest = parse_json_body(&headers, body)?;
        let requested_version = request.version.trim();
        let (current_version, target_version) = validate_upgrade_version(
            MAESTRO_VERSION,
            requested_version,
        )
        .map_err(|err| match err {
            UpgradeVersionError::InvalidCurrent(source) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid running Maestro version `{MAESTRO_VERSION}`: {source}"),
            ),
            UpgradeVersionError::InvalidTarget(source) => (
                StatusCode::BAD_REQUEST,
                format!("invalid upgrade version `{requested_version}`: {source}"),
            ),
            UpgradeVersionError::NotNewer { current, target } => (
                StatusCode::CONFLICT,
                format!("upgrade version {target} must be greater than current version {current}"),
            ),
        })?;
        let system_type = state.system_type.as_deref().unwrap_or("controller");
        eprintln!(
            "upgrade request system={system_type} current_version={current_version} target_version={target_version}"
        );
        state
            .store
            .put_system_upgrade_request(system_type)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        eprintln!(
            "upgrade request accepted system={system_type} current_version={current_version} target_version={target_version}"
        );
        Ok(Json(json!({
            "accepted": true,
            "system": system_type,
            "currentVersion": current_version.to_string(),
            "targetVersion": target_version.to_string(),
        })))
    }

    async fn restart_system(
        State(state): State<AppState>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        eprintln!("restart request received");
        state
            .store
            .put_system_restart_request()
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        eprintln!("restart request accepted");
        Ok(Json(json!({ "accepted": true })))
    }

    async fn get_disks() -> Json<Vec<DiskInfo>> {
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let host_root = std::env::var("MAESTRO_HOST_ROOT")
            .ok()
            .filter(|path| !path.trim().is_empty());
        let mut candidates = Vec::new();

        if let Some(host_root) = host_root.as_deref()
            && let Some(info) = disk_info_for_path(FsPath::new(host_root), "/", &disks)
        {
            candidates.push(info);
        }

        candidates.extend(disks.iter().filter_map(|disk| {
            let mount = disk.mount_point().to_string_lossy().to_string();
            if host_root.as_deref().is_some_and(|host_root| {
                mount == host_root || mount.starts_with(&format!("{host_root}/"))
            }) {
                return None;
            }
            if !is_relevant_disk(disk) {
                return None;
            }
            Some(disk_info_from_sysinfo(disk, mount))
        }));

        candidates.sort_by(|a, b| {
            disk_mount_priority(&a.mount_point)
                .cmp(&disk_mount_priority(&b.mount_point))
                .then_with(|| a.mount_point.cmp(&b.mount_point))
        });

        let mut seen = HashSet::new();
        let items = candidates
            .into_iter()
            .filter(|disk| {
                seen.insert((
                    disk.name.clone(),
                    disk.file_system.clone(),
                    disk.total_bytes,
                    disk.available_bytes,
                ))
            })
            .collect();
        Json(items)
    }
}

fn is_relevant_disk(disk: &sysinfo::Disk) -> bool {
    let mount = disk.mount_point().to_string_lossy();
    let name = disk.name().to_string_lossy();
    name.starts_with("/dev/")
        && !mount.starts_with("/certs")
        && !mount.starts_with("/dev/")
        && !mount.starts_with("/etc/")
        && !mount.starts_with("/proc/")
        && !mount.starts_with("/run/")
        && !mount.starts_with("/sys/")
}

fn disk_info_from_sysinfo(disk: &sysinfo::Disk, mount_point: String) -> DiskInfo {
    DiskInfo {
        name: disk.name().to_string_lossy().to_string(),
        mount_point,
        total_bytes: disk.total_space(),
        available_bytes: disk.available_space(),
        file_system: String::from_utf8_lossy(disk.file_system().as_encoded_bytes()).to_string(),
    }
}

fn disk_info_for_path(
    path: &FsPath,
    display_mount_point: &str,
    disks: &sysinfo::Disks,
) -> Option<DiskInfo> {
    let (total_bytes, available_bytes) = statvfs_space(path)?;
    let disk = best_disk_for_path(path, disks);
    Some(DiskInfo {
        name: disk
            .map(|disk| disk.name().to_string_lossy().to_string())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| path.display().to_string()),
        mount_point: display_mount_point.to_string(),
        total_bytes,
        available_bytes,
        file_system: disk
            .map(|disk| String::from_utf8_lossy(disk.file_system().as_encoded_bytes()).to_string())
            .unwrap_or_default(),
    })
}

fn best_disk_for_path<'a>(path: &FsPath, disks: &'a sysinfo::Disks) -> Option<&'a sysinfo::Disk> {
    disks
        .iter()
        .filter(|disk| path.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
}

#[cfg(unix)]
fn statvfs_space(path: &FsPath) -> Option<(u64, u64)> {
    use std::{ffi::CString, mem::MaybeUninit, os::unix::ffi::OsStrExt};

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    let block_size = (stat.f_frsize as u64).max(1);
    Some((
        (stat.f_blocks as u64).saturating_mul(block_size),
        (stat.f_bavail as u64).saturating_mul(block_size),
    ))
}

#[cfg(not(unix))]
fn statvfs_space(_path: &FsPath) -> Option<(u64, u64)> {
    None
}

fn disk_mount_priority(mount: &str) -> usize {
    match mount {
        "/" => 0,
        "/data" => 1,
        _ => 10,
    }
}

#[cfg(test)]
mod disk_tests {
    use super::*;

    #[test]
    fn disk_mount_priority_puts_host_root_first() {
        assert!(disk_mount_priority("/") < disk_mount_priority("/data"));
        assert!(disk_mount_priority("/data") < disk_mount_priority("/certs"));
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
    after: Option<i64>,
    before: Option<i64>,
    phase: Option<String>,
}

#[derive(serde::Deserialize)]
struct ForceQuery {
    force: Option<bool>,
}

fn parse_phase(phase: Option<&str>) -> Result<Option<LogOrigin>, (StatusCode, String)> {
    match phase {
        None => Ok(None),
        Some("build") => Ok(Some(LogOrigin::Build)),
        Some("deploy") | Some("service") => Ok(Some(LogOrigin::Service)),
        Some("system") => Ok(Some(LogOrigin::System)),
        Some(other) => Err((
            StatusCode::BAD_REQUEST,
            format!("invalid phase '{other}', expected one of: build, deploy, system"),
        )),
    }
}

async fn require_jwt(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let Some(secret) = state.jwt_secret_key.as_ref() else {
        return Ok(next.run(request).await);
    };
    let token = request
        .headers()
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
    Ok(next.run(request).await)
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
        crate::validation::validate_service_provider_config(&build, &image, &deploy)?;
    crate::validation::validate_ingress_config(&ingress)?;

    let secrets_hash = deploy.secrets.as_ref().map(|s| s.compute_secrets_hash());
    let secrets_source = deploy.secrets.as_ref().and_then(|s| s.source.as_ref());
    let version_payload = json!({
        "id": &service_id,
        "name": &service_name,
        "build": &build,
        "image": &image,
        "deploy": {
            "flags": &deploy.flags,
            "exposePorts": &deploy.expose_ports,
            "command": &deploy.command,
            "healthcheckPath": &deploy.healthcheck_path,
            "env": &deploy.env,
            "secretsHash": &secrets_hash,
            "secretsSource": &secrets_source,
            "secretsMountPath": deploy.secrets.as_ref().map(|s| &s.mount_path),
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

fn format_build_label(build: &crate::deployment::types::ServiceBuildConfig) -> String {
    let source = build.repo.as_deref().unwrap_or("[up]");
    format!("{source}:{}", build.dockerfile)
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
            from: old.build.as_ref().map(format_build_label),
            to: new_config.build.as_ref().map(format_build_label),
        });
    }

    if old.ingress != new_config.ingress {
        changes.push(RolloutChange {
            field: "ingress".into(),
            from: old.ingress.as_ref().map(|i| i.hosts().join(",")),
            to: new_config.ingress.as_ref().map(|i| i.hosts().join(",")),
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

    for (key, new_val) in &new_config.deploy.env.items {
        if let Some(old_val) = old.deploy.env.items.get(key) {
            if old_val != new_val {
                changes.push(RolloutChange {
                    field: format!("env.{key}"),
                    from: Some(old_val.as_str().to_string()),
                    to: Some(new_val.as_str().to_string()),
                });
            }
        } else {
            changes.push(RolloutChange {
                field: format!("env.{key}"),
                from: None,
                to: Some(new_val.as_str().to_string()),
            });
        }
    }
    for key in old.deploy.env.items.keys() {
        if !new_config.deploy.env.items.contains_key(key) {
            changes.push(RolloutChange {
                field: format!("env.{key}"),
                from: Some(old.deploy.env.items[key].as_str().to_string()),
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

fn slack_webhook_view(webhook: &crate::slack::SlackWebhook) -> SlackWebhookView {
    SlackWebhookView {
        id: webhook.id.clone(),
        name: webhook.name.clone(),
        url: mask_webhook_url(webhook.url.as_str()),
        categories: webhook.categories.clone(),
        enabled: webhook.enabled,
    }
}

fn mask_webhook_url(url: &str) -> String {
    if url.is_empty() {
        return String::new();
    }
    if let Some(idx) = url.rfind('/') {
        let (head, tail) = url.split_at(idx + 1);
        let visible_tail: String = tail.chars().take(4).collect();
        return format!("{head}{visible_tail}…");
    }
    "***".to_string()
}

fn validate_webhook_url(url: &str) -> Result<(), (StatusCode, String)> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "url cannot be empty".to_string()));
    }
    if !trimmed.starts_with("https://") {
        return Err((
            StatusCode::BAD_REQUEST,
            "webhook url must use https".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/server/mod.rs"]
mod tests;
