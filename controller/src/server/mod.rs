use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, patch, post},
};
use etcd_client::{
    Client as EtcdClient, Compare, CompareOp, GetOptions, SortOrder, SortTarget, Txn, TxnOp,
};
use nanoid::nanoid;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::service::{
    ActiveDeployment, DeploymentStatus, SERVICES_ROOT, ServiceBuildConfig, ServiceConfig,
    ServiceDeployConfig, ServiceDeployment, ServiceInfo, service_active_deployment_key,
    service_deployment_history_key, service_info_key,
};
use crate::supervisor;

const SERVICE_HISTORY_NEXT_INDEX_SUFFIX: &str = "/deployments/history-next-index";
const SERVICES_PREFIX: &str = "/maetro/services/";
const LIST_DEPLOYMENTS_LIMIT: i64 = 25;
const MAX_TXN_RETRIES: usize = 16;
const URL_SAFE_ALPHABET: [char; 62] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B',
    'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
    'V', 'W', 'X', 'Y', 'Z',
];

#[derive(Clone)]
struct AppState {
    etcd: EtcdV3HttpClient,
}

#[derive(Clone)]
struct EtcdV3HttpClient {
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceResponse {
    queued: bool,
    deployment_id: Option<String>,
    deployment_index: Option<usize>,
    service_id: String,
    status: Option<DeploymentStatus>,
    version: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ServiceListItem {
    #[serde(flatten)]
    service: ServiceConfig,
    status: Option<DeploymentStatus>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceRequest {
    id: String,
    name: String,
    #[serde(default)]
    build: Option<ServiceBuildConfig>,
    #[serde(default)]
    image: Option<String>,
    deploy: ServiceDeployConfig,
}

#[derive(Debug, Clone)]
struct CounterSnapshot {
    next_index: u64,
    mod_revision: Option<u64>,
}

#[derive(Debug, Clone)]
struct InfoSnapshot {
    info: ServiceInfo,
    mod_revision: u64,
}

#[derive(Debug, Clone)]
struct DeploymentSnapshot {
    key: String,
    mod_revision: u64,
    deployment: ServiceDeployment,
}

enum PatchServiceOutcome {
    Queued {
        deployment_index: usize,
        deployment: ServiceDeployment,
    },
    Unchanged {
        service_id: String,
        version: String,
    },
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelDeploymentResponse {
    canceled: bool,
    service_id: String,
    deployment_id: String,
    status: DeploymentStatus,
}

enum CancelDeploymentOutcome {
    Canceled(ServiceDeployment),
    NotCancelable(ServiceDeployment),
    NotFound,
}

pub async fn run_server(bind_addr: &str, etcd_endpoint: &str) -> crate::error::Result<()> {
    let state = AppState {
        etcd: EtcdV3HttpClient::new(etcd_endpoint).await?,
    };

    let app = Router::new()
        .route("/_healthy", get(healthy))
        .route("/api/services", get(list_services))
        .route("/api/services/patch", patch(patch_service))
        .route(
            "/api/services/{serviceId}/deployments",
            get(list_deployments),
        )
        .route(
            "/api/services/{serviceId}/deployments/{deploymentId}/cancel",
            patch(cancel_deployment),
        )
        .route("/api/services/{serviceId}/redeploy", post(redeploy_service))
        .with_state(state)
        .layer(middleware::from_fn(log_http));

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|err| format!("failed to bind {bind_addr}: {err}"))?;
    println!(
        "[maestro]: server listening on http://{bind_addr} [pid={}]",
        std::process::id()
    );
    println!("[maestro]: etcd endpoint {etcd_endpoint}");

    let server_future = axum::serve(listener, app);
    let supervisor_future = supervisor::run(etcd_endpoint);

    tokio::select! {
        result = server_future => result
            .map_err(|err| format!("server error: {err}"))
            .map_err(Into::into),
        result = supervisor_future => result,
    }
}

async fn healthy() -> &'static str {
    "ok"
}

async fn patch_service(
    State(state): State<AppState>,
    Json(request): Json<PatchServiceRequest>,
) -> Result<Json<PatchServiceResponse>, (StatusCode, String)> {
    let service_config = build_hashed_service_config(request).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid patch request payload: {err}"),
        )
    })?;

    let outcome = upsert_config_and_maybe_queue(&state.etcd, service_config)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;

    let response = match outcome {
        PatchServiceOutcome::Queued {
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
        PatchServiceOutcome::Unchanged {
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
        .etcd
        .list_service_infos()
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;

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
    validate_service_id(service_id, "serviceId").map_err(|err| (StatusCode::BAD_REQUEST, err))?;

    let deployments = state
        .etcd
        .list_service_deployments(service_id, LIST_DEPLOYMENTS_LIMIT)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;

    Ok(Json(deployments))
}

async fn cancel_deployment(
    Path((service_id, deployment_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<CancelDeploymentResponse>, (StatusCode, String)> {
    let service_id = service_id.trim().to_string();
    let deployment_id = deployment_id.trim().to_string();
    validate_service_id(&service_id, "serviceId").map_err(|err| (StatusCode::BAD_REQUEST, err))?;
    if deployment_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "deploymentId cannot be empty".to_string(),
        ));
    }

    let outcome = cancel_service_deployment(&state.etcd, &service_id, &deployment_id)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;

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
    validate_service_id(&service_id, "serviceId").map_err(|err| (StatusCode::BAD_REQUEST, err))?;

    let info_key = service_info_key(&service_id);
    let info_snapshot = state
        .etcd
        .read_service_info(&info_key)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;

    let Some(snapshot) = info_snapshot else {
        return Err((
            StatusCode::NOT_FOUND,
            format!("service `{service_id}` not found"),
        ));
    };

    let outcome = force_queue_deployment(&state.etcd, snapshot.info.config)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;

    Ok(Json(PatchServiceResponse {
        queued: true,
        deployment_id: Some(outcome.deployment.id.clone()),
        deployment_index: Some(outcome.deployment_index),
        service_id: outcome.deployment.config.id.clone(),
        status: Some(outcome.deployment.status),
        version: outcome.deployment.config.version.clone(),
    }))
}

struct ForceQueueOutcome {
    deployment_index: usize,
    deployment: ServiceDeployment,
}

async fn force_queue_deployment(
    etcd: &EtcdV3HttpClient,
    service_config: ServiceConfig,
) -> Result<ForceQueueOutcome, String> {
    let service_id = &service_config.id;
    let service_counter_key = service_history_next_index_key(service_id);
    let info_key = service_info_key(service_id);

    for _attempt in 0..MAX_TXN_RETRIES {
        let service_counter = etcd.read_counter(&service_counter_key).await?;
        let existing_info = etcd.read_service_info(&info_key).await?;

        let deployment_index_u64 = service_counter.next_index;
        let deployment_index = usize::try_from(deployment_index_u64)
            .map_err(|_| "deployment index overflowed usize".to_string())?;

        let deployment = ServiceDeployment {
            id: generate_deployment_id(),
            created_at: current_time_millis()?,
            deployed_at: None,
            status: DeploymentStatus::Queued,
            config: service_config.clone(),
            git_commit: None,
            build: None,
        };

        let deployment_json = serde_json::to_string(&deployment)
            .map_err(|err| format!("failed to serialize deployment: {err}"))?;

        let info = ServiceInfo {
            config: service_config.clone(),
            status: Some(DeploymentStatus::Queued),
        };
        let info_json = serde_json::to_string(&info)
            .map_err(|err| format!("failed to serialize service info: {err}"))?;

        let service_history_key = service_deployment_history_key(service_id, deployment_index);

        let compare = vec![
            compare_counter(&service_counter_key, &service_counter),
            compare_mod_revision_or_absent(
                &info_key,
                existing_info.as_ref().map(|s| s.mod_revision),
            ),
        ];
        let success = vec![
            request_put(
                &service_counter_key,
                &(deployment_index_u64 + 1).to_string(),
            ),
            request_put(&service_history_key, &deployment_json),
            request_put(&info_key, &info_json),
        ];

        let committed = etcd.txn(compare, success).await?;
        if committed {
            return Ok(ForceQueueOutcome {
                deployment_index,
                deployment,
            });
        }
    }

    Err("failed to queue deployment due to concurrent updates; retry".to_string())
}

async fn upsert_config_and_maybe_queue(
    etcd: &EtcdV3HttpClient,
    service_config: ServiceConfig,
) -> Result<PatchServiceOutcome, String> {
    let service_id = service_config.id.trim().to_string();
    validate_service_id(&service_id, "service id")?;

    let service_version = service_config.version.trim().to_string();
    if service_version.is_empty() {
        return Err("service version cannot be empty".to_string());
    }

    let info_key = service_info_key(&service_id);
    let service_counter_key = service_history_next_index_key(&service_id);

    for _attempt in 0..MAX_TXN_RETRIES {
        let existing_info = etcd.read_service_info(&info_key).await?;
        if let Some(snapshot) = existing_info.as_ref()
            && snapshot.info.config.version == service_version
            && etcd
                .is_service_active_with_version(&service_id, &service_version)
                .await?
        {
            return Ok(PatchServiceOutcome::Unchanged {
                service_id: service_id.clone(),
                version: service_version.clone(),
            });
        }

        let service_counter = etcd.read_counter(&service_counter_key).await?;

        let deployment_index_u64 = service_counter.next_index;
        let deployment_index = usize::try_from(deployment_index_u64)
            .map_err(|_| "deployment index overflowed usize".to_string())?;

        let deployment = ServiceDeployment {
            id: generate_deployment_id(),
            created_at: current_time_millis()?,
            deployed_at: None,
            status: DeploymentStatus::Queued,
            config: service_config.clone(),
            git_commit: None,
            build: None,
        };

        let deployment_json = serde_json::to_string(&deployment)
            .map_err(|err| format!("failed to serialize deployment: {err}"))?;

        let info = ServiceInfo {
            config: service_config.clone(),
            status: Some(DeploymentStatus::Queued),
        };
        let info_json = serde_json::to_string(&info)
            .map_err(|err| format!("failed to serialize service info: {err}"))?;

        let service_history_key = service_deployment_history_key(&service_id, deployment_index);

        let compare = vec![
            compare_mod_revision_or_absent(
                &info_key,
                existing_info.as_ref().map(|snapshot| snapshot.mod_revision),
            ),
            compare_counter(&service_counter_key, &service_counter),
        ];

        let success = vec![
            request_put(&info_key, &info_json),
            request_put(
                &service_counter_key,
                &(deployment_index_u64 + 1).to_string(),
            ),
            request_put(&service_history_key, &deployment_json),
        ];

        let committed = etcd.txn(compare, success).await?;
        if committed {
            return Ok(PatchServiceOutcome::Queued {
                deployment_index,
                deployment,
            });
        }
    }

    Err("failed to apply patch due to concurrent updates; retry".to_string())
}

async fn cancel_service_deployment(
    etcd: &EtcdV3HttpClient,
    service_id: &str,
    deployment_id: &str,
) -> Result<CancelDeploymentOutcome, String> {
    for _attempt in 0..MAX_TXN_RETRIES {
        let Some(snapshot) = etcd
            .find_service_deployment_by_id(service_id, deployment_id)
            .await?
        else {
            return Ok(CancelDeploymentOutcome::NotFound);
        };

        match snapshot.deployment.status {
            DeploymentStatus::Queued | DeploymentStatus::Building => {}
            DeploymentStatus::Canceled => {
                return Ok(CancelDeploymentOutcome::Canceled(snapshot.deployment));
            }
            _ => {
                return Ok(CancelDeploymentOutcome::NotCancelable(snapshot.deployment));
            }
        }

        let mut updated = snapshot.deployment.clone();
        updated.status = DeploymentStatus::Canceled;
        let updated_json = serde_json::to_string(&updated)
            .map_err(|err| format!("failed to serialize canceled deployment: {err}"))?;

        let compare = vec![compare_mod_revision_or_absent(
            &snapshot.key,
            Some(snapshot.mod_revision),
        )];
        let success = vec![request_put(&snapshot.key, &updated_json)];

        let committed = etcd.txn(compare, success).await?;
        if committed {
            return Ok(CancelDeploymentOutcome::Canceled(updated));
        }
    }

    Err("failed to cancel deployment due to concurrent updates; retry".to_string())
}

impl EtcdV3HttpClient {
    async fn new(endpoint: &str) -> Result<Self, String> {
        let endpoint = normalize_base_url(endpoint)?;
        let client = EtcdClient::connect([endpoint.as_str()], None)
            .await
            .map_err(|_| "failed to connect etcd client".to_string())?;
        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
        })
    }

    async fn get(
        &self,
        key: Vec<u8>,
        options: Option<GetOptions>,
    ) -> Result<etcd_client::GetResponse, String> {
        let mut client = self.client.lock().await;
        client
            .get(key, options)
            .await
            .map_err(|_| "failed etcd get request".to_string())
    }

    async fn txn(&self, compare: Vec<Compare>, success: Vec<TxnOp>) -> Result<bool, String> {
        let txn = Txn::new().when(compare).and_then(success);
        let mut client = self.client.lock().await;
        let response = client
            .txn(txn)
            .await
            .map_err(|_| "failed etcd txn request".to_string())?;
        Ok(response.succeeded())
    }

    async fn read_counter(&self, key: &str) -> Result<CounterSnapshot, String> {
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(CounterSnapshot {
                next_index: 0,
                mod_revision: None,
            });
        };

        let text = String::from_utf8(kv.value().to_vec())
            .map_err(|err| format!("counter value for key `{key}` is not utf8: {err}"))?;
        let next_index = text
            .trim()
            .parse::<u64>()
            .map_err(|err| format!("counter value for key `{key}` is not u64: {err}"))?;
        let mod_revision = decode_mod_revision(kv.mod_revision(), key)?;

        Ok(CounterSnapshot {
            next_index,
            mod_revision: Some(mod_revision),
        })
    }

    async fn read_service_info(&self, key: &str) -> Result<Option<InfoSnapshot>, String> {
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let info = serde_json::from_slice::<ServiceInfo>(kv.value())
            .map_err(|err| format!("invalid service info JSON at key `{key}`: {err}"))?;
        let mod_revision = decode_mod_revision(kv.mod_revision(), key)?;

        Ok(Some(InfoSnapshot { info, mod_revision }))
    }

    async fn read_active_deployment(
        &self,
        service_id: &str,
    ) -> Result<Option<ActiveDeployment>, String> {
        let key = service_active_deployment_key(service_id);
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let active = serde_json::from_slice::<ActiveDeployment>(kv.value())
            .map_err(|err| format!("invalid active deployment JSON at key `{key}`: {err}"))?;
        Ok(Some(active))
    }

    async fn is_service_active_with_version(
        &self,
        service_id: &str,
        service_version: &str,
    ) -> Result<bool, String> {
        let Some(active) = self.read_active_deployment(service_id).await? else {
            return Ok(false);
        };

        if let Some(active_version) = active.version.as_deref()
            && active_version != service_version
        {
            return Ok(false);
        }

        let Some(snapshot) = self
            .find_service_deployment_by_id(service_id, &active.deployment_id)
            .await?
        else {
            return Ok(false);
        };

        if snapshot.deployment.config.version != service_version {
            return Ok(false);
        }

        Ok(is_active_deployment_status(&snapshot.deployment.status))
    }

    async fn find_service_deployment_by_id(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<Option<DeploymentSnapshot>, String> {
        let prefix_key = format!("{SERVICES_ROOT}/{service_id}/deployments/history/");
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| "failed to compute range end for deployment lookup".to_string())?;
        let options = GetOptions::new().with_range(range_end);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        for kv in response.kvs() {
            let deployment = serde_json::from_slice::<ServiceDeployment>(kv.value())
                .map_err(|err| format!("invalid deployment JSON under `{prefix_key}`: {err}"))?;
            if deployment.id != deployment_id {
                continue;
            }

            let key = String::from_utf8(kv.key().to_vec())
                .map_err(|err| format!("deployment key for `{prefix_key}` is not utf8: {err}"))?;
            let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;

            return Ok(Some(DeploymentSnapshot {
                key,
                mod_revision,
                deployment,
            }));
        }

        Ok(None)
    }

    async fn list_service_deployments(
        &self,
        service_id: &str,
        limit: i64,
    ) -> Result<Vec<ServiceDeployment>, String> {
        let prefix_key = format!("/maetro/services/{service_id}/deployments/history/");
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| "failed to compute range end for deployments prefix".to_string())?;
        let options = GetOptions::new()
            .with_range(range_end)
            .with_limit(limit)
            .with_sort(SortTarget::Key, SortOrder::Descend);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut deployments = Vec::with_capacity(response.kvs().len());
        for kv in response.kvs() {
            let deployment =
                serde_json::from_slice::<ServiceDeployment>(kv.value()).map_err(|err| {
                    format!(
                        "failed to parse deployment JSON from etcd key prefix `{}`: {err}",
                        prefix_key
                    )
                })?;
            deployments.push(deployment);
        }

        Ok(deployments)
    }

    async fn list_service_infos(&self) -> Result<Vec<ServiceInfo>, String> {
        let prefix = SERVICES_PREFIX.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| "failed to compute range end for services prefix".to_string())?;
        let options = GetOptions::new()
            .with_range(range_end)
            .with_sort(SortTarget::Key, SortOrder::Ascend);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut infos = Vec::new();
        for kv in response.kvs() {
            let key = String::from_utf8(kv.key().to_vec()).map_err(|err| {
                format!("service key is not utf8 for prefix `{SERVICES_PREFIX}`: {err}")
            })?;
            if !key.ends_with("/info") {
                continue;
            }

            let info = serde_json::from_slice::<ServiceInfo>(kv.value())
                .map_err(|err| format!("failed to parse service info at key `{key}`: {err}"))?;
            infos.push(info);
        }
        infos.sort_by(|a, b| a.config.id.cmp(&b.config.id));
        Ok(infos)
    }
}

fn normalize_base_url(host: &str) -> Result<String, String> {
    let host = host.trim();
    if host.is_empty() {
        return Err("host cannot be empty".to_string());
    }

    let base = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    };

    Ok(base.trim_end_matches('/').to_string())
}

fn compare_counter(key: &str, snapshot: &CounterSnapshot) -> Compare {
    compare_mod_revision_or_absent(key, snapshot.mod_revision)
}

fn compare_mod_revision_or_absent(key: &str, mod_revision: Option<u64>) -> Compare {
    match mod_revision {
        Some(rev) => Compare::mod_revision(
            key.as_bytes().to_vec(),
            CompareOp::Equal,
            i64::try_from(rev).expect("mod_revision from etcd must fit i64"),
        ),
        None => Compare::version(key.as_bytes().to_vec(), CompareOp::Equal, 0),
    }
}

fn request_put(key: &str, value: &str) -> TxnOp {
    TxnOp::put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
}

fn decode_mod_revision(mod_revision: i64, key: &str) -> Result<u64, String> {
    u64::try_from(mod_revision)
        .map_err(|err| format!("invalid mod_revision `{mod_revision}` for key `{key}`: {err}"))
}

fn build_hashed_service_config(request: PatchServiceRequest) -> Result<ServiceConfig, String> {
    let PatchServiceRequest {
        id,
        name,
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
    let (build, image) = normalize_build_or_image(build, image)?;

    let version_payload = json!({
        "id": &service_id,
        "name": &service_name,
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
        build,
        image,
        deploy,
    })
}

fn normalize_build_or_image(
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
    if !is_url_safe_service_id(service_id) {
        return Err(format!(
            "{field_name} must be URL-safe and contain only letters, numbers, '-' or '_'"
        ));
    }
    Ok(())
}

fn is_url_safe_service_id(service_id: &str) -> bool {
    service_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

fn is_active_deployment_status(status: &DeploymentStatus) -> bool {
    matches!(
        status,
        DeploymentStatus::Queued | DeploymentStatus::Building | DeploymentStatus::Ready
    )
}

fn service_history_next_index_key(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}{SERVICE_HISTORY_NEXT_INDEX_SUFFIX}",)
}

fn generate_deployment_id() -> String {
    nanoid!(24, &URL_SAFE_ALPHABET)
}

fn current_time_millis() -> Result<u64, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system clock is before unix epoch: {err}"))?;
    u64::try_from(now.as_millis())
        .map_err(|_| "system time milliseconds overflowed u64".to_string())
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] < 0xff {
            end[idx] += 1;
            end.truncate(idx + 1);
            return Some(end);
        }
    }
    None
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
