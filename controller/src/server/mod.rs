use std::convert::TryFrom;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, patch},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::service::{
    DeploymentStatus, SERVICES_ROOT, ServiceBuildConfig, ServiceConfig, ServiceDeployConfig,
    ServiceDeployment, service_config_key, service_deployment_history_key,
};

const GLOBAL_DEPLOYMENTS_PREFIX: &str = "/maetro/deployments/history/";
const GLOBAL_DEPLOYMENTS_NEXT_INDEX_KEY: &str = "/maetro/deployments/meta/next-index";
const SERVICE_HISTORY_NEXT_INDEX_SUFFIX: &str = "/deployments/history-next-index";
const SERVICES_PREFIX: &str = "/maetro/services/";
const LIST_DEPLOYMENTS_LIMIT: i64 = 25;
const MAX_TXN_RETRIES: usize = 16;

#[derive(Clone)]
struct AppState {
    etcd: EtcdV3HttpClient,
}

#[derive(Clone)]
struct EtcdV3HttpClient {
    client: reqwest::Client,
    base_url: String,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceRequest {
    id: String,
    name: String,
    build: ServiceBuildConfig,
    deploy: ServiceDeployConfig,
}

#[derive(Debug, Deserialize)]
struct RangeResponse {
    #[serde(default)]
    kvs: Vec<RangeKv>,
}

#[derive(Debug, Deserialize)]
struct RangeKv {
    key: String,
    value: String,
    mod_revision: String,
}

#[derive(Debug, Deserialize)]
struct TxnResponse {
    succeeded: bool,
}

#[derive(Debug, Clone)]
struct CounterSnapshot {
    next_index: u64,
    mod_revision: Option<u64>,
}

#[derive(Debug, Clone)]
struct ConfigSnapshot {
    config: ServiceConfig,
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

pub async fn run_server(bind_addr: &str, etcd_endpoint: &str) -> Result<(), String> {
    let state = AppState {
        etcd: EtcdV3HttpClient::new(etcd_endpoint)?,
    };

    let app = Router::new()
        .route("/_healthy", get(healthy))
        .route("/api/services", get(list_services))
        .route("/api/services/patch", patch(patch_service))
        .route(
            "/api/service/{serviceId}/deployments",
            get(list_deployments),
        )
        .route(
            "/api/service/{serviceId}/deployments/{deploymentId}/cancel",
            patch(cancel_deployment),
        )
        .with_state(state)
        .layer(middleware::from_fn(log_http));

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|err| format!("failed to bind {bind_addr}: {err}"))?;
    println!("[maestro]: server listening on http://{bind_addr}");
    println!("[maestro]: etcd endpoint {etcd_endpoint}");

    axum::serve(listener, app)
        .await
        .map_err(|err| format!("server error: {err}"))
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
) -> Result<Json<Vec<ServiceConfig>>, (StatusCode, String)> {
    let services = state
        .etcd
        .list_services()
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;

    Ok(Json(services))
}

async fn list_deployments(
    Path(service_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<Vec<ServiceDeployment>>, (StatusCode, String)> {
    let service_id = service_id.trim();
    if service_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "serviceId cannot be empty".to_string(),
        ));
    }

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
    if service_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "serviceId cannot be empty".to_string(),
        ));
    }
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

async fn upsert_config_and_maybe_queue(
    etcd: &EtcdV3HttpClient,
    service_config: ServiceConfig,
) -> Result<PatchServiceOutcome, String> {
    let service_id = service_config.id.trim().to_string();
    if service_id.is_empty() {
        return Err("service id cannot be empty".to_string());
    }

    let service_version = service_config.version.trim().to_string();
    if service_version.is_empty() {
        return Err("service version cannot be empty".to_string());
    }

    let config_key = service_config_key(&service_id);
    let service_counter_key = service_history_next_index_key(&service_id);
    let service_config_json = serde_json::to_string(&service_config)
        .map_err(|err| format!("failed to serialize service config: {err}"))?;

    for _attempt in 0..MAX_TXN_RETRIES {
        let existing_config = etcd.read_service_config(&config_key).await?;
        if let Some(snapshot) = existing_config.as_ref()
            && snapshot.config.version == service_version
        {
            return Ok(PatchServiceOutcome::Unchanged {
                service_id: service_id.clone(),
                version: service_version.clone(),
            });
        }

        let global_counter = etcd.read_counter(GLOBAL_DEPLOYMENTS_NEXT_INDEX_KEY).await?;
        let service_counter = etcd.read_counter(&service_counter_key).await?;

        let global_index = global_counter.next_index;
        let deployment_index_u64 = service_counter.next_index;
        let deployment_index = usize::try_from(deployment_index_u64)
            .map_err(|_| "deployment index overflowed usize".to_string())?;

        let deployment = ServiceDeployment {
            id: generate_deployment_id(&service_id, global_index),
            status: DeploymentStatus::Queued,
            config: service_config.clone(),
            git_commit: None,
            build: None,
        };

        let deployment_json = serde_json::to_string(&deployment)
            .map_err(|err| format!("failed to serialize deployment: {err}"))?;

        let service_history_key = service_deployment_history_key(&service_id, deployment_index);
        let global_history_key = global_deployment_history_key(global_index);

        let compare = vec![
            compare_mod_revision_or_absent(
                &config_key,
                existing_config
                    .as_ref()
                    .map(|snapshot| snapshot.mod_revision),
            ),
            compare_counter(GLOBAL_DEPLOYMENTS_NEXT_INDEX_KEY, &global_counter),
            compare_counter(&service_counter_key, &service_counter),
        ];

        let success = vec![
            request_put(&config_key, &service_config_json),
            request_put(
                GLOBAL_DEPLOYMENTS_NEXT_INDEX_KEY,
                &(global_index + 1).to_string(),
            ),
            request_put(
                &service_counter_key,
                &(deployment_index_u64 + 1).to_string(),
            ),
            request_put(&service_history_key, &deployment_json),
            request_put(&global_history_key, &deployment_json),
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

        let mut compare = vec![compare_mod_revision_or_absent(
            &snapshot.key,
            Some(snapshot.mod_revision),
        )];
        let mut success = vec![request_put(&snapshot.key, &updated_json)];

        let global_index = parse_global_index_from_deployment_id(service_id, deployment_id)
            .ok_or_else(|| {
                format!(
                    "deployment id `{deployment_id}` has invalid format for service `{service_id}`"
                )
            })?;
        let global_key = global_deployment_history_key(global_index);
        let global_snapshot = etcd.read_deployment_by_key(&global_key).await?;
        compare.push(compare_mod_revision_or_absent(
            &global_key,
            global_snapshot.as_ref().map(|s| s.mod_revision),
        ));
        success.push(request_put(&global_key, &updated_json));

        let committed = etcd.txn(compare, success).await?;
        if committed {
            return Ok(CancelDeploymentOutcome::Canceled(updated));
        }
    }

    Err("failed to cancel deployment due to concurrent updates; retry".to_string())
}

impl EtcdV3HttpClient {
    fn new(endpoint: &str) -> Result<Self, String> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: normalize_base_url(endpoint)?,
        })
    }

    async fn read_counter(&self, key: &str) -> Result<CounterSnapshot, String> {
        let response = self.range(json!({ "key": encode(key.as_bytes()) })).await?;

        let Some(kv) = response.kvs.first() else {
            return Ok(CounterSnapshot {
                next_index: 0,
                mod_revision: None,
            });
        };

        let decoded = decode_to_bytes(&kv.value, key)?;
        let text = String::from_utf8(decoded)
            .map_err(|err| format!("counter value for key `{key}` is not utf8: {err}"))?;
        let next_index = text
            .trim()
            .parse::<u64>()
            .map_err(|err| format!("counter value for key `{key}` is not u64: {err}"))?;
        let mod_revision = kv
            .mod_revision
            .parse::<u64>()
            .map_err(|err| format!("invalid mod_revision for key `{key}`: {err}"))?;

        Ok(CounterSnapshot {
            next_index,
            mod_revision: Some(mod_revision),
        })
    }

    async fn read_service_config(&self, key: &str) -> Result<Option<ConfigSnapshot>, String> {
        let response = self.range(json!({ "key": encode(key.as_bytes()) })).await?;
        let Some(kv) = response.kvs.first() else {
            return Ok(None);
        };

        let bytes = decode_to_bytes(&kv.value, key)?;
        let config = serde_json::from_slice::<ServiceConfig>(&bytes)
            .map_err(|err| format!("invalid service config JSON at key `{key}`: {err}"))?;
        let mod_revision = kv
            .mod_revision
            .parse::<u64>()
            .map_err(|err| format!("invalid mod_revision for key `{key}`: {err}"))?;

        Ok(Some(ConfigSnapshot {
            config,
            mod_revision,
        }))
    }

    async fn read_deployment_by_key(
        &self,
        key: &str,
    ) -> Result<Option<DeploymentSnapshot>, String> {
        let response = self.range(json!({ "key": encode(key.as_bytes()) })).await?;
        let Some(kv) = response.kvs.first() else {
            return Ok(None);
        };

        let bytes = decode_to_bytes(&kv.value, key)?;
        let deployment = serde_json::from_slice::<ServiceDeployment>(&bytes)
            .map_err(|err| format!("invalid deployment JSON at key `{key}`: {err}"))?;
        let mod_revision = kv
            .mod_revision
            .parse::<u64>()
            .map_err(|err| format!("invalid mod_revision for key `{key}`: {err}"))?;

        Ok(Some(DeploymentSnapshot {
            key: key.to_string(),
            mod_revision,
            deployment,
        }))
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

        let response = self
            .range(json!({
                "key": encode(prefix),
                "range_end": encode(&range_end),
            }))
            .await?;

        for kv in response.kvs {
            let value_bytes = decode_to_bytes(&kv.value, &prefix_key)?;
            let deployment = serde_json::from_slice::<ServiceDeployment>(&value_bytes)
                .map_err(|err| format!("invalid deployment JSON under `{prefix_key}`: {err}"))?;
            if deployment.id != deployment_id {
                continue;
            }

            let key_bytes = decode_to_bytes(&kv.key, &prefix_key)?;
            let key = String::from_utf8(key_bytes)
                .map_err(|err| format!("deployment key for `{prefix_key}` is not utf8: {err}"))?;
            let mod_revision = kv
                .mod_revision
                .parse::<u64>()
                .map_err(|err| format!("invalid mod_revision for deployment key `{key}`: {err}"))?;

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

        let response = self
            .range(json!({
                "key": encode(prefix),
                "range_end": encode(&range_end),
                "limit": limit,
                "sort_order": "DESCEND",
                "sort_target": "KEY"
            }))
            .await?;

        let mut deployments = Vec::with_capacity(response.kvs.len());
        for kv in response.kvs {
            let bytes = decode_to_bytes(&kv.value, &prefix_key)?;
            let deployment =
                serde_json::from_slice::<ServiceDeployment>(&bytes).map_err(|err| {
                    format!(
                        "failed to parse deployment JSON from etcd key prefix `{}`: {err}",
                        prefix_key
                    )
                })?;
            deployments.push(deployment);
        }

        Ok(deployments)
    }

    async fn list_services(&self) -> Result<Vec<ServiceConfig>, String> {
        let prefix = SERVICES_PREFIX.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| "failed to compute range end for services prefix".to_string())?;

        let response = self
            .range(json!({
                "key": encode(prefix),
                "range_end": encode(&range_end),
                "sort_order": "ASCEND",
                "sort_target": "KEY"
            }))
            .await?;

        extract_service_configs(response.kvs)
    }

    async fn txn(&self, compare: Vec<Value>, success: Vec<Value>) -> Result<bool, String> {
        let body = json!({
            "compare": compare,
            "success": success,
            "failure": [],
        });

        let response = self
            .client
            .post(format!("{}/v3/kv/txn", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|err| format!("failed etcd txn request: {err}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let raw = response.text().await.unwrap_or_default();
            return Err(format!("etcd txn failed with status {status}: {raw}"));
        }

        let parsed = response
            .json::<TxnResponse>()
            .await
            .map_err(|err| format!("failed to decode etcd txn response: {err}"))?;
        Ok(parsed.succeeded)
    }

    async fn range(&self, body: Value) -> Result<RangeResponse, String> {
        let response = self
            .client
            .post(format!("{}/v3/kv/range", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|err| format!("failed etcd range request: {err}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let raw = response.text().await.unwrap_or_default();
            return Err(format!("etcd range failed with status {status}: {raw}"));
        }

        response
            .json::<RangeResponse>()
            .await
            .map_err(|err| format!("failed to decode etcd range response: {err}"))
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

fn compare_counter(key: &str, snapshot: &CounterSnapshot) -> Value {
    compare_mod_revision_or_absent(key, snapshot.mod_revision)
}

fn compare_mod_revision_or_absent(key: &str, mod_revision: Option<u64>) -> Value {
    match mod_revision {
        Some(rev) => json!({
            "key": encode(key.as_bytes()),
            "target": "MOD",
            "mod_revision": rev.to_string(),
        }),
        None => json!({
            "key": encode(key.as_bytes()),
            "target": "VERSION",
            "version": "0",
        }),
    }
}

fn request_put(key: &str, value: &str) -> Value {
    json!({
        "request_put": {
            "key": encode(key.as_bytes()),
            "value": encode(value.as_bytes()),
        }
    })
}

fn extract_service_configs(kvs: Vec<RangeKv>) -> Result<Vec<ServiceConfig>, String> {
    let mut services = Vec::new();

    for kv in kvs {
        let key_bytes = decode_to_bytes(&kv.key, SERVICES_PREFIX)?;
        let key = String::from_utf8(key_bytes).map_err(|err| {
            format!("service key is not utf8 for prefix `{SERVICES_PREFIX}`: {err}")
        })?;
        if !key.ends_with("/config") {
            continue;
        }

        let value_bytes = decode_to_bytes(&kv.value, &key)?;
        let config = serde_json::from_slice::<ServiceConfig>(&value_bytes)
            .map_err(|err| format!("failed to parse service config at key `{key}`: {err}"))?;
        services.push(config);
    }

    services.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(services)
}

fn build_hashed_service_config(request: PatchServiceRequest) -> Result<ServiceConfig, String> {
    let PatchServiceRequest {
        id,
        name,
        build,
        deploy,
    } = request;

    let service_id = id.trim().to_string();
    if service_id.is_empty() {
        return Err("id cannot be empty".to_string());
    }

    let service_name = name.trim().to_string();
    if service_name.is_empty() {
        return Err("name cannot be empty".to_string());
    }

    let version_payload = json!({
        "id": &service_id,
        "name": &service_name,
        "build": &build,
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
        deploy,
    })
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from_digit((byte >> 4) as u32, 16).expect("hex nibble"));
        output.push(char::from_digit((byte & 0x0f) as u32, 16).expect("hex nibble"));
    }
    output
}

fn global_deployment_history_key(index: u64) -> String {
    format!("{GLOBAL_DEPLOYMENTS_PREFIX}{index:020}")
}

fn service_history_next_index_key(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}{SERVICE_HISTORY_NEXT_INDEX_SUFFIX}",)
}

fn generate_deployment_id(service_id: &str, global_index: u64) -> String {
    format!("deployment-{service_id}-{global_index:020}")
}

fn parse_global_index_from_deployment_id(service_id: &str, deployment_id: &str) -> Option<u64> {
    let prefix = format!("deployment-{service_id}-");
    let suffix = deployment_id.strip_prefix(&prefix)?;
    suffix.parse::<u64>().ok()
}

fn encode(input: &[u8]) -> String {
    STANDARD.encode(input)
}

fn decode_to_bytes(encoded: &str, key_hint: &str) -> Result<Vec<u8>, String> {
    STANDARD
        .decode(encoded.as_bytes())
        .map_err(|err| format!("invalid base64 value in etcd for `{key_hint}`: {err}"))
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
mod tests {
    use super::*;
    use crate::service::{ArcCommand, ServiceBuildConfig, ServiceDeployConfig};

    fn sample_patch_request(id: &str, name: &str) -> PatchServiceRequest {
        PatchServiceRequest {
            id: id.to_string(),
            name: name.to_string(),
            build: ServiceBuildConfig {
                git_repo: "https://example.com/repo.git".to_string(),
                dockerfile_path: "./Dockerfile".to_string(),
                command: ArcCommand {
                    command: "arc-build".to_string(),
                    args: vec!["--release".to_string()],
                },
            },
            deploy: ServiceDeployConfig {
                command: ArcCommand {
                    command: "arc-deploy".to_string(),
                    args: vec!["--prod".to_string()],
                },
                healthcheck_path: "/_healthy".to_string(),
            },
        }
    }

    fn sample_service_config(id: &str) -> ServiceConfig {
        ServiceConfig {
            id: id.to_string(),
            name: format!("{id}-name"),
            version: "v1".to_string(),
            build: ServiceBuildConfig {
                git_repo: "https://example.com/repo.git".to_string(),
                dockerfile_path: "./Dockerfile".to_string(),
                command: ArcCommand {
                    command: "arc-build".to_string(),
                    args: vec!["--release".to_string()],
                },
            },
            deploy: ServiceDeployConfig {
                command: ArcCommand {
                    command: "arc-deploy".to_string(),
                    args: vec!["--prod".to_string()],
                },
                healthcheck_path: "/_healthy".to_string(),
            },
        }
    }

    #[test]
    fn global_deployment_history_key_is_zero_padded() {
        let key = global_deployment_history_key(42);
        assert_eq!(key, "/maetro/deployments/history/00000000000000000042");
    }

    #[test]
    fn parse_global_index_from_deployment_id_parses_expected_suffix() {
        let id = "deployment-svc-1-00000000000000000042";
        assert_eq!(parse_global_index_from_deployment_id("svc-1", id), Some(42));
        assert_eq!(
            parse_global_index_from_deployment_id("svc-2", id),
            None,
            "service id prefix must match"
        );
    }

    #[test]
    fn compare_counter_for_missing_uses_version_check() {
        let compare = compare_counter(
            "/maetro/services/svc-1/deployments/history-next-index",
            &CounterSnapshot {
                next_index: 0,
                mod_revision: None,
            },
        );

        assert_eq!(compare["target"], "VERSION");
        assert_eq!(compare["version"], "0");
    }

    #[test]
    fn compare_counter_for_existing_uses_mod_revision_check() {
        let compare = compare_counter(
            "/maetro/deployments/meta/next-index",
            &CounterSnapshot {
                next_index: 12,
                mod_revision: Some(99),
            },
        );

        assert_eq!(compare["target"], "MOD");
        assert_eq!(compare["mod_revision"], "99");
    }

    #[test]
    fn prefix_range_end_advances_prefix() {
        let prefix = b"/maetro/deployments/history/";
        let end = prefix_range_end(prefix).expect("should compute range end");
        assert!(end.as_slice() > prefix.as_slice());
    }

    #[test]
    fn normalize_base_url_adds_http_and_trims_slash() {
        let url = normalize_base_url("127.0.0.1:2379/").expect("should normalize");
        assert_eq!(url, "http://127.0.0.1:2379");
    }

    #[test]
    fn extract_service_configs_filters_and_sorts_by_id() {
        let config_b = sample_service_config("svc-b");
        let config_a = sample_service_config("svc-a");

        let kvs = vec![
            RangeKv {
                key: encode(format!("{SERVICES_ROOT}/svc-b/config").as_bytes()),
                value: encode(
                    serde_json::to_string(&config_b)
                        .expect("serialize config")
                        .as_bytes(),
                ),
                mod_revision: "1".to_string(),
            },
            RangeKv {
                key: encode(format!("{SERVICES_ROOT}/svc-b/deployments/history/1").as_bytes()),
                value: encode(b"{}"),
                mod_revision: "1".to_string(),
            },
            RangeKv {
                key: encode(format!("{SERVICES_ROOT}/svc-a/config").as_bytes()),
                value: encode(
                    serde_json::to_string(&config_a)
                        .expect("serialize config")
                        .as_bytes(),
                ),
                mod_revision: "1".to_string(),
            },
        ];

        let services = extract_service_configs(kvs).expect("extract service configs");
        assert_eq!(services.len(), 2);
        assert_eq!(services[0].id, "svc-a");
        assert_eq!(services[1].id, "svc-b");
    }

    #[test]
    fn build_hashed_service_config_is_deterministic() {
        let first = build_hashed_service_config(sample_patch_request("svc-1", "Service 1"))
            .expect("first hash should succeed");
        let second = build_hashed_service_config(sample_patch_request("svc-1", "Service 1"))
            .expect("second hash should succeed");

        assert_eq!(first.version, second.version);
        assert!(first.version.starts_with("cfg-"));
    }

    #[test]
    fn build_hashed_service_config_changes_when_config_changes() {
        let original = build_hashed_service_config(sample_patch_request("svc-1", "Service 1"))
            .expect("hash should succeed");
        let changed =
            build_hashed_service_config(sample_patch_request("svc-1", "Service 1 Updated"))
                .expect("hash should succeed");

        assert_ne!(original.version, changed.version);
    }
}
