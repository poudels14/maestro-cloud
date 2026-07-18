use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    io::Read,
    path::Path as FsPath,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{
        ConnectInfo, DefaultBodyLimit, Extension, Multipart, Path, Query, Request, State,
        ws::{Message as AxumWsMessage, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{any, delete, get, patch, post},
};
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast};
use tokio_util::io::ReaderStream;

use self::types::{
    BlockedIpRequest, BlockedIpsResponse, CancelDeploymentResponse, ClusterRestartRequest,
    ClusterUnfreezeRequest, ClusterUpgradeRequest, CreateSlackWebhookRequest, DeploymentListItem,
    RemoveDeploymentResponse, ReplicasOverrideRequest, ReplicasResponse, RolloutChange,
    RolloutDiffResponse, RolloutDiffStatus, RolloutServiceRequest, RolloutServiceResponse,
    ServiceListItem, SlackWebhookView, UpdateSlackWebhookRequest, UpgradeSystemRequest,
    UploadServiceResponse,
};
use crate::deployment::store::{
    ClusterStore, RequestClaim, SystemUpgradeRequest as StoredSystemUpgradeRequest,
    UpsertServiceOutcome,
};
use crate::deployment::types::{
    CancelDeploymentOutcome, Deployment, DeploymentBuildInfo, DeploymentStatus, SecretsConfig,
    ServiceConfig, ServiceDeployConfig, ServiceDeployment,
};
use crate::logs::{
    LogEntry, LogHistogram, LogHistogramBucket, LogHistogramGroupBy, LogHistogramQuery, LogOrigin,
    LogReadQuery, LogReadScope, LogSearchQuery,
};
use crate::signal::ShutdownEvent;

mod types;

const DEFAULT_LOG_LIMIT: usize = 1000;
const MAX_LOG_LIMIT: usize = 2000;
const DEFAULT_LOG_RANGE_MS: i64 = 3_600_000;
const MAX_LOG_RANGE_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const ONE_MINUTE_MS: i64 = 60_000;
const FIVE_MINUTES_MS: i64 = 5 * ONE_MINUTE_MS;
const TEN_MINUTES_MS: i64 = 10 * ONE_MINUTE_MS;
const TWO_HOURS_MS: i64 = 120 * ONE_MINUTE_MS;
const MAX_LOG_HISTOGRAM_BUCKETS: i64 = 1_000;
const MAX_REPLICAS_OVERRIDE: u32 = 25;
const MAESTRO_VERSION: &str = env!("CARGO_PKG_VERSION");
const INGESTION_TOKEN_HEADER: &str = "x-maestro-ingestion-token";
const MAX_CLUSTER_WRITE_BODY_BYTES: u64 = 1024 * 1024 * 1024;
const LOG_CURSOR_HEADER: &str = "x-maestro-log-cursor";
const EXEC_FORWARD_HEADER: &str = "x-maestro-telemetry-forwarded";
const EXEC_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAX_EXEC_SESSIONS: usize = 8;

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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterNodeView {
    #[serde(flatten)]
    info: crate::cluster::NodeInfo,
    admin_url: Option<String>,
    state: crate::cluster::NodeState,
    alive: bool,
    last_seen_at_ms: i64,
    lost_at_ms: Option<i64>,
}

#[derive(Clone)]
struct OperatorIdentity(String);

#[derive(Clone)]
struct AppState {
    store: Arc<dyn ClusterStore>,
    log_store: Option<Arc<crate::logs::DuckLogStore>>,
    jwt_secret_key: Option<String>,
    ingestion_token: Option<String>,
    internal_control_token: Option<String>,
    control_socket: Option<String>,
    system_type: Option<String>,
    cluster_name: String,
    cluster_alias: String,
    masked_config: Option<Arc<crate::config::MaskedConfig>>,
    slack: crate::slack::SlackNotifier,
    allow_cli_deployment: bool,
    upload_dir: std::path::PathBuf,
    controller_stats: crate::cluster_stats::SharedControllerStats,
    backup_stats: crate::cluster_stats::SharedBackupStats,
    probe_started_at: Instant,
    local_node_id: Option<String>,
    exec_sessions: Arc<Semaphore>,
}

#[derive(Clone)]
pub(crate) struct Server {
    state: AppState,
}

pub(crate) struct ServerConfig {
    pub jwt_secret_key: Option<String>,
    pub ingestion_token: Option<String>,
    pub internal_control_token: Option<String>,
    pub control_socket: Option<String>,
    pub system_type: Option<String>,
    pub cluster_name: String,
    pub cluster_alias: String,
    pub masked_config: Option<Arc<crate::config::MaskedConfig>>,
    pub slack: crate::slack::SlackNotifier,
    pub allow_cli_deployment: bool,
    pub upload_dir: std::path::PathBuf,
    pub controller_stats: crate::cluster_stats::SharedControllerStats,
    pub backup_stats: crate::cluster_stats::SharedBackupStats,
    pub local_node_id: Option<String>,
}

impl Server {
    pub(crate) fn new(
        store: Arc<dyn ClusterStore>,
        log_store: Option<Arc<crate::logs::DuckLogStore>>,
        config: ServerConfig,
    ) -> Self {
        let ServerConfig {
            jwt_secret_key,
            ingestion_token,
            internal_control_token,
            control_socket,
            system_type,
            cluster_name,
            cluster_alias,
            masked_config,
            slack,
            allow_cli_deployment,
            upload_dir,
            controller_stats,
            backup_stats,
            local_node_id,
        } = config;
        cleanup_stale_cluster_request_spools(&upload_dir);
        Self {
            state: AppState {
                store,
                log_store,
                jwt_secret_key,
                ingestion_token,
                internal_control_token,
                control_socket,
                system_type,
                cluster_name,
                cluster_alias,
                masked_config,
                slack,
                allow_cli_deployment,
                upload_dir,
                controller_stats,
                backup_stats,
                probe_started_at: Instant::now(),
                local_node_id,
                exec_sessions: Arc::new(Semaphore::new(MAX_EXEC_SESSIONS)),
            },
        }
    }

    fn app(&self) -> Router {
        let auth = middleware::from_fn_with_state(self.state.clone(), require_jwt);
        let cluster_write_proxy =
            middleware::from_fn_with_state(self.state.clone(), proxy_cluster_write);
        let node_read_proxy =
            middleware::from_fn_with_state(self.state.clone(), proxy_node_selected_read);
        let operator = Router::new()
            .route("/api/services/rollout", post(Self::rollout_service))
            .route(
                "/api/services/up",
                post(Self::upload_service).layer(DefaultBodyLimit::disable()),
            )
            .route("/api/system/upgrade", post(Self::upgrade_system))
            .route("/api/system/restart", post(Self::restart_system))
            .route("/api/cluster", get(Self::get_cluster_info))
            .route("/api/cluster/nodes", get(Self::get_cluster_nodes))
            .route(
                "/api/cluster/unschedulable",
                get(Self::get_cluster_unschedulable),
            )
            .route("/api/cluster/placements", get(Self::get_cluster_placements))
            .route(
                "/api/cluster/nodes/{nodeId}/drain",
                post(Self::drain_cluster_node),
            )
            .route(
                "/api/cluster/nodes/{nodeId}/restore",
                post(Self::restore_cluster_node),
            )
            .route(
                "/api/cluster/nodes/{nodeId}",
                delete(Self::remove_cluster_node),
            )
            .route("/api/cluster/admissions", post(Self::approve_cluster_node))
            .route(
                "/api/cluster/upgrade",
                get(Self::get_cluster_upgrade).post(Self::start_cluster_upgrade),
            )
            .route(
                "/api/cluster/restart",
                get(Self::get_cluster_upgrade).post(Self::start_cluster_restart),
            )
            .route(
                "/api/cluster/upgrade/unfreeze",
                post(Self::unfreeze_cluster_upgrade),
            )
            .route("/api/cluster/stats", get(Self::get_cluster_stats))
            .route(
                "/api/cluster/stats/nodes",
                get(Self::get_cluster_node_stats),
            )
            .route("/api/config", get(Self::get_config))
            .route("/api/services", get(Self::list_services))
            .route("/api/services/rollout/diff", post(Self::rollout_diff))
            .route(
                "/api/services/{serviceId}/deployments",
                get(Self::list_deployments),
            )
            .route("/api/services/{serviceId}/exec", get(Self::exec_service))
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
            .route(
                "/api/services/{serviceId}/logs/histogram",
                get(Self::get_service_log_histogram),
            )
            .route("/api/system/{name}/logs", get(Self::get_system_logs))
            .route(
                "/api/system/{name}/logs/histogram",
                get(Self::get_system_log_histogram),
            )
            .route("/api/metrics/node", get(Self::get_node_metrics))
            .route("/api/metrics/cluster", get(Self::get_cluster_metrics))
            .route("/api/metrics/stats", get(Self::get_stats_metrics))
            .route(
                "/api/services/{serviceId}/metrics",
                get(Self::get_service_metrics),
            )
            .route(
                "/api/services/{serviceId}/traffic",
                get(Self::get_service_traffic),
            )
            .route(
                "/api/services/{serviceId}/traffic/breakdown",
                get(Self::get_service_traffic_breakdown),
            )
            .route("/api/disks", get(Self::get_disks))
            .route("/api/disks/nodes", get(Self::get_node_disks))
            .route("/api/ingress/routes", get(Self::list_ingress_routes))
            .route("/api/ingress/traffic", get(Self::get_ingress_traffic))
            .route(
                "/api/ingress/blocked-ips",
                get(Self::get_blocked_ingress_ips).patch(Self::set_blocked_ingress_ip),
            )
            .route(
                "/api/ingress/blocked-traffic",
                get(Self::get_blocked_ingress_traffic),
            )
            .route(
                "/api/services/{serviceId}/metrics/containers",
                get(Self::get_container_metrics),
            )
            .route_layer(node_read_proxy)
            .route_layer(cluster_write_proxy)
            .route_layer(auth);

        let ingestion_auth =
            middleware::from_fn_with_state(self.state.clone(), require_ingestion_token);
        let ingestion = Router::new()
            .route("/api/logs", post(Self::ingest_logs))
            .route("/api/metrics", post(Self::ingest_metrics))
            .route_layer(ingestion_auth);

        let machine = Router::new()
            .route("/_healthy", get(Self::healthy))
            .route("/_ready", get(Self::ready))
            .route("/_maestro/ingress-denied", any(Self::ingress_denied));
        let joining = Router::new()
            .route("/api/cluster/ca", post(Self::discover_cluster_ca))
            .route(
                "/api/cluster/recovery-status",
                post(Self::cluster_recovery_status),
            )
            .route("/api/cluster/join", post(Self::join_cluster_node));

        machine
            .merge(joining)
            .merge(ingestion)
            .merge(operator)
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
            "server listening on http://{bind_addr} [pid={}]",
            std::process::id()
        );
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            loop {
                match shutdown_rx.recv().await {
                    Ok(ShutdownEvent::Graceful)
                    | Ok(ShutdownEvent::Force)
                    | Ok(ShutdownEvent::Restart) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
        .await
        .map_err(|err| format!("server error: {err}").into())
    }

    async fn ingress_denied() -> StatusCode {
        StatusCode::FORBIDDEN
    }

    pub(crate) async fn serve_tls(
        self,
        bind_addr: &str,
        certificate_path: &str,
        key_path: &str,
        client_ca_path: Option<&str>,
        mut shutdown_rx: broadcast::Receiver<ShutdownEvent>,
    ) -> crate::error::Result<()> {
        let app = self.app();
        let address = bind_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|err| format!("invalid TLS bind address `{bind_addr}`: {err}"))?;
        let config = build_api_tls_config(certificate_path, key_path, client_ca_path)
            .map_err(|err| format!("failed to load probe TLS identity: {err}"))?;
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            loop {
                match shutdown_rx.recv().await {
                    Ok(ShutdownEvent::Graceful) | Ok(ShutdownEvent::Restart) => {
                        shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
                        break;
                    }
                    Ok(ShutdownEvent::Force) | Err(broadcast::error::RecvError::Closed) => {
                        shutdown_handle.shutdown();
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        println!(
            "server listening on https://{bind_addr} [pid={}]",
            std::process::id()
        );
        axum_server::bind_rustls(address, config)
            .handle(handle)
            .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
            .map_err(|err| format!("TLS server error: {err}").into())
    }

    async fn healthy() -> &'static str {
        "ok"
    }

    async fn ready(State(state): State<AppState>) -> Result<&'static str, (StatusCode, String)> {
        let Some(node_id) = state.local_node_id.as_deref() else {
            return Ok("ready");
        };
        let nodes = state.store.list_cluster_nodes().await.map_err(|err| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to read local node readiness: {err}"),
            )
        })?;
        let now = crate::utils::time::current_time_millis()
            .ok()
            .and_then(|value| i64::try_from(value).ok())
            .unwrap_or_default();
        match nodes.into_iter().find(|node| node.node_id == node_id) {
            Some(node)
                if node.data_plane_ready
                    && now.saturating_sub(node.data_plane_checked_at_ms) <= 15_000 =>
            {
                Ok("ready")
            }
            Some(node) => Err((
                StatusCode::SERVICE_UNAVAILABLE,
                node.data_plane_error
                    .unwrap_or_else(|| "local workload data plane is unready".to_string()),
            )),
            None => Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "local cluster node is not registered".to_string(),
            )),
        }
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

    async fn get_cluster_info(
        State(state): State<AppState>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let canonical_domain = format!("{}.maestro.internal", state.cluster_name);
        let alias_domain = format!("{}.maestro.internal", state.cluster_alias);
        let upgrade_run = state.store.read_cluster_upgrade().await.ok().flatten();
        let coordinated_upgrade = upgrade_run.as_ref().is_some_and(|run| {
            !run.phase.is_terminal() && run.kind == crate::cluster::ClusterMaintenanceKind::Upgrade
        });
        let restarting = upgrade_run.as_ref().is_some_and(|run| {
            !run.phase.is_terminal() && run.kind == crate::cluster::ClusterMaintenanceKind::Restart
        });
        let upgrading = coordinated_upgrade
            || state
                .store
                .read_system_upgrade_request(state.local_node_id.as_deref())
                .await
                .ok()
                .flatten()
                .is_some();
        let nodes = cluster_node_views(&state).await?;
        let leader = state
            .store
            .read_cluster_leader()
            .await
            .map_err(internal_error)?;
        let meta = state
            .store
            .read_cluster_meta()
            .await
            .map_err(internal_error)?;
        let traffic = state
            .store
            .list_cluster_traffic()
            .await
            .map_err(internal_error)?;
        let mut services = Vec::new();
        for generation in &traffic {
            let states = state
                .store
                .list_replica_states(&generation.service_id, &generation.deployment_id)
                .await
                .unwrap_or_default();
            let active = generation
                .active_assignment_ids
                .iter()
                .collect::<HashSet<_>>();
            let endpoints = states
                .into_iter()
                .filter(|replica| {
                    replica
                        .assignment_id
                        .as_ref()
                        .is_some_and(|assignment| active.contains(assignment))
                })
                .filter_map(|replica| replica.endpoint)
                .collect::<Vec<_>>();
            services.push(json!({
                "serviceId": generation.service_id,
                "deploymentId": generation.deployment_id,
                "trafficEpoch": generation.traffic_epoch,
                "activeAssignmentIds": generation.active_assignment_ids,
                "endpoints": endpoints,
            }));
        }
        Ok(Json(serde_json::json!({
            "clusterId": meta.as_ref().map(|meta| meta.cluster_id.as_str()),
            "clusterName": state.cluster_name,
            "clusterAlias": state.cluster_alias,
            "canonicalDomain": canonical_domain,
            "aliasDomain": alias_domain,
            "version": MAESTRO_VERSION,
            "upgrading": upgrading,
            "restarting": restarting,
            "upgradeRun": upgrade_run,
            "thisNodeId": state.local_node_id,
            "leader": leader.as_ref().map(|leader| leader.node_id.as_str()),
            "leaderNodeId": leader.map(|leader| leader.node_id),
            "nodes": nodes,
            "services": services,
        })))
    }

    async fn get_cluster_nodes(
        State(state): State<AppState>,
    ) -> Result<Json<Vec<ClusterNodeView>>, (StatusCode, String)> {
        let nodes = cluster_node_views(&state).await?;
        Ok(Json(nodes))
    }

    async fn get_cluster_unschedulable(
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::cluster::UnschedulableReplica>>, (StatusCode, String)> {
        state
            .store
            .list_unschedulable_replicas()
            .await
            .map(Json)
            .map_err(internal_error)
    }

    async fn get_cluster_placements(
        State(state): State<AppState>,
        Query(query): Query<PlacementQuery>,
    ) -> Result<Json<Vec<crate::cluster::PlacementHistory>>, (StatusCode, String)> {
        state
            .store
            .list_placement_history(
                query.service_id.as_deref(),
                query.deployment_id.as_deref(),
                query.replica_index,
            )
            .await
            .map(Json)
            .map_err(internal_error)
    }

    async fn drain_cluster_node(
        State(state): State<AppState>,
        Path(node_id): Path<String>,
        headers: HeaderMap,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        set_cluster_node_drain_state(&state, &headers, &node_id, true).await?;
        Ok(Json(json!({ "nodeId": node_id, "unschedulable": true })))
    }

    async fn restore_cluster_node(
        State(state): State<AppState>,
        Path(node_id): Path<String>,
        headers: HeaderMap,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        set_cluster_node_drain_state(&state, &headers, &node_id, false).await?;
        Ok(Json(json!({ "nodeId": node_id, "unschedulable": false })))
    }

    async fn remove_cluster_node(
        State(state): State<AppState>,
        Path(node_id): Path<String>,
    ) -> Result<Response, (StatusCode, String)> {
        let socket = state.control_socket.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control socket is unavailable".to_string(),
            )
        })?;
        let token = state.internal_control_token.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control authentication is unavailable".to_string(),
            )
        })?;
        let value = crate::cluster::control::send_command_with_response(
            socket,
            token,
            crate::cluster::control::ControlCommand::RemoveNode {
                node_id: node_id.clone(),
            },
        )
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon returned no removal state".to_string(),
            )
        })?;
        let outcome: crate::cluster::join::RemoveNodeOutcome =
            serde_json::from_value(value).map_err(internal_error)?;
        let status = match outcome {
            crate::cluster::join::RemoveNodeOutcome::Removed => StatusCode::OK,
            crate::cluster::join::RemoveNodeOutcome::Draining
            | crate::cluster::join::RemoveNodeOutcome::LeadershipTransferRequired => {
                StatusCode::ACCEPTED
            }
        };
        Ok((
            status,
            Json(json!({
                "nodeId": node_id,
                "state": outcome,
            })),
        )
            .into_response())
    }

    async fn approve_cluster_node(
        State(state): State<AppState>,
        Json(mut admission): Json<crate::cluster::join::JoinAdmission>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        if admission.created_at_ms == 0 {
            admission.created_at_ms = crate::cluster_stats::now_ms();
        }
        let socket = state.control_socket.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control socket is unavailable".to_string(),
            )
        })?;
        let token = state.internal_control_token.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control authentication is unavailable".to_string(),
            )
        })?;
        crate::cluster::control::send_command(
            socket,
            token,
            crate::cluster::control::ControlCommand::ApproveNode {
                admission: admission.clone(),
            },
        )
        .await
        .map_err(internal_error)?;
        Ok(Json(json!({
            "nodeId": admission.node_id,
            "approved": true,
        })))
    }

    async fn get_cluster_upgrade(
        State(state): State<AppState>,
    ) -> Result<Json<Option<crate::cluster::UpgradeRun>>, (StatusCode, String)> {
        state
            .store
            .read_cluster_upgrade()
            .await
            .map(Json)
            .map_err(internal_error)
    }

    async fn start_cluster_upgrade(
        State(state): State<AppState>,
        Json(request): Json<ClusterUpgradeRequest>,
    ) -> Result<(StatusCode, Json<crate::cluster::UpgradeRun>), (StatusCode, String)> {
        let socket = state.control_socket.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control socket is unavailable".to_string(),
            )
        })?;
        let token = state.internal_control_token.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control authentication is unavailable".to_string(),
            )
        })?;
        let value = crate::cluster::control::send_command_with_response(
            socket,
            token,
            crate::cluster::control::ControlCommand::StartUpgrade {
                target_version: request.target_version,
            },
        )
        .await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon returned no upgrade run".to_string(),
            )
        })?;
        let run = serde_json::from_value(value).map_err(internal_error)?;
        Ok((StatusCode::ACCEPTED, Json(run)))
    }

    async fn start_cluster_restart(
        State(state): State<AppState>,
        Json(request): Json<ClusterRestartRequest>,
    ) -> Result<(StatusCode, Json<crate::cluster::UpgradeRun>), (StatusCode, String)> {
        let node_id = match (request.node_id, request.all) {
            (Some(node_id), false) if !node_id.trim().is_empty() => Some(node_id),
            (None, true) => None,
            (Some(_), true) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "choose either `nodeId` or `all`, not both".to_string(),
                ));
            }
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "a non-empty `nodeId` or `all: true` is required".to_string(),
                ));
            }
        };
        let socket = state.control_socket.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control socket is unavailable".to_string(),
            )
        })?;
        let token = state.internal_control_token.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control authentication is unavailable".to_string(),
            )
        })?;
        let value = crate::cluster::control::send_command_with_response(
            socket,
            token,
            crate::cluster::control::ControlCommand::StartRestart { node_id },
        )
        .await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon returned no restart run".to_string(),
            )
        })?;
        let run = serde_json::from_value(value).map_err(internal_error)?;
        Ok((StatusCode::ACCEPTED, Json(run)))
    }

    async fn unfreeze_cluster_upgrade(
        State(state): State<AppState>,
        Json(request): Json<ClusterUnfreezeRequest>,
    ) -> Result<Json<crate::cluster::UpgradeRun>, (StatusCode, String)> {
        let socket = state.control_socket.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control socket is unavailable".to_string(),
            )
        })?;
        let token = state.internal_control_token.as_deref().ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon control authentication is unavailable".to_string(),
            )
        })?;
        let value = crate::cluster::control::send_command_with_response(
            socket,
            token,
            crate::cluster::control::ControlCommand::UnfreezeUpgrade {
                run_id: request.upgrade_run_id,
            },
        )
        .await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon returned no upgrade run".to_string(),
            )
        })?;
        serde_json::from_value(value)
            .map(Json)
            .map_err(internal_error)
    }

    async fn join_cluster_node(
        State(state): State<AppState>,
        ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
        headers: HeaderMap,
        Json(request): Json<crate::cluster::join::JoinRequest>,
    ) -> Result<Json<crate::cluster::join::JoinEnvelope>, (StatusCode, String)> {
        let signature = headers
            .get("x-maestro-join-signature")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let source_ip = match peer.ip() {
            std::net::IpAddr::V4(ip) => Some(ip),
            std::net::IpAddr::V6(ip) => ip.to_ipv4_mapped(),
        }
        .ok_or_else(join_forbidden)?;
        let socket = state.control_socket.as_deref().ok_or_else(join_forbidden)?;
        let token = state
            .internal_control_token
            .as_deref()
            .ok_or_else(join_forbidden)?;
        let result = crate::cluster::control::send_command_with_response(
            socket,
            token,
            crate::cluster::control::ControlCommand::JoinNode {
                request,
                signature,
                source_ip,
            },
        )
        .await;
        let value = match result {
            Ok(Some(value)) => value,
            Ok(None) => {
                eprintln!("cluster join rejected: daemon returned no response payload");
                return Err(join_forbidden());
            }
            Err(error) => {
                eprintln!("cluster join rejected from {source_ip}: {error}");
                return Err(join_forbidden());
            }
        };
        let envelope = serde_json::from_value(value).map_err(|error| {
            eprintln!("cluster join response encoding failed: {error}");
            join_forbidden()
        })?;
        Ok(Json(envelope))
    }

    async fn discover_cluster_ca(
        State(state): State<AppState>,
        Json(request): Json<crate::cluster::join::CaDiscoveryRequest>,
    ) -> Result<Json<crate::cluster::join::CaDiscoveryResponse>, (StatusCode, String)> {
        let socket = state
            .control_socket
            .as_deref()
            .ok_or_else(join_unavailable)?;
        let token = state
            .internal_control_token
            .as_deref()
            .ok_or_else(join_unavailable)?;
        let value = crate::cluster::control::send_command_with_response(
            socket,
            token,
            crate::cluster::control::ControlCommand::DiscoverClusterCa { request },
        )
        .await
        .map_err(|error| {
            eprintln!("cluster CA discovery failed: {error}");
            join_unavailable()
        })?
        .ok_or_else(join_unavailable)?;
        serde_json::from_value(value).map(Json).map_err(|error| {
            eprintln!("cluster CA discovery response encoding failed: {error}");
            join_unavailable()
        })
    }

    async fn cluster_recovery_status(
        State(state): State<AppState>,
        Json(request): Json<crate::cluster::recovery::RecoveryStatusRequest>,
    ) -> Result<Json<crate::cluster::recovery::RecoveryStatusResponse>, (StatusCode, String)> {
        let socket = state
            .control_socket
            .as_deref()
            .ok_or_else(join_unavailable)?;
        let token = state
            .internal_control_token
            .as_deref()
            .ok_or_else(join_unavailable)?;
        let value = crate::cluster::control::send_command_with_response(
            socket,
            token,
            crate::cluster::control::ControlCommand::RecoveryStatus { request },
        )
        .await
        .map_err(|error| {
            eprintln!("cluster recovery status failed: {error}");
            join_unavailable()
        })?
        .ok_or_else(join_unavailable)?;
        serde_json::from_value(value).map(Json).map_err(|error| {
            eprintln!("cluster recovery status response encoding failed: {error}");
            join_unavailable()
        })
    }

    async fn get_cluster_stats(
        State(state): State<AppState>,
    ) -> Json<crate::cluster_stats::ClusterStatsResponse> {
        let now = crate::cluster_stats::now_ms();
        let controller = state
            .controller_stats
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        let backup = state
            .backup_stats
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        let controller_heartbeat_age_ms = controller.as_ref().map(|snapshot| {
            u64::try_from(now.saturating_sub(snapshot.reported_at_ms)).unwrap_or_default()
        });
        let warnings = stats_warnings(
            state.masked_config.as_deref(),
            controller.as_ref(),
            &backup,
            controller_heartbeat_age_ms,
            now,
        );

        Json(crate::cluster_stats::ClusterStatsResponse {
            generated_at_ms: now,
            probe: crate::cluster_stats::ProbeStatsSnapshot {
                version: MAESTRO_VERSION.to_string(),
                uptime_ms: state
                    .probe_started_at
                    .elapsed()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
            },
            controller,
            controller_heartbeat_age_ms,
            backup,
            warnings,
        })
    }

    async fn get_cluster_node_stats(
        State(state): State<AppState>,
    ) -> Result<
        Json<BTreeMap<String, crate::cluster_stats::ControllerStatsSnapshot>>,
        (StatusCode, String),
    > {
        state
            .store
            .list_node_stats()
            .await
            .map(Json)
            .map_err(internal_error)
    }

    async fn rollout_service(
        headers: HeaderMap,
        Query(query): Query<ForceQuery>,
        State(state): State<AppState>,
        body: Bytes,
    ) -> Result<Json<RolloutServiceResponse>, (StatusCode, String)> {
        reject_cluster_freeze(&state).await?;
        let request: RolloutServiceRequest = parse_json_body(&headers, body)?;
        let service_config = build_service_config(request).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid rollout request payload: {err}"),
            )
        })?;
        validate_cluster_build_registry(&state, &service_config)?;
        reject_unconfigured_preview(&state, &service_config)?;
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
            .map_err(deployment_mutation_error)?;

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
        validate_cluster_build_registry(&state, &service_config)?;
        reject_unconfigured_preview(&state, &service_config)?;
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
        reject_cluster_freeze(&state).await?;
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
        if let Err(error) = validate_cluster_build_registry(&state, &service_config) {
            cleanup_temp();
            return Err(error);
        }
        reject_unconfigured_preview(&state, &service_config)?;

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
                return Err(deployment_mutation_error(err));
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
            items.push(ServiceListItem::new(
                info.config,
                status,
                false,
                info.deploy_frozen,
                info.replicas_override,
            ));
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
            items.push(ServiceListItem::new(
                ServiceConfig {
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
                        exec: true,
                        max_restarts: None,
                        env: Default::default(),
                        secrets: None,
                        volumes: vec![],
                        node_affinity: None,
                        egress: Default::default(),
                    },
                    ingress: None,
                    preview: None,
                    preview_source: None,
                },
                Some(crate::deployment::types::DeploymentStatus::Ready),
                true,
                false,
                None,
            ));
        }

        Ok(Json(items))
    }

    async fn list_deployments(
        Path(service_id): Path<String>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<DeploymentListItem>>, (StatusCode, String)> {
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

        Ok(Json(
            deployments
                .into_iter()
                .map(DeploymentListItem::new)
                .collect(),
        ))
    }

    async fn exec_service(
        State(state): State<AppState>,
        Path(service_id): Path<String>,
        Query(query): Query<ExecQuery>,
        Extension(identity): Extension<OperatorIdentity>,
        headers: HeaderMap,
        upgrade: WebSocketUpgrade,
    ) -> Result<Response, (StatusCode, String)> {
        crate::validation::validate_service_id(&service_id, "serviceId")
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
        if !state
            .masked_config
            .as_ref()
            .is_some_and(|config| config.allow_exec)
        {
            return Err((
                StatusCode::FORBIDDEN,
                "CLI exec is disabled; set allow-exec to true in the cluster config".to_string(),
            ));
        }
        if state
            .masked_config
            .as_ref()
            .is_some_and(|config| config.runtime == "docker")
        {
            return Err((
                StatusCode::NOT_IMPLEMENTED,
                "interactive exec is not supported for the docker runtime".to_string(),
            ));
        }
        let command = match query.command.as_deref() {
            Some(encoded) => serde_json::from_str::<Vec<String>>(encoded).map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("command must be a JSON-encoded argv array: {error}"),
                )
            })?,
            None => vec!["/bin/sh".to_string()],
        };
        if command.is_empty()
            || command.len() > 256
            || command.iter().map(String::len).sum::<usize>() > 64 * 1024
        {
            return Err((
                StatusCode::BAD_REQUEST,
                "command must contain between 1 and 256 arguments totaling at most 64 KiB"
                    .to_string(),
            ));
        }
        let tty = query.tty.unwrap_or(true);
        let initial_size = match (query.cols, query.rows) {
            (None, None) => None,
            (Some(cols), Some(rows)) if cols > 0 && rows > 0 => {
                Some(crate::exec::TerminalSize { cols, rows })
            }
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "cols and rows must be provided together and be greater than zero".to_string(),
                ));
            }
        };

        let deployments = state
            .store
            .list_service_deployments_with_replicas(&service_id)
            .await
            .map_err(internal_error)?;
        let selected = if let Some(deployment_id) = query.deployment_id.as_deref() {
            crate::validation::validate_service_id(deployment_id, "deploymentId")
                .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
            deployments
                .into_iter()
                .find(|item| item.deployment.id == deployment_id)
                .ok_or_else(|| {
                    (
                        StatusCode::NOT_FOUND,
                        format!(
                            "deployment `{deployment_id}` for service `{service_id}` was not found"
                        ),
                    )
                })?
        } else {
            deployments
                .into_iter()
                .filter(|item| item.deployment.status == DeploymentStatus::Ready)
                .max_by_key(|item| item.deployment.created_at)
                .ok_or_else(|| {
                    (
                        StatusCode::NOT_FOUND,
                        format!("service `{service_id}` has no active deployment"),
                    )
                })?
        };
        let deployment_id = selected.deployment.id.clone();
        if !selected.deployment.config.deploy.exec {
            return Err((
                StatusCode::FORBIDDEN,
                format!("CLI exec is disabled for service `{service_id}`"),
            ));
        }
        if !matches!(
            selected.deployment.status,
            DeploymentStatus::PendingReady | DeploymentStatus::Ready | DeploymentStatus::Draining
        ) {
            return Err((
                StatusCode::CONFLICT,
                format!("deployment `{deployment_id}` is not running"),
            ));
        }
        let mut placements = state
            .store
            .list_placement_history(Some(&service_id), Some(&deployment_id), None)
            .await
            .map_err(internal_error)?
            .into_iter()
            .filter(|placement| placement.ended_at_ms.is_none())
            .collect::<Vec<_>>();
        placements.sort_by_key(|placement| std::cmp::Reverse(placement.started_at_ms));
        placements.dedup_by_key(|placement| placement.replica_index);
        let running_replicas = if placements.is_empty() {
            selected
                .replicas
                .iter()
                .filter(|replica| {
                    matches!(
                        replica.status,
                        DeploymentStatus::PendingReady
                            | DeploymentStatus::Ready
                            | DeploymentStatus::Draining
                    )
                })
                .map(|replica| replica.replica_index)
                .collect::<Vec<_>>()
        } else {
            placements
                .iter()
                .map(|placement| placement.replica_index)
                .collect::<Vec<_>>()
        };
        let replica_index = match query.replica_index {
            Some(replica_index) if running_replicas.contains(&replica_index) => replica_index,
            Some(replica_index) => {
                return Err((
                    StatusCode::NOT_FOUND,
                    format!(
                        "replica #{replica_index} of deployment `{deployment_id}` is not running"
                    ),
                ));
            }
            None if running_replicas.len() == 1 => running_replicas[0],
            None if running_replicas.is_empty() => {
                return Err((
                    StatusCode::NOT_FOUND,
                    format!("deployment `{deployment_id}` has no running replicas"),
                ));
            }
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "replicaIndex is required when more than one replica is running".to_string(),
                ));
            }
        };
        let placement = placements
            .into_iter()
            .find(|placement| placement.replica_index == replica_index);
        let permit = state
            .exec_sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    format!("this node already has {MAX_EXEC_SESSIONS} active exec sessions"),
                )
            })?;
        let exec_command = crate::cluster::control::ControlCommand::ExecSession {
            service_id: service_id.clone(),
            deployment_id: deployment_id.clone(),
            replica_index,
            argv: command.clone(),
            tty,
            initial_size,
            client: identity.0,
        };
        let is_remote = placement.as_ref().is_some_and(|placement| {
            state
                .local_node_id
                .as_deref()
                .is_some_and(|local| placement.node_id != local)
        });
        if is_remote {
            if headers.contains_key(EXEC_FORWARD_HEADER) {
                return Err((
                    StatusCode::LOOP_DETECTED,
                    "exec forwarding loop detected".to_string(),
                ));
            }
            let placement = placement.expect("remote exec has a placement");
            let remote = connect_remote_exec(
                &placement,
                &service_id,
                &deployment_id,
                replica_index,
                &command,
                tty,
                initial_size,
                &headers,
                state.local_node_id.as_deref(),
            )
            .await
            .map_err(internal_error)?;
            Ok(upgrade
                .on_upgrade(move |websocket| relay_remote_exec(websocket, remote, permit))
                .into_response())
        } else {
            let socket = state.control_socket.as_deref().ok_or_else(|| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "daemon control socket is unavailable".to_string(),
                )
            })?;
            let token = state.internal_control_token.as_deref().ok_or_else(|| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "daemon control authentication is unavailable".to_string(),
                )
            })?;
            let control = crate::cluster::control::open_exec_stream(socket, token, exec_command)
                .await
                .map_err(exec_http_error)?;
            Ok(upgrade
                .on_upgrade(move |websocket| relay_local_exec(websocket, control, permit))
                .into_response())
        }
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
        if info.config.preview_source.is_some() && body.replicas != configured {
            return Err((
                StatusCode::BAD_REQUEST,
                "preview services are fixed at 1 replica".to_string(),
            ));
        }
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

    async fn set_blocked_ingress_ip(
        State(state): State<AppState>,
        Json(request): Json<BlockedIpRequest>,
    ) -> Result<Json<BlockedIpsResponse>, (StatusCode, String)> {
        let address = request
            .ip
            .trim()
            .parse::<std::net::IpAddr>()
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("invalid IP address `{}`", request.ip),
                )
            })?
            .to_string();

        let blocked_ips = state
            .store
            .set_blocked_ingress_ip(&address, request.blocked)
            .await
            .map_err(deployment_mutation_error)?;

        Ok(Json(BlockedIpsResponse { blocked_ips }))
    }

    async fn get_blocked_ingress_ips(
        State(state): State<AppState>,
    ) -> Result<Json<BlockedIpsResponse>, (StatusCode, String)> {
        let blocked_ips = state
            .store
            .read_ingress_blocklist()
            .await
            .map_err(internal_error)?;
        Ok(Json(BlockedIpsResponse { blocked_ips }))
    }

    async fn get_ingress_traffic(
        Query(query): Query<TrafficBreakdownQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<crate::logs::IngressTrafficBreakdown>, (StatusCode, String)> {
        let Some(log_store) = &state.log_store else {
            return Ok(Json(crate::logs::IngressTrafficBreakdown::default()));
        };
        let (from, to) = metrics_time_range(query.from, query.to);
        if from > to {
            return Err((
                StatusCode::BAD_REQUEST,
                "traffic range `from` cannot be after `to`".to_string(),
            ));
        }
        let limit = query.limit.unwrap_or(100).clamp(1, 500);
        log_store
            .read_cluster_ingress_traffic(from, to, limit)
            .await
            .map(Json)
            .map_err(internal_error)
    }

    async fn get_blocked_ingress_traffic(
        Query(query): Query<TrafficBreakdownQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<crate::logs::IngressTrafficBreakdown>, (StatusCode, String)> {
        let Some(log_store) = &state.log_store else {
            return Ok(Json(crate::logs::IngressTrafficBreakdown::default()));
        };
        let (from, to) = metrics_time_range(query.from, query.to);
        if from > to {
            return Err((
                StatusCode::BAD_REQUEST,
                "traffic range `from` cannot be after `to`".to_string(),
            ));
        }
        let limit = query.limit.unwrap_or(100).clamp(1, 500);
        log_store
            .read_blocked_ingress_traffic(from, to, limit)
            .await
            .map(Json)
            .map_err(internal_error)
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
        reject_cluster_freeze(&state).await?;
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
        validate_cluster_build_registry(&state, &config)?;

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
            .map_err(deployment_mutation_error)?;

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
        reject_cluster_freeze(&state).await?;
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

        let previous = restart_source_deployment(&deployments).ok_or_else(|| {
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
        if previous.config.build.is_none()
            && !crate::runtime::is_immutable_image_reference(&previous_image)
        {
            return Err((
                StatusCode::CONFLICT,
                format!(
                    "service `{service_id}` was deployed from mutable image `{previous_image}`; redeploy it once to pin an immutable digest before restarting"
                ),
            ));
        }
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
            .map_err(deployment_mutation_error)?;

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
    ) -> Result<Response, (StatusCode, String)> {
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
            let read = build_log_read_query(LogReadScope::Prefix(prefix), origin, &query, tail)?;
            let cursor = log_store
                .latest_log_seq(&read.scope)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
            let entries = log_store
                .read_logs(read)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
            return Ok(logs_response(entries, cursor));
        }

        Ok(logs_response(Vec::new(), 0))
    }

    async fn get_service_logs(
        Path(service_id): Path<String>,
        Query(query): Query<LogsQuery>,
        State(state): State<AppState>,
    ) -> Result<Response, (StatusCode, String)> {
        let service_id = service_id.trim();
        crate::validation::validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;

        let tail = query.tail.unwrap_or(DEFAULT_LOG_LIMIT).min(MAX_LOG_LIMIT);
        let origin = parse_phase(query.phase.as_deref())?;

        if let Some(log_store) = &state.log_store {
            let prefix = format!("{service_id}/");
            let read = build_log_read_query(LogReadScope::Prefix(prefix), origin, &query, tail)?;
            let cursor = log_store
                .latest_log_seq(&read.scope)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
            let entries = log_store
                .read_logs(read)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
            return Ok(logs_response(entries, cursor));
        }

        Ok(logs_response(Vec::new(), 0))
    }

    async fn get_service_log_histogram(
        Path(service_id): Path<String>,
        Query(query): Query<LogHistogramHttpQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<LogHistogram>, (StatusCode, String)> {
        let service_id = service_id.trim();
        crate::validation::validate_service_id(service_id, "serviceId")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        let origin = parse_phase(query.phase.as_deref())?;
        let request = build_log_histogram_query(
            LogReadScope::Prefix(format!("{service_id}/")),
            origin,
            &query,
        )?;
        let buckets = match &state.log_store {
            Some(log_store) => log_store
                .read_log_histogram(request.clone())
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?,
            None => Vec::new(),
        };
        Ok(Json(complete_log_histogram(&request, buckets)))
    }

    async fn get_system_logs(
        Path(name): Path<String>,
        Query(query): Query<LogsQuery>,
        State(state): State<AppState>,
    ) -> Result<Response, (StatusCode, String)> {
        let name = name.trim();
        crate::validation::validate_service_id(name, "name")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        let tail = query.tail.unwrap_or(DEFAULT_LOG_LIMIT).min(MAX_LOG_LIMIT);

        let Some(log_store) = &state.log_store else {
            return Ok(logs_response(Vec::new(), 0));
        };
        let sources = system_log_sources(name);
        let read = build_log_read_query(LogReadScope::Sources(sources), None, &query, tail)?;
        let cursor = log_store
            .latest_log_seq(&read.scope)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        let entries = log_store
            .read_logs(read)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(logs_response(entries, cursor))
    }

    async fn get_system_log_histogram(
        Path(name): Path<String>,
        Query(query): Query<LogHistogramHttpQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<LogHistogram>, (StatusCode, String)> {
        let name = name.trim();
        crate::validation::validate_service_id(name, "name")
            .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
        let request = build_log_histogram_query(
            LogReadScope::Sources(system_log_sources(name)),
            None,
            &query,
        )?;
        let buckets = match &state.log_store {
            Some(log_store) => log_store
                .read_log_histogram(request.clone())
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?,
            None => Vec::new(),
        };
        Ok(Json(complete_log_histogram(&request, buckets)))
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
        let payload: crate::metrics::TypedMetricBatch = parse_json_body(&headers, body)?;
        match payload {
            crate::metrics::TypedMetricBatch::Resource(entries) => {
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
            }
            crate::metrics::TypedMetricBatch::ControllerStats(snapshot) => {
                let Some(log_store) = &state.log_store else {
                    return Err((
                        StatusCode::SERVICE_UNAVAILABLE,
                        "log store not configured".to_string(),
                    ));
                };
                log_store
                    .append_stats_metrics(&snapshot.metric_points())
                    .await
                    .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                if let Some(node_id) = state.local_node_id.as_deref() {
                    state
                        .store
                        .publish_node_stats(node_id, &snapshot)
                        .await
                        .map_err(internal_error)?;
                }
                *state
                    .controller_stats
                    .write()
                    .unwrap_or_else(|err| err.into_inner()) = Some(snapshot);
            }
        }
        Ok("ok")
    }

    async fn get_stats_metrics(
        Query(query): Query<StatsMetricsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::cluster_stats::StatsMetricPoint>>, (StatusCode, String)> {
        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let (from, to) = metrics_time_range(query.from, query.to);
        let entries = log_store
            .read_stats_metrics(query.name.as_deref(), from, to)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(entries))
    }

    async fn get_node_metrics(
        Query(query): Query<MetricsQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::metrics::MetricPoint>>, (StatusCode, String)> {
        let Some(log_store) = &state.log_store else {
            return Ok(Json(Vec::new()));
        };
        let (from, to) = metrics_time_range(query.from, query.to);
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
        let (from, to) = metrics_time_range(query.from, query.to);
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
        let (from, to) = metrics_time_range(query.from, query.to);
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
        let (from, to) = metrics_time_range(query.from, query.to);
        let entries = log_store
            .read_traffic_metrics(service_id, from, to)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(entries))
    }

    async fn get_service_traffic_breakdown(
        Path(service_id): Path<String>,
        Query(query): Query<TrafficBreakdownQuery>,
        State(state): State<AppState>,
    ) -> Result<Json<crate::logs::IngressTrafficBreakdown>, (StatusCode, String)> {
        let service_id = service_id.trim();
        crate::validation::validate_service_id(service_id, "serviceId")
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
        let Some(log_store) = &state.log_store else {
            return Ok(Json(crate::logs::IngressTrafficBreakdown::default()));
        };
        let (from, to) = metrics_time_range(query.from, query.to);
        if from > to {
            return Err((
                StatusCode::BAD_REQUEST,
                "traffic range `from` cannot be after `to`".to_string(),
            ));
        }
        let limit = query.limit.unwrap_or(100).clamp(1, 500);
        log_store
            .read_ingress_traffic(service_id, from, to, limit)
            .await
            .map(Json)
            .map_err(internal_error)
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
        let (from, to) = metrics_time_range(query.from, query.to);
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
        let run_id = request
            .run_id
            .as_deref()
            .map(str::trim)
            .filter(|run_id| !run_id.is_empty());
        if request.run_id.is_some() && run_id.is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                "upgrade runId cannot be empty".to_string(),
            ));
        }
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
        let stored_request =
            StoredSystemUpgradeRequest::new(system_type, target_version.to_string())
                .with_run_id(run_id.map(str::to_string));
        state
            .store
            .put_system_upgrade_request(state.local_node_id.as_deref(), &stored_request)
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
            .put_system_restart_request(state.local_node_id.as_deref())
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        eprintln!("restart request accepted");
        Ok(Json(json!({ "accepted": true })))
    }

    async fn get_node_disks(
        State(state): State<AppState>,
    ) -> Result<Json<BTreeMap<String, Vec<crate::cluster::NodeDiskInfo>>>, (StatusCode, String)>
    {
        state
            .store
            .list_node_disks()
            .await
            .map(Json)
            .map_err(internal_error)
    }

    async fn get_disks(
        State(state): State<AppState>,
    ) -> Result<Json<Vec<crate::cluster::NodeDiskInfo>>, (StatusCode, String)> {
        if let Some(node_id) = state.local_node_id.as_deref()
            && let Some(disks) = state
                .store
                .list_node_disks()
                .await
                .map_err(internal_error)?
                .remove(node_id)
        {
            return Ok(Json(disks));
        }
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
        let items: Vec<DiskInfo> = candidates
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
        Ok(Json(
            items
                .into_iter()
                .map(|disk: DiskInfo| crate::cluster::NodeDiskInfo {
                    name: disk.name,
                    mount_point: disk.mount_point,
                    total_bytes: disk.total_bytes,
                    available_bytes: disk.available_bytes,
                    file_system: disk.file_system,
                })
                .collect(),
        ))
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
#[allow(clippy::unnecessary_cast)] // statvfs field widths differ between Unix platforms.
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

fn stats_warnings(
    config: Option<&crate::config::MaskedConfig>,
    controller: Option<&crate::cluster_stats::ControllerStatsSnapshot>,
    backup: &crate::cluster_stats::BackupStatsSnapshot,
    heartbeat_age_ms: Option<u64>,
    now_ms: i64,
) -> Vec<crate::cluster_stats::StatsWarning> {
    use crate::cluster_stats::StatsWarning;

    let mut warnings = Vec::new();
    let mut push = |code: &str, severity: &str, message: String| {
        warnings.push(StatsWarning {
            code: code.to_string(),
            severity: severity.to_string(),
            message,
        });
    };

    match heartbeat_age_ms {
        None => push(
            "controller-heartbeat-missing",
            "error",
            "No stats report has been received from the controller".to_string(),
        ),
        Some(age) if age > 30_000 => push(
            "controller-heartbeat-stale",
            "error",
            format!("Controller stats report is {}s old", age / 1_000),
        ),
        _ => {}
    }

    if let Some(controller) = controller {
        if controller.version != MAESTRO_VERSION {
            push(
                "component-version-mismatch",
                "warning",
                format!(
                    "Controller {} and probe {} are running different versions",
                    controller.version, MAESTRO_VERSION
                ),
            );
        }
        for sink in &controller.sinks {
            if sink.consecutive_failures > 0 {
                push(
                    &format!("sink-{}-failing", sink.id),
                    "error",
                    format!(
                        "{} log sink has failed {} consecutive time(s)",
                        sink.id, sink.consecutive_failures
                    ),
                );
            } else if let Some(oldest) = sink.oldest_pending_at_ms {
                let age = now_ms.saturating_sub(oldest);
                let progressing = sink
                    .last_cursor_advance_at_ms
                    .is_some_and(|last| now_ms.saturating_sub(last) <= 30_000);
                if sink.pending_entries > 0 && age > 60_000 && !progressing {
                    push(
                        &format!("sink-{}-behind", sink.id),
                        "warning",
                        format!(
                            "{} log sink is {}s behind with {} pending entries",
                            sink.id,
                            age / 1_000,
                            sink.pending_entries
                        ),
                    );
                }
            }
        }
        if controller.dead_letters.count > 0 {
            let severity = if controller.dead_letters.count.saturating_mul(10)
                >= controller.dead_letters.capacity.saturating_mul(9)
            {
                "error"
            } else {
                "warning"
            };
            push(
                "datadog-dead-letters",
                severity,
                format!(
                    "Datadog has {} quarantined log entries",
                    controller.dead_letters.count
                ),
            );
        }
    }

    if backup.configured && backup.last_error_at_ms > backup.last_success_at_ms {
        push(
            "log-backup-failing",
            "error",
            "The latest S3 log-backup attempt failed".to_string(),
        );
    }

    if let Some(config) = config {
        if config.disable_etcd_cert {
            push(
                "etcd-mtls-disabled",
                "warning",
                "etcd mTLS is disabled".to_string(),
            );
        }
        if config.encryption_key.is_none() {
            push(
                "encryption-key-missing",
                "error",
                "The cluster encryption key is missing or empty".to_string(),
            );
        }
        if config.datadog.as_ref().is_some_and(|datadog| {
            datadog
                .site
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
        }) {
            push(
                "datadog-site-missing",
                "error",
                "Datadog is configured without a site".to_string(),
            );
        }
    }

    warnings
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

#[cfg(test)]
mod cluster_stats_tests {
    use super::*;

    #[test]
    fn warnings_surface_sink_failure_and_dead_letters_without_disabled_backup() {
        let controller = crate::cluster_stats::ControllerStatsSnapshot {
            reported_at_ms: 1_000_000,
            version: MAESTRO_VERSION.to_string(),
            uptime_ms: 10_000,
            spool: crate::cluster_stats::SpoolStatsSnapshot {
                row_count: 10,
                high_watermark: 10,
                oldest_entry_at_ms: Some(900_000),
                database_bytes: 4_096,
            },
            sinks: vec![crate::cluster_stats::SinkStatsSnapshot {
                id: "datadog".to_string(),
                cursor: 5,
                pending_entries: 5,
                oldest_pending_at_ms: Some(900_000),
                last_success_at_ms: None,
                last_error_at_ms: Some(999_000),
                last_error: Some("HTTP 403".to_string()),
                consecutive_failures: 2,
                last_cursor_advance_at_ms: None,
                filtered_entries: 0,
            }],
            dead_letters: crate::cluster_stats::DeadLetterStatsSnapshot {
                count: 1,
                capacity: 100_000,
                payload_bytes: 100,
                latest_at_ms: Some(999_000),
                latest_status: Some(413),
                latest_error: Some("too large".to_string()),
            },
        };
        let warnings = stats_warnings(
            None,
            Some(&controller),
            &crate::cluster_stats::BackupStatsSnapshot::default(),
            Some(1_000),
            1_000_000,
        );
        let codes = warnings
            .iter()
            .map(|warning| warning.code.as_str())
            .collect::<HashSet<_>>();

        assert!(codes.contains("sink-datadog-failing"));
        assert!(codes.contains("datadog-dead-letters"));
        assert!(!codes.contains("log-backup-disabled"));
    }
}

#[derive(serde::Deserialize)]
struct MetricsQuery {
    from: Option<i64>,
    to: Option<i64>,
}

#[derive(serde::Deserialize)]
struct StatsMetricsQuery {
    name: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
}

#[derive(serde::Deserialize)]
struct TrafficBreakdownQuery {
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<usize>,
}

fn metrics_time_range(from: Option<i64>, to: Option<i64>) -> (i64, i64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let from = from.unwrap_or(now - 3_600_000);
    let to = to.unwrap_or(now);
    (from, to)
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    tail: Option<usize>,
    after: Option<i64>,
    before: Option<i64>,
    from: Option<i64>,
    to: Option<i64>,
    phase: Option<String>,
    query: Option<String>,
}

#[derive(serde::Deserialize)]
struct LogHistogramHttpQuery {
    from: Option<i64>,
    to: Option<i64>,
    phase: Option<String>,
    query: Option<String>,
    #[serde(rename = "bucketMs")]
    bucket_ms: Option<i64>,
    #[serde(rename = "groupBy")]
    group_by: Option<String>,
}

#[derive(serde::Deserialize)]
struct ForceQuery {
    force: Option<bool>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlacementQuery {
    service_id: Option<String>,
    deployment_id: Option<String>,
    replica_index: Option<u32>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecQuery {
    deployment_id: Option<String>,
    replica_index: Option<u32>,
    command: Option<String>,
    tty: Option<bool>,
    cols: Option<u16>,
    rows: Option<u16>,
}

async fn cluster_node_views(
    state: &AppState,
) -> Result<Vec<ClusterNodeView>, (StatusCode, String)> {
    let live = state
        .store
        .list_cluster_nodes()
        .await
        .map_err(internal_error)?
        .into_iter()
        .map(|info| (info.node_id.clone(), info))
        .collect::<BTreeMap<_, _>>();
    let records = state
        .store
        .list_cluster_node_records()
        .await
        .map_err(internal_error)?
        .into_iter()
        .map(|record| (record.last_info.node_id.clone(), record))
        .collect::<BTreeMap<_, _>>();
    let node_ids = live
        .keys()
        .chain(records.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let now = crate::cluster_stats::now_ms();
    let mut views = Vec::with_capacity(node_ids.len());
    for node_id in node_ids {
        let record = records.get(&node_id);
        let alive = live.contains_key(&node_id);
        let info = live
            .get(&node_id)
            .cloned()
            .or_else(|| record.map(|record| record.last_info.clone()))
            .expect("node id came from live or durable records");
        let node_state = state
            .store
            .read_cluster_node_state(&node_id)
            .await
            .map_err(internal_error)?;
        let admin_url = node_admin_url(info.role, &info.subnet);
        views.push(ClusterNodeView {
            info,
            admin_url,
            state: node_state,
            alive,
            last_seen_at_ms: if alive {
                now
            } else {
                record
                    .map(|record| record.last_seen_at_ms)
                    .unwrap_or_default()
            },
            lost_at_ms: record.and_then(|record| record.lost_at_ms),
        });
    }
    Ok(views)
}

fn node_admin_url(role: crate::cluster::NodeRole, subnet: &str) -> Option<String> {
    if role == crate::cluster::NodeRole::Worker {
        return None;
    }
    let address = crate::cluster::network::Ipv4Cidr::parse(subnet)
        .ok()?
        .system_address_from_end(5)?;
    Some(format!("http://{address}"))
}

async fn set_cluster_node_drain_state(
    state: &AppState,
    headers: &HeaderMap,
    node_id: &str,
    unschedulable: bool,
) -> Result<(), (StatusCode, String)> {
    let leader = state
        .store
        .read_cluster_leader()
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "cluster leader is not available".to_string(),
            )
        })?;
    let local_node_id = state.local_node_id.as_deref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "cluster node identity is unavailable".to_string(),
        )
    })?;
    if leader.node_id != local_node_id {
        if headers.contains_key("x-maestro-forwarded") {
            return Err((
                StatusCode::LOOP_DETECTED,
                "cluster write forwarding loop detected".to_string(),
            ));
        }
        let leader_node = state
            .store
            .list_cluster_nodes()
            .await
            .map_err(internal_error)?
            .into_iter()
            .find(|node| node.node_id == leader.node_id)
            .ok_or_else(|| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "leader has no live cluster address".to_string(),
                )
            })?;
        let operation = if unschedulable { "drain" } else { "restore" };
        let url = format!(
            "https://{}:{}/api/cluster/nodes/{}/{}",
            leader_node.cluster_host_ip, leader_node.cluster_api_port, node_id, operation
        );
        let mut request = cluster_http_client()
            .map_err(internal_error)?
            .post(url)
            .header("X-Maestro-Forwarded", local_node_id);
        for name in [
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderName::from_static("idempotency-key"),
        ] {
            if let Some(value) = headers.get(&name) {
                request = request.header(name.as_str(), value.as_bytes());
            }
        }
        let response = request.send().await.map_err(internal_error)?;
        if !response.status().is_success() {
            let status =
                StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            return Err((status, response.text().await.unwrap_or_default()));
        }
        return Ok(());
    }

    let socket = state.control_socket.as_deref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon control socket is unavailable".to_string(),
        )
    })?;
    let token = state.internal_control_token.as_deref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon control authentication is unavailable".to_string(),
        )
    })?;
    crate::cluster::control::send_command(
        socket,
        token,
        crate::cluster::control::ControlCommand::SetNodeState {
            node_id: node_id.to_string(),
            unschedulable,
            reason: unschedulable.then(|| "drain".to_string()),
        },
    )
    .await
    .map_err(internal_error)
}

fn cluster_http_client() -> anyhow::Result<reqwest::Client> {
    let ca = std::fs::read("/certs/ca.pem")?;
    let mut identity = std::fs::read("/certs/probe-client.pem")?;
    identity.extend_from_slice(b"\n");
    identity.extend_from_slice(&std::fs::read("/certs/probe-client-key.pem")?);
    Ok(reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
        .identity(reqwest::Identity::from_pem(&identity)?)
        .https_only(true)
        .timeout(std::time::Duration::from_secs(30))
        .build()?)
}

async fn connect_remote_exec(
    placement: &crate::cluster::PlacementHistory,
    service_id: &str,
    deployment_id: &str,
    replica_index: u32,
    command: &[String],
    tty: bool,
    initial_size: Option<crate::exec::TerminalSize>,
    headers: &HeaderMap,
    local_node_id: Option<&str>,
) -> anyhow::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let mut url = reqwest::Url::parse(&format!(
        "wss://{}:{}/api/services/{service_id}/exec",
        placement.cluster_host_ip, placement.cluster_api_port
    ))?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("deploymentId", deployment_id);
        query.append_pair("replicaIndex", &replica_index.to_string());
        query.append_pair("command", &serde_json::to_string(command)?);
        query.append_pair("tty", if tty { "true" } else { "false" });
        if let Some(size) = initial_size {
            query.append_pair("cols", &size.cols.to_string());
            query.append_pair("rows", &size.rows.to_string());
        }
    }
    let mut request = url.as_str().into_client_request()?;
    if let Some(authorization) = headers.get(axum::http::header::AUTHORIZATION) {
        request
            .headers_mut()
            .insert(axum::http::header::AUTHORIZATION, authorization.clone());
    }
    request.headers_mut().insert(
        EXEC_FORWARD_HEADER,
        HeaderValue::from_str(local_node_id.unwrap_or("standalone"))?,
    );
    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(cluster_ws_tls_config()?));
    let (websocket, _) =
        tokio_tungstenite::connect_async_tls_with_config(request, None, false, Some(connector))
            .await?;
    Ok(websocket)
}

fn cluster_ws_tls_config() -> anyhow::Result<rustls::ClientConfig> {
    cluster_ws_tls_config_from_paths(
        std::path::Path::new("/certs/ca.pem"),
        std::path::Path::new("/certs/probe-client.pem"),
        std::path::Path::new("/certs/probe-client-key.pem"),
    )
}

fn cluster_ws_tls_config_from_paths(
    ca_path: &std::path::Path,
    certificate_path: &std::path::Path,
    key_path: &std::path::Path,
) -> anyhow::Result<rustls::ClientConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    let ca_pem = std::fs::read(ca_path)?;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(&ca_pem) {
        roots.add(certificate?)?;
    }
    let certificate_pem = std::fs::read(certificate_path)?;
    let certificates =
        CertificateDer::pem_slice_iter(&certificate_pem).collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(key_path)?;
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certificates, key)?)
}

async fn relay_local_exec(
    websocket: WebSocket,
    control: tokio::net::UnixStream,
    _permit: OwnedSemaphorePermit,
) {
    let (mut websocket_writer, mut websocket_reader) = websocket.split();
    let (mut control_reader, mut control_writer) = control.into_split();
    let idle = tokio::time::sleep(EXEC_IDLE_TIMEOUT);
    tokio::pin!(idle);
    loop {
        tokio::select! {
            incoming = websocket_reader.next() => {
                idle.as_mut().reset(tokio::time::Instant::now() + EXEC_IDLE_TIMEOUT);
                match incoming {
                    Some(Ok(AxumWsMessage::Binary(encoded))) => {
                        match crate::exec::ExecFrame::decode(&encoded) {
                            Ok(crate::exec::ExecFrame::Stdin(_)
                                | crate::exec::ExecFrame::Resize(_)
                                | crate::exec::ExecFrame::Ping) => {
                                if let Ok(frame) = crate::exec::ExecFrame::decode(&encoded)
                                    && crate::exec::write_length_prefixed(&mut control_writer, &frame).await.is_err()
                                {
                                    break;
                                }
                            }
                            _ => {
                                let _ = send_axum_exec_error(&mut websocket_writer, "client sent an invalid exec frame").await;
                                break;
                            }
                        }
                    }
                    Some(Ok(AxumWsMessage::Ping(payload))) => {
                        if websocket_writer.send(AxumWsMessage::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(AxumWsMessage::Pong(_))) => {}
                    Some(Ok(AxumWsMessage::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(AxumWsMessage::Text(_))) => {
                        let _ = send_axum_exec_error(&mut websocket_writer, "exec accepts binary frames only").await;
                        break;
                    }
                }
            }
            frame = crate::exec::read_length_prefixed(&mut control_reader) => {
                idle.as_mut().reset(tokio::time::Instant::now() + EXEC_IDLE_TIMEOUT);
                match frame {
                    Ok(Some(frame)) => {
                        let terminal = frame.terminal();
                        let encoded = match frame.encode() {
                            Ok(encoded) => encoded,
                            Err(_) => break,
                        };
                        if websocket_writer.send(AxumWsMessage::Binary(encoded.into())).await.is_err() {
                            break;
                        }
                        if terminal {
                            break;
                        }
                    }
                    Ok(None) => {
                        let _ = send_axum_exec_error(&mut websocket_writer, "controller exec stream closed unexpectedly").await;
                        break;
                    }
                    Err(error) => {
                        let _ = send_axum_exec_error(&mut websocket_writer, &error.to_string()).await;
                        break;
                    }
                }
            }
            _ = &mut idle => {
                let _ = send_axum_exec_error(&mut websocket_writer, "exec session idle timeout").await;
                break;
            }
        }
    }
    let _ = control_writer.shutdown().await;
    let _ = websocket_writer.close().await;
}

async fn relay_remote_exec(
    websocket: WebSocket,
    remote: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    _permit: OwnedSemaphorePermit,
) {
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

    let (mut client_writer, mut client_reader) = websocket.split();
    let (mut remote_writer, mut remote_reader) = remote.split();
    let idle = tokio::time::sleep(EXEC_IDLE_TIMEOUT);
    tokio::pin!(idle);
    loop {
        tokio::select! {
            incoming = client_reader.next() => {
                idle.as_mut().reset(tokio::time::Instant::now() + EXEC_IDLE_TIMEOUT);
                match incoming {
                    Some(Ok(AxumWsMessage::Binary(encoded))) => {
                        if remote_writer.send(TungsteniteMessage::Binary(encoded)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(AxumWsMessage::Ping(payload))) => {
                        if client_writer.send(AxumWsMessage::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(AxumWsMessage::Pong(_))) => {}
                    Some(Ok(AxumWsMessage::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(AxumWsMessage::Text(_))) => {
                        let _ = send_axum_exec_error(&mut client_writer, "exec accepts binary frames only").await;
                        break;
                    }
                }
            }
            incoming = remote_reader.next() => {
                idle.as_mut().reset(tokio::time::Instant::now() + EXEC_IDLE_TIMEOUT);
                match incoming {
                    Some(Ok(TungsteniteMessage::Binary(encoded))) => {
                        let terminal = crate::exec::ExecFrame::decode(&encoded)
                            .is_ok_and(|frame| frame.terminal());
                        if client_writer.send(AxumWsMessage::Binary(encoded)).await.is_err() {
                            break;
                        }
                        if terminal {
                            break;
                        }
                    }
                    Some(Ok(TungsteniteMessage::Ping(payload))) => {
                        if remote_writer.send(TungsteniteMessage::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(TungsteniteMessage::Pong(_))) => {}
                    Some(Ok(TungsteniteMessage::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(TungsteniteMessage::Text(_) | TungsteniteMessage::Frame(_))) => {
                        let _ = send_axum_exec_error(&mut client_writer, "remote exec sent a non-binary frame").await;
                        break;
                    }
                }
            }
            _ = &mut idle => {
                let _ = send_axum_exec_error(&mut client_writer, "exec session idle timeout").await;
                break;
            }
        }
    }
    let _ = remote_writer.close().await;
    let _ = client_writer.close().await;
}

async fn send_axum_exec_error<S>(writer: &mut S, message: &str) -> Result<(), axum::Error>
where
    S: futures_util::Sink<AxumWsMessage, Error = axum::Error> + Unpin,
{
    let encoded = crate::exec::ExecFrame::Error(message.to_string())
        .encode()
        .unwrap_or_else(|_| vec![4]);
    writer.send(AxumWsMessage::Binary(encoded.into())).await
}

fn exec_http_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    let message = error.to_string();
    let status = if message.contains("disabled") {
        StatusCode::FORBIDDEN
    } else if message.contains("not supported") {
        StatusCode::NOT_IMPLEMENTED
    } else if message.contains("not found") || message.contains("not running") {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, message)
}

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, error.to_string())
}

fn deployment_mutation_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    let message = error.to_string();
    if message.contains("cluster deploys are frozen") {
        (StatusCode::CONFLICT, message)
    } else if message.contains("ingress blocklist") {
        (StatusCode::BAD_REQUEST, message)
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}

fn restart_source_deployment(deployments: &[ServiceDeployment]) -> Option<&ServiceDeployment> {
    deployments
        .iter()
        .find(|deployment| {
            deployment.status == DeploymentStatus::Ready && deployment.build.is_some()
        })
        .or_else(|| {
            deployments
                .iter()
                .find(|deployment| deployment.build.is_some())
        })
}

async fn reject_cluster_freeze(state: &AppState) -> Result<(), (StatusCode, String)> {
    if let Some(freeze) = state
        .store
        .read_cluster_freeze()
        .await
        .map_err(internal_error)?
    {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "cluster deploys are frozen by upgrade run `{}`: {}",
                freeze.upgrade_run_id, freeze.reason
            ),
        ));
    }
    Ok(())
}

fn build_api_tls_config(
    certificate_path: &str,
    key_path: &str,
    client_ca_path: Option<&str>,
) -> anyhow::Result<axum_server::tls_rustls::RustlsConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    let certificate_pem = std::fs::read(certificate_path)?;
    let certificates =
        CertificateDer::pem_slice_iter(&certificate_pem).collect::<Result<Vec<_>, _>>()?;
    let private_key = PrivateKeyDer::from_pem_file(key_path)?;
    let builder = rustls::ServerConfig::builder();
    let mut server = if let Some(client_ca_path) = client_ca_path {
        let ca_pem = std::fs::read(client_ca_path)?;
        let mut roots = rustls::RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(&ca_pem) {
            roots.add(certificate?)?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .allow_unauthenticated()
            .build()?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)?
    };
    server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(axum_server::tls_rustls::RustlsConfig::from_config(
        Arc::new(server),
    ))
}

fn join_forbidden() -> (StatusCode, String) {
    (StatusCode::FORBIDDEN, "join rejected".to_string())
}

fn join_unavailable() -> (StatusCode, String) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "cluster join service unavailable".to_string(),
    )
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

fn build_log_read_query(
    scope: LogReadScope,
    origin: Option<LogOrigin>,
    query: &LogsQuery,
    limit: usize,
) -> Result<LogReadQuery, (StatusCode, String)> {
    let search = parse_log_search(query.query.as_deref())?;
    validate_log_time_range(query.from, query.to)?;
    Ok(LogReadQuery {
        scope,
        origin,
        search,
        from: query.from,
        to: query.to,
        after: query.before.is_none().then_some(query.after).flatten(),
        before: query.before,
        limit,
    })
}

fn build_log_histogram_query(
    scope: LogReadScope,
    origin: Option<LogOrigin>,
    query: &LogHistogramHttpQuery,
) -> Result<LogHistogramQuery, (StatusCode, String)> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let to = query.to.unwrap_or(now);
    let from = query.from.unwrap_or(to - DEFAULT_LOG_RANGE_MS);
    validate_log_time_range(Some(from), Some(to))?;
    let range = to - from;
    let default_bucket_ms = if range <= DEFAULT_LOG_RANGE_MS {
        ONE_MINUTE_MS
    } else if range <= 6 * 60 * 60 * 1000 {
        FIVE_MINUTES_MS
    } else if range <= 24 * 60 * 60 * 1000 {
        TEN_MINUTES_MS
    } else {
        TWO_HOURS_MS
    };
    let bucket_ms = query
        .bucket_ms
        .unwrap_or(default_bucket_ms)
        .max(range / MAX_LOG_HISTOGRAM_BUCKETS)
        .max(ONE_MINUTE_MS);
    let group_by = match query.group_by.as_deref() {
        None | Some("level") => LogHistogramGroupBy::Level,
        Some("status") => LogHistogramGroupBy::HttpStatusClass,
        Some(other) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unknown histogram groupBy `{other}`"),
            ));
        }
    };
    Ok(LogHistogramQuery {
        scope,
        origin,
        search: parse_log_search(query.query.as_deref())?,
        from,
        to,
        bucket_ms,
        group_by,
    })
}

fn parse_log_search(query: Option<&str>) -> Result<Option<LogSearchQuery>, (StatusCode, String)> {
    query
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(str::parse::<LogSearchQuery>)
        .transpose()
        .map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid log query: {error}"),
            )
        })
}

fn validate_log_time_range(from: Option<i64>, to: Option<i64>) -> Result<(), (StatusCode, String)> {
    if from.is_some() != to.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "log time range requires both `from` and `to`".to_string(),
        ));
    }
    if from.is_some_and(|value| value < 0) || to.is_some_and(|value| value < 0) {
        return Err((
            StatusCode::BAD_REQUEST,
            "log time range cannot be negative".to_string(),
        ));
    }
    if let (Some(from), Some(to)) = (from, to) {
        if from >= to {
            return Err((
                StatusCode::BAD_REQUEST,
                "log time range `from` must be before `to`".to_string(),
            ));
        }
        if to - from > MAX_LOG_RANGE_MS {
            return Err((
                StatusCode::BAD_REQUEST,
                "log time range cannot exceed 7 days".to_string(),
            ));
        }
    }
    Ok(())
}

fn complete_log_histogram(
    query: &LogHistogramQuery,
    buckets: Vec<LogHistogramBucket>,
) -> LogHistogram {
    let mut buckets = buckets
        .into_iter()
        .map(|bucket| (bucket.ts, bucket))
        .collect::<BTreeMap<_, _>>();
    let mut ts = query.from - query.from.rem_euclid(query.bucket_ms);
    let mut completed = Vec::new();
    while ts < query.to {
        completed.push(buckets.remove(&ts).unwrap_or_else(|| LogHistogramBucket {
            ts,
            count: 0,
            levels: BTreeMap::new(),
        }));
        let next = ts.saturating_add(query.bucket_ms);
        if next <= ts {
            break;
        }
        ts = next;
    }
    LogHistogram {
        from: query.from,
        to: query.to,
        bucket_ms: query.bucket_ms,
        buckets: completed,
    }
}

fn system_log_sources(name: &str) -> Vec<String> {
    if name == "maestro-probe" {
        vec![
            "maestro-probe".to_string(),
            "maestro-controller".to_string(),
        ]
    } else {
        vec![name.to_string()]
    }
}

fn logs_response(entries: Vec<LogEntry>, cursor: i64) -> Response {
    let mut response = Json(entries).into_response();
    if let Ok(cursor) = HeaderValue::from_str(&cursor.to_string()) {
        response.headers_mut().insert(LOG_CURSOR_HEADER, cursor);
    }
    response
}

async fn require_jwt(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let Some(secret) = state.jwt_secret_key.as_ref() else {
        request
            .extensions_mut()
            .insert(OperatorIdentity("operator".to_string()));
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
    let claims = validate_jwt(token, secret).map_err(|err| {
        (
            StatusCode::UNAUTHORIZED,
            format!("invalid auth token: {err}"),
        )
    })?;
    let identity = claims
        .get("sub")
        .and_then(serde_json::Value::as_str)
        .filter(|subject| !subject.trim().is_empty())
        .unwrap_or("operator")
        .to_string();
    request.extensions_mut().insert(OperatorIdentity(identity));
    Ok(next.run(request).await)
}

fn validate_jwt(token: &str, secret: &str) -> jsonwebtoken::errors::Result<serde_json::Value> {
    let key = jsonwebtoken::DecodingKey::from_secret(secret.as_bytes());
    let validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    jsonwebtoken::decode::<serde_json::Value>(token, &key, &validation).map(|data| data.claims)
}

struct SpooledClusterRequest {
    path: std::path::PathBuf,
    fingerprint: String,
}

impl SpooledClusterRequest {
    async fn body(&self) -> Result<Body, (StatusCode, String)> {
        let file = tokio::fs::File::open(&self.path).await.map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to reopen cluster request body: {error}"),
            )
        })?;
        Ok(Body::from_stream(ReaderStream::new(file)))
    }
}

impl Drop for SpooledClusterRequest {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "failed to remove spooled cluster request `{}`: {error}",
                self.path.display()
            );
        }
    }
}

fn cleanup_stale_cluster_request_spools(upload_dir: &FsPath) {
    let entries = match std::fs::read_dir(upload_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            eprintln!(
                "failed to inspect cluster request spool directory `{}`: {error}",
                upload_dir.display()
            );
            return;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name
            .to_string_lossy()
            .starts_with(".maestro-cluster-request-")
        {
            continue;
        }
        if let Err(error) = std::fs::remove_file(entry.path()) {
            eprintln!(
                "failed to remove stale cluster request spool `{}`: {error}",
                entry.path().display()
            );
        }
    }
}

async fn spool_cluster_request(
    upload_dir: &FsPath,
    parts: &axum::http::request::Parts,
    mut body: Body,
    max_bytes: u64,
) -> Result<SpooledClusterRequest, (StatusCode, String)> {
    tokio::fs::create_dir_all(upload_dir)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create the request spool directory: {error}"),
            )
        })?;
    let path = upload_dir.join(format!(
        ".maestro-cluster-request-{}-{}",
        std::process::id(),
        crate::utils::nanoid::unique_id(20)
    ));
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options.open(&path).await.map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to create a cluster request spool: {error}"),
        )
    })?;
    let mut spooled = SpooledClusterRequest {
        path,
        fingerprint: String::new(),
    };
    let mut digest = cluster_request_fingerprint_hasher(parts);
    let mut length = 0_u64;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        length = length.checked_add(data.len() as u64).ok_or_else(|| {
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                "cluster request body is too large".to_string(),
            )
        })?;
        if length > max_bytes {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("cluster request body exceeds the {max_bytes} byte limit"),
            ));
        }
        digest.update(&data);
        file.write_all(&data).await.map_err(|error| {
            (
                StatusCode::INSUFFICIENT_STORAGE,
                format!("failed to spool the cluster request body: {error}"),
            )
        })?;
    }
    file.flush().await.map_err(|error| {
        (
            StatusCode::INSUFFICIENT_STORAGE,
            format!("failed to flush the cluster request body: {error}"),
        )
    })?;
    drop(file);
    spooled.fingerprint = format!("{:x}", digest.finalize());
    Ok(spooled)
}

async fn proxy_cluster_write(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    if !is_cluster_write(request.method(), request.uri().path()) || state.local_node_id.is_none() {
        return Ok(next.run(request).await);
    }
    let request_id = request
        .headers()
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| crate::utils::nanoid::unique_id(32));
    request.headers_mut().insert(
        "idempotency-key",
        axum::http::HeaderValue::from_str(&request_id).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "invalid idempotency key".to_string(),
            )
        })?,
    );
    let leader = state
        .store
        .read_cluster_leader()
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "cluster leader is not available; retry after 2 seconds".to_string(),
            )
        })?;
    let local_node_id = state.local_node_id.as_deref().expect("checked above");
    if leader.node_id == local_node_id {
        let (parts, body) = request.into_parts();
        let spooled = spool_cluster_request(
            &state.upload_dir,
            &parts,
            body,
            MAX_CLUSTER_WRITE_BODY_BYTES,
        )
        .await?;
        let fingerprint = spooled.fingerprint.clone();
        let claim = state
            .store
            .claim_cluster_request(
                local_node_id,
                &request_id,
                &fingerprint,
                crate::cluster_stats::now_ms(),
            )
            .await
            .map_err(internal_error)?;
        match claim {
            RequestClaim::Conflict => {
                return Ok(cluster_request_response(
                    StatusCode::CONFLICT,
                    None,
                    "Idempotency-Key was already used for a different request".into(),
                    &request_id,
                ));
            }
            RequestClaim::InProgress => {
                return Ok(cluster_request_response(
                    StatusCode::ACCEPTED,
                    None,
                    "request is already in progress".into(),
                    &request_id,
                ));
            }
            RequestClaim::Complete {
                status_code,
                content_type,
                body,
            } => {
                let status =
                    StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                return Ok(cluster_request_response(
                    status,
                    content_type.as_deref(),
                    body.into(),
                    &request_id,
                ));
            }
            RequestClaim::Started => {}
        }

        let request = Request::from_parts(parts, spooled.body().await?);
        let response = next.run(request).await;
        let (parts, body) = response.into_parts();
        let body = to_bytes(body, 1024 * 1024)
            .await
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        let content_type = parts
            .headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        let receipt_result = state
            .store
            .complete_cluster_request(
                local_node_id,
                &request_id,
                &fingerprint,
                parts.status.as_u16(),
                content_type,
                &body,
                crate::cluster_stats::now_ms(),
            )
            .await;
        let mut response = Response::from_parts(parts, axum::body::Body::from(body));
        if let Err(error) = receipt_result {
            eprintln!("failed to finalize cluster request receipt `{request_id}`: {error}");
            response.headers_mut().insert(
                "x-maestro-receipt-status",
                axum::http::HeaderValue::from_static("incomplete"),
            );
        }
        set_cluster_request_id(&mut response, &request_id);
        return Ok(response);
    }
    if request.headers().contains_key("x-maestro-forwarded") {
        return Err((
            StatusCode::LOOP_DETECTED,
            "cluster write forwarding loop detected".to_string(),
        ));
    }
    let leader_node = state
        .store
        .list_cluster_nodes()
        .await
        .map_err(internal_error)?
        .into_iter()
        .find(|node| node.node_id == leader.node_id)
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "leader has no live cluster address; retry after 2 seconds".to_string(),
            )
        })?;
    let (parts, body) = request.into_parts();
    let url = format!(
        "https://{}:{}{}",
        leader_node.cluster_host_ip, leader_node.cluster_api_port, parts.uri
    );
    let mut forwarded = cluster_http_client()
        .map_err(internal_error)?
        .request(parts.method, url)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .header("X-Maestro-Forwarded", local_node_id);
    for (name, value) in &parts.headers {
        if !is_hop_by_hop_header(name) && name != axum::http::header::CONTENT_LENGTH {
            forwarded = forwarded.header(name, value);
        }
    }
    let response = forwarded.send().await.map_err(internal_error)?;
    let status = response.status();
    let headers = response.headers().clone();
    let mut output = Response::builder()
        .status(status)
        .body(Body::from_stream(response.bytes_stream()))
        .map_err(internal_error)?;
    for (name, value) in &headers {
        if !is_hop_by_hop_header(name) && name != axum::http::header::CONTENT_LENGTH {
            output.headers_mut().insert(name.clone(), value.clone());
        }
    }
    output.headers_mut().insert(
        "x-maestro-request-id",
        axum::http::HeaderValue::from_str(&request_id)
            .expect("generated request id is a valid header"),
    );
    Ok(output)
}

#[cfg(test)]
fn cluster_request_fingerprint(parts: &axum::http::request::Parts, body: &Bytes) -> String {
    let mut digest = cluster_request_fingerprint_hasher(parts);
    digest.update(body);
    format!("{:x}", digest.finalize())
}

fn cluster_request_fingerprint_hasher(parts: &axum::http::request::Parts) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(parts.method.as_str().as_bytes());
    digest.update([0]);
    digest.update(
        parts
            .uri
            .path_and_query()
            .map_or("", |value| value.as_str())
            .as_bytes(),
    );
    digest.update([0]);
    if let Some(authorization) = parts.headers.get(axum::http::header::AUTHORIZATION) {
        digest.update(authorization.as_bytes());
    }
    digest.update([0]);
    digest
}

fn is_hop_by_hop_header(name: &axum::http::HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
    )
}

fn cluster_request_response(
    status: StatusCode,
    content_type: Option<&str>,
    body: Bytes,
    request_id: &str,
) -> Response {
    let mut response = Response::builder()
        .status(status)
        .body(axum::body::Body::from(body))
        .expect("static cluster request response is valid");
    if let Some(content_type) = content_type
        && let Ok(value) = axum::http::HeaderValue::from_str(content_type)
    {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_TYPE, value);
    }
    set_cluster_request_id(&mut response, request_id);
    response
}

fn set_cluster_request_id(response: &mut Response, request_id: &str) {
    response.headers_mut().insert(
        "x-maestro-request-id",
        axum::http::HeaderValue::from_str(request_id)
            .expect("validated request id is a valid header"),
    );
}

async fn proxy_node_selected_read(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    if request.method() != axum::http::Method::GET
        || !is_node_local_read(request.uri().path())
        || state.local_node_id.is_none()
    {
        return Ok(next.run(request).await);
    }
    let local_node_id = state.local_node_id.as_deref().expect("checked above");
    let parsed = reqwest::Url::parse(&format!("http://maestro.local{}", request.uri()))
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let mut selected_node = parsed
        .query_pairs()
        .find(|(key, _)| key == "nodeId")
        .map(|(_, value)| value.into_owned());
    if selected_node.is_none() {
        let replica_index = parsed
            .query_pairs()
            .find(|(key, _)| key == "replicaIndex")
            .and_then(|(_, value)| value.parse::<u32>().ok());
        if let Some(replica_index) = replica_index {
            let segments = request
                .uri()
                .path()
                .trim_matches('/')
                .split('/')
                .collect::<Vec<_>>();
            if segments.len() >= 6
                && segments[0] == "api"
                && segments[1] == "services"
                && segments[3] == "deployments"
            {
                selected_node = state
                    .store
                    .list_placement_history(
                        Some(segments[2]),
                        Some(segments[4]),
                        Some(replica_index),
                    )
                    .await
                    .map_err(internal_error)?
                    .first()
                    .map(|placement| placement.node_id.clone());
            }
        }
    }
    let Some(selected_node) = selected_node else {
        let mut response = next.run(request).await;
        response.headers_mut().insert(
            "x-maestro-node-id",
            axum::http::HeaderValue::from_str(local_node_id)
                .expect("node id is a valid header value"),
        );
        return Ok(response);
    };
    if selected_node == local_node_id {
        let mut response = next.run(request).await;
        response.headers_mut().insert(
            "x-maestro-node-id",
            axum::http::HeaderValue::from_str(local_node_id)
                .expect("node id is a valid header value"),
        );
        return Ok(response);
    }
    if request
        .headers()
        .contains_key("x-maestro-telemetry-forwarded")
    {
        return Err((
            StatusCode::LOOP_DETECTED,
            "telemetry forwarding loop detected".to_string(),
        ));
    }
    let live_destination = state
        .store
        .list_cluster_nodes()
        .await
        .map_err(internal_error)?
        .into_iter()
        .find(|node| node.node_id == selected_node);
    let destination = match live_destination {
        Some(node) => Some(node),
        None => state
            .store
            .list_cluster_node_records()
            .await
            .map_err(internal_error)?
            .into_iter()
            .find(|record| record.last_info.node_id == selected_node)
            .map(|record| record.last_info),
    }
    .ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("cluster node `{selected_node}` is unknown"),
        )
    })?;
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 1024 * 1024)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let url = format!(
        "https://{}:{}{}",
        destination.cluster_host_ip, destination.cluster_api_port, parts.uri
    );
    let mut forwarded = cluster_http_client()
        .map_err(internal_error)?
        .request(parts.method, url)
        .body(body.to_vec())
        .header("X-Maestro-Telemetry-Forwarded", local_node_id);
    for (name, value) in &parts.headers {
        if name != axum::http::header::HOST && name != axum::http::header::CONTENT_LENGTH {
            forwarded = forwarded.header(name, value);
        }
    }
    let response = forwarded.send().await.map_err(internal_error)?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await.map_err(internal_error)?;
    let mut output = Response::builder()
        .status(status)
        .body(axum::body::Body::from(body))
        .map_err(internal_error)?;
    for (name, value) in &headers {
        if name != axum::http::header::CONTENT_LENGTH {
            output.headers_mut().insert(name.clone(), value.clone());
        }
    }
    output.headers_mut().insert(
        "x-maestro-node-id",
        axum::http::HeaderValue::from_str(&selected_node).expect("node id is a valid header value"),
    );
    Ok(output)
}

fn is_cluster_write(method: &axum::http::Method, path: &str) -> bool {
    if method == axum::http::Method::GET || method == axum::http::Method::HEAD {
        return false;
    }
    if matches!(path, "/api/logs" | "/api/metrics")
        || path.starts_with("/api/system/")
        || path == "/api/services/rollout/diff"
    {
        return false;
    }
    path.starts_with("/api/")
}

fn is_node_local_read(path: &str) -> bool {
    path.contains("/logs")
        || path.contains("/metrics")
        || path.contains("/traffic")
        || path == "/api/ingress/blocked-traffic"
        || path == "/api/disks"
        || path == "/api/cluster/stats"
}

async fn require_ingestion_token(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let Some(expected) = state.ingestion_token.as_deref() else {
        return Ok(next.run(request).await);
    };
    let presented = request
        .headers()
        .get(INGESTION_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !constant_time_token_matches(expected, presented) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "missing or invalid ingestion token".to_string(),
        ));
    }
    Ok(next.run(request).await)
}

fn constant_time_token_matches(expected: &str, presented: &str) -> bool {
    let expected = Sha256::digest(expected.as_bytes());
    let presented = Sha256::digest(presented.as_bytes());
    expected
        .iter()
        .zip(presented.iter())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
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

fn validate_cluster_build_registry(
    state: &AppState,
    config: &ServiceConfig,
) -> Result<(), (StatusCode, String)> {
    crate::validation::validate_cluster_build_registry(&config.build, state.local_node_id.is_some())
        .map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid service `{}`: {error}", config.id),
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
        preview,
    } = request;

    let service_id = id.trim().to_string();
    crate::validation::validate_user_service_id(&service_id, "id")?;

    let service_name = name.trim().to_string();
    if service_name.is_empty() {
        return Err("name cannot be empty".to_string());
    }
    let (build, image, deploy) =
        crate::validation::validate_service_provider_config(&build, &image, &deploy)?;
    crate::validation::validate_ingress_config(&ingress)?;
    crate::validation::validate_preview_config(&service_id, &preview, &build, &ingress)?;

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
            "exec": deploy.exec,
            "egress": &deploy.egress,
            "env": &deploy.env,
            "secretsHash": &secrets_hash,
            "secretsSource": &secrets_source,
            "secretsMountPath": deploy.secrets.as_ref().map(|s| &s.mount_path),
        },
        "ingress": &ingress,
        "preview": &preview
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
        preview,
        preview_source: None,
    })
}

fn reject_unconfigured_preview(
    state: &AppState,
    service: &ServiceConfig,
) -> Result<(), (StatusCode, String)> {
    let github_configured = state
        .masked_config
        .as_ref()
        .and_then(|config| config.github.as_ref())
        .is_some();
    validate_preview_integration(service, github_configured)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))
}

fn validate_preview_integration(
    service: &ServiceConfig,
    github_configured: bool,
) -> Result<(), String> {
    if service
        .preview
        .as_ref()
        .is_some_and(|preview| preview.enabled)
        && !github_configured
    {
        Err("preview.enabled requires github configuration in maestro.jsonc".to_string())
    } else {
        Ok(())
    }
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
        deployment: Box::new(outcome.deployment),
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

    if old.deploy.egress != new_config.deploy.egress {
        changes.push(RolloutChange {
            field: "egress.allow".into(),
            from: Some(
                serde_json::to_string(&old.deploy.egress.allow)
                    .map_err(|error| error.to_string())?,
            ),
            to: Some(
                serde_json::to_string(&new_config.deploy.egress.allow)
                    .map_err(|error| error.to_string())?,
            ),
        });
    }

    diff_env(&old.deploy.env, &new_config.deploy.env, &mut changes);

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

fn diff_env(
    old: &crate::deployment::types::EnvConfig,
    new: &crate::deployment::types::EnvConfig,
    changes: &mut Vec<RolloutChange>,
) {
    for (key, new_val) in &new.items {
        if let Some(old_val) = old.items.get(key) {
            if old_val != new_val {
                changes.push(RolloutChange {
                    field: format!("env.{key}"),
                    from: Some(old_val.masked()),
                    to: Some(new_val.masked()),
                });
            }
        } else {
            changes.push(RolloutChange {
                field: format!("env.{key}"),
                from: None,
                to: Some(new_val.masked()),
            });
        }
    }
    for key in old.items.keys() {
        if !new.items.contains_key(key) {
            changes.push(RolloutChange {
                field: format!("env.{key}"),
                from: Some(old.items[key].masked()),
                to: None,
            });
        }
    }
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
