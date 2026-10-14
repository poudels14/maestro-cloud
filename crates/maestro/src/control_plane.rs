use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use maestro_agent::{
    ADMIN_CONTAINER_ID, BuildKitBuilder, BuildKitSettings, BuildRequest, DepotBuild,
    SystemSupervisor, admin_container,
};
use maestro_control::{
    AgentHub, ApiAuth, ApiState, BuildWatcher, BuiltImage, Deployer, DeployerSettings,
    ImageBuilder, RemoteHeads, require_token, router, run_publisher,
};
use maestro_core::{
    ClusterConfig, ClusterSettings, Deployment, NodeId, NodeRole, NodeState, ServiceSpec,
};
use maestro_proto::NodeCredentials;
use maestro_proto::pb::cluster_server::ClusterServer;
use maestro_proto::pb::node_server::NodeServer;
use maestro_runtime::{ContainerdRuntime, RegistryClient, Runtime};
use maestro_store::{Consensus, ConsensusSettings, SecretKey};
use maestro_telemetry::{BackupConfig, S3Archive};
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;

use crate::archive;
use crate::cluster_service::ClusterService;
use crate::exec::{ExecState, exec_service};
use crate::github::GitHub;
use crate::identity::NodeIdentity;
use crate::join::JoinAuthority;
use crate::node_service::{
    ClusterAccess, ForwardingWriter, NodeClients, NodeService, cluster_disks, cluster_logs,
    local_disks, remove_cluster_node,
};
use crate::observability;
use crate::previews::{PreviewSettings, PreviewWatcher};
use crate::secret_sources;
use crate::server::{api_secret, bind_api_socket, encryption_key_path};
use crate::slack::{self, SlackState};
use crate::uploads::{self, UploadState, upload_service};

const NODE_EXPIRY: Duration = Duration::from_secs(30);
const DEPLOYER_INTERVAL: Duration = Duration::from_secs(1);
const WATCH_INTERVAL: Duration = Duration::from_secs(30);
const GITHUB_API: &str = "https://api.github.com";
const MAX_UPLOAD_BYTES: usize = 1 << 30;
const ARCHIVE_CHUNK: usize = 1 << 20;
const MEMBERSHIP_INTERVAL: Duration = Duration::from_secs(5);
const LEARNER_CATCH_UP: Duration = Duration::from_secs(120);
const BUILDKIT_ADDRESS: &str = "unix:///run/buildkit/buildkitd.sock";
const ADMIN_DOCKERFILE: &str = "Dockerfile.admin";
const ADMIN_BUILD_RETRY: Duration = Duration::from_secs(60);

pub struct ControlPlaneSettings {
    pub data_dir: PathBuf,
    pub identity: NodeIdentity,
    pub credentials: NodeCredentials,
    pub config: ClusterConfig,
    pub grpc_listen: SocketAddr,
    pub api_listen: SocketAddr,
    pub api_socket: PathBuf,
    pub telemetry_socket: PathBuf,
    pub admin_source: Option<PathBuf>,
}

struct InfoState {
    consensus: Arc<Consensus>,
    cluster: ClusterSettings,
    build_logs: PathBuf,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterInfo {
    name: String,
    cidr: String,
    version: &'static str,
    node_id: NodeId,
    term: u64,
    leader: Option<ClusterMember>,
    voters: Vec<ClusterMember>,
    learners: Vec<ClusterMember>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterMember {
    id: NodeId,
    address: String,
}

struct GitHeads;

struct SealedSpecBuilder {
    node_id: NodeId,
    runtime: Arc<ContainerdRuntime>,
    depot_token: Option<String>,
    uploads: PathBuf,
    work_dir: PathBuf,
    builder: BuildKitBuilder,
    registry: RegistryClient,
    key: Arc<SecretKey>,
}

pub async fn spawn(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    settings: ControlPlaneSettings,
    runtime: Arc<ContainerdRuntime>,
) -> anyhow::Result<Arc<Consensus>> {
    let key = Arc::new(SecretKey::load_or_create(&encryption_key_path(
        &settings.data_dir,
    ))?);
    let consensus = Arc::new(
        open_consensus(
            &settings.data_dir,
            &settings.identity,
            settings.credentials.clone(),
        )
        .await?,
    );
    let hub = AgentHub::new(NODE_EXPIRY);
    let authority = Arc::new(JoinAuthority::load(&settings.data_dir)?);
    let grpc = tonic::transport::Server::builder()
        .tls_config(settings.credentials.server_tls())?
        .add_service(consensus.raft_service())
        .add_service(ClusterServer::new(ClusterService::new(
            consensus.clone(),
            hub.clone(),
            authority,
            settings.identity.cluster.clone(),
        )))
        .add_service(NodeServer::new(
            NodeService::new(
                settings.telemetry_socket.clone(),
                Some(consensus.clone()),
                runtime.clone(),
            )
            .with_uploads(key.clone(), uploads_dir(&settings.data_dir)),
        ))
        .serve(settings.grpc_listen);
    tasks.spawn(async move { grpc.await.context("gRPC server stopped") });
    tasks.spawn(publish(
        hub.clone(),
        consensus.clone(),
        key.clone(),
        settings.identity.cluster.name.clone(),
    ));
    tasks.spawn(run_membership(consensus.clone()));

    let buildkit = BuildKitSettings {
        buildctl: PathBuf::from("buildctl"),
        address: BUILDKIT_ADDRESS.to_string(),
        work_dir: settings.data_dir.join("builds/work"),
        log_dir: settings.data_dir.join("builds/logs"),
        git_token: None,
    };
    tasks.spawn(run_admin(
        runtime.clone(),
        BuildKitBuilder::new(buildkit.clone()),
        settings.admin_source.clone(),
        settings.api_socket.clone(),
    ));
    let observed = Arc::new(hub);
    let deployer = Deployer::new(
        consensus.clone(),
        observed.clone(),
        Arc::new(SealedSpecBuilder {
            node_id: settings.identity.id,
            runtime: runtime.clone(),
            depot_token: settings
                .config
                .depot
                .as_ref()
                .map(|depot| depot.token.clone()),
            uploads: uploads_dir(&settings.data_dir),
            work_dir: settings.data_dir.join("builds/work"),
            builder: BuildKitBuilder::new(buildkit),
            registry: RegistryClient::new(),
            key: key.clone(),
        }),
        DeployerSettings {
            defaults: settings.config.defaults.resources.clone(),
            ..DeployerSettings::default()
        },
    );
    tasks.spawn(run_deployer(deployer, consensus.clone()));
    if let Some(backup) = settings.config.log_backup.clone() {
        let archive = S3Archive::connect(BackupConfig {
            bucket: backup.bucket,
            kms_key_id: backup.kms_key_id,
            region: backup.region,
            prefix: backup
                .prefix
                .unwrap_or_else(|| settings.identity.cluster.name.clone()),
            node: settings.identity.name.clone(),
            retention_days: None,
        })
        .await;
        let archiver = archive::run_archiver(
            consensus.clone(),
            archive,
            settings.data_dir.join("builds/logs"),
        );
        tasks.spawn(async move {
            archiver.await;
            Ok(())
        });
    }
    if let Some(github) = settings.config.github.clone() {
        let watcher = PreviewWatcher::new(
            consensus.clone(),
            key.clone(),
            GitHub::new(GITHUB_API.to_string(), github.token.clone()),
            PreviewSettings {
                domain: github.preview_domain.clone(),
                max_concurrent: github.max_concurrent_previews as usize,
                homepage: settings
                    .config
                    .homepage
                    .clone()
                    .map(|homepage| homepage.trim_end_matches('/').to_string()),
            },
        );
        tasks.spawn(run_previews(
            watcher,
            consensus.clone(),
            Duration::from_secs(github.poll_interval_secs),
        ));
    }
    tasks.spawn(run_watcher(
        BuildWatcher::new(consensus.clone(), Arc::new(GitHeads), key.clone()),
        consensus.clone(),
    ));

    let clients = NodeClients::new(settings.credentials.clone());
    let logs = axum::Router::new()
        .route("/api/logs", axum::routing::get(cluster_logs))
        .route("/api/disks", axum::routing::get(local_disks))
        .route("/api/disks/nodes", axum::routing::get(cluster_disks))
        .route(
            "/api/metrics/node",
            axum::routing::get(observability::node_metrics),
        )
        .route(
            "/api/cluster/stats",
            axum::routing::get(observability::cluster_stats),
        )
        .route(
            "/api/logs/histogram",
            axum::routing::get(observability::log_histogram),
        )
        .route(
            "/api/metrics/cluster",
            axum::routing::get(observability::cluster_metrics),
        )
        .route(
            "/api/services/{service_id}/metrics",
            axum::routing::get(observability::service_metrics),
        )
        .route(
            "/api/ingress/traffic",
            axum::routing::get(observability::ingress_traffic),
        )
        .route(
            "/api/ingress/blocked-traffic",
            axum::routing::get(observability::blocked_ingress_traffic),
        )
        .route(
            "/api/services/{service_id}/traffic/breakdown",
            axum::routing::get(observability::service_traffic_breakdown),
        )
        .route(
            "/api/services/{service_id}/traffic",
            axum::routing::get(observability::service_traffic),
        )
        .route(
            "/api/services/{service_id}/metrics/containers",
            axum::routing::get(observability::container_metrics),
        )
        .route(
            "/api/cluster/nodes/{node_id}",
            axum::routing::delete(remove_cluster_node),
        )
        .with_state(ClusterAccess {
            consensus: consensus.clone(),
            clients: clients.clone(),
        });
    let uploads = axum::Router::new()
        .route(
            "/api/services/up",
            axum::routing::post(upload_service)
                .layer(axum::extract::DefaultBodyLimit::max(MAX_UPLOAD_BYTES)),
        )
        .with_state(UploadState {
            consensus: consensus.clone(),
            clients: clients.clone(),
            key: key.clone(),
            directory: uploads_dir(&settings.data_dir),
            allowed: settings.config.allow_cli_deployment,
        });
    let exec = axum::Router::new()
        .route(
            "/api/services/{service_id}/exec",
            axum::routing::get(exec_service),
        )
        .with_state(ExecState {
            consensus: consensus.clone(),
            clients: clients.clone(),
            allowed: settings.config.allow_exec,
        });
    let writer = Arc::new(ForwardingWriter::new(consensus.clone(), clients.clone()));
    let slack = SlackState {
        writer: writer.clone(),
        key: key.clone(),
        cluster: settings.identity.cluster.name.clone(),
        config_webhook: settings
            .config
            .slack
            .as_ref()
            .map(|slack| slack.webhook_url.clone())
            .filter(|url| !url.trim().is_empty()),
        http: reqwest::Client::new(),
    };
    tasks.spawn({
        let notifier = slack::run_notifier(slack.clone(), consensus.clone(), observed.clone());
        async move {
            notifier.await;
            Ok(())
        }
    });
    let webhooks = axum::Router::new()
        .route(
            "/api/webhooks/slack",
            axum::routing::get(slack::list_webhooks).post(slack::create_webhook),
        )
        .route(
            "/api/webhooks/slack/{webhook_id}",
            axum::routing::patch(slack::update_webhook).delete(slack::delete_webhook),
        )
        .route(
            "/api/webhooks/slack/{webhook_id}/test",
            axum::routing::post(slack::test_webhook),
        )
        .with_state(slack);
    let info = axum::Router::new()
        .route("/api/cluster", axum::routing::get(cluster_info))
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/build-log",
            axum::routing::get(build_log),
        )
        .with_state(Arc::new(InfoState {
            consensus: consensus.clone(),
            cluster: settings.identity.cluster.clone(),
            build_logs: settings.data_dir.join("builds/logs"),
        }));
    let api = router(Arc::new(ApiState {
        writer: writer.clone(),
        observed,
        key,
    }))
    .merge(logs)
    .merge(uploads)
    .merge(webhooks)
    .merge(exec)
    .merge(info);
    let auth = Arc::new(ApiAuth::new(&api_secret(&settings.data_dir)?));
    let authenticated_api = api
        .clone()
        .layer(axum::middleware::from_fn_with_state(auth, require_token));
    let listener = tokio::net::TcpListener::bind(settings.api_listen)
        .await
        .with_context(|| format!("failed to listen on {}", settings.api_listen))?;
    let socket_listener = bind_api_socket(&settings.api_socket)?;
    tasks.spawn(async move {
        axum::serve(listener, authenticated_api)
            .await
            .context("API server stopped")
    });
    tasks.spawn(async move {
        axum::serve(socket_listener, api)
            .await
            .context("API socket stopped")
    });
    Ok(consensus)
}

pub async fn open_consensus(
    data_dir: &Path,
    identity: &NodeIdentity,
    credentials: NodeCredentials,
) -> anyhow::Result<Consensus> {
    Ok(Consensus::open(
        ConsensusSettings::new(
            identity.id,
            identity.advertise.clone(),
            data_dir.join("raft"),
        ),
        Some(credentials),
    )
    .await?)
}

impl RemoteHeads for GitHeads {
    async fn head(
        &self,
        repo: String,
        branch: String,
        token: Option<String>,
    ) -> Result<String, String> {
        maestro_agent::remote_head(&repo, &branch, token.as_ref()).await
    }
}

impl ImageBuilder for SealedSpecBuilder {
    async fn build(&self, deployment: Deployment) -> Result<BuiltImage, String> {
        let spec = self
            .key
            .open_spec(&deployment.spec)
            .map_err(|error| error.to_string())?;
        let build = spec
            .build
            .ok_or_else(|| "deployment has no build configuration".to_string())?;
        let built_on = build.registry.is_none().then_some(self.node_id);
        let upload_context = match &build.repo {
            Some(_) => None,
            None => {
                let destination = self.work_dir.join(format!("{}-upload", deployment.id));
                uploads::unpack(
                    uploads::archive_path(
                        &self.uploads,
                        &deployment.service_id,
                        deployment.id.as_str(),
                    ),
                    destination.clone(),
                )
                .await?;
                Some(destination)
            }
        };
        let request = BuildRequest {
            deployment_id: deployment.id.to_string(),
            service_id: deployment.service_id.to_string(),
            repo: build.repo,
            branch: build.branch,
            context_dir: upload_context.clone(),
            dockerfile: build.dockerfile,
            registry: build.registry,
            image: None,
            build_args: build.env.items,
            secrets: build.secrets.items,
            depot: build
                .depot
                .zip(self.depot_token.clone())
                .filter(|(depot, _)| !depot.project.trim().is_empty())
                .map(|(depot, token)| DepotBuild {
                    project: depot.project,
                    token,
                }),
        };
        let result = self.builder.build(&request).await;
        if let Some(context) = upload_context {
            let _ = tokio::fs::remove_dir_all(context).await;
            let _ = tokio::fs::remove_file(uploads::archive_path(
                &self.uploads,
                &deployment.service_id,
                deployment.id.as_str(),
            ))
            .await;
        }
        let output = result?;
        if let Some(archive) = &output.archive {
            let imported = import_archive(&self.runtime, &output.image, archive).await;
            let _ = tokio::fs::remove_file(archive).await;
            imported?;
        }
        Ok(BuiltImage {
            image: output.image,
            commit: output.commit,
            built_on,
        })
    }

    async fn resolve_sources(&self, deployment: Deployment) -> Result<ServiceSpec, String> {
        let spec = self
            .key
            .open_spec(&deployment.spec)
            .map_err(|error| error.to_string())?;
        let resolved = secret_sources::resolve_spec(spec).await?;
        Ok(self.key.seal_spec(&resolved))
    }

    async fn resolve(&self, image: String) -> Result<String, String> {
        self.registry
            .pin(&image)
            .await
            .map_err(|error| error.to_string())
    }
}

async fn cluster_info(State(info): State<Arc<InfoState>>) -> Json<ClusterInfo> {
    let metrics = info.consensus.metrics();
    let members = |members: BTreeMap<NodeId, String>| {
        members
            .into_iter()
            .map(|(id, address)| ClusterMember { id, address })
            .collect::<Vec<_>>()
    };
    Json(ClusterInfo {
        name: info.cluster.name.clone(),
        cidr: info.cluster.cidr.clone(),
        version: env!("CARGO_PKG_VERSION"),
        node_id: metrics.node_id,
        term: metrics.term,
        leader: info
            .consensus
            .current_leader()
            .map(|(id, address)| ClusterMember { id, address }),
        voters: members(metrics.voters),
        learners: members(metrics.learners),
    })
}

async fn build_log(
    State(info): State<Arc<InfoState>>,
    axum::extract::Path((_, deployment_id)): axum::extract::Path<(String, String)>,
) -> axum::response::Response {
    let valid = !deployment_id.is_empty()
        && deployment_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-');
    let contents = match valid {
        true => tokio::fs::read_to_string(info.build_logs.join(format!("{deployment_id}.log")))
            .await
            .ok(),
        false => None,
    };
    match contents {
        Some(text) => (
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8",
            )],
            text,
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no build log on this node for that deployment" })),
        )
            .into_response(),
    }
}

async fn publish(
    hub: AgentHub,
    consensus: Arc<Consensus>,
    key: Arc<SecretKey>,
    cluster: String,
) -> anyhow::Result<()> {
    run_publisher(hub, consensus.state_handle(), key, cluster).await;
    Ok(())
}

async fn run_membership(consensus: Arc<Consensus>) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(MEMBERSHIP_INTERVAL);
    loop {
        ticker.tick().await;
        if consensus.is_leader()
            && let Err(error) = converge_membership(&consensus).await
        {
            tracing::warn!(%error, "failed to converge raft membership");
        }
    }
}

async fn converge_membership(consensus: &Consensus) -> anyhow::Result<()> {
    let metrics = consensus.metrics();
    let pending = consensus
        .state()
        .nodes()?
        .into_iter()
        .filter(|node| {
            node.role == NodeRole::Voter
                && node.state != NodeState::Removed
                && !metrics.voters.contains_key(&node.id)
        })
        .collect::<Vec<_>>();
    for node in pending {
        if !metrics.learners.contains_key(&node.id) {
            tracing::info!(node = %node.name, endpoint = %node.endpoint, "adding voter as learner");
            tokio::time::timeout(
                LEARNER_CATCH_UP,
                consensus.add_learner(node.id, &node.endpoint),
            )
            .await
            .context("learner did not catch up in time")??;
        }
        tracing::info!(node = %node.name, "promoting learner to voter");
        consensus.promote(BTreeSet::from([node.id])).await?;
    }
    Ok(())
}

async fn run_deployer(
    deployer: Deployer<Consensus, AgentHub, SealedSpecBuilder>,
    consensus: Arc<Consensus>,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(DEPLOYER_INTERVAL);
    loop {
        ticker.tick().await;
        if consensus.is_leader()
            && let Err(error) = deployer.tick().await
        {
            tracing::warn!(%error, "deployer tick failed");
        }
    }
}

async fn run_previews(
    mut watcher: PreviewWatcher<Consensus>,
    consensus: Arc<Consensus>,
    interval: Duration,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if consensus.is_leader()
            && let Err(error) = watcher.poll().await
        {
            tracing::warn!(%error, "preview poll failed");
        }
    }
}

async fn run_watcher(
    watcher: BuildWatcher<Consensus, GitHeads>,
    consensus: Arc<Consensus>,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(WATCH_INTERVAL);
    loop {
        ticker.tick().await;
        if consensus.is_leader()
            && let Err(error) = watcher.poll().await
        {
            tracing::warn!(%error, "build watcher poll failed");
        }
    }
}

async fn run_admin(
    runtime: Arc<ContainerdRuntime>,
    builder: BuildKitBuilder,
    source: Option<PathBuf>,
    api_socket: PathBuf,
) -> anyhow::Result<()> {
    match source {
        None => {
            tracing::info!("admin UI disabled; pass --admin-source to enable it");
            std::future::pending::<anyhow::Result<()>>().await
        }
        Some(source) => {
            let version = env!("CARGO_PKG_VERSION");
            let image = format!("maestro.local/maestro-admin:{version}");
            while runtime.pull(&image).await.is_err() {
                tracing::info!(%image, "building the admin UI image");
                let request = BuildRequest {
                    deployment_id: format!("maestro-admin-{version}"),
                    service_id: ADMIN_CONTAINER_ID.to_string(),
                    repo: None,
                    branch: None,
                    context_dir: Some(source.clone()),
                    dockerfile: ADMIN_DOCKERFILE.to_string(),
                    registry: None,
                    image: Some(image.clone()),
                    build_args: Default::default(),
                    secrets: Default::default(),
                    depot: None,
                };
                if let Err(error) = builder.build(&request).await {
                    tracing::warn!(%error, "admin UI image build failed; retrying");
                    tokio::time::sleep(ADMIN_BUILD_RETRY).await;
                }
            }
            SystemSupervisor::new(runtime, vec![admin_container(&image, &api_socket)])
                .run()
                .await;
            Ok(())
        }
    }
}

async fn import_archive(
    runtime: &ContainerdRuntime,
    image: &str,
    archive: &Path,
) -> Result<(), String> {
    let mut file = tokio::fs::File::open(archive)
        .await
        .map_err(|error| format!("failed to open the depot image archive: {error}"))?;
    let (chunks, receiver) = tokio::sync::mpsc::channel(16);
    let reader = tokio::spawn(async move {
        let mut buffer = vec![0u8; ARCHIVE_CHUNK];
        while let Ok(read) = file.read(&mut buffer).await
            && read > 0
            && chunks.send(buffer[..read].to_vec()).await.is_ok()
        {}
    });
    let imported = runtime
        .import_image(image, receiver)
        .await
        .map_err(|error| format!("failed to import the depot image: {error}"));
    let _ = reader.await;
    imported
}

fn uploads_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("uploads")
}
