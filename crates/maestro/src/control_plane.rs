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
    ADMIN_CONTAINER_ID, BuildKitBuilder, BuildKitSettings, BuildRequest, SystemSupervisor,
    admin_container,
};
use maestro_control::{
    AgentHub, ApiAuth, ApiState, Deployer, DeployerSettings, ImageBuilder, require_token, router,
    run_publisher,
};
use maestro_core::{ClusterConfig, ClusterSettings, Deployment, NodeId, NodeRole, NodeState};
use maestro_proto::NodeCredentials;
use maestro_proto::pb::cluster_server::ClusterServer;
use maestro_proto::pb::node_server::NodeServer;
use maestro_runtime::{ContainerdRuntime, RegistryClient, Runtime};
use maestro_store::{Consensus, ConsensusSettings, SecretKey};
use serde::Serialize;
use tokio::task::JoinSet;

use crate::cluster_service::ClusterService;
use crate::identity::NodeIdentity;
use crate::join::JoinAuthority;
use crate::node_service::{
    ClusterAccess, ForwardingWriter, NodeClients, NodeService, cluster_logs, remove_cluster_node,
};
use crate::server::{api_secret, bind_api_socket, encryption_key_path};

const NODE_EXPIRY: Duration = Duration::from_secs(30);
const DEPLOYER_INTERVAL: Duration = Duration::from_secs(1);
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

struct SealedSpecBuilder {
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
        .add_service(NodeServer::new(NodeService::new(
            settings.telemetry_socket.clone(),
            Some(consensus.clone()),
        )))
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
        runtime,
        BuildKitBuilder::new(buildkit.clone()),
        settings.admin_source.clone(),
        settings.api_socket.clone(),
    ));
    let observed = Arc::new(hub);
    let deployer = Deployer::new(
        consensus.clone(),
        observed.clone(),
        Arc::new(SealedSpecBuilder {
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

    let clients = NodeClients::new(settings.credentials.clone());
    let logs = axum::Router::new()
        .route("/api/logs", axum::routing::get(cluster_logs))
        .route(
            "/api/cluster/nodes/{node_id}",
            axum::routing::delete(remove_cluster_node),
        )
        .with_state(ClusterAccess {
            consensus: consensus.clone(),
            clients: clients.clone(),
        });
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
        writer: Arc::new(ForwardingWriter::new(consensus.clone(), clients)),
        observed,
        key,
    }))
    .merge(logs)
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

impl ImageBuilder for SealedSpecBuilder {
    async fn build(&self, deployment: Deployment) -> Result<String, String> {
        let spec = self
            .key
            .open_spec(&deployment.spec)
            .map_err(|error| error.to_string())?;
        let build = spec
            .build
            .ok_or_else(|| "deployment has no build configuration".to_string())?;
        match (&build.env.source, &build.secrets.source) {
            (None, None) => {
                let request = BuildRequest {
                    deployment_id: deployment.id.to_string(),
                    service_id: deployment.service_id.to_string(),
                    repo: build.repo,
                    branch: build.branch,
                    context_dir: None,
                    dockerfile: build.dockerfile,
                    registry: build.registry,
                    image: None,
                    build_args: build.env.items,
                    secrets: build.secrets.items,
                };
                self.builder.build(&request).await
            }
            _ => Err("external secret sources are not supported yet".to_string()),
        }
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
