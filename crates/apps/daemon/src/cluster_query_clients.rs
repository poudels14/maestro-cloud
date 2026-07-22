use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::NodeId;
use kernel_store::Store;
use logs::{ControllerStatsProvider, LogQueryStore, StatsMetricStore, TrafficQueryStore};
use metrics::{HostMetricQueryStore, WorkloadMetricQueryStore};
use node_agent::{ArtifactPeerSource, ArtifactPeerSourceError, NodeExecService, NodeExecSettings};
use runtime::{ArtifactByteStream, ArtifactDigest};

use crate::control_plane::{DaemonRoleFactory, role_error};
use crate::{DaemonPlan, RoleError, RoleSpec};

pub(crate) fn exec_sessions<MeshBackendType, FirewallBackendType, BridgeBackendType>(
    factory: &DaemonRoleFactory<MeshBackendType, FirewallBackendType, BridgeBackendType>,
    plan: &DaemonPlan,
    spec: &RoleSpec,
    store: Arc<dyn Store>,
) -> Result<server::HttpClusterExecSessions, RoleError> {
    let trust_root = required_trust_root(&factory.api_settings, "exec")?;
    let identity = required_identity(&factory.api_settings, "exec")?;
    let jwt_secret = required_jwt_secret(&factory.api_settings, "exec")?;
    let local = Arc::new(
        NodeExecService::new(
            store,
            factory.workload_runtime.clone(),
            NodeExecSettings {
                cluster_id: spec.cluster_id.clone(),
                node_id: spec.node_id.clone(),
                maximum_sessions: 8,
            },
        )
        .map_err(|error| role_error("construct local exec service", error))?,
    );
    server::HttpClusterExecSessions::new(
        plan.node_id().clone(),
        node_endpoints(plan),
        trust_root,
        identity,
        jwt_secret,
        local,
    )
    .map_err(|error| role_error("construct cluster exec proxy", error))
}

pub(crate) fn log_query_store(
    plan: &DaemonPlan,
    settings: &server::ServerSettings,
    local: Arc<dyn LogQueryStore>,
    local_traffic: Arc<dyn TrafficQueryStore>,
) -> Result<server::HttpNodeLogQueryStore, RoleError> {
    let trust_root = required_trust_root(settings, "log")?;
    let identity = required_identity(settings, "log")?;
    let jwt_secret = required_jwt_secret(settings, "log")?;
    server::HttpNodeLogQueryStore::new(
        plan.node_id().clone(),
        node_endpoints(plan),
        trust_root,
        identity,
        jwt_secret,
        local,
        local_traffic,
    )
    .map_err(|error| role_error("construct cluster log proxy", error))
}

pub(crate) fn metric_query_store(
    plan: &DaemonPlan,
    settings: &server::ServerSettings,
    workloads: Arc<dyn WorkloadMetricQueryStore>,
    hosts: Arc<dyn HostMetricQueryStore>,
) -> Result<server::HttpNodeMetricQueryStore, RoleError> {
    let trust_root = required_trust_root(settings, "metric")?;
    let identity = required_identity(settings, "metric")?;
    let jwt_secret = required_jwt_secret(settings, "metric")?;
    server::HttpNodeMetricQueryStore::new(
        plan.node_id().clone(),
        node_endpoints(plan),
        trust_root,
        identity,
        jwt_secret,
        workloads,
        hosts,
    )
    .map_err(|error| role_error("construct cluster metric proxy", error))
}

pub(crate) fn stats_query_store(
    plan: &DaemonPlan,
    settings: &server::ServerSettings,
    local: Arc<dyn ControllerStatsProvider>,
    local_metrics: Arc<dyn StatsMetricStore>,
) -> Result<server::HttpNodeStatsQueryStore, RoleError> {
    let trust_root = required_trust_root(settings, "stats")?;
    let identity = required_identity(settings, "stats")?;
    let jwt_secret = required_jwt_secret(settings, "stats")?;
    server::HttpNodeStatsQueryStore::new(
        plan.node_id().clone(),
        node_endpoints(plan),
        trust_root,
        identity,
        jwt_secret,
        local,
        local_metrics,
    )
    .map_err(|error| role_error("construct cluster stats proxy", error))
}

pub(crate) fn artifact_source(
    plan: &DaemonPlan,
    settings: &server::ServerSettings,
) -> Result<Arc<dyn ArtifactPeerSource>, RoleError> {
    let trust_root = required_trust_root(settings, "artifact")?;
    let identity = required_identity(settings, "artifact")?;
    let jwt_secret = required_jwt_secret(settings, "artifact")?;
    let client = server::HttpNodeArtifactClient::new(
        plan.node_id().clone(),
        node_endpoints(plan),
        trust_root,
        identity,
        jwt_secret,
    )
    .map_err(|error| role_error("construct cluster artifact client", error))?;
    Ok(Arc::new(HttpArtifactPeerSource(client)))
}

struct HttpArtifactPeerSource(server::HttpNodeArtifactClient);

#[async_trait]
impl ArtifactPeerSource for HttpArtifactPeerSource {
    async fn export(
        &self,
        node_id: &NodeId,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactPeerSourceError> {
        self.0
            .export(node_id, digest)
            .await
            .map_err(|error| match error {
                server::NodeArtifactTransferError::Rejected { message } => {
                    ArtifactPeerSourceError::Rejected { message }
                }
                server::NodeArtifactTransferError::Unavailable { message } => {
                    ArtifactPeerSourceError::Unavailable { message }
                }
            })
    }
}

fn node_endpoints(plan: &DaemonPlan) -> BTreeMap<NodeId, SocketAddr> {
    plan.cluster()
        .nodes
        .iter()
        .map(|(node_id, node)| {
            (
                node_id.clone(),
                SocketAddr::new(
                    IpAddr::V4(node.endpoint.host_address),
                    node.endpoint.api_port,
                ),
            )
        })
        .collect()
}

fn required_trust_root<'a>(
    settings: &'a server::ServerSettings,
    kind: &str,
) -> Result<&'a str, RoleError> {
    settings
        .cluster_trust_root_pem
        .as_deref()
        .ok_or_else(|| RoleError::new(format!("cluster {kind} proxy has no trust root")))
}

fn required_identity<'a>(
    settings: &'a server::ServerSettings,
    kind: &str,
) -> Result<&'a server::TlsIdentity, RoleError> {
    settings
        .cluster_client_identity
        .as_ref()
        .ok_or_else(|| RoleError::new(format!("cluster {kind} proxy has no TLS identity")))
}

fn required_jwt_secret<'a>(
    settings: &'a server::ServerSettings,
    kind: &str,
) -> Result<&'a kernel_api::SecretValue, RoleError> {
    settings
        .jwt_secret_key
        .as_ref()
        .ok_or_else(|| RoleError::new(format!("cluster {kind} proxy has no JWT secret")))
}
