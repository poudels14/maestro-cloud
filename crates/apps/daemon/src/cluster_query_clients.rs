use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use kernel_api::NodeId;
use logs::LogQueryStore;
use metrics::{HostMetricQueryStore, WorkloadMetricQueryStore};

use crate::control_plane::role_error;
use crate::{DaemonPlan, RoleError};

pub(crate) fn log_query_store(
    plan: &DaemonPlan,
    settings: &server::ServerSettings,
    local: Arc<dyn LogQueryStore>,
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
