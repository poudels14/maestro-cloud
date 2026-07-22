use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use kernel_api::{AnnotationKey, BuiltinResource, ClusterId, NodeRole};
use serde::{Deserialize, Serialize};

use crate::LegacyEntry;
use crate::legacy_convert::LegacyPlanError;
use crate::legacy_nodes::{LegacyControlEndpoint, LegacyNodeCatalog};

const CLUSTER_META_KEY: &str = "/maetro/system/cluster-meta";
const CLUSTER_META_ANNOTATION: &str = "migration.maestro.dev/legacy-cluster-meta";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyClusterIdentity {
    cluster_id: ClusterId,
    meta: LegacyClusterMeta,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyClusterIdentity {
    pub(crate) fn decode(
        entries: &[LegacyEntry],
        nodes: &LegacyNodeCatalog,
    ) -> Result<Self, LegacyIdentityError> {
        let mut meta = None;
        let mut unclaimed = Vec::new();
        for entry in entries {
            if entry.key() == CLUSTER_META_KEY {
                meta = Some(decode_json(entry)?);
            } else {
                unclaimed.push(entry.clone());
            }
        }
        let meta: LegacyClusterMeta = meta.ok_or(LegacyIdentityError::MissingClusterMeta)?;
        let cluster_id = validate_meta(&meta, nodes)?;
        Ok(Self {
            cluster_id,
            meta,
            unclaimed,
        })
    }

    pub(crate) const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub(crate) fn annotate_master(
        &self,
        resources: &mut [BuiltinResource],
    ) -> Result<(), LegacyPlanError> {
        let value = serde_json::to_string(&self.meta).map_err(|error| {
            LegacyPlanError::InvalidClusterState {
                resource_id: self.cluster_id.to_string(),
                message: format!("could not preserve legacy cluster metadata: {error}"),
            }
        })?;
        let master = resources.iter_mut().find_map(|resource| match resource {
            BuiltinResource::Node(node)
                if node.spec.role == NodeRole::Master
                    && node.spec.host_address == self.meta.bootstrap_host_ip =>
            {
                Some(node)
            }
            _ => None,
        });
        let master = master.ok_or_else(|| LegacyPlanError::InvalidClusterState {
            resource_id: self.cluster_id.to_string(),
            message: "converted master node is missing".to_owned(),
        })?;
        master
            .meta
            .annotations
            .insert(AnnotationKey(CLUSTER_META_ANNOTATION.to_owned()), value);
        Ok(())
    }
}

fn validate_meta(
    meta: &LegacyClusterMeta,
    nodes: &LegacyNodeCatalog,
) -> Result<ClusterId, LegacyIdentityError> {
    if meta.cluster_id.len() != 32
        || !meta
            .cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            "clusterId must be 32 lowercase hexadecimal characters",
        ));
    }
    let cluster_id = ClusterId::new(&meta.cluster_id)
        .map_err(|error| invalid(format!("clusterId is invalid: {error}")))?;
    if meta.name.trim().is_empty() || meta.name.len() > 256 || meta.name.trim() != meta.name {
        return Err(invalid(
            "cluster display name is empty, oversized, or padded",
        ));
    }
    if !valid_host(meta.bootstrap_host_ip) {
        return Err(invalid(
            "bootstrap host address must be private and routable",
        ));
    }
    if nodes.master_host() != Some(meta.bootstrap_host_ip) {
        return Err(invalid(
            "bootstrap host does not identify the migrated master node",
        ));
    }
    let endpoints = initial_endpoints(meta)?;
    if meta.initial_voter_host_ips.first() != Some(&meta.bootstrap_host_ip) {
        return Err(invalid("bootstrap host must be the first initial voter"));
    }
    let host_ips = meta
        .initial_voter_host_ips
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if !matches!(host_ips.len(), 1 | 3) || host_ips.len() != meta.initial_voter_host_ips.len() {
        return Err(invalid(
            "initial voters must contain one or three unique host addresses",
        ));
    }
    let endpoint_hosts = endpoints
        .iter()
        .map(|endpoint| endpoint.host_ip)
        .collect::<BTreeSet<_>>();
    if endpoint_hosts != host_ips || endpoint_hosts.len() != endpoints.len() {
        return Err(invalid(
            "initial voter endpoint and host-address sets disagree",
        ));
    }
    for endpoint in endpoints {
        validate_endpoint(endpoint)?;
        if let Some(current) = nodes.control_endpoint(endpoint.host_ip)
            && current != endpoint.into()
        {
            return Err(invalid(format!(
                "initial endpoint for {} disagrees with its active reservation",
                endpoint.host_ip
            )));
        }
    }
    Ok(cluster_id)
}

fn initial_endpoints(
    meta: &LegacyClusterMeta,
) -> Result<Vec<LegacyClusterNodeEndpoint>, LegacyIdentityError> {
    if meta.initial_voter_endpoints.is_empty() {
        Ok(meta
            .initial_voter_host_ips
            .iter()
            .copied()
            .map(LegacyClusterNodeEndpoint::with_default_ports)
            .collect())
    } else if meta.initial_voter_endpoints.len() == meta.initial_voter_host_ips.len() {
        Ok(meta.initial_voter_endpoints.clone())
    } else {
        Err(invalid(
            "initial voter endpoint and host-address counts disagree",
        ))
    }
}

fn validate_endpoint(endpoint: LegacyClusterNodeEndpoint) -> Result<(), LegacyIdentityError> {
    let ports = [
        endpoint.api_port,
        endpoint.gateway_port,
        endpoint.etcd_client_port,
        endpoint.etcd_peer_port,
    ];
    if !valid_host(endpoint.host_ip)
        || ports.contains(&0)
        || ports.into_iter().collect::<BTreeSet<_>>().len() != ports.len()
    {
        Err(invalid(format!(
            "initial endpoint {} has an invalid address or port allocation",
            endpoint.host_ip
        )))
    } else {
        Ok(())
    }
}

fn valid_host(address: Ipv4Addr) -> bool {
    address.is_private() && !address.is_loopback() && !address.is_unspecified()
}

fn decode_json<Value: serde::de::DeserializeOwned>(
    entry: &LegacyEntry,
) -> Result<Value, LegacyIdentityError> {
    serde_json::from_slice(entry.value()).map_err(|error| invalid(format!("invalid JSON: {error}")))
}

fn invalid(message: impl Into<String>) -> LegacyIdentityError {
    LegacyIdentityError::InvalidClusterMeta {
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyIdentityError {
    #[error("legacy cluster metadata is missing")]
    MissingClusterMeta,
    #[error("legacy cluster metadata is invalid: {message}")]
    InvalidClusterMeta { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LegacyClusterMeta {
    cluster_id: String,
    name: String,
    bootstrap_host_ip: Ipv4Addr,
    initial_voter_host_ips: Vec<Ipv4Addr>,
    #[serde(default)]
    initial_voter_endpoints: Vec<LegacyClusterNodeEndpoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LegacyClusterNodeEndpoint {
    host_ip: Ipv4Addr,
    api_port: u16,
    gateway_port: u16,
    etcd_client_port: u16,
    etcd_peer_port: u16,
}

impl LegacyClusterNodeEndpoint {
    const fn with_default_ports(host_ip: Ipv4Addr) -> Self {
        Self {
            host_ip,
            api_port: 3_000,
            gateway_port: 3_002,
            etcd_client_port: 2_379,
            etcd_peer_port: 2_380,
        }
    }
}

impl From<LegacyClusterNodeEndpoint> for LegacyControlEndpoint {
    fn from(endpoint: LegacyClusterNodeEndpoint) -> Self {
        Self {
            host_ip: endpoint.host_ip,
            api_port: endpoint.api_port,
            gateway_port: endpoint.gateway_port,
            etcd_client_port: endpoint.etcd_client_port,
            etcd_peer_port: endpoint.etcd_peer_port,
        }
    }
}
