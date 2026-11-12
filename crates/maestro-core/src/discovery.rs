use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ids::{DeploymentId, NodeId, ReplicaId, ServiceId};

pub const DISCOVERY_PORT: u16 = 7480;
pub const DISCOVERY_HOST_ENV: &str = "MAESTRO_API_HOST";
pub const DISCOVERY_KEY_ENV: &str = "MAESTRO_API_KEY";

/// What every agent needs to answer service-discovery requests from its node's containers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryTable {
    pub services: BTreeMap<ServiceId, Vec<DiscoveredReplica>>,
    /// The hash of every service's API key; a caller is the service owning its container IP and
    /// must present that service's key.
    pub key_hashes: BTreeMap<ServiceId, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredReplica {
    pub replica_id: ReplicaId,
    pub deployment_id: DeploymentId,
    pub version: Option<String>,
    pub host: String,
    pub ip: IpAddr,
    pub port: Option<u16>,
    pub healthy: bool,
    pub node_id: NodeId,
}

pub fn discovery_key_hash(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}
