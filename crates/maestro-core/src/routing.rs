use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ids::{ReplicaId, ServiceId};

const INTERNAL_SUFFIX: &str = "maestro.internal";
const AFFINITY_TOKEN_DOMAIN: &[u8] = b"maestro-affinity-token\0";
const AFFINITY_TOKEN_LENGTH: usize = 32;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteTable {
    pub routes: BTreeMap<ServiceId, ServiceRoute>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_ips: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRoute {
    pub hosts: Vec<String>,
    pub backends: Vec<Backend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_header: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Backend {
    pub replica_id: ReplicaId,
    pub address: SocketAddr,
}

pub fn affinity_token(replica_id: &ReplicaId) -> String {
    let digest = Sha256::new()
        .chain_update(AFFINITY_TOKEN_DOMAIN)
        .chain_update(replica_id.as_str())
        .finalize();
    hex::encode(digest)[..AFFINITY_TOKEN_LENGTH].to_string()
}

pub fn internal_names(cluster: &str, label: &str) -> [String; 2] {
    [
        format!("{label}.{INTERNAL_SUFFIX}"),
        format!("{label}.{cluster}.{INTERNAL_SUFFIX}"),
    ]
}
