use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::ids::{ReplicaId, ServiceId};

const INTERNAL_SUFFIX: &str = "maestro.internal";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteTable {
    pub routes: BTreeMap<ServiceId, ServiceRoute>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRoute {
    pub hosts: Vec<String>,
    pub backends: Vec<Backend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Backend {
    pub replica_id: ReplicaId,
    pub address: SocketAddr,
}

pub fn internal_names(cluster: &str, label: &str) -> [String; 2] {
    [
        format!("{label}.{INTERNAL_SUFFIX}"),
        format!("{label}.{cluster}.{INTERNAL_SUFFIX}"),
    ]
}
