use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::ids::{ReplicaId, ServiceId};

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
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Backend {
    pub replica_id: ReplicaId,
    pub address: SocketAddr,
}
