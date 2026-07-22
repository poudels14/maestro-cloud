use std::net::Ipv4Addr;

use kernel_api::NodeId;
use serde::{Deserialize, Serialize};

use crate::legacy_node_schema::LegacyNodeRole;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyJoinIntent {
    pub(crate) node_id: NodeId,
    pub(crate) role: LegacyNodeRole,
    pub(crate) cluster_host_ip: Ipv4Addr,
    pub(crate) cluster_api_port: u16,
    pub(crate) etcd_peer_port: u16,
    pub(crate) subnet: String,
    pub(crate) public_key_sha256: String,
    pub(crate) member_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyJoinAdmission {
    pub(crate) node_id: NodeId,
    pub(crate) role: LegacyNodeRole,
    pub(crate) cluster_host_ip: Ipv4Addr,
    pub(crate) cluster_api_port: u16,
    pub(crate) subnet: String,
    pub(crate) public_key_sha256: String,
    pub(crate) created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyMembershipHistory {
    pub(crate) node_id: NodeId,
    pub(crate) host_ip: Ipv4Addr,
    pub(crate) role: LegacyNodeRole,
    pub(crate) removed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyRemovedNode {
    pub(crate) node_id: NodeId,
    pub(crate) host_ip: Ipv4Addr,
    pub(crate) role: LegacyNodeRole,
    pub(crate) requested_at_ms: i64,
}
