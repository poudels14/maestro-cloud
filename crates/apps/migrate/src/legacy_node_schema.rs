use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use kernel_api::NodeRole;
use serde::{Deserialize, Serialize};

use crate::legacy_nodes::LegacyNodeError;

pub(crate) const LIVE_PREFIX: &str = "/maetro/cluster/nodes/";
pub(crate) const RECORD_PREFIX: &str = "/maetro/cluster/node-records/";
const STATE_PREFIX: &str = "/maetro/cluster/node-state/";
const SUBNET_PREFIX: &str = "/maetro/cluster/subnets/";
pub(crate) const CONTROL_PREFIX: &str = "/maetro/cluster/control-addresses/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum LegacyNodeRole {
    Hybrid,
    Master,
    Voter,
    Worker,
}

impl LegacyNodeRole {
    pub(crate) const fn is_control_plane(self) -> bool {
        matches!(self, Self::Hybrid | Self::Master | Self::Voter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyNodeInfo {
    pub(crate) node_id: String,
    pub(crate) instance_id: String,
    pub(crate) hostname: String,
    pub(crate) role: LegacyNodeRole,
    pub(crate) cluster_host_ip: Ipv4Addr,
    pub(crate) cluster_api_port: u16,
    #[serde(default = "default_gateway_port")]
    pub(crate) cluster_gateway_port: u16,
    pub(crate) subnet: String,
    pub(crate) tailscale_ip: Option<Ipv4Addr>,
    pub(crate) data_plane_ready: bool,
    pub(crate) data_plane_checked_at_ms: i64,
    pub(crate) data_plane_error: Option<String>,
    pub(crate) version: String,
    pub(crate) started_at_ms: i64,
    pub(crate) labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyNodeRecord {
    pub(crate) last_info: LegacyNodeInfo,
    pub(crate) last_seen_at_ms: i64,
    pub(crate) lost_at_ms: Option<i64>,
    pub(crate) data_plane_lost_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) control_plane_alerted_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) data_plane_alerted_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyNodeState {
    pub(crate) unschedulable: bool,
    pub(crate) drained_at_ms: Option<i64>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacySubnetReservation {
    pub(crate) cidr: String,
    pub(crate) node_id: String,
    pub(crate) state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyControlReservation {
    pub(crate) host_ip: Ipv4Addr,
    pub(crate) api_port: u16,
    pub(crate) gateway_port: u16,
    pub(crate) etcd_client_port: u16,
    pub(crate) etcd_peer_port: u16,
    pub(crate) node_id: Option<String>,
    pub(crate) state: String,
}

const fn default_gateway_port() -> u16 {
    3_002
}

pub(crate) enum NodeKey {
    Live(String),
    Record(String),
    State(String),
    Subnet(String),
    Control(String),
}

pub(crate) fn classify_key(key: &str) -> Result<Option<NodeKey>, LegacyNodeError> {
    for (prefix, constructor) in [
        (RECORD_PREFIX, NodeKey::Record as fn(String) -> NodeKey),
        (LIVE_PREFIX, NodeKey::Live),
        (STATE_PREFIX, NodeKey::State),
        (SUBNET_PREFIX, NodeKey::Subnet),
        (CONTROL_PREFIX, NodeKey::Control),
    ] {
        if let Some(value) = key.strip_prefix(prefix) {
            if value.is_empty() || value.contains('/') {
                return Err(LegacyNodeError::InvalidState {
                    key: key.to_owned(),
                    message: "node-state key is malformed".to_owned(),
                });
            }
            return Ok(Some(constructor(value.to_owned())));
        }
    }
    Ok(None)
}

pub(crate) fn is_hostname(value: &str) -> bool {
    !value.is_empty() && value.len() <= 253 && value.split('.').all(is_dns_label)
}

pub(crate) fn is_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && value
            .bytes()
            .next_back()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

pub(crate) const fn convert_role(role: LegacyNodeRole) -> NodeRole {
    match role {
        LegacyNodeRole::Master => NodeRole::Master,
        LegacyNodeRole::Hybrid => NodeRole::Hybrid,
        LegacyNodeRole::Voter => NodeRole::ControlPlane,
        LegacyNodeRole::Worker => NodeRole::Worker,
    }
}
