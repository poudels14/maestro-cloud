use std::fmt;

use serde::{Deserialize, Serialize};

pub type NodeId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeRole {
    Controller,
    Worker,
    Both,
}

impl Default for NodeRole {
    fn default() -> Self {
        NodeRole::Both
    }
}

impl fmt::Display for NodeRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Controller => write!(formatter, "controller"),
            NodeRole::Worker => write!(formatter, "worker"),
            NodeRole::Both => write!(formatter, "both"),
        }
    }
}

impl std::str::FromStr for NodeRole {
    type Err = String;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        match input {
            "controller" => Ok(NodeRole::Controller),
            "worker" => Ok(NodeRole::Worker),
            "both" => Ok(NodeRole::Both),
            other => Err(format!(
                "unsupported node role: {other} (use 'controller', 'worker', or 'both')"
            )),
        }
    }
}

impl NodeRole {
    pub fn can_lead(&self) -> bool {
        matches!(self, NodeRole::Controller | NodeRole::Both)
    }

    pub fn can_run_workloads(&self) -> bool {
        matches!(self, NodeRole::Worker | NodeRole::Both)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub hostname: String,
    pub role: NodeRole,
    pub tailscale_ip: Option<String>,
    pub api_port: u16,
    pub version: String,
    pub started_at_ms: u64,
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    /// Set during `drain` so the scheduler stops placing new replicas here.
    /// Cleared on `restore`. Persisted in the node registry alongside the
    /// rest of NodeInfo.
    #[serde(default)]
    pub unschedulable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LeaderInfo {
    pub node_id: NodeId,
    pub elected_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipState {
    Following(Option<LeaderInfo>),
    Leading(LeaderInfo),
    Unknown,
}

impl LeadershipState {
    pub fn leader(&self) -> Option<&LeaderInfo> {
        match self {
            LeadershipState::Following(Some(info)) | LeadershipState::Leading(info) => Some(info),
            LeadershipState::Following(None) | LeadershipState::Unknown => None,
        }
    }

    pub fn is_leader(&self) -> bool {
        matches!(self, LeadershipState::Leading(_))
    }
}
