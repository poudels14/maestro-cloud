use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddrV4};

use serde::{Deserialize, Serialize};

pub type NodeId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterNodeEndpoint {
    pub host_ip: Ipv4Addr,
    pub api_port: u16,
    pub gateway_port: u16,
    pub etcd_client_port: u16,
    pub etcd_peer_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_api_port: Option<u16>,
}

impl ClusterNodeEndpoint {
    pub fn api_address(self) -> SocketAddrV4 {
        SocketAddrV4::new(self.host_ip, self.api_port)
    }

    pub fn client_url(self) -> String {
        format!("https://{}:{}", self.host_ip, self.etcd_client_port)
    }

    pub fn peer_url(self) -> String {
        format!("https://{}:{}", self.host_ip, self.etcd_peer_port)
    }

    pub fn identity_suffix(self) -> String {
        match self.identity_api_port {
            Some(port) => format!("{:08x}-{port:04x}", u32::from(self.host_ip)),
            None => format!("{:08x}", u32::from(self.host_ip)),
        }
    }

    pub fn member_name(self) -> String {
        format!("maestro-{}", self.identity_suffix())
    }
}

impl From<Ipv4Addr> for ClusterNodeEndpoint {
    fn from(host_ip: Ipv4Addr) -> Self {
        Self {
            host_ip,
            api_port: 3001,
            gateway_port: 3002,
            etcd_client_port: 2379,
            etcd_peer_port: 2380,
            identity_api_port: None,
        }
    }
}

impl std::str::FromStr for ClusterNodeEndpoint {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Ok(address) = value.parse::<SocketAddrV4>() {
            let api_port = address.port();
            return Ok(Self {
                host_ip: *address.ip(),
                api_port,
                gateway_port: api_port
                    .checked_add(1)
                    .ok_or_else(|| "node API port is too high".to_string())?,
                etcd_client_port: api_port
                    .checked_add(2)
                    .ok_or_else(|| "node API port is too high".to_string())?,
                etcd_peer_port: api_port
                    .checked_add(3)
                    .ok_or_else(|| "node API port is too high".to_string())?,
                identity_api_port: Some(api_port),
            });
        }
        value
            .parse::<Ipv4Addr>()
            .map(Into::into)
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaderInfo {
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipToken {
    pub info: LeaderInfo,
    pub election_key: Vec<u8>,
    pub create_revision: i64,
    pub lease_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipState {
    Leading(LeadershipToken),
    Following(Option<LeaderInfo>),
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    #[default]
    Hybrid,
    Master,
    Voter,
    Worker,
}

impl NodeRole {
    pub fn is_voter(self) -> bool {
        matches!(self, Self::Hybrid | Self::Master | Self::Voter)
    }

    pub fn runs_workloads(self) -> bool {
        matches!(self, Self::Hybrid | Self::Master | Self::Worker)
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Hybrid => "hybrid",
            Self::Master => "master",
            Self::Voter => "voter",
            Self::Worker => "worker",
        })
    }
}

impl std::str::FromStr for NodeRole {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "hybrid" => Ok(Self::Hybrid),
            "master" => Ok(Self::Master),
            "voter" => Ok(Self::Voter),
            "worker" => Ok(Self::Worker),
            _ => Err(format!(
                "invalid node role `{value}`; expected master, hybrid, voter, or worker"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRuntime {
    pub cluster_id: String,
    pub node_id: NodeId,
    pub instance_id: String,
    pub host_ip: Ipv4Addr,
    pub role: NodeRole,
    pub initial_voters: Vec<ClusterNodeEndpoint>,
    pub voter_endpoints: Vec<ClusterNodeEndpoint>,
    pub subnet: String,
    pub control_allow_cidrs: Vec<String>,
    pub api_port: u16,
    pub gateway_port: u16,
    pub etcd_client_port: u16,
    pub etcd_peer_port: u16,
    pub shared_registry: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub identity_api_port: Option<u16>,
}

impl ClusterRuntime {
    pub fn local_endpoint(&self) -> ClusterNodeEndpoint {
        ClusterNodeEndpoint {
            host_ip: self.host_ip,
            api_port: self.api_port,
            gateway_port: self.gateway_port,
            etcd_client_port: self.etcd_client_port,
            etcd_peer_port: self.etcd_peer_port,
            identity_api_port: self.identity_api_port,
        }
    }

    pub fn is_seed(&self) -> bool {
        self.role == NodeRole::Master
    }

    pub fn client_endpoints(&self) -> Vec<String> {
        self.voter_endpoints
            .iter()
            .map(|node| node.client_url())
            .collect()
    }

    pub fn peer_url(&self) -> String {
        self.local_endpoint().peer_url()
    }

    pub fn member_name(&self) -> String {
        self.local_endpoint().member_name()
    }

    pub fn resource_suffix(&self) -> Option<String> {
        self.identity_api_port.map(|port| format!("node-{port}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMeta {
    pub cluster_id: String,
    pub name: String,
    pub bootstrap_host_ip: Ipv4Addr,
    pub initial_voter_host_ips: Vec<Ipv4Addr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initial_voter_endpoints: Vec<ClusterNodeEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub instance_id: String,
    pub hostname: String,
    pub role: NodeRole,
    pub cluster_host_ip: Ipv4Addr,
    pub cluster_api_port: u16,
    #[serde(default = "default_cluster_gateway_port")]
    pub cluster_gateway_port: u16,
    pub subnet: String,
    pub tailscale_ip: Option<Ipv4Addr>,
    pub data_plane_ready: bool,
    pub data_plane_checked_at_ms: i64,
    pub data_plane_error: Option<String>,
    pub version: String,
    pub started_at_ms: i64,
    pub labels: BTreeMap<String, String>,
}

fn default_cluster_gateway_port() -> u16 {
    3002
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRecord {
    pub last_info: NodeInfo,
    pub last_seen_at_ms: i64,
    pub lost_at_ms: Option<i64>,
    pub data_plane_lost_at_ms: Option<i64>,
    #[serde(default)]
    pub control_plane_alerted_at_ms: Option<i64>,
    #[serde(default)]
    pub data_plane_alerted_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeState {
    pub unschedulable: bool,
    pub drained_at_ms: Option<i64>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterFreeze {
    pub reason: String,
    pub upgrade_run_id: String,
    pub at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterMaintenanceKind {
    #[default]
    Upgrade,
    Restart,
}

impl std::fmt::Display for ClusterMaintenanceKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Upgrade => "upgrade",
            Self::Restart => "restart",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpgradePhase {
    Draining,
    AwaitingLeadershipTransfer,
    UpgradeRequested,
    SelfRestartPending,
    Verifying,
    Restoring,
    Succeeded,
    Failed,
}

impl UpgradePhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpgradeNodeStatus {
    Pending,
    Draining,
    Upgrading,
    Verifying,
    Restoring,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeNodeStep {
    pub node_id: NodeId,
    pub hostname: String,
    pub role: NodeRole,
    pub from_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_instance_id: Option<String>,
    pub status: UpgradeNodeStatus,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    #[serde(default)]
    pub upgrade_started_at_ms: Option<i64>,
    #[serde(default)]
    pub last_upgrade_request_at_ms: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeEvent {
    pub at_ms: i64,
    pub phase: UpgradePhase,
    pub node_id: Option<NodeId>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeRun {
    pub run_id: String,
    #[serde(default)]
    pub kind: ClusterMaintenanceKind,
    pub target_version: String,
    pub requested_at_ms: i64,
    pub updated_at_ms: i64,
    pub requested_by_node_id: NodeId,
    pub phase: UpgradePhase,
    pub phase_started_at_ms: i64,
    pub current_node_index: usize,
    pub nodes: Vec<UpgradeNodeStep>,
    pub history: Vec<UpgradeEvent>,
    pub failure: Option<String>,
}

impl UpgradeRun {
    pub fn current_node(&self) -> Option<&UpgradeNodeStep> {
        self.nodes.get(self.current_node_index)
    }

    pub fn current_node_mut(&mut self) -> Option<&mut UpgradeNodeStep> {
        self.nodes.get_mut(self.current_node_index)
    }

    pub fn operation_name(&self) -> &'static str {
        match self.kind {
            ClusterMaintenanceKind::Upgrade => "upgrade",
            ClusterMaintenanceKind::Restart => "restart",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct NodeAffinity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub assignment_id: String,
    pub placement_epoch: u64,
    pub service_id: String,
    pub deployment_id: String,
    pub replica_index: u32,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_ip: Option<Ipv4Addr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaces_assignment_id: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentManifest {
    pub node_id: NodeId,
    pub generation: u64,
    #[serde(default)]
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaEndpoint {
    pub container_ip: String,
    pub container_hostname: String,
    pub ingress_container_port: u16,
    pub gateway: NodeGatewayEndpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGatewayEndpoint {
    pub host_ip: Ipv4Addr,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentGroup {
    pub deployment_id: String,
    pub replicas: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceScheduleSpec {
    pub service_id: String,
    pub groups: Vec<DeploymentGroup>,
    pub node_affinity: Option<NodeAffinity>,
    pub unhealthy_slots: BTreeSet<(String, NodeId, u32, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnschedulableReplica {
    pub service_id: String,
    pub deployment_id: String,
    pub replica_index: u32,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulePlan {
    pub assignments: Vec<Assignment>,
    pub unschedulable: Vec<UnschedulableReplica>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlacementHistory {
    pub assignment_id: String,
    pub service_id: String,
    pub deployment_id: String,
    pub replica_index: u32,
    pub node_id: NodeId,
    pub cluster_host_ip: String,
    pub cluster_api_port: u16,
    pub container_hostname: String,
    pub started_at_ms: i64,
    pub ended_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDiskInfo {
    pub name: String,
    pub mount_point: String,
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub file_system: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficGeneration {
    pub service_id: String,
    pub deployment_id: String,
    pub traffic_epoch: u64,
    pub active_assignment_ids: Vec<String>,
    #[serde(default)]
    pub active_node_ids: Vec<NodeId>,
    pub generation: String,
    #[serde(default)]
    pub routing_fingerprint: String,
    pub switched_at_ms: i64,
    pub drain_old_after_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DnsRecordSet {
    pub service_id: String,
    pub stable_fqdn: String,
    #[serde(default)]
    pub via_ingress: bool,
    pub addresses: Vec<String>,
    pub replica_records: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraefikServiceIdentity {
    pub service_id: String,
    pub deployment_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
}
