use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    AssignmentId, Condition, DeploymentId, DnsRecordId, FirewallPolicyId, Generation,
    IngressRouteId, NodeId, Object, ServiceId, TrafficGenerationId,
};

/// Optional header-based affinity applied by ingress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionAffinity {
    /// HTTP header carrying the opaque affinity token.
    pub header: String,
}

/// Desired host routing for one service endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IngressRouteSpec {
    /// Service receiving matching requests.
    pub service_id: ServiceId,
    /// Canonical hostnames accepted by the route.
    pub hosts: Vec<String>,
    /// Optional path prefix required after the host matches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
    /// Workload port receiving requests.
    pub target_port: u16,
    /// Optional opaque header affinity policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_affinity: Option<SessionAffinity>,
}

/// Observed route publication state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IngressRouteStatus {
    /// Route generation most recently published by ingress.
    pub applied_generation: Generation,
    /// Generic publication and validation evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// An ingress route resource.
pub type IngressRoute = Object<IngressRouteId, IngressRouteSpec, IngressRouteStatus>;

/// One cluster-routable workload selected for ingress traffic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrafficTarget {
    /// Assignment owning this target.
    pub assignment_id: AssignmentId,
    /// Workload address and ingress port dialed directly over the mesh.
    pub endpoint: SocketAddr,
}

/// Desired immutable target set staged for an ingress cutover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrafficGenerationSpec {
    /// Service whose traffic is changing.
    pub service_id: ServiceId,
    /// Deployment receiving traffic in this generation.
    pub deployment_id: DeploymentId,
    /// Routes published with the target set.
    pub route_ids: Vec<IngressRouteId>,
    /// Ready workload targets included in the generation.
    pub targets: Vec<TrafficTarget>,
}

/// Persisted cutover lifecycle of a traffic generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TrafficGenerationPhase {
    /// Configuration is prepared but does not yet receive traffic.
    Staged,
    /// Configuration is published and receives traffic.
    Active,
    /// Configuration is no longer referenced and may be collected.
    Retired,
}

impl TrafficGenerationPhase {
    /// Whether blue/green cutover permits this phase transition.
    pub fn can_transition_to(self, target: Self) -> bool {
        self == target
            || matches!(
                (self, target),
                (Self::Staged, Self::Active) | (Self::Active, Self::Retired)
            )
    }
}

/// Observed publication and garbage-collection state for one generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrafficGenerationStatus {
    /// Current cutover phase.
    pub phase: TrafficGenerationPhase,
    /// Generic readiness and collection evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// An ingress traffic generation resource.
pub type TrafficGeneration =
    Object<TrafficGenerationId, TrafficGenerationSpec, TrafficGenerationStatus>;

/// Network direction governed by a firewall policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FirewallDirection {
    /// Traffic initiated by workloads.
    Egress,
    /// Traffic entering a node's protected host plane.
    HostInput,
}

/// Resource scope that a firewall policy governs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "id", rename_all = "camelCase")]
pub enum FirewallSubject {
    /// Every workload or node, depending on direction.
    Global,
    /// Workloads belonging to one service.
    Service(ServiceId),
    /// Host-input traffic for one node.
    Node(NodeId),
}

/// Transport protocol selected by a firewall rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TransportProtocol {
    /// Transmission Control Protocol.
    Tcp,
    /// User Datagram Protocol.
    Udp,
    /// Any transport protocol.
    Any,
}

/// Inclusive transport port range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PortRange {
    /// First port included by the range.
    pub start: u16,
    /// Last port included by the range.
    pub end: u16,
}

/// Action applied when a firewall rule matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FirewallVerdict {
    /// Permit matching traffic.
    Allow,
    /// Reject matching traffic.
    Deny,
}

/// One ordered firewall match and action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FirewallRule {
    /// Canonical destination or source CIDR, depending on policy direction.
    pub cidr: String,
    /// Transport protocol to match.
    pub protocol: TransportProtocol,
    /// Port ranges to match, or every port when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortRange>,
    /// Action applied to matching traffic.
    pub verdict: FirewallVerdict,
}

/// Desired ordered firewall policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FirewallPolicySpec {
    /// Network direction governed by the policy.
    pub direction: FirewallDirection,
    /// Resource scope governed by the policy.
    pub subject: FirewallSubject,
    /// Ordered rules evaluated before the default verdict.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<FirewallRule>,
    /// Verdict when no rule matches.
    pub default_verdict: FirewallVerdict,
}

/// Observed atomic application state of a firewall policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FirewallPolicyStatus {
    /// Desired generation most recently applied to the backend.
    pub applied_generation: Generation,
    /// Hash of the exact ruleset handed to the firewall backend.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ruleset_digest: Option<String>,
    /// Generic validation and application evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A firewall policy resource.
pub type FirewallPolicy = Object<FirewallPolicyId, FirewallPolicySpec, FirewallPolicyStatus>;

/// A typed value stored in one authoritative DNS record set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "camelCase")]
pub enum DnsRecordValue {
    /// IPv4 address record.
    A(Ipv4Addr),
    /// IPv6 address record.
    Aaaa(Ipv6Addr),
    /// Canonical-name target.
    Cname(String),
    /// Arbitrary text record.
    Txt(String),
    /// Service location record.
    Srv {
        /// Target selection priority.
        priority: u16,
        /// Relative target weight within one priority.
        weight: u16,
        /// Target service port.
        port: u16,
        /// Canonical target hostname.
        target: String,
    },
}

/// Desired authoritative DNS record set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DnsRecordSpec {
    /// Fully qualified record name.
    pub name: String,
    /// Homogeneous values published for the name.
    pub values: Vec<DnsRecordValue>,
    /// DNS cache lifetime.
    pub ttl_secs: u32,
}

/// Observed publication state of a DNS record set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DnsRecordStatus {
    /// Desired generation most recently loaded by node resolvers.
    pub applied_generation: Generation,
    /// Nodes that acknowledged this record generation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub published_nodes: Vec<NodeId>,
    /// Generic validation and publication evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// An authoritative cluster DNS record resource.
pub type DnsRecord = Object<DnsRecordId, DnsRecordSpec, DnsRecordStatus>;

/// A validated address used by firewall dry-run request types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NetworkAddress(pub IpAddr);
