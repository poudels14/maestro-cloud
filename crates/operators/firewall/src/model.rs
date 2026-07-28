use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use kernel_api::{
    Assignment, FirewallPolicy, FirewallPolicyId, FirewallPolicyStatus, NodeId, NodeNetwork,
    ResourceRevision, Service, ServiceId,
};

/// Transport supported by one host-to-workload publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HostPortProtocol {
    /// TCP listener.
    Tcp,
    /// UDP listener.
    Udp,
}

/// One daemon-owned host endpoint routed to a local system workload.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HostPortRoute {
    /// System service receiving traffic.
    pub service_id: ServiceId,
    /// Public host port.
    pub host_port: u16,
    /// Destination port inside the workload.
    pub workload_port: u16,
    /// Transport protocol.
    pub protocol: HostPortProtocol,
}

/// One system workload allowed to reach selected protected host listeners.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SystemHostAccess {
    /// System service whose running assignment addresses are trusted.
    pub service_id: ServiceId,
    /// Protected TCP host ports reachable by that service.
    pub host_ports: Vec<u16>,
    /// Exact bridge-only TCP endpoints reachable by that service.
    pub endpoints: Vec<SystemHostEndpoint>,
}

/// One exact host address and TCP port reserved for system-plane access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SystemHostEndpoint {
    /// Bridge address that owns the listener.
    pub address: Ipv4Addr,
    /// TCP listener port.
    pub port: u16,
}

/// Static cluster security settings compiled beside resource policies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirewallSettings {
    /// Owned nftables table replaced by the backend.
    pub table_name: String,
    /// Owned workload bridge whose forwarded traffic enters egress policy.
    pub workload_interface: String,
    /// Bridge-only authoritative DNS listener port.
    pub dns_port: u16,
    /// Host TCP listeners restricted to the control-plane allowlist.
    pub protected_host_ports: Vec<u16>,
    /// Canonical source CIDRs allowed to reach protected host ports.
    pub control_allow_cidrs: Vec<String>,
    /// Ordinary services exempt from user egress policies because they form the system plane.
    pub system_services: BTreeSet<ServiceId>,
    /// Narrow protected-listener grants for selected system workloads.
    pub system_host_access: Vec<SystemHostAccess>,
    /// Public endpoints translated to a ready local system workload.
    pub host_port_routes: Vec<HostPortRoute>,
}

/// Complete typed snapshot consumed by one firewall compilation pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirewallInput {
    /// Static settings and system-plane exemptions.
    pub settings: FirewallSettings,
    /// Desired global, service, and node firewall policies.
    pub policies: Vec<FirewallPolicy>,
    /// Services referenced by scoped policies and assignments.
    pub services: Vec<Service>,
    /// Current workload sources grouped by service and node.
    pub assignments: Vec<Assignment>,
    /// Node workload subnets and bridge gateway addresses.
    pub node_networks: Vec<NodeNetwork>,
}

/// Exact nftables artifact intended for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirewallRuleset {
    /// Node whose local host and forward hooks own this ruleset.
    pub node_id: NodeId,
    /// Owned nftables table name.
    pub table_name: String,
    /// Complete deterministic nftables table definition.
    pub script: String,
    /// SHA-256 digest of `script`.
    pub digest: String,
}

/// Complete immutable artifact handed to the firewall side-effect boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FirewallBundle {
    /// Deterministically ordered complete rulesets for every active node network.
    pub rulesets: Vec<FirewallRuleset>,
    /// SHA-256 digest over the ordered node identities and exact ruleset bytes.
    pub digest: String,
}

/// Optimistic status replacement after the exact bundle is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirewallPolicyStatusUpdate {
    /// Policy receiving an application acknowledgement.
    pub policy_id: FirewallPolicyId,
    /// Store revision from which this update was planned.
    pub observed_revision: ResourceRevision,
    /// Complete desired policy status.
    pub status: FirewallPolicyStatus,
}

/// Desired per-node artifacts and policy acknowledgements from one compilation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FirewallPlan {
    /// Complete node rulesets handed to the backend as one idempotent bundle.
    pub rulesets: Vec<FirewallRuleset>,
    /// Digest of ordered node identity and ruleset bytes for the complete bundle.
    pub bundle_digest: String,
    /// Policies whose generation or applied bundle digest is stale.
    pub policy_updates: Vec<FirewallPolicyStatusUpdate>,
}

impl FirewallPlan {
    /// Copies the exact backend artifact out of this pure plan.
    pub fn bundle(&self) -> FirewallBundle {
        FirewallBundle {
            rulesets: self.rulesets.clone(),
            digest: self.bundle_digest.clone(),
        }
    }
}
