use std::collections::BTreeSet;

use kernel_api::{
    Assignment, FirewallPolicy, FirewallPolicyId, FirewallPolicyStatus, NodeId, NodeNetwork,
    ResourceRevision, Service, ServiceId,
};

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
