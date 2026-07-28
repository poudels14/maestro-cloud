use std::net::IpAddr;

use kernel_api::{
    AssignmentId, FirewallDirection, FirewallPolicyId, FirewallSubject, NodeId, ServiceId,
};

use crate::render::{bundle_digest, render};
use crate::validation::validate;
use crate::{FirewallInput, FirewallPlan, FirewallPolicyStatusUpdate};

/// Compiles one deterministic per-node nftables bundle and policy acknowledgement set.
pub fn plan(input: FirewallInput) -> Result<FirewallPlan, FirewallPlanError> {
    let input = validate(input)?;
    let rulesets = render(&input);
    let bundle_digest = bundle_digest(&rulesets);
    let mut policy_updates = input
        .policies
        .values()
        .filter_map(|policy| {
            let current = &policy.resource;
            if current.status.applied_generation == current.meta.generation
                && current.status.ruleset_digest.as_deref() == Some(bundle_digest.as_str())
            {
                return None;
            }
            let mut status = current.status.clone();
            status.applied_generation = current.meta.generation;
            status.ruleset_digest = Some(bundle_digest.clone());
            status.conditions.clear();
            Some(FirewallPolicyStatusUpdate {
                policy_id: current.meta.id.clone(),
                observed_revision: current.meta.revision,
                status,
            })
        })
        .collect::<Vec<_>>();
    policy_updates.sort_by(|left, right| left.policy_id.cmp(&right.policy_id));
    Ok(FirewallPlan {
        rulesets,
        bundle_digest,
        policy_updates,
    })
}

/// Invalid firewall settings, policies, or placement topology.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FirewallPlanError {
    /// The configured nftables table cannot be represented as one identifier.
    #[error("invalid nftables table name `{table_name}`")]
    InvalidTableName { table_name: String },
    /// The configured workload bridge cannot be represented as a Linux interface name.
    #[error("invalid workload interface name `{interface_name}`")]
    InvalidInterfaceName { interface_name: String },
    /// Port zero cannot identify the authoritative DNS listener.
    #[error("firewall DNS port must be greater than zero")]
    ZeroDnsPort,
    /// Port zero cannot identify a protected host listener.
    #[error("protected host ports must be greater than zero")]
    ZeroProtectedHostPort,
    /// Port zero cannot identify a host publication endpoint.
    #[error("host port routes must use nonzero host and workload ports")]
    ZeroHostPortRoute,
    /// Two system routes cannot own the same host endpoint.
    #[error("host port `{port}/{protocol:?}` is routed more than once")]
    DuplicateHostPortRoute {
        port: u16,
        protocol: crate::HostPortProtocol,
    },
    /// Host routing is restricted to declared system services.
    #[error("host port route references non-system Service `{service_id}`")]
    HostPortRouteNotSystem { service_id: ServiceId },
    /// A public workload route cannot bypass a protected control-plane listener.
    #[error("host port route `{port}` conflicts with a protected host port")]
    HostPortRouteConflictsProtected { port: u16 },
    /// A configured or resource CIDR was malformed or noncanonical.
    #[error("invalid CIDR `{value}` at `{field}`: {message}")]
    InvalidCidr {
        field: String,
        value: String,
        message: String,
    },
    /// One typed identity occurred more than once in the input snapshot.
    #[error("{kind} `{resource_id}` occurs more than once in one firewall snapshot")]
    DuplicateResource {
        kind: &'static str,
        resource_id: String,
    },
    /// Two node-network resources claimed the same node.
    #[error("node `{node_id}` has more than one active NodeNetwork")]
    DuplicateNodeNetwork { node_id: NodeId },
    /// At least one active policy exists before any node network is available.
    #[error("active firewall policies require at least one NodeNetwork")]
    NoNodeNetworks,
    /// Workload networking currently requires one IPv4 subnet per node.
    #[error("node `{node_id}` workload subnet `{subnet}` is not IPv4")]
    UnsupportedWorkloadSubnet { node_id: NodeId, subnet: String },
    /// A subnet had no usable first host address for the bridge and DNS listener.
    #[error("node `{node_id}` workload subnet `{subnet}` has no bridge address")]
    WorkloadSubnetHasNoBridge { node_id: NodeId, subnet: String },
    /// A subnet had no usable fixed Admin address.
    #[error("node `{node_id}` workload subnet `{subnet}` has no reserved Admin address")]
    WorkloadSubnetHasNoAdmin { node_id: NodeId, subnet: String },
    /// A system-plane exemption references a service absent from the snapshot.
    #[error("system firewall exemption references missing Service `{service_id}`")]
    MissingSystemService { service_id: ServiceId },
    /// Protected host access was granted to an ordinary workload.
    #[error("protected host access references non-system Service `{service_id}`")]
    SystemHostAccessNotSystem { service_id: ServiceId },
    /// Protected host access omitted every destination port.
    #[error("protected host access for Service `{service_id}` must include at least one port")]
    EmptySystemHostAccess { service_id: ServiceId },
    /// Port zero cannot identify a protected host destination.
    #[error("protected host access for Service `{service_id}` contains port zero")]
    ZeroSystemHostAccessPort { service_id: ServiceId },
    /// A system workload may only be granted access to an already protected listener.
    #[error("protected host access for Service `{service_id}` references unprotected port {port}")]
    SystemHostAccessPortNotProtected { service_id: ServiceId, port: u16 },
    /// The same protected host grant was declared more than once.
    #[error("protected host access for Service `{service_id}` repeats port {port}")]
    DuplicateSystemHostAccess { service_id: ServiceId, port: u16 },
    /// Port zero cannot identify an exact system host destination.
    #[error(
        "protected host access for Service `{service_id}` contains endpoint `{address}` with port zero"
    )]
    ZeroSystemHostEndpointPort {
        service_id: ServiceId,
        address: std::net::Ipv4Addr,
    },
    /// The same exact system-host grant was declared more than once.
    #[error("protected host access for Service `{service_id}` repeats endpoint `{address}:{port}`")]
    DuplicateSystemHostEndpoint {
        service_id: ServiceId,
        address: std::net::Ipv4Addr,
        port: u16,
    },
    /// An assignment references a service absent from the snapshot.
    #[error("Assignment `{assignment_id}` references missing Service `{service_id}`")]
    MissingAssignmentService {
        assignment_id: AssignmentId,
        service_id: ServiceId,
    },
    /// An assignment references a node without a live network resource.
    #[error("Assignment `{assignment_id}` references node `{node_id}` without a NodeNetwork")]
    MissingAssignmentNode {
        assignment_id: AssignmentId,
        node_id: NodeId,
    },
    /// A cluster-routed assignment omitted its scheduler-owned source address.
    #[error("Assignment `{assignment_id}` on routed node `{node_id}` has no workload address")]
    MissingAssignmentAddress {
        assignment_id: AssignmentId,
        node_id: NodeId,
    },
    /// An assignment source address was not owned by its selected node subnet.
    #[error("Assignment `{assignment_id}` address `{address}` is outside node subnet `{subnet}`")]
    AssignmentAddressOutsideSubnet {
        assignment_id: AssignmentId,
        address: IpAddr,
        subnet: String,
    },
    /// Policy direction and subject variants cannot be enforced together.
    #[error(
        "FirewallPolicy `{policy_id}` has incompatible direction {direction:?} and subject {subject:?}"
    )]
    InvalidPolicySubject {
        policy_id: FirewallPolicyId,
        direction: FirewallDirection,
        subject: FirewallSubject,
    },
    /// A service-scoped policy references a service absent from the snapshot.
    #[error("firewall policy references missing Service `{service_id}`")]
    MissingPolicyService { service_id: ServiceId },
    /// A node-scoped policy references a node without a live network resource.
    #[error("firewall policy references node `{node_id}` without a NodeNetwork")]
    MissingPolicyNode { node_id: NodeId },
    /// More than one active policy claimed an exact direction and subject.
    #[error("FirewallPolicies `{first_policy_id}` and `{second_policy_id}` claim the same scope")]
    DuplicatePolicyScope {
        first_policy_id: FirewallPolicyId,
        second_policy_id: FirewallPolicyId,
    },
    /// A transport port range was reversed or contained port zero.
    #[error("FirewallPolicy `{policy_id}` rule {rule_index} has invalid port range {start}-{end}")]
    InvalidPortRange {
        policy_id: FirewallPolicyId,
        rule_index: usize,
        start: u16,
        end: u16,
    },
}
