use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use kernel_api::{
    Assignment, FirewallDirection, FirewallPolicy, FirewallRule, FirewallSubject, FirewallVerdict,
    NodeId, PortRange, ServiceId, TransportProtocol,
};

use crate::cidr::{AddressFamily, CanonicalCidr};
use crate::{FirewallInput, FirewallPlanError, FirewallSettings};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SubjectKey {
    EgressGlobal,
    EgressService(ServiceId),
    HostGlobal,
    HostNode(NodeId),
}

pub(crate) struct ValidatedInput {
    pub(crate) settings: FirewallSettings,
    pub(crate) control_cidrs: Vec<CanonicalCidr>,
    pub(crate) policies: BTreeMap<SubjectKey, ValidatedPolicy>,
    pub(crate) assignments: Vec<Assignment>,
    pub(crate) nodes: BTreeMap<NodeId, NodeContext>,
}

pub(crate) struct NodeContext {
    pub(crate) node_id: NodeId,
    pub(crate) workload_subnet: CanonicalCidr,
    pub(crate) bridge_address: Ipv4Addr,
    pub(crate) admin_address: Ipv4Addr,
}

pub(crate) struct ValidatedPolicy {
    pub(crate) resource: FirewallPolicy,
    pub(crate) rules: Vec<ValidatedRule>,
}

pub(crate) struct ValidatedRule {
    pub(crate) cidr: CanonicalCidr,
    pub(crate) protocol: TransportProtocol,
    pub(crate) ports: Vec<PortRange>,
    pub(crate) verdict: FirewallVerdict,
}

pub(crate) fn validate(input: FirewallInput) -> Result<ValidatedInput, FirewallPlanError> {
    let mut settings = input.settings;
    validate_settings(&mut settings)?;
    let services = index_services(input.services)?;
    for service_id in &settings.system_services {
        if !services.contains(service_id) {
            return Err(FirewallPlanError::MissingSystemService {
                service_id: service_id.clone(),
            });
        }
    }
    for route in &settings.host_port_routes {
        if !settings.system_services.contains(&route.service_id) {
            return Err(FirewallPlanError::HostPortRouteNotSystem {
                service_id: route.service_id.clone(),
            });
        }
    }
    for access in &settings.system_host_access {
        if !settings.system_services.contains(&access.service_id) {
            return Err(FirewallPlanError::SystemHostAccessNotSystem {
                service_id: access.service_id.clone(),
            });
        }
    }
    let nodes = index_nodes(input.node_networks)?;
    let assignments = validate_assignments(input.assignments, &services, &nodes)?;
    let control_cidrs = settings
        .control_allow_cidrs
        .iter()
        .map(|cidr| CanonicalCidr::parse(cidr, "controlAllowCidrs"))
        .collect::<Result<Vec<_>, _>>()?;
    let policies = index_policies(input.policies, &services, &nodes)?;
    if !policies.is_empty() && nodes.is_empty() {
        return Err(FirewallPlanError::NoNodeNetworks);
    }
    Ok(ValidatedInput {
        settings,
        control_cidrs,
        policies,
        assignments,
        nodes,
    })
}

fn validate_settings(settings: &mut FirewallSettings) -> Result<(), FirewallPlanError> {
    if settings.table_name.is_empty()
        || !settings
            .table_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || !settings
            .table_name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
    {
        return Err(FirewallPlanError::InvalidTableName {
            table_name: settings.table_name.clone(),
        });
    }
    if settings.workload_interface.is_empty()
        || settings.workload_interface.len() > 15
        || !settings
            .workload_interface
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(FirewallPlanError::InvalidInterfaceName {
            interface_name: settings.workload_interface.clone(),
        });
    }
    if settings.dns_port == 0 {
        return Err(FirewallPlanError::ZeroDnsPort);
    }
    if settings.protected_host_ports.contains(&0) {
        return Err(FirewallPlanError::ZeroProtectedHostPort);
    }
    let mut system_host_endpoints = BTreeSet::new();
    let mut exact_system_host_endpoints = BTreeSet::new();
    for access in &mut settings.system_host_access {
        if access.host_ports.is_empty() && access.endpoints.is_empty() {
            return Err(FirewallPlanError::EmptySystemHostAccess {
                service_id: access.service_id.clone(),
            });
        }
        for port in &access.host_ports {
            if *port == 0 {
                return Err(FirewallPlanError::ZeroSystemHostAccessPort {
                    service_id: access.service_id.clone(),
                });
            }
            if !settings.protected_host_ports.contains(port) {
                return Err(FirewallPlanError::SystemHostAccessPortNotProtected {
                    service_id: access.service_id.clone(),
                    port: *port,
                });
            }
            if !system_host_endpoints.insert((access.service_id.clone(), *port)) {
                return Err(FirewallPlanError::DuplicateSystemHostAccess {
                    service_id: access.service_id.clone(),
                    port: *port,
                });
            }
        }
        for endpoint in &access.endpoints {
            if endpoint.port == 0 {
                return Err(FirewallPlanError::ZeroSystemHostEndpointPort {
                    service_id: access.service_id.clone(),
                    address: endpoint.address,
                });
            }
            if !exact_system_host_endpoints.insert((
                access.service_id.clone(),
                endpoint.address,
                endpoint.port,
            )) {
                return Err(FirewallPlanError::DuplicateSystemHostEndpoint {
                    service_id: access.service_id.clone(),
                    address: endpoint.address,
                    port: endpoint.port,
                });
            }
        }
        for cidr in &access.trusted_source_cidrs {
            let field = format!("systemHostAccess[{}].trustedSourceCidrs", access.service_id);
            let source = CanonicalCidr::parse(cidr, &field)?;
            if source.family() != AddressFamily::V4 {
                return Err(FirewallPlanError::InvalidCidr {
                    field,
                    value: cidr.clone(),
                    message: "system host access sources must be IPv4".to_owned(),
                });
            }
        }
        access.trusted_source_cidrs.sort();
        access.trusted_source_cidrs.dedup();
        access.host_ports.sort_unstable();
        access.endpoints.sort_unstable();
    }
    settings.system_host_access.sort();
    if settings
        .host_port_routes
        .iter()
        .any(|route| route.host_port == 0 || route.workload_port == 0)
    {
        return Err(FirewallPlanError::ZeroHostPortRoute);
    }
    settings.host_port_routes.sort();
    let mut endpoints = BTreeSet::new();
    for route in &settings.host_port_routes {
        if settings.protected_host_ports.contains(&route.host_port) {
            return Err(FirewallPlanError::HostPortRouteConflictsProtected {
                port: route.host_port,
            });
        }
        if !endpoints.insert((route.host_port, route.protocol)) {
            return Err(FirewallPlanError::DuplicateHostPortRoute {
                port: route.host_port,
                protocol: route.protocol,
            });
        }
    }
    settings.protected_host_ports.sort_unstable();
    settings.protected_host_ports.dedup();
    settings.control_allow_cidrs.sort();
    settings.control_allow_cidrs.dedup();
    Ok(())
}

fn index_services(
    services: Vec<kernel_api::Service>,
) -> Result<BTreeSet<ServiceId>, FirewallPlanError> {
    let mut indexed = BTreeSet::new();
    for service in services {
        if !indexed.insert(service.meta.id.clone()) {
            return Err(FirewallPlanError::DuplicateResource {
                kind: "Service",
                resource_id: service.meta.id.to_string(),
            });
        }
    }
    Ok(indexed)
}

fn index_nodes(
    networks: Vec<kernel_api::NodeNetwork>,
) -> Result<BTreeMap<NodeId, NodeContext>, FirewallPlanError> {
    let mut resource_ids = BTreeSet::new();
    let mut nodes = BTreeMap::new();
    for network in networks
        .into_iter()
        .filter(|network| network.meta.deletion_timestamp.is_none())
    {
        if !resource_ids.insert(network.meta.id.clone()) {
            return Err(FirewallPlanError::DuplicateResource {
                kind: "NodeNetwork",
                resource_id: network.meta.id.to_string(),
            });
        }
        let subnet = CanonicalCidr::parse(
            &network.spec.workload_subnet,
            &format!("NodeNetwork/{}/workloadSubnet", network.meta.id),
        )?;
        if subnet.family() != AddressFamily::V4 {
            return Err(FirewallPlanError::UnsupportedWorkloadSubnet {
                node_id: network.spec.node_id,
                subnet: subnet.to_string(),
            });
        }
        let bridge_address = subnet.bridge_address().ok_or_else(|| {
            FirewallPlanError::WorkloadSubnetHasNoBridge {
                node_id: network.spec.node_id.clone(),
                subnet: subnet.to_string(),
            }
        })?;
        let admin_address =
            subnet
                .admin_address()
                .ok_or_else(|| FirewallPlanError::WorkloadSubnetHasNoAdmin {
                    node_id: network.spec.node_id.clone(),
                    subnet: subnet.to_string(),
                })?;
        let node_id = network.spec.node_id;
        if nodes
            .insert(
                node_id.clone(),
                NodeContext {
                    node_id: node_id.clone(),
                    workload_subnet: subnet,
                    bridge_address,
                    admin_address,
                },
            )
            .is_some()
        {
            return Err(FirewallPlanError::DuplicateNodeNetwork { node_id });
        }
    }
    Ok(nodes)
}

fn validate_assignments(
    assignments: Vec<Assignment>,
    services: &BTreeSet<ServiceId>,
    nodes: &BTreeMap<NodeId, NodeContext>,
) -> Result<Vec<Assignment>, FirewallPlanError> {
    let mut ids = BTreeSet::new();
    let mut validated = Vec::new();
    for assignment in assignments {
        if !ids.insert(assignment.meta.id.clone()) {
            return Err(FirewallPlanError::DuplicateResource {
                kind: "Assignment",
                resource_id: assignment.meta.id.to_string(),
            });
        }
        if !services.contains(&assignment.spec.service_id) {
            return Err(FirewallPlanError::MissingAssignmentService {
                assignment_id: assignment.meta.id,
                service_id: assignment.spec.service_id,
            });
        }
        let Some(address) = assignment.spec.workload_address else {
            if nodes.contains_key(&assignment.spec.node_id) {
                return Err(FirewallPlanError::MissingAssignmentAddress {
                    assignment_id: assignment.meta.id,
                    node_id: assignment.spec.node_id,
                });
            }
            continue;
        };
        let node = nodes.get(&assignment.spec.node_id).ok_or_else(|| {
            FirewallPlanError::MissingAssignmentNode {
                assignment_id: assignment.meta.id.clone(),
                node_id: assignment.spec.node_id.clone(),
            }
        })?;
        if !node.workload_subnet.contains(&address) {
            return Err(FirewallPlanError::AssignmentAddressOutsideSubnet {
                assignment_id: assignment.meta.id,
                address,
                subnet: node.workload_subnet.to_string(),
            });
        }
        validated.push(assignment);
    }
    validated.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    Ok(validated)
}

fn index_policies(
    policies: Vec<FirewallPolicy>,
    services: &BTreeSet<ServiceId>,
    nodes: &BTreeMap<NodeId, NodeContext>,
) -> Result<BTreeMap<SubjectKey, ValidatedPolicy>, FirewallPlanError> {
    let mut ids = BTreeSet::new();
    let mut indexed = BTreeMap::new();
    for policy in policies
        .into_iter()
        .filter(|policy| policy.meta.deletion_timestamp.is_none())
    {
        if !ids.insert(policy.meta.id.clone()) {
            return Err(FirewallPlanError::DuplicateResource {
                kind: "FirewallPolicy",
                resource_id: policy.meta.id.to_string(),
            });
        }
        let key = subject_key(&policy)?;
        validate_subject(&key, services, nodes)?;
        let rules = policy
            .spec
            .rules
            .iter()
            .enumerate()
            .map(|(index, rule)| validate_rule(&policy, index, rule))
            .collect::<Result<Vec<_>, _>>()?;
        let second_policy_id = policy.meta.id.clone();
        if let Some(existing) = indexed.insert(
            key.clone(),
            ValidatedPolicy {
                resource: policy,
                rules,
            },
        ) {
            return Err(FirewallPlanError::DuplicatePolicyScope {
                first_policy_id: existing.resource.meta.id,
                second_policy_id,
            });
        }
    }
    Ok(indexed)
}

fn subject_key(policy: &FirewallPolicy) -> Result<SubjectKey, FirewallPlanError> {
    match (&policy.spec.direction, &policy.spec.subject) {
        (FirewallDirection::Egress, FirewallSubject::Global) => Ok(SubjectKey::EgressGlobal),
        (FirewallDirection::Egress, FirewallSubject::Service(service_id)) => {
            Ok(SubjectKey::EgressService(service_id.clone()))
        }
        (FirewallDirection::HostInput, FirewallSubject::Global) => Ok(SubjectKey::HostGlobal),
        (FirewallDirection::HostInput, FirewallSubject::Node(node_id)) => {
            Ok(SubjectKey::HostNode(node_id.clone()))
        }
        _ => Err(FirewallPlanError::InvalidPolicySubject {
            policy_id: policy.meta.id.clone(),
            direction: policy.spec.direction,
            subject: policy.spec.subject.clone(),
        }),
    }
}

fn validate_subject(
    key: &SubjectKey,
    services: &BTreeSet<ServiceId>,
    nodes: &BTreeMap<NodeId, NodeContext>,
) -> Result<(), FirewallPlanError> {
    match key {
        SubjectKey::EgressService(service_id) if !services.contains(service_id) => {
            Err(FirewallPlanError::MissingPolicyService {
                service_id: service_id.clone(),
            })
        }
        SubjectKey::HostNode(node_id) if !nodes.contains_key(node_id) => {
            Err(FirewallPlanError::MissingPolicyNode {
                node_id: node_id.clone(),
            })
        }
        _ => Ok(()),
    }
}

fn validate_rule(
    policy: &FirewallPolicy,
    index: usize,
    rule: &FirewallRule,
) -> Result<ValidatedRule, FirewallPlanError> {
    let cidr = CanonicalCidr::parse(
        &rule.cidr,
        &format!("FirewallPolicy/{}/rules/{index}/cidr", policy.meta.id),
    )?;
    let mut ports = rule.ports.clone();
    for range in &ports {
        if range.start == 0 || range.end == 0 || range.start > range.end {
            return Err(FirewallPlanError::InvalidPortRange {
                policy_id: policy.meta.id.clone(),
                rule_index: index,
                start: range.start,
                end: range.end,
            });
        }
    }
    ports.sort_by_key(|range| (range.start, range.end));
    ports.dedup();
    Ok(ValidatedRule {
        cidr,
        protocol: rule.protocol,
        ports,
        verdict: rule.verdict,
    })
}
