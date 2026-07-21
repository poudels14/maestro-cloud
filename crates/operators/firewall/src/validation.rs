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
        let node_id = network.spec.node_id;
        if nodes
            .insert(
                node_id.clone(),
                NodeContext {
                    node_id: node_id.clone(),
                    workload_subnet: subnet,
                    bridge_address,
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
        let node = nodes.get(&assignment.spec.node_id).ok_or_else(|| {
            FirewallPlanError::MissingAssignmentNode {
                assignment_id: assignment.meta.id.clone(),
                node_id: assignment.spec.node_id.clone(),
            }
        })?;
        if !node
            .workload_subnet
            .contains(&assignment.spec.workload_address)
        {
            return Err(FirewallPlanError::AssignmentAddressOutsideSubnet {
                assignment_id: assignment.meta.id,
                address: assignment.spec.workload_address,
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
