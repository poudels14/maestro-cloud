use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{
    ADMIN_ADDRESS_OFFSET, Assignment, NodeId, SYSTEM_SERVICE_ADDRESS_END,
    SYSTEM_SERVICE_ADDRESS_START, USER_WORKLOAD_ADDRESS_START, WorkloadNetworkMode,
    is_system_service,
};

use crate::model::{ScheduleNode, UnschedulableReason};
use crate::plan::PlannedAssignment;

pub(crate) fn allocate_addresses(
    assignments: &mut [PlannedAssignment],
    nodes: &[ScheduleNode],
    current: &[Assignment],
) -> Vec<(usize, UnschedulableReason)> {
    let mut used = BTreeMap::<NodeId, BTreeSet<Ipv4Addr>>::new();
    for assignment in current {
        if let Some(IpAddr::V4(address)) = assignment.spec.workload_address {
            used.entry(assignment.spec.node_id.clone())
                .or_default()
                .insert(address);
        }
    }

    let subnets = nodes
        .iter()
        .map(|node| {
            (
                node.node_id.clone(),
                node.workload_subnet
                    .as_deref()
                    .ok_or(())
                    .and_then(WorkloadSubnet::parse),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut failures = Vec::new();
    for (index, assignment) in assignments.iter_mut().enumerate() {
        if assignment.workload_address.is_some() {
            continue;
        }
        if nodes.iter().any(|node| {
            node.node_id == assignment.node_id
                && node.workload_network_mode == WorkloadNetworkMode::RuntimeDelegated
        }) {
            continue;
        }
        let Some(subnet) = subnets.get(&assignment.node_id) else {
            failures.push((
                index,
                UnschedulableReason::InvalidWorkloadSubnet {
                    node_id: assignment.node_id.clone(),
                    subnet: String::new(),
                },
            ));
            continue;
        };
        let Ok(subnet) = subnet else {
            let text = nodes
                .iter()
                .find(|node| node.node_id == assignment.node_id)
                .and_then(|node| node.workload_subnet.clone())
                .unwrap_or_default();
            failures.push((
                index,
                UnschedulableReason::InvalidWorkloadSubnet {
                    node_id: assignment.node_id.clone(),
                    subnet: text,
                },
            ));
            continue;
        };
        let node_used = used.entry(assignment.node_id.clone()).or_default();
        assignment.workload_address = subnet
            .addresses(is_system_service(&assignment.service_id))
            .find(|address| node_used.insert(*address));
        if assignment.workload_address.is_none() {
            failures.push((
                index,
                UnschedulableReason::WorkloadAddressCapacityExhausted {
                    node_id: assignment.node_id.clone(),
                },
            ));
        }
    }
    failures
}

pub(crate) fn assignment_address_matches_pool(
    assignment: &Assignment,
    nodes: &[ScheduleNode],
) -> bool {
    let Some(node) = nodes
        .iter()
        .find(|node| node.node_id == assignment.spec.node_id)
    else {
        return true;
    };
    if node.workload_network_mode == WorkloadNetworkMode::RuntimeDelegated {
        return assignment.spec.workload_address.is_none();
    }
    let Some(subnet) = node
        .workload_subnet
        .as_deref()
        .and_then(|value| WorkloadSubnet::parse(value).ok())
    else {
        return true;
    };
    let Some(IpAddr::V4(address)) = assignment.spec.workload_address else {
        return false;
    };
    subnet.address_matches_pool(address, is_system_service(&assignment.spec.service_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkloadSubnet {
    network: Ipv4Addr,
    prefix: u8,
}

impl WorkloadSubnet {
    fn parse(value: &str) -> Result<Self, ()> {
        let (address, prefix) = value.split_once('/').ok_or(())?;
        let network = address.parse::<Ipv4Addr>().map_err(|_| ())?;
        let prefix = prefix.parse::<u8>().map_err(|_| ())?;
        if prefix > 32 || Ipv4Addr::from(u32::from(network) & prefix_mask(prefix)) != network {
            return Err(());
        }
        Ok(Self { network, prefix })
    }

    fn addresses(self, system_service: bool) -> impl Iterator<Item = Ipv4Addr> {
        let network = u32::from(self.network);
        let broadcast = u32::from(self.network) | !prefix_mask(self.prefix);
        (network.saturating_add(1)..broadcast)
            .filter(move |address| {
                let offset = address.saturating_sub(network);
                if system_service {
                    (SYSTEM_SERVICE_ADDRESS_START..=SYSTEM_SERVICE_ADDRESS_END).contains(&offset)
                        && offset != ADMIN_ADDRESS_OFFSET
                } else {
                    offset >= USER_WORKLOAD_ADDRESS_START
                }
            })
            .map(Ipv4Addr::from)
    }

    fn address_matches_pool(self, address: Ipv4Addr, system_service: bool) -> bool {
        let address = u32::from(address);
        let network = u32::from(self.network);
        let broadcast = network | !prefix_mask(self.prefix);
        if address <= network || address >= broadcast {
            return false;
        }
        let offset = address - network;
        if system_service {
            (SYSTEM_SERVICE_ADDRESS_START..=SYSTEM_SERVICE_ADDRESS_END).contains(&offset)
                && offset != ADMIN_ADDRESS_OFFSET
        } else {
            offset >= USER_WORKLOAD_ADDRESS_START
        }
    }
}

fn prefix_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}
