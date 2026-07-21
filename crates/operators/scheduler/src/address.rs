use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{Assignment, NodeId};

use crate::model::{ScheduleNode, UnschedulableReason};
use crate::plan::PlannedAssignment;

const SYSTEM_RESERVED_HOSTS: u32 = 55;

pub(crate) fn allocate_addresses(
    assignments: &mut [PlannedAssignment],
    nodes: &[ScheduleNode],
    current: &[Assignment],
) -> Vec<(usize, UnschedulableReason)> {
    let mut used = BTreeMap::<NodeId, BTreeSet<Ipv4Addr>>::new();
    for assignment in current {
        if let IpAddr::V4(address) = assignment.spec.workload_address {
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
                WorkloadSubnet::parse(&node.workload_subnet),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut failures = Vec::new();
    for (index, assignment) in assignments.iter_mut().enumerate() {
        if assignment.workload_address.is_some() {
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
                .map(|node| node.workload_subnet.clone())
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
            .workload_addresses()
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

    fn workload_addresses(self) -> impl Iterator<Item = Ipv4Addr> {
        let first = u32::from(self.network).saturating_add(2);
        let broadcast = u32::from(self.network) | !prefix_mask(self.prefix);
        let end = broadcast.saturating_sub(SYSTEM_RESERVED_HOSTS);
        (first..end).map(Ipv4Addr::from)
    }
}

fn prefix_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}
