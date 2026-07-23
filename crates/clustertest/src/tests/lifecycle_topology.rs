use std::collections::{BTreeMap, BTreeSet};

use crate::{DeploymentSnapshot, FixtureNodeName};

pub(super) fn assign_replica_placements(
    deployment: &mut DeploymentSnapshot<u64>,
    topology_nodes: &[FixtureNodeName],
    draining_nodes: &BTreeSet<FixtureNodeName>,
    affinity: Option<&FixtureNodeName>,
    next_workload_instance: &mut u64,
) {
    let mut eligible = match affinity {
        Some(node) if topology_nodes.contains(node) => vec![node.clone()],
        _ => topology_nodes
            .iter()
            .filter(|node| !draining_nodes.contains(*node))
            .cloned()
            .collect::<Vec<_>>(),
    };
    if eligible.is_empty() {
        eligible = topology_nodes.to_vec();
    }
    let mut counts = eligible
        .iter()
        .cloned()
        .map(|node| (node, 0_u32))
        .collect::<BTreeMap<_, _>>();
    for replica in &deployment.replicas {
        if let Some(node) = replica.node.as_ref()
            && let Some(count) = counts.get_mut(node)
        {
            *count = count.saturating_add(1);
        }
    }
    for replica in &mut deployment.replicas {
        let placement_is_valid = replica
            .node
            .as_ref()
            .is_some_and(|node| eligible.contains(node));
        if !placement_is_valid {
            let selected = eligible
                .iter()
                .min_by(|left, right| {
                    counts
                        .get(*left)
                        .cmp(&counts.get(*right))
                        .then_with(|| left.cmp(right))
                })
                .cloned();
            if let Some(node) = selected {
                if let Some(count) = counts.get_mut(&node) {
                    *count = count.saturating_add(1);
                }
                replica.node = Some(node);
                replica.workload_instance = None;
            }
        }
        if replica.workload_instance.is_none() {
            replica.workload_instance = Some(format!(
                "workload-{}-{}-{}",
                deployment.id, replica.index, *next_workload_instance
            ));
            *next_workload_instance = next_workload_instance.saturating_add(1);
        }
    }
}
