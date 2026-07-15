use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use sha2::{Digest, Sha256};

use crate::cluster::{
    NodeRole,
    types::{
        Assignment, NodeId, NodeInfo, NodeState, SchedulePlan, ServiceScheduleSpec,
        UnschedulableReplica,
    },
};

const DATA_PLANE_FRESHNESS_MS: i64 = 15_000;

pub struct ScheduleInput {
    pub cluster_id: String,
    pub services: Vec<ServiceScheduleSpec>,
    pub nodes: Vec<NodeInfo>,
    pub node_states: BTreeMap<NodeId, NodeState>,
    pub current: Vec<Assignment>,
    pub held: BTreeSet<String>,
    pub now_ms: i64,
}

pub fn plan(mut input: ScheduleInput) -> SchedulePlan {
    input.services.sort_by(|left, right| {
        let left_pinned = left
            .node_affinity
            .as_ref()
            .and_then(|affinity| affinity.node_id.as_ref())
            .is_some();
        let right_pinned = right
            .node_affinity
            .as_ref()
            .and_then(|affinity| affinity.node_id.as_ref())
            .is_some();
        right_pinned
            .cmp(&left_pinned)
            .then_with(|| {
                let left_count: u32 = left.groups.iter().map(|group| group.replicas).sum();
                let right_count: u32 = right.groups.iter().map(|group| group.replicas).sum();
                right_count.cmp(&left_count)
            })
            .then_with(|| left.service_id.cmp(&right.service_id))
    });
    input
        .nodes
        .sort_by(|left, right| left.node_id.cmp(&right.node_id));

    let held_nodes: HashSet<&str> = input
        .current
        .iter()
        .filter(|assignment| input.held.contains(&assignment.assignment_id))
        .map(|assignment| assignment.node_id.as_str())
        .collect();
    let mut current_by_slot: BTreeMap<(String, String, u32), Vec<&Assignment>> = BTreeMap::new();
    for assignment in &input.current {
        current_by_slot
            .entry((
                assignment.service_id.clone(),
                assignment.deployment_id.clone(),
                assignment.replica_index,
            ))
            .or_default()
            .push(assignment);
    }
    for assignments in current_by_slot.values_mut() {
        assignments.sort_by(|left, right| {
            input
                .held
                .contains(&right.assignment_id)
                .cmp(&input.held.contains(&left.assignment_id))
                .then_with(|| right.placement_epoch.cmp(&left.placement_epoch))
                .then_with(|| left.assignment_id.cmp(&right.assignment_id))
        });
    }

    let mut output = SchedulePlan::default();
    let mut load: HashMap<NodeId, usize> = HashMap::new();
    let mut deployment_load = HashMap::<(String, String, NodeId), usize>::new();
    let mut rollout_preference = HashMap::<(String, u32), NodeId>::new();

    for spec in input.services {
        let candidates = eligible_nodes(
            &spec,
            &input.nodes,
            &input.node_states,
            &held_nodes,
            input.now_ms,
        );
        for (group_index, group) in spec.groups.iter().enumerate() {
            for replica_index in 0..group.replicas {
                let slot = (
                    spec.service_id.clone(),
                    group.deployment_id.clone(),
                    replica_index,
                );
                let existing = current_by_slot
                    .get(&slot)
                    .and_then(|values| values.first())
                    .copied();
                let unhealthy = existing.is_some_and(|assignment| {
                    spec.unhealthy_slots.contains(&(
                        group.deployment_id.clone(),
                        assignment.node_id.clone(),
                        replica_index,
                        assignment.assignment_id.clone(),
                    ))
                });
                let existing_is_eligible = existing.is_some_and(|assignment| {
                    input.held.contains(&assignment.assignment_id)
                        || candidates
                            .iter()
                            .any(|candidate| candidate.node_id == assignment.node_id)
                });
                if let Some(existing) = existing
                    && existing_is_eligible
                    && !unhealthy
                {
                    retain_assignment(
                        existing,
                        &mut output.assignments,
                        &mut deployment_load,
                        &mut load,
                    );
                    if group_index == 0 {
                        rollout_preference.insert(
                            (spec.service_id.clone(), replica_index),
                            existing.node_id.clone(),
                        );
                    }
                    continue;
                }

                let previous_node = existing.map(|assignment| assignment.node_id.as_str());
                let preferred_node = (group_index > 0)
                    .then(|| {
                        rollout_preference
                            .get(&(spec.service_id.clone(), replica_index))
                            .map(String::as_str)
                    })
                    .flatten();
                let selected = candidates
                    .iter()
                    .filter(|node| !unhealthy || Some(node.node_id.as_str()) != previous_node)
                    .min_by_key(|node| {
                        (
                            preferred_node != Some(node.node_id.as_str()),
                            deployment_load
                                .get(&(
                                    spec.service_id.clone(),
                                    group.deployment_id.clone(),
                                    node.node_id.clone(),
                                ))
                                .copied()
                                .unwrap_or(0),
                            load.get(&node.node_id).copied().unwrap_or(0),
                            node.role != NodeRole::Worker,
                            node.node_id.as_str(),
                        )
                    });

                if let Some(node) = selected {
                    let epoch = existing
                        .map(|assignment| assignment.placement_epoch.saturating_add(1))
                        .unwrap_or(1);
                    let assignment_id = assignment_id(
                        &input.cluster_id,
                        &spec.service_id,
                        &group.deployment_id,
                        replica_index,
                        &node.node_id,
                        epoch,
                    );
                    let assignment = Assignment {
                        assignment_id,
                        placement_epoch: epoch,
                        service_id: spec.service_id.clone(),
                        deployment_id: group.deployment_id.clone(),
                        replica_index,
                        node_id: node.node_id.clone(),
                        replaces_assignment_id: existing
                            .map(|assignment| assignment.assignment_id.clone()),
                        created_at_ms: input.now_ms,
                    };
                    retain_assignment(
                        &assignment,
                        &mut output.assignments,
                        &mut deployment_load,
                        &mut load,
                    );
                    if group_index == 0 {
                        rollout_preference.insert(
                            (spec.service_id.clone(), replica_index),
                            node.node_id.clone(),
                        );
                    }
                } else {
                    if let Some(existing) = existing {
                        retain_assignment(
                            existing,
                            &mut output.assignments,
                            &mut deployment_load,
                            &mut load,
                        );
                    }
                    output.unschedulable.push(UnschedulableReplica {
                        service_id: spec.service_id.clone(),
                        deployment_id: group.deployment_id.clone(),
                        replica_index,
                        reason: unschedulable_reason(&spec, &input.nodes, candidates.len()),
                    });
                }
            }
        }
    }

    output.assignments.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.service_id.cmp(&right.service_id))
            .then_with(|| left.deployment_id.cmp(&right.deployment_id))
            .then_with(|| left.replica_index.cmp(&right.replica_index))
            .then_with(|| left.assignment_id.cmp(&right.assignment_id))
    });
    output.unschedulable.sort_by(|left, right| {
        left.service_id
            .cmp(&right.service_id)
            .then_with(|| left.deployment_id.cmp(&right.deployment_id))
            .then_with(|| left.replica_index.cmp(&right.replica_index))
    });
    output
}

fn eligible_nodes<'a>(
    spec: &ServiceScheduleSpec,
    nodes: &'a [NodeInfo],
    states: &BTreeMap<NodeId, NodeState>,
    held_nodes: &HashSet<&str>,
    now_ms: i64,
) -> Vec<&'a NodeInfo> {
    nodes
        .iter()
        .filter(|node| node.role.runs_workloads())
        .filter(|node| node.data_plane_ready)
        .filter(|node| {
            now_ms.saturating_sub(node.data_plane_checked_at_ms) <= DATA_PLANE_FRESHNESS_MS
        })
        .filter(|node| {
            !states
                .get(&node.node_id)
                .is_some_and(|state| state.unschedulable)
        })
        .filter(|node| !held_nodes.contains(node.node_id.as_str()))
        .filter(|node| {
            spec.node_affinity.as_ref().is_none_or(|affinity| {
                affinity
                    .node_id
                    .as_ref()
                    .is_none_or(|node_id| node_id == &node.node_id)
                    && affinity
                        .labels
                        .iter()
                        .all(|(key, value)| node.labels.get(key) == Some(value))
            })
        })
        .collect()
}

fn unschedulable_reason(
    spec: &ServiceScheduleSpec,
    nodes: &[NodeInfo],
    eligible_count: usize,
) -> String {
    if let Some(affinity) = &spec.node_affinity
        && (affinity.node_id.is_some() || !affinity.labels.is_empty())
        && !nodes.iter().any(|node| {
            affinity
                .node_id
                .as_ref()
                .is_none_or(|node_id| node_id == &node.node_id)
                && affinity
                    .labels
                    .iter()
                    .all(|(key, value)| node.labels.get(key) == Some(value))
        })
    {
        return "affinity matches no node".to_string();
    }
    if eligible_count == 0 {
        "no schedulable node".to_string()
    } else {
        "no alternate schedulable node for replacement".to_string()
    }
}

fn retain_assignment(
    assignment: &Assignment,
    assignments: &mut Vec<Assignment>,
    deployment_load: &mut HashMap<(String, String, NodeId), usize>,
    load: &mut HashMap<NodeId, usize>,
) {
    *deployment_load
        .entry((
            assignment.service_id.clone(),
            assignment.deployment_id.clone(),
            assignment.node_id.clone(),
        ))
        .or_default() += 1;
    *load.entry(assignment.node_id.clone()).or_default() += 1;
    assignments.push(assignment.clone());
}

pub(crate) fn assignment_id(
    cluster_id: &str,
    service_id: &str,
    deployment_id: &str,
    replica_index: u32,
    node_id: &str,
    epoch: u64,
) -> String {
    let mut hasher = Sha256::new();
    for value in [
        cluster_id,
        service_id,
        deployment_id,
        &replica_index.to_string(),
        node_id,
        &epoch.to_string(),
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())[..24].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::types::DeploymentGroup;
    use std::net::Ipv4Addr;

    fn node(id: &str, ready: bool, labels: &[(&str, &str)]) -> NodeInfo {
        NodeInfo {
            node_id: id.to_string(),
            instance_id: format!("instance-{id}"),
            hostname: id.to_string(),
            role: NodeRole::Hybrid,
            cluster_host_ip: Ipv4Addr::new(10, 0, 0, 1),
            cluster_api_port: 3001,
            cluster_gateway_port: 3002,
            subnet: "172.20.1.0/24".to_string(),
            tailscale_ip: None,
            data_plane_ready: ready,
            data_plane_checked_at_ms: 1_000,
            data_plane_error: None,
            version: "1".to_string(),
            started_at_ms: 0,
            labels: labels
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }

    fn service(replicas: u32) -> ServiceScheduleSpec {
        ServiceScheduleSpec {
            service_id: "web".to_string(),
            groups: vec![DeploymentGroup {
                deployment_id: "dep1".to_string(),
                replicas,
            }],
            node_affinity: None,
            unhealthy_slots: BTreeSet::new(),
        }
    }

    fn input(replicas: u32) -> ScheduleInput {
        ScheduleInput {
            cluster_id: "cluster".to_string(),
            services: vec![service(replicas)],
            nodes: vec![node("node-b", true, &[]), node("node-a", true, &[])],
            node_states: BTreeMap::new(),
            current: Vec::new(),
            held: BTreeSet::new(),
            now_ms: 1_000,
        }
    }

    #[test]
    fn is_deterministic_and_spreads_one_deployment_per_node() {
        let first = plan(input(2));
        let second = plan(input(2));
        assert_eq!(first, second);
        assert_eq!(first.assignments.len(), 2);
        assert_ne!(first.assignments[0].node_id, first.assignments[1].node_id);
    }

    #[test]
    fn preserves_a_healthy_existing_assignment() {
        let initial = plan(input(1));
        let mut next = input(1);
        next.current = initial.assignments.clone();
        next.now_ms = 2_000;
        assert_eq!(plan(next).assignments, initial.assignments);
    }

    #[test]
    fn moves_an_unhealthy_slot_and_increments_its_epoch() {
        let initial = plan(input(1));
        let old = initial.assignments[0].clone();
        let mut next = input(1);
        next.current = vec![old.clone()];
        next.services[0].unhealthy_slots.insert((
            old.deployment_id.clone(),
            old.node_id.clone(),
            old.replica_index,
            old.assignment_id.clone(),
        ));
        let moved = plan(next).assignments.remove(0);
        assert_ne!(moved.node_id, old.node_id);
        assert_eq!(moved.placement_epoch, 2);
        assert_eq!(
            moved.replaces_assignment_id.as_deref(),
            Some(old.assignment_id.as_str())
        );
    }

    #[test]
    fn retains_existing_when_no_replacement_is_available() {
        let initial = plan(input(1));
        let old = initial.assignments[0].clone();
        let mut next = input(1);
        next.current = vec![old.clone()];
        next.nodes
            .iter_mut()
            .for_each(|node| node.data_plane_ready = false);
        let output = plan(next);
        assert_eq!(output.assignments, vec![old]);
        assert_eq!(output.unschedulable.len(), 1);
    }

    #[test]
    fn spreads_replicas_evenly_when_replica_count_exceeds_nodes() {
        let output = plan(input(5));
        let counts = output.assignments.iter().fold(
            HashMap::<String, usize>::new(),
            |mut counts, assignment| {
                *counts.entry(assignment.node_id.clone()).or_default() += 1;
                counts
            },
        );

        assert_eq!(output.assignments.len(), 5);
        assert!(output.unschedulable.is_empty());
        assert_eq!(counts.get("node-a"), Some(&3));
        assert_eq!(counts.get("node-b"), Some(&2));
    }

    #[test]
    fn scale_up_preserves_existing_placements_before_spreading_new_slots() {
        let initial = plan(input(2));
        let existing_ids = initial
            .assignments
            .iter()
            .map(|assignment| assignment.assignment_id.clone())
            .collect::<HashSet<_>>();
        let mut scaled = input(5);
        scaled.current = initial.assignments;

        let output = plan(scaled);

        assert_eq!(output.assignments.len(), 5);
        assert!(existing_ids.iter().all(|id| {
            output
                .assignments
                .iter()
                .any(|assignment| &assignment.assignment_id == id)
        }));
    }

    #[test]
    fn honors_hard_and_label_affinity() {
        let mut value = input(1);
        value.nodes[0]
            .labels
            .insert("zone".to_string(), "west".to_string());
        value.services[0].node_affinity = Some(crate::cluster::NodeAffinity {
            node_id: Some(value.nodes[0].node_id.clone()),
            labels: BTreeMap::from([("zone".to_string(), "west".to_string())]),
        });
        let output = plan(value);
        assert_eq!(output.assignments[0].node_id, "node-b");
    }

    #[test]
    fn prefers_workers_when_placement_load_is_equal() {
        let mut value = input(1);
        value.nodes[1].role = NodeRole::Worker;

        let output = plan(value);

        assert_eq!(output.assignments[0].node_id, "node-a");
    }

    #[test]
    fn worker_preference_does_not_weaken_replica_spreading() {
        let mut value = input(2);
        value.nodes[1].role = NodeRole::Worker;

        let output = plan(value);

        assert_eq!(output.assignments.len(), 2);
        assert!(
            output
                .assignments
                .iter()
                .any(|item| item.node_id == "node-a")
        );
        assert!(
            output
                .assignments
                .iter()
                .any(|item| item.node_id == "node-b")
        );
    }

    #[test]
    fn worker_preference_does_not_move_a_healthy_hybrid_assignment() {
        let mut value = input(1);
        value.nodes[1].role = NodeRole::Worker;
        value.current = vec![Assignment {
            assignment_id: "existing".to_string(),
            placement_epoch: 1,
            service_id: "web".to_string(),
            deployment_id: "dep1".to_string(),
            replica_index: 0,
            node_id: "node-b".to_string(),
            replaces_assignment_id: None,
            created_at_ms: 0,
        }];

        let output = plan(value);

        assert_eq!(output.assignments[0].assignment_id, "existing");
        assert_eq!(output.assignments[0].node_id, "node-b");
    }

    #[test]
    fn control_plane_only_voters_do_not_receive_workloads() {
        let mut value = input(1);
        value.nodes[1].role = NodeRole::Voter;

        let output = plan(value);

        assert_eq!(output.assignments[0].node_id, "node-b");
    }
}
