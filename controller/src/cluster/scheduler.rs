//! Cluster-wide replica scheduler.
//!
//! Pure planning logic: given the desired set of services + the available
//! nodes, produces an assignment plan. Stateless (no I/O), trivially testable.
//! Honors:
//!   * one replica per service per node
//!   * `node_affinity.node_id` pins to a specific node
//!   * `node_affinity.labels` constrains to nodes with matching labels
//!   * sticky placement — existing assignments are kept if still valid
//!   * least-loaded preference for new placements

use std::collections::{BTreeMap, HashMap, HashSet};

use super::scheduling::{
    Assignment, NodeCapacity, ReplicaSlot, SchedulePlan, ServiceScheduleSpec, UnschedulableReplica,
};
use super::types::NodeId;

pub trait Scheduler: Send + Sync {
    fn plan(
        &self,
        services: &[ServiceScheduleSpec],
        nodes: &[NodeCapacity],
        existing: &[Assignment],
        unhealthy: &[ReplicaSlot],
        now_ms: u64,
    ) -> SchedulePlan;
}

#[derive(Debug, Default, Clone)]
pub struct DefaultScheduler;

impl DefaultScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Scheduler for DefaultScheduler {
    fn plan(
        &self,
        services: &[ServiceScheduleSpec],
        nodes: &[NodeCapacity],
        existing: &[Assignment],
        unhealthy: &[ReplicaSlot],
        now_ms: u64,
    ) -> SchedulePlan {
        let workload_nodes: Vec<&NodeCapacity> =
            nodes.iter().filter(|node| node.can_run_workloads).collect();

        let unhealthy_set: HashSet<ReplicaSlot> = unhealthy.iter().cloned().collect();
        let mut existing_by_slot: HashMap<ReplicaSlot, &Assignment> = HashMap::new();
        let mut avoid_for_slot: HashMap<ReplicaSlot, NodeId> = HashMap::new();
        for assignment in existing {
            if unhealthy_set.contains(&assignment.slot()) {
                avoid_for_slot.insert(assignment.slot(), assignment.node_id.clone());
                continue;
            }
            existing_by_slot.insert(assignment.slot(), assignment);
        }

        let valid_nodes: HashSet<NodeId> = workload_nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect();

        let mut plan = SchedulePlan::default();
        let mut load: HashMap<NodeId, u32> = workload_nodes
            .iter()
            .map(|node| (node.node_id.clone(), 0))
            .collect();
        let mut used_per_service: HashMap<String, HashSet<NodeId>> = HashMap::new();

        let mut service_specs = services.to_vec();
        service_specs.sort_by(|left, right| {
            let left_pinned = is_pinned(left);
            let right_pinned = is_pinned(right);
            right_pinned
                .cmp(&left_pinned)
                .then_with(|| right.desired_replicas.cmp(&left.desired_replicas))
        });

        for spec in &service_specs {
            let port = match spec.assigned_port {
                Some(port) => port,
                None => {
                    for replica_index in 0..spec.desired_replicas {
                        plan.unschedulable.push(UnschedulableReplica {
                            slot: ReplicaSlot {
                                service_id: spec.service_id.clone(),
                                replica_index,
                            },
                            reason: "service has no assigned port".to_string(),
                        });
                    }
                    continue;
                }
            };
            let candidate_nodes: Vec<&NodeCapacity> = workload_nodes
                .iter()
                .copied()
                .filter(|node| spec_allows_node(spec, node))
                .collect();

            for replica_index in 0..spec.desired_replicas {
                let slot = ReplicaSlot {
                    service_id: spec.service_id.clone(),
                    replica_index,
                };
                let used = used_per_service.entry(spec.service_id.clone()).or_default();

                let existing_assignment = existing_by_slot.get(&slot).copied();
                let keep_existing = existing_assignment.filter(|assignment| {
                    assignment.deployment_id == spec.deployment_id
                        && valid_nodes.contains(&assignment.node_id)
                        && candidate_nodes
                            .iter()
                            .any(|node| node.node_id == assignment.node_id)
                        && !used.contains(&assignment.node_id)
                        && assignment.port == port
                });
                if let Some(existing) = keep_existing {
                    used.insert(existing.node_id.clone());
                    if let Some(count) = load.get_mut(&existing.node_id) {
                        *count += 1;
                    }
                    plan.assignments.push(existing.clone());
                    continue;
                }

                let avoid_node = avoid_for_slot.get(&slot).cloned();
                let next_node = candidate_nodes
                    .iter()
                    .filter(|node| !used.contains(&node.node_id))
                    .filter(|node| {
                        avoid_node
                            .as_ref()
                            .map(|avoid| &node.node_id != avoid || candidate_nodes.len() == 1)
                            .unwrap_or(true)
                    })
                    .min_by_key(|node| load.get(&node.node_id).copied().unwrap_or(0));
                match next_node {
                    Some(node) => {
                        used.insert(node.node_id.clone());
                        if let Some(count) = load.get_mut(&node.node_id) {
                            *count += 1;
                        }
                        plan.assignments.push(Assignment {
                            service_id: spec.service_id.clone(),
                            deployment_id: spec.deployment_id.clone(),
                            replica_index,
                            node_id: node.node_id.clone(),
                            port,
                            created_at_ms: now_ms,
                        });
                    }
                    None => {
                        let reason = if candidate_nodes.is_empty() {
                            "no nodes satisfy affinity".to_string()
                        } else {
                            "no nodes available without exceeding 1-replica-per-node".to_string()
                        };
                        plan.unschedulable
                            .push(UnschedulableReplica { slot, reason });
                    }
                }
            }
        }

        plan.assignments.sort_by(|left, right| {
            left.service_id
                .cmp(&right.service_id)
                .then_with(|| left.replica_index.cmp(&right.replica_index))
        });
        plan.unschedulable.sort_by(|left, right| {
            left.slot
                .service_id
                .cmp(&right.slot.service_id)
                .then_with(|| left.slot.replica_index.cmp(&right.slot.replica_index))
        });
        plan
    }
}

fn is_pinned(spec: &ServiceScheduleSpec) -> bool {
    spec.node_affinity
        .as_ref()
        .map(|affinity| affinity.is_pinned())
        .unwrap_or(false)
}

fn spec_allows_node(spec: &ServiceScheduleSpec, node: &NodeCapacity) -> bool {
    let labels = labels_for(node);
    match spec.node_affinity.as_ref() {
        Some(affinity) => affinity.matches(&node.node_id, &labels),
        None => true,
    }
}

fn labels_for(node: &NodeCapacity) -> BTreeMap<String, String> {
    node.labels.clone()
}
