use std::collections::{BTreeMap, HashMap};

use maestro_core::{
    Assignment, ByteSize, Deployment, DeploymentId, Millicores, NodeAffinity, NodeCapacity, NodeId,
    NodeRecord, ReplicaId, ResourceDefaults, ResourceSpec, ServiceId,
};

#[derive(Clone, Debug)]
pub struct PlacementRequest {
    pub replica_id: ReplicaId,
    pub service_id: ServiceId,
    pub deployment_id: DeploymentId,
    pub index: u32,
    pub image: String,
    pub resources: ResourceSpec,
    pub affinity: Option<NodeAffinity>,
    pub writable_volume: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Placement {
    pub assignments: Vec<Assignment>,
    pub unschedulable: Vec<(ReplicaId, String)>,
}

struct NodeLoad<'a> {
    node: &'a NodeRecord,
    free_cpu: Millicores,
    free_memory: ByteSize,
    replicas_by_deployment: HashMap<DeploymentId, usize>,
}

pub fn place(
    nodes: &[NodeRecord],
    existing: &[Assignment],
    requests: &[PlacementRequest],
    defaults: &ResourceDefaults,
) -> Placement {
    let mut loads = nodes
        .iter()
        .filter(|node| node.is_schedulable())
        .map(|node| (node.id, NodeLoad::new(node, existing)))
        .collect::<BTreeMap<NodeId, NodeLoad>>();
    requests
        .iter()
        .fold(Placement::default(), |mut placement, request| {
            match place_one(&mut loads, request, defaults) {
                Ok(assignment) => placement.assignments.push(assignment),
                Err(reason) => placement
                    .unschedulable
                    .push((request.replica_id.clone(), reason)),
            }
            placement
        })
}

fn place_one(
    loads: &mut BTreeMap<NodeId, NodeLoad>,
    request: &PlacementRequest,
    defaults: &ResourceDefaults,
) -> Result<Assignment, String> {
    let pinned_node = request
        .affinity
        .as_ref()
        .and_then(|affinity| affinity.node.as_ref());
    let eligible = loads
        .values()
        .filter(|load| request.matches(load.node))
        .map(|load| load.node.id)
        .collect::<Vec<_>>();
    match (
        request.writable_volume && pinned_node.is_none(),
        eligible.is_empty(),
    ) {
        (true, _) => Err("writable host volumes require deploy.nodeAffinity.node".to_string()),
        (false, true) if request.affinity.is_some() && !loads.is_empty() => {
            Err("node affinity matches no schedulable node".to_string())
        }
        _ => place_on_eligible(loads, eligible, request, defaults),
    }
}

fn place_on_eligible(
    loads: &mut BTreeMap<NodeId, NodeLoad>,
    eligible: Vec<NodeId>,
    request: &PlacementRequest,
    defaults: &ResourceDefaults,
) -> Result<Assignment, String> {
    let no_candidates = eligible.is_empty();
    let candidates = eligible
        .iter()
        .filter_map(|node_id| loads.get(node_id))
        .map(|load| {
            let resolved = request
                .resources
                .resolve(defaults, &load.node.allocatable());
            (load, resolved)
        })
        .collect::<Vec<_>>();
    let resolve_error = candidates
        .iter()
        .find_map(|(_, resolved)| resolved.as_ref().err().map(ToString::to_string));
    let best = candidates
        .into_iter()
        .filter_map(|(load, resolved)| resolved.ok().map(|resources| (load, resources)))
        .filter(|(load, resources)| load.fits(&resources.request))
        .min_by(|(left, _), (right, _)| {
            let left_key = (
                left.deployment_replicas(&request.deployment_id),
                left.utilization_permille(),
            );
            let right_key = (
                right.deployment_replicas(&request.deployment_id),
                right.utilization_permille(),
            );
            left_key
                .cmp(&right_key)
                .then(left.node.id.cmp(&right.node.id))
        })
        .map(|(load, resources)| (load.node.id, resources));
    match (best, resolve_error) {
        (Some((node_id, resources)), _) => {
            if let Some(load) = loads.get_mut(&node_id) {
                load.reserve(&request.deployment_id, &resources.request);
            }
            Ok(Assignment {
                replica_id: request.replica_id.clone(),
                service_id: request.service_id.clone(),
                deployment_id: request.deployment_id.clone(),
                node_id,
                index: request.index,
                image: request.image.clone(),
                resources,
                retiring_since_ms: None,
            })
        }
        (None, Some(error)) => Err(error),
        (None, None) if no_candidates => Err("no schedulable nodes".to_string()),
        (None, None) => Err("insufficient cpu or memory on every node".to_string()),
    }
}

impl PlacementRequest {
    pub fn for_replica(deployment: &Deployment, index: u32, image: String) -> Self {
        let deploy = &deployment.spec.deploy;
        Self {
            replica_id: ReplicaId::for_deployment(&deployment.id, index),
            service_id: deployment.service_id.clone(),
            deployment_id: deployment.id.clone(),
            index,
            image,
            resources: deploy.resources.clone(),
            affinity: deploy.node_affinity.clone(),
            writable_volume: deploy.volumes.iter().any(|volume| !volume.read_only),
        }
    }

    fn matches(&self, node: &NodeRecord) -> bool {
        self.affinity.as_ref().is_none_or(|affinity| {
            let node_matches = affinity
                .node
                .as_ref()
                .is_none_or(|pinned| *pinned == node.name || *pinned == node.id.to_string());
            let labels_match = affinity
                .labels
                .iter()
                .all(|(key, value)| node.labels.get(key) == Some(value));
            node_matches && labels_match
        })
    }
}

impl<'a> NodeLoad<'a> {
    fn new(node: &'a NodeRecord, existing: &[Assignment]) -> Self {
        let on_node = existing
            .iter()
            .filter(|assignment| assignment.node_id == node.id)
            .collect::<Vec<_>>();
        let used_cpu = on_node
            .iter()
            .map(|assignment| assignment.resources.request.cpu.get())
            .sum::<u32>();
        let used_memory = on_node
            .iter()
            .map(|assignment| assignment.resources.request.memory.bytes())
            .sum::<u64>();
        let replicas_by_deployment = on_node.iter().fold(
            HashMap::new(),
            |mut counts: HashMap<DeploymentId, usize>, assignment| {
                *counts.entry(assignment.deployment_id.clone()).or_default() += 1;
                counts
            },
        );
        let allocatable: NodeCapacity = node.allocatable();
        Self {
            node,
            free_cpu: allocatable.cpu.saturating_sub(Millicores::new(used_cpu)),
            free_memory: allocatable
                .memory
                .saturating_sub(ByteSize::new(used_memory)),
            replicas_by_deployment,
        }
    }

    fn fits(&self, request: &maestro_core::ResolvedRequest) -> bool {
        request.cpu <= self.free_cpu && request.memory <= self.free_memory
    }

    fn reserve(&mut self, deployment_id: &DeploymentId, request: &maestro_core::ResolvedRequest) {
        self.free_cpu = self.free_cpu.saturating_sub(request.cpu);
        self.free_memory = self.free_memory.saturating_sub(request.memory);
        *self
            .replicas_by_deployment
            .entry(deployment_id.clone())
            .or_default() += 1;
    }

    fn deployment_replicas(&self, deployment_id: &DeploymentId) -> usize {
        self.replicas_by_deployment
            .get(deployment_id)
            .copied()
            .unwrap_or_default()
    }

    fn utilization_permille(&self) -> u64 {
        let allocatable = self.node.allocatable().memory.bytes().max(1);
        let used = allocatable.saturating_sub(self.free_memory.bytes());
        used * 1000 / allocatable
    }
}

#[cfg(test)]
mod tests {
    use maestro_core::{EffectiveResources, NodeRole, NodeState, ResolvedRequest, ResourceRequest};

    use super::*;

    const GIB: u64 = 1 << 30;

    fn node(id: u64, cores: u32, memory_gib: u64) -> NodeRecord {
        NodeRecord {
            id: NodeId(id),
            name: format!("node{id}"),
            role: NodeRole::Voter,
            endpoint: String::new(),
            subnet: None,
            runs_workloads: true,
            capacity: NodeCapacity {
                cpu: Millicores::new(cores * 1000),
                memory: ByteSize::new(memory_gib * GIB),
            },
            reserved: NodeCapacity {
                cpu: Millicores::new(1000),
                memory: ByteSize::new(2 * GIB),
            },
            labels: Default::default(),
            state: NodeState::Active,
            wireguard: None,
        }
    }

    fn request(service: &str, index: u32, cpu_request: Option<u32>) -> PlacementRequest {
        PlacementRequest {
            replica_id: ReplicaId::from(format!("{service}-d1-{index}").as_str()),
            service_id: ServiceId::from(service),
            deployment_id: DeploymentId::from("d1"),
            index,
            image: "nginx".to_string(),
            resources: ResourceSpec {
                request: cpu_request.map(|cpu| ResourceRequest {
                    cpu: Some(Millicores::new(cpu)),
                    memory: None,
                }),
                ..ResourceSpec::default()
            },
            affinity: None,
            writable_volume: false,
        }
    }

    fn existing(node_id: u64, service: &str, cpu: u32, memory_gib: u64) -> Assignment {
        Assignment {
            replica_id: ReplicaId::from(format!("{service}-old").as_str()),
            service_id: ServiceId::from(service),
            deployment_id: DeploymentId::from("d0"),
            node_id: NodeId(node_id),
            index: 0,
            image: "nginx".to_string(),
            resources: EffectiveResources {
                cpu_limit: Millicores::new(4000),
                memory_limit: ByteSize::new(4 * GIB),
                request: ResolvedRequest {
                    cpu: Millicores::new(cpu),
                    memory: ByteSize::new(memory_gib * GIB),
                },
            },
            retiring_since_ms: None,
        }
    }

    #[test]
    fn spreads_replicas_of_a_service_across_nodes() {
        let nodes = [node(1, 8, 32), node(2, 8, 32), node(3, 8, 32)];
        let requests = (0..3)
            .map(|index| request("api", index, None))
            .collect::<Vec<_>>();
        let placement = place(&nodes, &[], &requests, &ResourceDefaults::default());
        let mut node_ids = placement
            .assignments
            .iter()
            .map(|assignment| assignment.node_id.0)
            .collect::<Vec<_>>();
        node_ids.sort();
        assert_eq!(node_ids, [1, 2, 3]);
        assert!(placement.unschedulable.is_empty());
    }

    #[test]
    fn resolves_memory_limit_against_chosen_node() {
        let nodes = [node(1, 8, 34)];
        let placement = place(
            &nodes,
            &[],
            &[request("api", 0, None)],
            &ResourceDefaults::default(),
        );
        let resources = placement.assignments[0].resources;
        assert_eq!(resources.memory_limit, ByteSize::new(16 * GIB));
        assert_eq!(resources.cpu_limit, Millicores::new(4000));
    }

    #[test]
    fn packs_by_requests_not_limits() {
        let nodes = [node(1, 5, 34)];
        let requests = (0..16)
            .map(|index| request("api", index, None))
            .collect::<Vec<_>>();
        let placement = place(&nodes, &[], &requests, &ResourceDefaults::default());
        assert_eq!(placement.assignments.len(), 16);
        assert!(placement.unschedulable.is_empty());
    }

    #[test]
    fn reports_unschedulable_when_requests_exceed_free_capacity() {
        let nodes = [node(1, 3, 34)];
        let busy = [existing(1, "worker", 1500, 4)];
        let requests = [request("api", 0, Some(400)), request("api", 1, Some(400))];
        let placement = place(&nodes, &busy, &requests, &ResourceDefaults::default());
        assert_eq!(placement.assignments.len(), 1);
        assert_eq!(placement.unschedulable.len(), 1);
        assert_eq!(
            placement.unschedulable[0].1,
            "insufficient cpu or memory on every node"
        );
    }

    #[test]
    fn spreads_a_new_deployment_regardless_of_draining_replicas() {
        let nodes = [node(1, 8, 32), node(2, 8, 32)];
        let draining = (0..4)
            .map(|index| Assignment {
                replica_id: ReplicaId::from(format!("old-{index}").as_str()),
                ..existing(1, "api", 250, 1)
            })
            .collect::<Vec<_>>();
        let requests = (0..4)
            .map(|index| request("api", index, None))
            .collect::<Vec<_>>();
        let placement = place(&nodes, &draining, &requests, &ResourceDefaults::default());
        let on_first = placement
            .assignments
            .iter()
            .filter(|assignment| assignment.node_id == NodeId(1))
            .count();
        assert_eq!(on_first, 2);
    }

    #[test]
    fn prefers_less_utilized_nodes() {
        let nodes = [node(1, 8, 34), node(2, 8, 34)];
        let busy = [existing(1, "worker", 1000, 16)];
        let placement = place(
            &nodes,
            &busy,
            &[request("api", 0, None)],
            &ResourceDefaults::default(),
        );
        assert_eq!(placement.assignments[0].node_id, NodeId(2));
    }

    #[test]
    fn skips_draining_and_workerless_nodes() {
        let mut draining = node(1, 8, 32);
        draining.state = NodeState::Draining;
        let mut control_only = node(2, 8, 32);
        control_only.runs_workloads = false;
        let placement = place(
            &[draining, control_only],
            &[],
            &[request("api", 0, None)],
            &ResourceDefaults::default(),
        );
        assert_eq!(placement.unschedulable[0].1, "no schedulable nodes");
    }

    #[test]
    fn honors_node_and_label_affinity() {
        let mut labelled = node(2, 8, 32);
        labelled.labels.insert("gpu".to_string(), "a10".to_string());
        let nodes = [node(1, 8, 32), labelled];
        let by_label = PlacementRequest {
            affinity: Some(NodeAffinity {
                node: None,
                labels: [("gpu".to_string(), "a10".to_string())].into(),
            }),
            ..request("ml", 0, None)
        };
        let by_name = PlacementRequest {
            affinity: Some(NodeAffinity {
                node: Some("node1".to_string()),
                labels: Default::default(),
            }),
            ..request("db", 0, None)
        };
        let placement = place(
            &nodes,
            &[],
            &[by_label, by_name],
            &ResourceDefaults::default(),
        );
        let placed = placement
            .assignments
            .iter()
            .map(|assignment| (assignment.service_id.to_string(), assignment.node_id.0))
            .collect::<Vec<_>>();
        assert_eq!(placed, [("ml".to_string(), 2), ("db".to_string(), 1)]);

        let unmatched = PlacementRequest {
            affinity: Some(NodeAffinity {
                node: Some("node9".to_string()),
                labels: Default::default(),
            }),
            ..request("db", 0, None)
        };
        let placement = place(&nodes, &[], &[unmatched], &ResourceDefaults::default());
        assert_eq!(
            placement.unschedulable[0].1,
            "node affinity matches no schedulable node"
        );
    }

    #[test]
    fn writable_volumes_need_a_pinned_node() {
        let nodes = [node(1, 8, 32)];
        let unpinned = PlacementRequest {
            writable_volume: true,
            ..request("db", 0, None)
        };
        let pinned = PlacementRequest {
            writable_volume: true,
            affinity: Some(NodeAffinity {
                node: Some("1".to_string()),
                labels: Default::default(),
            }),
            ..request("cache", 0, None)
        };
        let placement = place(
            &nodes,
            &[],
            &[unpinned, pinned],
            &ResourceDefaults::default(),
        );
        assert_eq!(placement.assignments.len(), 1);
        assert_eq!(
            placement.assignments[0].service_id,
            ServiceId::from("cache")
        );
        assert!(placement.unschedulable[0].1.contains("nodeAffinity.node"));
    }
}
