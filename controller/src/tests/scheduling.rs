//! Tests for [`DefaultScheduler`] and the [`InMemoryPortAllocator`].

use std::collections::BTreeMap;

use crate::cluster::port_allocator::{InMemoryPortAllocator, PortAllocator, PortRange};
use crate::cluster::scheduler::{DefaultScheduler, Scheduler};
use crate::cluster::scheduling::{Assignment, NodeCapacity, ReplicaSlot, ServiceScheduleSpec};
use crate::deployment::types::NodeAffinity;

fn node(id: &str) -> NodeCapacity {
    NodeCapacity {
        node_id: id.to_string(),
        labels: BTreeMap::new(),
        can_run_workloads: true,
    }
}

fn labeled_node(id: &str, labels: &[(&str, &str)]) -> NodeCapacity {
    let labels = labels
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    NodeCapacity {
        node_id: id.to_string(),
        labels,
        can_run_workloads: true,
    }
}

fn service(id: &str, replicas: u32, port: u16) -> ServiceScheduleSpec {
    ServiceScheduleSpec {
        service_id: id.to_string(),
        deployment_id: format!("{id}-dep"),
        desired_replicas: replicas,
        node_affinity: None,
        assigned_port: Some(port),
    }
}

#[test]
fn assigns_replicas_across_available_nodes() {
    let scheduler = DefaultScheduler::new();
    let services = vec![service("web", 3, 8080)];
    let nodes = vec![node("a"), node("b"), node("c")];
    let plan = scheduler.plan(&services, &nodes, &[], &[], 100);

    assert_eq!(plan.assignments.len(), 3);
    let assigned_nodes: Vec<&str> = plan
        .assignments
        .iter()
        .map(|assignment| assignment.node_id.as_str())
        .collect();
    assert!(assigned_nodes.contains(&"a"));
    assert!(assigned_nodes.contains(&"b"));
    assert!(assigned_nodes.contains(&"c"));
    assert!(plan.unschedulable.is_empty());
}

#[test]
fn enforces_max_one_replica_per_node_per_service() {
    let scheduler = DefaultScheduler::new();
    let services = vec![service("web", 5, 8080)];
    let nodes = vec![node("a"), node("b"), node("c")];
    let plan = scheduler.plan(&services, &nodes, &[], &[], 100);

    assert_eq!(
        plan.assignments.len(),
        3,
        "only 3 nodes available so only 3 replicas should be assigned"
    );
    assert_eq!(plan.unschedulable.len(), 2);
}

#[test]
fn pinned_service_only_lands_on_matching_node() {
    let scheduler = DefaultScheduler::new();
    let mut svc = service("db", 1, 5432);
    svc.node_affinity = Some(NodeAffinity {
        node_id: Some("b".to_string()),
        labels: BTreeMap::new(),
    });
    let plan = scheduler.plan(&[svc], &[node("a"), node("b"), node("c")], &[], &[], 100);
    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(plan.assignments[0].node_id, "b");
}

#[test]
fn pinned_service_unschedulable_when_node_missing() {
    let scheduler = DefaultScheduler::new();
    let mut svc = service("db", 1, 5432);
    svc.node_affinity = Some(NodeAffinity {
        node_id: Some("missing".to_string()),
        labels: BTreeMap::new(),
    });
    let plan = scheduler.plan(&[svc], &[node("a"), node("b")], &[], &[], 100);
    assert!(plan.assignments.is_empty());
    assert_eq!(plan.unschedulable.len(), 1);
}

#[test]
fn label_affinity_filters_candidates() {
    let scheduler = DefaultScheduler::new();
    let mut svc = service("gpu", 2, 9000);
    let mut required_labels = BTreeMap::new();
    required_labels.insert("gpu".to_string(), "true".to_string());
    svc.node_affinity = Some(NodeAffinity {
        node_id: None,
        labels: required_labels,
    });
    let nodes = vec![
        node("a"),
        labeled_node("b", &[("gpu", "true")]),
        labeled_node("c", &[("gpu", "true")]),
    ];
    let plan = scheduler.plan(&[svc], &nodes, &[], &[], 100);
    assert_eq!(plan.assignments.len(), 2);
    let placed: Vec<&str> = plan
        .assignments
        .iter()
        .map(|assignment| assignment.node_id.as_str())
        .collect();
    assert!(placed.contains(&"b"));
    assert!(placed.contains(&"c"));
    assert!(!placed.contains(&"a"));
}

#[test]
fn sticky_placement_keeps_existing_assignments_when_valid() {
    let scheduler = DefaultScheduler::new();
    let services = vec![service("web", 2, 8080)];
    let nodes = vec![node("a"), node("b"), node("c")];
    let existing = vec![
        Assignment {
            service_id: "web".to_string(),
            deployment_id: "web-dep".to_string(),
            replica_index: 0,
            node_id: "c".to_string(),
            port: 8080,
            created_at_ms: 50,
        },
        Assignment {
            service_id: "web".to_string(),
            deployment_id: "web-dep".to_string(),
            replica_index: 1,
            node_id: "a".to_string(),
            port: 8080,
            created_at_ms: 50,
        },
    ];
    let plan = scheduler.plan(&services, &nodes, &existing, &[], 100);
    assert_eq!(plan.assignments.len(), 2);
    for assignment in &plan.assignments {
        assert!(assignment.node_id == "a" || assignment.node_id == "c");
        assert_eq!(assignment.created_at_ms, 50, "kept existing created_at");
    }
}

#[test]
fn rescheduling_redeploys_when_deployment_id_changes() {
    let scheduler = DefaultScheduler::new();
    let services = vec![ServiceScheduleSpec {
        service_id: "web".to_string(),
        deployment_id: "web-v2".to_string(),
        desired_replicas: 1,
        node_affinity: None,
        assigned_port: Some(8080),
    }];
    let existing = vec![Assignment {
        service_id: "web".to_string(),
        deployment_id: "web-v1".to_string(),
        replica_index: 0,
        node_id: "a".to_string(),
        port: 8080,
        created_at_ms: 50,
    }];
    let plan = scheduler.plan(&services, &[node("a"), node("b")], &existing, &[], 100);
    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(plan.assignments[0].deployment_id, "web-v2");
    assert_eq!(plan.assignments[0].created_at_ms, 100);
}

#[test]
fn unhealthy_replica_is_rescheduled_to_different_node() {
    let scheduler = DefaultScheduler::new();
    let services = vec![service("web", 1, 8080)];
    let nodes = vec![node("a"), node("b")];
    let existing = vec![Assignment {
        service_id: "web".to_string(),
        deployment_id: "web-dep".to_string(),
        replica_index: 0,
        node_id: "a".to_string(),
        port: 8080,
        created_at_ms: 50,
    }];
    let unhealthy = vec![ReplicaSlot {
        service_id: "web".to_string(),
        replica_index: 0,
    }];
    let plan = scheduler.plan(&services, &nodes, &existing, &unhealthy, 100);
    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(
        plan.assignments[0].node_id, "b",
        "unhealthy replica should move off node-a"
    );
}

#[test]
fn unschedulable_node_does_not_receive_new_replicas() {
    let scheduler = DefaultScheduler::new();
    let services = vec![service("web", 2, 8080)];
    let mut draining = node("node-a");
    draining.can_run_workloads = false;
    let plan = scheduler.plan(&services, &[draining, node("node-b")], &[], &[], 100);
    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(plan.assignments[0].node_id, "node-b");
}

#[test]
fn controller_only_node_does_not_get_workloads() {
    let scheduler = DefaultScheduler::new();
    let services = vec![service("web", 1, 8080)];
    let mut controller_node = node("controller-only");
    controller_node.can_run_workloads = false;
    let plan = scheduler.plan(&services, &[controller_node], &[], &[], 100);
    assert!(plan.assignments.is_empty());
    assert_eq!(plan.unschedulable.len(), 1);
}

#[test]
fn service_without_assigned_port_is_unschedulable() {
    let scheduler = DefaultScheduler::new();
    let services = vec![ServiceScheduleSpec {
        service_id: "web".to_string(),
        deployment_id: "web-dep".to_string(),
        desired_replicas: 1,
        node_affinity: None,
        assigned_port: None,
    }];
    let plan = scheduler.plan(&services, &[node("a")], &[], &[], 100);
    assert!(plan.assignments.is_empty());
    assert_eq!(plan.unschedulable.len(), 1);
}

#[tokio::test]
async fn port_allocator_returns_stable_port_across_calls() {
    let allocator = InMemoryPortAllocator::new(PortRange::new(20_000, 20_100));
    let first = allocator.allocate("svc-a").await.unwrap();
    let second = allocator.allocate("svc-a").await.unwrap();
    assert_eq!(first, second);
}

#[tokio::test]
async fn port_allocator_assigns_different_ports_to_different_services() {
    let allocator = InMemoryPortAllocator::new(PortRange::new(20_000, 20_100));
    let port_a = allocator.allocate("svc-a").await.unwrap();
    let port_b = allocator.allocate("svc-b").await.unwrap();
    assert_ne!(port_a, port_b);
}

#[tokio::test]
async fn port_allocator_returns_error_when_pool_exhausted() {
    let allocator = InMemoryPortAllocator::new(PortRange::new(20_000, 20_001));
    let _ = allocator.allocate("a").await.unwrap();
    let _ = allocator.allocate("b").await.unwrap();
    let result = allocator.allocate("c").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn port_allocator_release_frees_port() {
    let allocator = InMemoryPortAllocator::new(PortRange::new(20_000, 20_000));
    let first = allocator.allocate("a").await.unwrap();
    allocator.release("a").await.unwrap();
    let reused = allocator.allocate("b").await.unwrap();
    assert_eq!(first, reused);
}
