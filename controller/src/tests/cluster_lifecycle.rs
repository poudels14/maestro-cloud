//! End-to-end tests for the cluster module using [`InMemoryCluster`] —
//! exercises node registration, heartbeat expiry, leader election, failover.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use crate::cluster::elector::LeaderElector;
use crate::cluster::in_memory::{InMemoryCluster, InMemoryLeaderElector, InMemoryNodeRegistry};
use crate::cluster::registry::NodeRegistry;
use crate::cluster::service::{ClusterService, wait_for_leadership};
use crate::cluster::types::{LeadershipState, NodeId, NodeInfo, NodeRole};
use crate::signal::ShutdownEvent;

fn make_node(node_id: &str, role: NodeRole, started_at_ms: u64) -> NodeInfo {
    NodeInfo {
        node_id: node_id.to_string(),
        hostname: format!("{node_id}-host"),
        role,
        tailscale_ip: None,
        api_port: 3001,
        version: "test".to_string(),
        started_at_ms,
        labels: Default::default(),
        unschedulable: false,
    }
}

fn build_service(
    cluster: &InMemoryCluster,
    info: NodeInfo,
) -> (
    ClusterService,
    Arc<InMemoryLeaderElector>,
    Arc<InMemoryNodeRegistry>,
) {
    let registry = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        info.node_id.clone(),
    ));
    let elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        info.node_id.clone(),
    ));
    let service = ClusterService::new(
        info,
        registry.clone() as Arc<dyn NodeRegistry>,
        elector.clone() as Arc<dyn LeaderElector>,
    )
    .with_heartbeat_interval(Duration::from_millis(20));
    (service, elector, registry)
}

#[tokio::test]
async fn single_node_registers_and_becomes_leader() {
    let cluster = InMemoryCluster::new();
    let info = make_node("node-a", NodeRole::Both, 100);
    let (service, elector, _registry) = build_service(&cluster, info.clone());
    let (shutdown_tx, _) = broadcast::channel::<ShutdownEvent>(4);
    let handles = service.clone().spawn(shutdown_tx.subscribe());

    let elected = wait_for_leadership(elector.as_ref(), Duration::from_secs(2))
        .await
        .expect("wait_for_leadership");
    assert!(elected, "single node should win election");

    let nodes = service.registry().list_nodes().await.expect("list_nodes");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].node_id, "node-a");

    handles.abort();
}

#[tokio::test]
async fn three_nodes_elect_exactly_one_leader() {
    let cluster = InMemoryCluster::new();
    let (shutdown_tx, _) = broadcast::channel::<ShutdownEvent>(4);

    let node_a = make_node("node-a", NodeRole::Both, 100);
    let node_b = make_node("node-b", NodeRole::Both, 200);
    let node_c = make_node("node-c", NodeRole::Both, 300);

    let (svc_a, elector_a, _) = build_service(&cluster, node_a.clone());
    let (svc_b, elector_b, _) = build_service(&cluster, node_b.clone());
    let (svc_c, elector_c, _) = build_service(&cluster, node_c.clone());

    let handles_a = svc_a.spawn(shutdown_tx.subscribe());
    let handles_b = svc_b.spawn(shutdown_tx.subscribe());
    let handles_c = svc_c.spawn(shutdown_tx.subscribe());

    tokio::time::sleep(Duration::from_millis(100)).await;

    let leaders: Vec<&dyn LeaderElector> =
        vec![elector_a.as_ref(), elector_b.as_ref(), elector_c.as_ref()];
    let leader_count = leaders
        .iter()
        .filter(|elector| matches!(elector.state(), LeadershipState::Leading(_)))
        .count();
    assert_eq!(
        leader_count, 1,
        "exactly one node should be leader; saw {leader_count}"
    );
    let leader_node = cluster
        .current_leader()
        .expect("a leader should be elected")
        .node_id;
    let followers: Vec<NodeId> = ["node-a", "node-b", "node-c"]
        .iter()
        .map(|id| id.to_string())
        .filter(|id| id != &leader_node)
        .collect();
    for node_id in &followers {
        let elector = match node_id.as_str() {
            "node-a" => elector_a.as_ref(),
            "node-b" => elector_b.as_ref(),
            "node-c" => elector_c.as_ref(),
            _ => unreachable!(),
        };
        let state = elector.state();
        match state {
            LeadershipState::Following(Some(info)) => {
                assert_eq!(info.node_id, leader_node);
            }
            other => panic!("expected {node_id} to be following, got {other:?}"),
        }
    }

    handles_a.abort();
    handles_b.abort();
    handles_c.abort();
}

#[tokio::test]
async fn worker_only_node_does_not_lead() {
    let cluster = InMemoryCluster::new();
    let (shutdown_tx, _) = broadcast::channel::<ShutdownEvent>(4);

    let worker = make_node("worker-1", NodeRole::Worker, 50);
    let controller = make_node("controller-1", NodeRole::Controller, 100);

    let (worker_svc, worker_elector, _) = build_service(&cluster, worker.clone());
    let (controller_svc, controller_elector, _) = build_service(&cluster, controller.clone());

    let worker_handles = worker_svc.spawn(shutdown_tx.subscribe());
    let controller_handles = controller_svc.spawn(shutdown_tx.subscribe());

    let elected = wait_for_leadership(controller_elector.as_ref(), Duration::from_secs(2))
        .await
        .expect("controller wait");
    assert!(elected);

    let worker_state = worker_elector.state();
    assert!(
        matches!(
            worker_state,
            LeadershipState::Following(_) | LeadershipState::Unknown
        ),
        "worker-only node should never lead, was {worker_state:?}"
    );
    assert_eq!(cluster.current_leader().unwrap().node_id, "controller-1");

    worker_handles.abort();
    controller_handles.abort();
}

#[tokio::test]
async fn heartbeat_expiry_removes_node_and_triggers_failover() {
    let cluster = InMemoryCluster::new();

    let leader_info = make_node("leader-a", NodeRole::Both, 100);
    let follower_info = make_node("follower-b", NodeRole::Both, 200);

    let leader_registry = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        leader_info.node_id.clone(),
    ));
    let leader_elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        leader_info.node_id.clone(),
    ));
    let follower_registry = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        follower_info.node_id.clone(),
    ));
    let follower_elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        follower_info.node_id.clone(),
    ));

    leader_registry
        .register(&leader_info)
        .await
        .expect("register leader");
    follower_registry
        .register(&follower_info)
        .await
        .expect("register follower");

    leader_elector.campaign().await.expect("leader campaign");
    assert!(matches!(
        leader_elector.state(),
        LeadershipState::Leading(_)
    ));

    cluster.advance_clock(11_000);

    let nodes = leader_registry.list_nodes().await.expect("list");
    assert_eq!(nodes.len(), 0, "all heartbeats should have expired");

    follower_registry
        .register(&follower_info)
        .await
        .expect("re-register follower");
    follower_elector
        .campaign()
        .await
        .expect("failover campaign");
    assert!(matches!(
        follower_elector.state(),
        LeadershipState::Leading(_)
    ));
    assert_eq!(
        cluster.current_leader().unwrap().node_id,
        "follower-b",
        "follower should take over after leader heartbeat expiry"
    );
}

#[tokio::test]
async fn resigning_leader_triggers_election_of_another_node() {
    let cluster = InMemoryCluster::new();

    let info_a = make_node("a", NodeRole::Both, 100);
    let info_b = make_node("b", NodeRole::Both, 200);

    let registry_a = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        info_a.node_id.clone(),
    ));
    let elector_a = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        info_a.node_id.clone(),
    ));
    let registry_b = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        info_b.node_id.clone(),
    ));
    let elector_b = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        info_b.node_id.clone(),
    ));

    registry_a.register(&info_a).await.unwrap();
    registry_b.register(&info_b).await.unwrap();

    elector_a.campaign().await.unwrap();
    assert_eq!(cluster.current_leader().unwrap().node_id, "a");

    elector_a.resign().await.unwrap();

    assert_eq!(
        cluster.current_leader().unwrap().node_id,
        "b",
        "after a resigns, b should be elected"
    );
    assert!(
        matches!(elector_b.state(), LeadershipState::Leading(_)),
        "b's watcher should observe its own leadership; got {:?}",
        elector_b.state()
    );
    match elector_a.state() {
        LeadershipState::Following(Some(info)) => assert_eq!(info.node_id, "b"),
        other => panic!("a should be Following b, got {other:?}"),
    }
}

#[tokio::test]
async fn registry_reports_all_active_nodes() {
    let cluster = InMemoryCluster::new();
    let node_a = make_node("a", NodeRole::Both, 100);
    let node_b = make_node("b", NodeRole::Worker, 200);
    let node_c = make_node("c", NodeRole::Controller, 300);

    let registry_a = InMemoryNodeRegistry::new(cluster.clone(), node_a.node_id.clone());
    let registry_b = InMemoryNodeRegistry::new(cluster.clone(), node_b.node_id.clone());
    let registry_c = InMemoryNodeRegistry::new(cluster.clone(), node_c.node_id.clone());

    registry_a.register(&node_a).await.unwrap();
    registry_b.register(&node_b).await.unwrap();
    registry_c.register(&node_c).await.unwrap();

    let nodes = registry_a.list_nodes().await.unwrap();
    assert_eq!(nodes.len(), 3);
    assert_eq!(
        nodes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c"]
    );

    registry_b.deregister().await.unwrap();
    let nodes = registry_a.list_nodes().await.unwrap();
    assert_eq!(nodes.len(), 2);
    assert!(
        nodes.iter().all(|node| node.node_id != "b"),
        "b should be gone after deregister"
    );
}

#[tokio::test]
async fn leader_watcher_observes_transitions() {
    let cluster = InMemoryCluster::new();
    let info_a = make_node("a", NodeRole::Both, 100);
    let info_b = make_node("b", NodeRole::Both, 200);

    let registry_a = InMemoryNodeRegistry::new(cluster.clone(), info_a.node_id.clone());
    let elector_a = InMemoryLeaderElector::new(cluster.clone(), info_a.node_id.clone());
    let registry_b = InMemoryNodeRegistry::new(cluster.clone(), info_b.node_id.clone());
    let elector_b = InMemoryLeaderElector::new(cluster.clone(), info_b.node_id.clone());

    let mut watcher_b = elector_b.subscribe();
    registry_a.register(&info_a).await.unwrap();
    registry_b.register(&info_b).await.unwrap();

    elector_a.campaign().await.unwrap();
    watcher_b
        .changed()
        .await
        .expect("watcher should see leader change");
    match watcher_b.borrow().clone() {
        LeadershipState::Following(Some(info)) => assert_eq!(info.node_id, "a"),
        other => panic!("expected Following(a), got {other:?}"),
    }
}
