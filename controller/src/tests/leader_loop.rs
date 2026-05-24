//! End-to-end integration test: services + nodes flow through the full
//! pipeline (catalog → port allocation → scheduling → assignment store →
//! Traefik aggregator) using only in-memory implementations.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::cluster::assignment_store::{AssignmentStore, InMemoryAssignmentStore};
use crate::cluster::elector::LeaderElector;
use crate::cluster::in_memory::{InMemoryCluster, InMemoryLeaderElector, InMemoryNodeRegistry};
use crate::cluster::leader_loop::{LeaderLoop, ServiceCatalog, TraefikConfigSink};
use crate::cluster::port_allocator::{InMemoryPortAllocator, PortRange};
use crate::cluster::registry::NodeRegistry;
use crate::cluster::scheduler::DefaultScheduler;
use crate::cluster::traefik_aggregator::TraefikDynamicConfig;
use crate::cluster::types::{NodeInfo, NodeRole};
use crate::deployment::types::{Command, IngressConfig, ServiceConfig, ServiceDeployConfig};

#[derive(Default)]
struct StaticCatalog {
    services: Mutex<Vec<ServiceConfig>>,
    deployment_ids: Mutex<std::collections::HashMap<String, String>>,
}

#[async_trait]
impl ServiceCatalog for StaticCatalog {
    async fn list(&self) -> Result<Vec<ServiceConfig>> {
        Ok(self.services.lock().unwrap().clone())
    }
    async fn current_deployment_id(&self, service_id: &str) -> Result<Option<String>> {
        Ok(self.deployment_ids.lock().unwrap().get(service_id).cloned())
    }
}

#[derive(Default)]
struct CapturingTraefikSink {
    last: Mutex<Option<TraefikDynamicConfig>>,
}

#[async_trait]
impl TraefikConfigSink for CapturingTraefikSink {
    async fn write(&self, config: &TraefikDynamicConfig) -> Result<()> {
        *self.last.lock().unwrap() = Some(config.clone());
        Ok(())
    }
}

fn node_info(node_id: &str, ip: &str, started_at_ms: u64) -> NodeInfo {
    NodeInfo {
        node_id: node_id.to_string(),
        hostname: format!("{node_id}.local"),
        role: NodeRole::Both,
        tailscale_ip: Some(ip.to_string()),
        api_port: 3001,
        version: "test".to_string(),
        started_at_ms,
        labels: BTreeMap::new(),
        unschedulable: false,
    }
}

fn web_service(id: &str, replicas: u32, host: &str) -> ServiceConfig {
    ServiceConfig {
        id: id.to_string(),
        name: id.to_string(),
        version: "v1".to_string(),
        build: None,
        image: Some("example:latest".to_string()),
        deploy: ServiceDeployConfig {
            flags: vec![],
            expose_ports: vec![],
            command: Some(Command {
                command: "run".to_string(),
                args: vec![],
            }),
            healthcheck_path: None,
            healthcheck_interval: 60,
            replicas,
            max_restarts: None,
            env: Default::default(),
            secrets: None,
            volumes: vec![],
            node_affinity: None,
        },
        ingress: Some(IngressConfig {
            host: Some(host.to_string()),
            hosts: vec![],
            port: None,
        }),
    }
}

#[tokio::test]
async fn full_pipeline_assigns_replicas_and_emits_traefik_config() {
    let cluster = InMemoryCluster::new();
    let leader_info = node_info("node-a", "100.64.0.1", 100);
    let follower_info = node_info("node-b", "100.64.0.2", 200);
    let follower_c_info = node_info("node-c", "100.64.0.3", 300);

    let registry_a = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        "node-a".to_string(),
    ));
    let registry_b = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        "node-b".to_string(),
    ));
    let registry_c = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        "node-c".to_string(),
    ));
    registry_a.register(&leader_info).await.unwrap();
    registry_b.register(&follower_info).await.unwrap();
    registry_c.register(&follower_c_info).await.unwrap();

    let elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        "node-a".to_string(),
    ));
    elector.campaign().await.unwrap();

    let catalog = Arc::new(StaticCatalog::default());
    catalog
        .services
        .lock()
        .unwrap()
        .push(web_service("web", 3, "web.example.com"));
    catalog
        .services
        .lock()
        .unwrap()
        .push(web_service("api", 2, "api.example.com"));
    catalog
        .deployment_ids
        .lock()
        .unwrap()
        .insert("web".to_string(), "web-d1".to_string());
    catalog
        .deployment_ids
        .lock()
        .unwrap()
        .insert("api".to_string(), "api-d1".to_string());

    let traefik_sink = Arc::new(CapturingTraefikSink::default());
    let assignments = Arc::new(InMemoryAssignmentStore::new());
    let leader_loop = LeaderLoop {
        catalog: catalog.clone(),
        registry: registry_a.clone() as Arc<dyn NodeRegistry>,
        assignments: assignments.clone() as Arc<dyn AssignmentStore>,
        port_allocator: Arc::new(InMemoryPortAllocator::new(PortRange::new(20_000, 20_100))),
        scheduler: Arc::new(DefaultScheduler::new()),
        traefik_sink: traefik_sink.clone(),
        elector: elector.clone() as Arc<dyn LeaderElector>,
        tick_interval: Duration::from_millis(50),
        default_entry_point: "web".to_string(),
        on_plan_applied: None,
        metrics: None,
    };

    let plan = leader_loop
        .run_once(1_000)
        .await
        .expect("plan should succeed");
    assert_eq!(plan.assignments.len(), 5, "3 web + 2 api replicas");

    let node_a_assignments = assignments
        .list_for_node(&"node-a".to_string())
        .await
        .unwrap();
    let node_b_assignments = assignments
        .list_for_node(&"node-b".to_string())
        .await
        .unwrap();
    let node_c_assignments = assignments
        .list_for_node(&"node-c".to_string())
        .await
        .unwrap();
    let total: usize =
        node_a_assignments.len() + node_b_assignments.len() + node_c_assignments.len();
    assert_eq!(total, 5);

    let traefik = traefik_sink
        .last
        .lock()
        .unwrap()
        .clone()
        .expect("traefik config emitted");
    assert!(traefik.http.services.contains_key("web"));
    assert!(traefik.http.services.contains_key("api"));
    let web_servers = &traefik
        .http
        .services
        .get("web")
        .unwrap()
        .load_balancer
        .servers;
    assert_eq!(web_servers.len(), 3);
}

#[tokio::test]
async fn re_running_pipeline_is_stable_when_inputs_unchanged() {
    let cluster = InMemoryCluster::new();
    let leader_info = node_info("node-a", "100.64.0.1", 100);
    let follower_info = node_info("node-b", "100.64.0.2", 200);
    let registry_a = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        "node-a".to_string(),
    ));
    let registry_b = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        "node-b".to_string(),
    ));
    registry_a.register(&leader_info).await.unwrap();
    registry_b.register(&follower_info).await.unwrap();
    let elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        "node-a".to_string(),
    ));
    elector.campaign().await.unwrap();
    let catalog = Arc::new(StaticCatalog::default());
    catalog
        .services
        .lock()
        .unwrap()
        .push(web_service("web", 2, "web.example.com"));
    catalog
        .deployment_ids
        .lock()
        .unwrap()
        .insert("web".to_string(), "web-d1".to_string());

    let leader_loop = LeaderLoop {
        catalog: catalog.clone(),
        registry: registry_a.clone() as Arc<dyn NodeRegistry>,
        assignments: Arc::new(InMemoryAssignmentStore::new()) as Arc<dyn AssignmentStore>,
        port_allocator: Arc::new(InMemoryPortAllocator::new(PortRange::new(20_000, 20_100))),
        scheduler: Arc::new(DefaultScheduler::new()),
        traefik_sink: Arc::new(CapturingTraefikSink::default()),
        elector: elector.clone() as Arc<dyn LeaderElector>,
        tick_interval: Duration::from_millis(50),
        default_entry_point: "web".to_string(),
        on_plan_applied: None,
        metrics: None,
    };
    let plan_1 = leader_loop.run_once(1_000).await.unwrap();
    let plan_2 = leader_loop.run_once(2_000).await.unwrap();
    let nodes_first: Vec<String> = plan_1
        .assignments
        .iter()
        .map(|assignment| assignment.node_id.clone())
        .collect();
    let nodes_second: Vec<String> = plan_2
        .assignments
        .iter()
        .map(|assignment| assignment.node_id.clone())
        .collect();
    assert_eq!(
        nodes_first, nodes_second,
        "sticky placement should keep nodes stable"
    );
}

#[tokio::test]
async fn sweep_drops_assignments_for_vanished_nodes() {
    use crate::cluster::scheduling::Assignment;
    let cluster = InMemoryCluster::new();
    let leader_info = node_info("node-a", "100.64.0.1", 100);
    let registry_a = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        "node-a".to_string(),
    ));
    registry_a.register(&leader_info).await.unwrap();
    let elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        "node-a".to_string(),
    ));
    elector.campaign().await.unwrap();

    let assignments = Arc::new(InMemoryAssignmentStore::new());
    let stale_assignments = vec![Assignment {
        service_id: "web".to_string(),
        deployment_id: "web-d1".to_string(),
        replica_index: 0,
        node_id: "ghost-node".to_string(),
        port: 20_000,
        created_at_ms: 0,
    }];
    assignments
        .replace_for_node(&"ghost-node".to_string(), &stale_assignments)
        .await
        .unwrap();

    let leader_loop = LeaderLoop {
        catalog: Arc::new(StaticCatalog::default()),
        registry: registry_a as Arc<dyn NodeRegistry>,
        assignments: assignments.clone() as Arc<dyn AssignmentStore>,
        port_allocator: Arc::new(InMemoryPortAllocator::new(PortRange::new(20_000, 20_100))),
        scheduler: Arc::new(DefaultScheduler::new()),
        traefik_sink: Arc::new(CapturingTraefikSink::default()),
        elector: elector as Arc<dyn LeaderElector>,
        tick_interval: Duration::from_millis(50),
        default_entry_point: "web".to_string(),
        on_plan_applied: None,
        metrics: None,
    };
    leader_loop.run_once(1_000).await.unwrap();

    let ghost_assignments = assignments
        .list_for_node(&"ghost-node".to_string())
        .await
        .unwrap();
    assert!(
        ghost_assignments.is_empty(),
        "assignments for vanished node should be cleared, got {ghost_assignments:?}"
    );
}

#[tokio::test]
async fn four_node_cluster_only_schedules_to_three() {
    let cluster = InMemoryCluster::new();
    let leader = node_info("node-a", "100.64.0.1", 100);
    let nodes = [
        node_info("node-b", "100.64.0.2", 200),
        node_info("node-c", "100.64.0.3", 300),
        node_info("node-d", "100.64.0.4", 400),
    ];
    let registries: Vec<_> = std::iter::once(&leader)
        .chain(nodes.iter())
        .map(|info| {
            let registry = Arc::new(InMemoryNodeRegistry::new(
                cluster.clone(),
                info.node_id.clone(),
            ));
            (registry, info.clone())
        })
        .collect();
    for (registry, info) in &registries {
        registry.register(info).await.unwrap();
    }
    let elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        "node-a".to_string(),
    ));
    elector.campaign().await.unwrap();

    let catalog = Arc::new(StaticCatalog::default());
    catalog
        .services
        .lock()
        .unwrap()
        .push(web_service("web", 4, "web.example.com"));
    catalog
        .deployment_ids
        .lock()
        .unwrap()
        .insert("web".to_string(), "web-d1".to_string());

    let assignments = Arc::new(InMemoryAssignmentStore::new());
    let leader_loop = LeaderLoop {
        catalog,
        registry: registries[0].0.clone() as Arc<dyn NodeRegistry>,
        assignments: assignments.clone() as Arc<dyn AssignmentStore>,
        port_allocator: Arc::new(InMemoryPortAllocator::new(PortRange::new(20_000, 20_100))),
        scheduler: Arc::new(DefaultScheduler::new()),
        traefik_sink: Arc::new(CapturingTraefikSink::default()),
        elector: elector as Arc<dyn LeaderElector>,
        tick_interval: Duration::from_millis(50),
        default_entry_point: "web".to_string(),
        on_plan_applied: None,
        metrics: None,
    };
    let plan = leader_loop.run_once(1_000).await.unwrap();
    assert_eq!(
        plan.assignments.len(),
        3,
        "4 desired replicas, but quorum filter drops node-d → 3 placements"
    );
    let used_nodes: std::collections::HashSet<&str> = plan
        .assignments
        .iter()
        .map(|assignment| assignment.node_id.as_str())
        .collect();
    assert!(
        !used_nodes.contains("node-d"),
        "node-d (lex-last) should be excluded"
    );
}

#[tokio::test]
async fn run_once_fails_when_not_leader() {
    let cluster = InMemoryCluster::new();
    let info = node_info("node-a", "100.64.0.1", 100);
    let registry = Arc::new(InMemoryNodeRegistry::new(
        cluster.clone(),
        "node-a".to_string(),
    ));
    registry.register(&info).await.unwrap();
    let elector = Arc::new(InMemoryLeaderElector::new(
        cluster.clone(),
        "node-a".to_string(),
    ));

    let leader_loop = LeaderLoop {
        catalog: Arc::new(StaticCatalog::default()),
        registry: registry as Arc<dyn NodeRegistry>,
        assignments: Arc::new(InMemoryAssignmentStore::new()) as Arc<dyn AssignmentStore>,
        port_allocator: Arc::new(InMemoryPortAllocator::new(PortRange::new(20_000, 20_100))),
        scheduler: Arc::new(DefaultScheduler::new()),
        traefik_sink: Arc::new(CapturingTraefikSink::default()),
        elector: elector as Arc<dyn LeaderElector>,
        tick_interval: Duration::from_millis(50),
        default_entry_point: "web".to_string(),
        on_plan_applied: None,
        metrics: None,
    };
    let result = leader_loop.run_once(0).await;
    assert!(
        result.is_err(),
        "non-leader should refuse to run scheduling"
    );
}
