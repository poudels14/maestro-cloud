//! Tests for [`build_traefik_config`].

use std::collections::BTreeMap;

use crate::cluster::scheduling::Assignment;
use crate::cluster::traefik_aggregator::{AggregatorInput, build_traefik_config};
use crate::cluster::types::{NodeInfo, NodeRole};
use crate::deployment::types::{Command, IngressConfig, ServiceConfig, ServiceDeployConfig};

fn node_info(node_id: &str, tailscale_ip: Option<&str>) -> NodeInfo {
    NodeInfo {
        node_id: node_id.to_string(),
        hostname: format!("{node_id}.local"),
        role: NodeRole::Both,
        tailscale_ip: tailscale_ip.map(str::to_string),
        api_port: 3001,
        version: "test".to_string(),
        started_at_ms: 0,
        labels: BTreeMap::new(),
        unschedulable: false,
    }
}

fn service_with_ingress(id: &str, hosts: Vec<&str>) -> ServiceConfig {
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
            replicas: 1,
            max_restarts: None,
            env: Default::default(),
            secrets: None,
            volumes: vec![],
            node_affinity: None,
        },
        ingress: Some(IngressConfig {
            host: hosts.first().map(|host| host.to_string()),
            hosts: hosts.iter().skip(1).map(|host| host.to_string()).collect(),
            port: None,
        }),
    }
}

fn assignment(service_id: &str, replica_index: u32, node_id: &str, port: u16) -> Assignment {
    Assignment {
        service_id: service_id.to_string(),
        deployment_id: format!("{service_id}-dep"),
        replica_index,
        node_id: node_id.to_string(),
        port,
        created_at_ms: 0,
    }
}

#[test]
fn builds_router_and_service_per_ingress_service() {
    let services = vec![service_with_ingress("web", vec!["web.example.com"])];
    let assignments = vec![assignment("web", 0, "node-a", 8080)];
    let nodes = vec![node_info("node-a", Some("100.64.0.1"))];
    let config = build_traefik_config(AggregatorInput {
        services: &services,
        assignments: &assignments,
        nodes: &nodes,
        default_entry_point: "web",
    });
    assert_eq!(config.http.routers.len(), 1);
    assert_eq!(config.http.services.len(), 1);
    let router = config.http.routers.values().next().unwrap();
    assert_eq!(router.rule, "Host(`web.example.com`)");
    let service = config.http.services.get("web").expect("service exists");
    assert_eq!(service.load_balancer.servers.len(), 1);
    assert_eq!(
        service.load_balancer.servers[0].url,
        "http://100.64.0.1:8080"
    );
}

#[test]
fn aggregates_servers_across_multiple_nodes_for_same_service() {
    let services = vec![service_with_ingress("web", vec!["web.example.com"])];
    let assignments = vec![
        assignment("web", 0, "node-a", 8080),
        assignment("web", 1, "node-b", 8080),
        assignment("web", 2, "node-c", 8080),
    ];
    let nodes = vec![
        node_info("node-a", Some("100.64.0.1")),
        node_info("node-b", Some("100.64.0.2")),
        node_info("node-c", Some("100.64.0.3")),
    ];
    let config = build_traefik_config(AggregatorInput {
        services: &services,
        assignments: &assignments,
        nodes: &nodes,
        default_entry_point: "web",
    });
    let service = config.http.services.get("web").unwrap();
    let urls: Vec<&str> = service
        .load_balancer
        .servers
        .iter()
        .map(|server| server.url.as_str())
        .collect();
    assert!(urls.contains(&"http://100.64.0.1:8080"));
    assert!(urls.contains(&"http://100.64.0.2:8080"));
    assert!(urls.contains(&"http://100.64.0.3:8080"));
}

#[test]
fn falls_back_to_hostname_when_tailscale_ip_missing() {
    let services = vec![service_with_ingress("web", vec!["web.example.com"])];
    let assignments = vec![assignment("web", 0, "node-a", 8080)];
    let nodes = vec![node_info("node-a", None)];
    let config = build_traefik_config(AggregatorInput {
        services: &services,
        assignments: &assignments,
        nodes: &nodes,
        default_entry_point: "web",
    });
    let server = &config
        .http
        .services
        .get("web")
        .unwrap()
        .load_balancer
        .servers[0];
    assert_eq!(server.url, "http://node-a.local:8080");
}

#[test]
fn creates_router_per_host() {
    let services = vec![service_with_ingress(
        "web",
        vec!["a.example.com", "b.example.com"],
    )];
    let assignments = vec![assignment("web", 0, "node-a", 8080)];
    let nodes = vec![node_info("node-a", Some("100.64.0.1"))];
    let config = build_traefik_config(AggregatorInput {
        services: &services,
        assignments: &assignments,
        nodes: &nodes,
        default_entry_point: "web",
    });
    assert_eq!(config.http.routers.len(), 2);
}

#[test]
fn services_without_ingress_or_assignments_are_skipped() {
    let services = vec![
        service_with_ingress("web", vec!["web.example.com"]),
        ServiceConfig {
            id: "internal".to_string(),
            name: "internal".to_string(),
            version: "v1".to_string(),
            build: None,
            image: Some("example:latest".to_string()),
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports: vec![],
                command: None,
                healthcheck_path: None,
                healthcheck_interval: 60,
                replicas: 1,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
                volumes: vec![],
                node_affinity: None,
            },
            ingress: None,
        },
    ];
    let assignments = vec![assignment("web", 0, "node-a", 8080)];
    let nodes = vec![node_info("node-a", Some("100.64.0.1"))];
    let config = build_traefik_config(AggregatorInput {
        services: &services,
        assignments: &assignments,
        nodes: &nodes,
        default_entry_point: "web",
    });
    assert_eq!(config.http.routers.len(), 1);
    assert!(config.http.services.contains_key("web"));
    assert!(!config.http.services.contains_key("internal"));
}
