//! Tests for in-memory-driveable parts of the cluster adapters —
//! specifically the [`ClusterDnsWriter`] which only depends on a [`DnsManager`].

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::cluster::adapters::ClusterDnsWriter;
use crate::cluster::scheduling::Assignment;
use crate::cluster::types::{NodeInfo, NodeRole};
use crate::deployment::dns::DnsManager;

fn node(node_id: &str, ip: Option<&str>) -> NodeInfo {
    NodeInfo {
        node_id: node_id.to_string(),
        hostname: format!("{node_id}.local"),
        role: NodeRole::Both,
        tailscale_ip: ip.map(str::to_string),
        api_port: 3001,
        version: "test".to_string(),
        started_at_ms: 0,
        labels: BTreeMap::new(),
        unschedulable: false,
    }
}

fn assignment(service_id: &str, replica_index: u32, node_id: &str) -> Assignment {
    Assignment {
        service_id: service_id.to_string(),
        deployment_id: format!("{service_id}-dep"),
        replica_index,
        node_id: node_id.to_string(),
        port: 9000,
        created_at_ms: 0,
    }
}

#[test]
fn dns_writer_records_all_node_ips_for_a_service() {
    let temp_dir = std::env::temp_dir().join(format!("dns-writer-test-{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let dns_manager = Arc::new(DnsManager::new(temp_dir.clone()));
    let writer = ClusterDnsWriter::new(dns_manager.clone(), "test.maestro.internal".to_string());

    let assignments = vec![
        assignment("web", 0, "node-a"),
        assignment("web", 1, "node-b"),
    ];
    let nodes = vec![
        node("node-a", Some("100.64.0.1")),
        node("node-b", Some("100.64.0.2")),
    ];
    writer.apply(&assignments, &nodes);

    let resolved = dns_manager.lookup("web", "test.maestro.internal");
    assert_eq!(resolved.len(), 2);
    assert!(resolved.contains(&"100.64.0.1".to_string()));
    assert!(resolved.contains(&"100.64.0.2".to_string()));
}

#[test]
fn dns_writer_falls_back_to_hostname_when_no_ip() {
    let temp_dir = std::env::temp_dir().join(format!("dns-writer-fallback-{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let dns_manager = Arc::new(DnsManager::new(temp_dir));
    let writer = ClusterDnsWriter::new(dns_manager.clone(), "test.maestro.internal".to_string());

    let assignments = vec![assignment("web", 0, "node-a")];
    let nodes = vec![node("node-a", None)];
    writer.apply(&assignments, &nodes);

    let resolved = dns_manager.lookup("web", "test.maestro.internal");
    assert_eq!(resolved, vec!["node-a.local".to_string()]);
}

#[test]
fn dns_writer_ignores_assignments_for_unknown_nodes() {
    let temp_dir = std::env::temp_dir().join(format!("dns-writer-unknown-{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let dns_manager = Arc::new(DnsManager::new(temp_dir));
    let writer = ClusterDnsWriter::new(dns_manager.clone(), "test.maestro.internal".to_string());

    let assignments = vec![
        assignment("web", 0, "node-a"),
        assignment("web", 1, "ghost"),
    ];
    let nodes = vec![node("node-a", Some("100.64.0.1"))];
    writer.apply(&assignments, &nodes);

    let resolved = dns_manager.lookup("web", "test.maestro.internal");
    assert_eq!(resolved, vec!["100.64.0.1".to_string()]);
}
