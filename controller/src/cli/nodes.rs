use serde::Deserialize;

use crate::cluster::NodeRole;
use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeView {
    node_id: String,
    hostname: String,
    role: NodeRole,
    cluster_host_ip: String,
    subnet: String,
    data_plane_ready: bool,
    data_plane_error: Option<String>,
    version: String,
}

pub async fn run_nodes(host: &str) -> Result<()> {
    let base = crate::cli::contexts::normalize_base_url(host)?;
    let client = crate::cli::contexts::build_http_client()?;
    let response = client
        .get(format!("{base}/api/cluster/nodes"))
        .send()
        .await
        .map_err(|err| Error::internal(format!("failed to query cluster nodes: {err}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::internal(format!(
            "cluster nodes request failed ({status}): {body}"
        )));
    }
    let nodes: Vec<NodeView> = response
        .json()
        .await
        .map_err(|err| Error::internal(format!("invalid cluster nodes response: {err}")))?;
    if nodes.is_empty() {
        println!("No live cluster nodes are registered.");
        return Ok(());
    }

    println!(
        "{:<14} {:<18} {:<8} {:<15} {:<18} {:<8} VERSION",
        "NODE", "HOSTNAME", "ROLE", "CONTROL IP", "SUBNET", "DATA"
    );
    for node in nodes {
        let data_plane = if node.data_plane_ready {
            "ready"
        } else {
            "unready"
        };
        println!(
            "{:<14} {:<18} {:<8} {:<15} {:<18} {:<8} {}",
            node.node_id,
            node.hostname,
            match node.role {
                NodeRole::Voter => "voter",
                NodeRole::Worker => "worker",
            },
            node.cluster_host_ip,
            node.subnet,
            data_plane,
            node.version,
        );
        if let Some(error) = node.data_plane_error.filter(|error| !error.is_empty()) {
            println!("  data-plane error: {error}");
        }
    }
    Ok(())
}

pub async fn set_drain_state(host: &str, node_id: &str, drain: bool) -> Result<()> {
    let node_id = node_id.trim();
    if node_id.is_empty() {
        return Err(Error::invalid_input("node id cannot be empty"));
    }
    let base = crate::cli::contexts::normalize_base_url(host)?;
    let operation = if drain { "drain" } else { "restore" };
    let request = crate::cli::contexts::build_http_client()?
        .post(format!("{base}/api/cluster/nodes/{node_id}/{operation}"));
    let response = crate::cli::idempotent(request)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to {operation} node: {err}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "node {operation} failed ({status}): {body}"
        )));
    }
    println!("[maestro]: node `{node_id}` {operation} accepted");
    Ok(())
}
