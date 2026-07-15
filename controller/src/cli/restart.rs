use std::io::IsTerminal;

use serde::{Deserialize, Serialize};

use crate::cli::contexts;
use crate::cluster::NodeRole;
use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
struct RestartResponse {
    accepted: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestartClusterInfo {
    #[serde(default)]
    this_node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestartNode {
    node_id: String,
    hostname: String,
    role: NodeRole,
    version: String,
}

impl std::fmt::Display for RestartNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}  {}  {}  Maestro {}",
            self.node_id, self.hostname, self.role, self.version
        )
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterRestartRequest<'a> {
    node_id: Option<&'a str>,
    all: bool,
}

pub async fn run_coordinated_restart(
    host: &str,
    node_id: Option<&str>,
    all: bool,
    yes: bool,
) -> Result<()> {
    let node_id = if all {
        None
    } else if let Some(node_id) = node_id {
        let node_id = node_id.trim();
        if node_id.is_empty() {
            return Err(Error::invalid_input("node id cannot be empty"));
        }
        Some(node_id.to_string())
    } else if cluster_node_id(host).await?.is_none() {
        return run_restart(host, yes).await;
    } else {
        Some(select_restart_node(host).await?)
    };
    let scope = node_id.as_deref().map_or_else(
        || "every cluster node".to_string(),
        |node_id| format!("cluster node `{node_id}`"),
    );
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        &format!("About to gracefully restart {scope}"),
        &[
            "Deploys will be frozen for the duration of the run".to_string(),
            "Workloads will drain before each selected node restarts".to_string(),
        ],
        yes,
    )
    .await?;
    if !confirmed {
        println!("[maestro]: aborted");
        return Ok(());
    }

    let base = normalize_base_url(host)?;
    let response = crate::cli::idempotent(
        contexts::build_http_client()?.post(format!("{base}/api/cluster/restart")),
    )
    .json(&ClusterRestartRequest {
        node_id: node_id.as_deref(),
        all: node_id.is_none(),
    })
    .send()
    .await
    .map_err(|error| Error::external(format!("failed to start cluster restart: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "cluster restart was rejected ({status}): {body}"
        )));
    }
    let run: crate::cluster::UpgradeRun = response
        .json()
        .await
        .map_err(|error| Error::external(format!("invalid cluster restart response: {error}")))?;
    println!("[maestro]: cluster restart `{}` started", run.run_id);
    crate::cli::upgrade::stream_cluster_maintenance(&base, run).await
}

async fn cluster_node_id(host: &str) -> Result<Option<String>> {
    let base = normalize_base_url(host)?;
    let response = contexts::build_http_client()?
        .get(format!("{base}/api/cluster"))
        .send()
        .await
        .map_err(|error| Error::external(format!("failed to inspect cluster mode: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "cluster info request failed ({status}): {body}"
        )));
    }
    response
        .json::<RestartClusterInfo>()
        .await
        .map(|info| info.this_node_id)
        .map_err(|error| Error::external(format!("invalid cluster info response: {error}")))
}

async fn select_restart_node(host: &str) -> Result<String> {
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Err(Error::invalid_input(
            "a node id or --all is required when input is not interactive",
        ));
    }
    let base = normalize_base_url(host)?;
    let response = contexts::build_http_client()?
        .get(format!("{base}/api/cluster/nodes"))
        .send()
        .await
        .map_err(|error| Error::external(format!("failed to list cluster nodes: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "cluster nodes request failed ({status}): {body}"
        )));
    }
    let mut nodes = response
        .json::<Vec<RestartNode>>()
        .await
        .map_err(|error| Error::external(format!("invalid cluster nodes response: {error}")))?;
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    if nodes.is_empty() {
        return Err(Error::external("no live cluster nodes are registered"));
    }
    inquire::Select::new("Select a cluster node to restart", nodes)
        .prompt()
        .map(|node| node.node_id)
        .map_err(|error| Error::external(format!("node selection failed: {error}")))
}

pub async fn run_restart(host: &str, yes: bool) -> Result<()> {
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        "About to restart the controller (all containers will be stopped)",
        &[],
        yes,
    )
    .await?;
    if !confirmed {
        println!("[maestro]: aborted");
        return Ok(());
    }

    let base = normalize_base_url(host)?;
    let endpoint = format!("{base}/api/system/restart");
    let response = crate::cli::idempotent(contexts::build_http_client()?.post(&endpoint))
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call restart endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "restart request failed with status {status}: {body}"
        )));
    }

    let payload = response
        .json::<RestartResponse>()
        .await
        .map_err(|err| Error::external(format!("failed to decode restart response: {err}")))?;

    if payload.accepted {
        println!(
            "[maestro]: local restart accepted; the controller will stop all containers and restart"
        );
    } else {
        println!("[maestro]: restart was not accepted");
    }

    Ok(())
}

fn normalize_base_url(host: &str) -> Result<String> {
    let host = host.trim();
    if host.is_empty() {
        return Err(Error::invalid_input("host cannot be empty"));
    }

    let base = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    };

    Ok(base.trim_end_matches('/').to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinated_restart_request_selects_one_node_or_all_nodes() {
        let selected = serde_json::to_value(ClusterRestartRequest {
            node_id: Some("node-a"),
            all: false,
        })
        .expect("serialize selected-node restart");
        assert_eq!(
            selected,
            serde_json::json!({ "nodeId": "node-a", "all": false })
        );

        let all = serde_json::to_value(ClusterRestartRequest {
            node_id: None,
            all: true,
        })
        .expect("serialize all-node restart");
        assert_eq!(all, serde_json::json!({ "nodeId": null, "all": true }));
    }
}
