use std::{net::Ipv4Addr, path::Path, time::Duration};

use crate::{
    cluster::{self, NodeRole},
    error::{Error, Result},
};

pub async fn approve_node(
    host: &str,
    node_id: String,
    role: NodeRole,
    cluster_host_ip: Ipv4Addr,
    cluster_api_port: Option<u16>,
    subnet: String,
    public_key_sha256: String,
) -> Result<()> {
    let admission = cluster::join::JoinAdmission {
        node_id,
        role,
        cluster_host_ip,
        cluster_api_port: cluster_api_port.unwrap_or_default(),
        identity_api_port: cluster_api_port,
        subnet,
        public_key_sha256,
        created_at_ms: crate::cluster_stats::now_ms(),
    };
    let base = crate::cli::contexts::normalize_base_url(host)?;
    let response = crate::cli::idempotent(
        crate::cli::contexts::build_http_client()?
            .post(format!("{base}/api/cluster/admissions"))
            .json(&admission),
    )
    .send()
    .await
    .map_err(|error| Error::external(format!("failed to approve voter: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "voter approval failed ({status}): {body}"
        )));
    }
    println!("[maestro]: voter admission created");
    Ok(())
}

pub async fn remove_node(host: &str, node_id: &str) -> Result<()> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RemoveResponse {
        state: cluster::join::RemoveNodeOutcome,
    }

    let base = crate::cli::contexts::normalize_base_url(host)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let response = crate::cli::idempotent(
            crate::cli::contexts::build_http_client()?
                .delete(format!("{base}/api/cluster/nodes/{node_id}")),
        )
        .send()
        .await
        .map_err(|error| Error::external(format!("failed to remove node: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::external(format!(
                "node removal failed ({status}): {body}"
            )));
        }
        let outcome: RemoveResponse = response
            .json()
            .await
            .map_err(|error| Error::external(format!("invalid node removal response: {error}")))?;
        match outcome.state {
            cluster::join::RemoveNodeOutcome::Removed => {
                println!("[maestro]: node `{node_id}` removed");
                println!("rotate the join secret and cluster CA if this node may be compromised");
                return Ok(());
            }
            cluster::join::RemoveNodeOutcome::Draining => {
                println!("[maestro]: waiting for node `{node_id}` to drain");
            }
            cluster::join::RemoveNodeOutcome::LeadershipTransferRequired => {
                println!("[maestro]: leadership transferred; waiting for the new leader");
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::external(
                "timed out waiting for the node to drain and leave the cluster",
            ));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

pub async fn prepare_join(config_source: &str, base_data_dir: &Path) -> Result<()> {
    let (config, data_dir, host_ip) = load_join_config(config_source, base_data_dir).await?;
    let node_id = cluster::identity::load_or_create_node_id(&data_dir)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let private_key = cluster::join::load_or_create_join_key(&data_dir)
        .map_err(|error| Error::internal(error.to_string()))?;
    let subnet = config
        .subnet
        .as_deref()
        .ok_or_else(|| Error::invalid_config("local subnet is required for cluster join"))?;
    println!("node id: {node_id}");
    println!("role: {}", config.node.role);
    println!("host IP: {host_ip}");
    println!("API port: {}", config.cluster.api_port);
    println!("subnet: {subnet}");
    println!(
        "public key SHA-256: {}",
        cluster::join::join_key_fingerprint(&private_key)
    );
    if config.node.role.is_voter() {
        println!("approve this exact identity on the current leader before joining");
    }
    Ok(())
}

pub async fn join_cluster(
    leader_address: &str,
    config_source: &str,
    base_data_dir: &Path,
) -> Result<()> {
    let (config, data_dir, host_ip) = load_join_config(config_source, base_data_dir).await?;
    cluster::provision::join_once_via(&config, &data_dir, host_ip, Some(leader_address)).await?;
    println!("[maestro]: cluster identity is installed");
    Ok(())
}

async fn load_join_config(
    config_source: &str,
    base_data_dir: &Path,
) -> Result<(crate::config::StartConfig, std::path::PathBuf, Ipv4Addr)> {
    let config = crate::config::load_config(config_source)
        .await
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    cluster::network::validate_cluster_config(
        &config.cluster,
        config.subnet.as_deref(),
        config.node.role,
    )
    .map_err(|error| Error::invalid_config(error.to_string()))?;
    if config.cluster.nodes.is_empty() {
        return Err(Error::invalid_config(
            "cluster.nodes must contain at least the bootstrap voter for join",
        ));
    }
    let data_dir = base_data_dir.join(config.cluster.name.to_lowercase());
    std::fs::create_dir_all(&data_dir)?;
    let host_ip =
        cluster::network::resolve_cluster_host_ip(&config.cluster, &data_dir, config.node.role)
            .map_err(|error| Error::invalid_config(error.to_string()))?
            .ok_or_else(|| Error::invalid_config("failed to resolve cluster host IP"))?;
    Ok((config, data_dir, host_ip))
}
