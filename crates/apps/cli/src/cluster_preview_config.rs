use std::collections::BTreeSet;
use std::io::Write;

use cluster::{ClusterConfig, PreviewLaunchConfig};
use kernel_api::{
    MaskedClusterConfig, NodeId, PreviewLaunchConfigUpdateRequest,
    PreviewLaunchConfigUpdateResponse,
};

use crate::CliError;
use crate::api_client::ApiClient;
use crate::config::load_cluster_with_default_node;
use crate::config_source::ConfigSourceReader;
use crate::contexts::{Context, ContextStore};

pub(crate) async fn sync(
    config_source: &str,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let loaded = load_cluster_with_default_node(config_source, reader).await?;
    let preview = loaded.launch_policy.preview.ok_or_else(|| {
        CliError::invalid_input("cluster config does not define a preview integration")
    })?;
    let contexts = ContextStore::from_environment()?;
    let context = contexts.active()?;
    let active_client = ApiClient::new(context.clone())?;
    let live: MaskedClusterConfig = active_client.get("/api/config").await?;
    validate_live_topology(&loaded.cluster, &live)?;

    for (node_id, host) in admin_targets(&loaded.cluster)? {
        let client = ApiClient::new(Context {
            host,
            ..context.clone()
        })?;
        let response: PreviewLaunchConfigUpdateResponse = client
            .put_exact(
                "/api/config/preview",
                &request(&loaded.cluster, &node_id, &preview),
            )
            .await?;
        if response.node_id != node_id {
            return Err(CliError::invalid_api_response(
                "preview launch-config receipt does not match the contacted node",
            ));
        }
        let action = if response.changed {
            "updated"
        } else {
            "verified"
        };
        writeln!(
            output,
            "[maestro]: {action} preview launch config for node `{node_id}`"
        )
        .map_err(|source| CliError::io("failed to write command output", source))?;
    }
    writeln!(
        output,
        "[maestro]: restart the cluster to activate the preview integration"
    )
    .map_err(|source| CliError::io("failed to write command output", source))
}

fn validate_live_topology(
    configured: &ClusterConfig,
    live: &MaskedClusterConfig,
) -> Result<(), CliError> {
    let configured_nodes = configured.nodes.keys().cloned().collect::<BTreeSet<_>>();
    let live_nodes = live
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    if configured.cluster_id != live.cluster_id || configured_nodes != live_nodes {
        return Err(CliError::invalid_input(
            "cluster config does not match the active context topology",
        ));
    }
    Ok(())
}

pub(crate) fn admin_targets(cluster: &ClusterConfig) -> Result<Vec<(NodeId, String)>, CliError> {
    cluster
        .nodes
        .iter()
        .map(|(node_id, node)| {
            let address = node.workload_subnet.admin_address().ok_or_else(|| {
                CliError::invalid_input(format!(
                    "node `{node_id}` subnet has no predictable Admin address"
                ))
            })?;
            Ok((node_id.clone(), format!("http://{address}")))
        })
        .collect()
}

fn request(
    cluster: &ClusterConfig,
    node_id: &NodeId,
    preview: &PreviewLaunchConfig,
) -> PreviewLaunchConfigUpdateRequest {
    PreviewLaunchConfigUpdateRequest {
        cluster_id: cluster.cluster_id.clone(),
        node_id: node_id.clone(),
        domain: preview.domain.clone(),
        github_token: preview.github_token.clone(),
        max_concurrent_previews: preview.max_concurrent_previews,
    }
}
