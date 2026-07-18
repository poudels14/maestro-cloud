pub mod auth;
pub mod cancel;
pub mod cluster_lifecycle;
pub mod config;
pub mod confirm;
pub mod contexts;
pub mod dead_letters;
pub mod exec;
pub mod info;
pub mod logs;
pub mod nodes;
pub mod redeploy;
pub mod restart;
pub mod rollout;
pub mod services;
pub mod up;
pub mod upgrade;

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DeploymentTargetConfig {
    #[serde(default)]
    cluster: DeploymentTargetCluster,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DeploymentTargetCluster {
    #[serde(default)]
    nodes: std::collections::BTreeMap<String, serde_json::Value>,
}

pub(crate) async fn target_uses_multinode(
    client: &reqwest::Client,
    base_url: &str,
) -> crate::error::Result<bool> {
    let endpoint = format!("{base_url}/api/config");
    let response = client.get(&endpoint).send().await.map_err(|error| {
        crate::error::Error::external(format!(
            "failed to determine whether the deployment target is multi-node: {error}"
        ))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(crate::error::Error::external(format!(
            "failed to determine whether the deployment target is multi-node ({status}): {body}"
        )));
    }
    let config = response
        .json::<DeploymentTargetConfig>()
        .await
        .map_err(|error| {
            crate::error::Error::external(format!(
                "failed to decode deployment target configuration: {error}"
            ))
        })?;
    Ok(!config.cluster.nodes.is_empty())
}

pub(crate) fn validate_target_build_registry(
    service_id: &str,
    build: &Option<crate::deployment::types::ServiceBuildConfig>,
    cluster_mode: bool,
) -> crate::error::Result<()> {
    crate::validation::validate_cluster_build_registry(build, cluster_mode).map_err(|error| {
        crate::error::Error::invalid_config(format!("service `{service_id}` {error}"))
    })
}

pub(crate) fn idempotent(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request.header("Idempotency-Key", crate::utils::nanoid::unique_id(32))
}

#[cfg(test)]
#[path = "../tests/cli/mod.rs"]
mod tests;
