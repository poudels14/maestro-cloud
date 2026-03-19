use anyhow::{Result, anyhow};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StartConfig {
    pub cluster: ClusterConfig,
    pub ingress: IngressConfig,
    #[serde(default)]
    pub subnet: Option<String>,
    pub encryption_key: String,
    #[serde(default)]
    pub tailscale: Option<TailscaleConfig>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub datadog: Option<DatadogConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ClusterConfig {
    pub name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IngressConfig {
    pub port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TailscaleConfig {
    pub auth_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatadogConfig {
    pub api_key: String,
    #[serde(default)]
    pub site: Option<String>,
    #[serde(default)]
    pub include_system_logs: bool,
}

pub async fn load_config(source: &str) -> Result<StartConfig> {
    if let Some(secret_name) = source.strip_prefix("aws-secrets://") {
        load_from_aws(secret_name).await
    } else {
        let path = source.strip_prefix("file://").unwrap_or(source);
        load_from_file(path)
    }
}

fn load_from_file(path: &str) -> Result<StartConfig> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| anyhow!("failed to read config file `{path}`: {err}"))?;
    let config: StartConfig = json5::from_str(&raw)
        .map_err(|err| anyhow!("failed to parse config file `{path}`: {err}"))?;
    Ok(config)
}

async fn load_from_aws(secret_name: &str) -> Result<StartConfig> {
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_secretsmanager::Client::new(&aws_config);
    let result = client
        .get_secret_value()
        .secret_id(secret_name)
        .send()
        .await
        .map_err(|err| anyhow!("failed to fetch secret `{secret_name}` from AWS: {err}"))?;
    let secret_string = result
        .secret_string()
        .ok_or_else(|| anyhow!("secret `{secret_name}` has no string value"))?;
    let config: StartConfig = serde_json::from_str(secret_string)
        .map_err(|err| anyhow!("failed to parse secret `{secret_name}`: {err}"))?;
    Ok(config)
}
