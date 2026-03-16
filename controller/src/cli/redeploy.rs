use serde::Deserialize;

use crate::deployment::types::DeploymentStatus;
use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RedeployResponse {
    service_id: String,
    deployment_id: Option<String>,
    version: String,
    status: Option<DeploymentStatus>,
}

pub async fn run_redeploy(host: &str, service_id: &str) -> Result<()> {
    let service_id = service_id.trim();
    if service_id.is_empty() {
        return Err(Error::invalid_input("service id cannot be empty"));
    }

    let endpoint = redeploy_endpoint(host, service_id)?;
    let response = reqwest::Client::new()
        .post(&endpoint)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call redeploy endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "redeploy failed with status {status}: {body}"
        )));
    }

    let payload = response
        .json::<RedeployResponse>()
        .await
        .map_err(|err| Error::external(format!("failed to decode redeploy response: {err}")))?;

    println!(
        "[maestro]: queued redeploy for service `{}` (deployment `{}`, version `{}`)",
        payload.service_id,
        payload.deployment_id.as_deref().unwrap_or("?"),
        payload.version
    );

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

fn redeploy_endpoint(host: &str, service_id: &str) -> Result<String> {
    let base = normalize_base_url(host)?;
    Ok(format!("{base}/api/services/{service_id}/redeploy"))
}
