use serde::Deserialize;

use crate::error::{Error, Result};
use crate::service::DeploymentStatus;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CancelDeploymentResponse {
    canceled: bool,
    service_id: String,
    deployment_id: String,
    status: DeploymentStatus,
}

pub async fn run_cancel(host: &str, service_id: &str, deployment_id: &str) -> Result<()> {
    let service_id = service_id.trim();
    let deployment_id = deployment_id.trim();

    if service_id.is_empty() {
        return Err(Error::invalid_input("service id cannot be empty"));
    }
    if deployment_id.is_empty() {
        return Err(Error::invalid_input("deployment id cannot be empty"));
    }

    let endpoint = cancel_endpoint(host, service_id, deployment_id)?;
    let response = reqwest::Client::new()
        .patch(&endpoint)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call cancel endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "cancel failed with status {status}: {body}"
        )));
    }

    let payload = response
        .json::<CancelDeploymentResponse>()
        .await
        .map_err(|err| Error::external(format!("failed to decode cancel response: {err}")))?;

    if payload.canceled {
        println!(
            "[maestro]: canceled deployment `{}` for service `{}` (status: {:?})",
            payload.deployment_id, payload.service_id, payload.status
        );
    } else {
        println!(
            "[maestro]: deployment `{}` for service `{}` was not canceled (status: {:?})",
            payload.deployment_id, payload.service_id, payload.status
        );
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

fn cancel_endpoint(host: &str, service_id: &str, deployment_id: &str) -> Result<String> {
    let base = normalize_base_url(host)?;
    Ok(format!(
        "{base}/api/services/{service_id}/deployments/{deployment_id}/cancel"
    ))
}

#[cfg(test)]
#[path = "../tests/cli/cancel.rs"]
mod tests;
