use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::service::{ServiceBuildConfig, ServiceDeployConfig};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterConfig {
    services: BTreeMap<String, ServiceTemplate>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceTemplate {
    name: String,
    #[serde(default)]
    build: Option<ServiceBuildConfig>,
    #[serde(default)]
    image: Option<String>,
    deploy: ServiceDeployConfig,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceRequest {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    build: Option<ServiceBuildConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<String>,
    deploy: ServiceDeployConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceResponse {
    queued: bool,
    service_id: String,
    version: String,
}

pub async fn run_rollout(config_path: &Path, host: &str) -> Result<()> {
    let raw = std::fs::read_to_string(config_path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::not_found(format!("{} does not exist", config_path.display()))
        } else {
            Error::invalid_config(format!("failed to read {}: {err}", config_path.display()))
        }
    })?;
    let cluster = parse_config(&raw)?;

    if cluster.services.is_empty() {
        return Err(Error::invalid_config(format!(
            "no services configured in {}",
            config_path.display()
        )));
    }

    let patch_url = patch_endpoint(host)?;
    println!("[maestro]: rolling out services via {patch_url}");

    let client = reqwest::Client::new();

    for (service_id, service_template) in &cluster.services {
        let payload = service_payload(service_id, service_template)?;
        let response = call_patch_endpoint(&client, &patch_url, &payload, service_id).await?;

        if response.queued {
            println!(
                "[maestro]: queued service `{}` with version `{}`",
                response.service_id, response.version
            );
        } else {
            println!(
                "[maestro]: skipped service `{}`; config unchanged at version `{}`",
                response.service_id, response.version
            );
        }
    }

    Ok(())
}

async fn call_patch_endpoint(
    client: &reqwest::Client,
    endpoint: &str,
    payload: &PatchServiceRequest,
    service_id: &str,
) -> Result<PatchServiceResponse> {
    let response = client
        .patch(endpoint)
        .json(payload)
        .send()
        .await
        .map_err(|err| {
            Error::external(format!(
                "failed to call patch endpoint for service `{service_id}`: {err}"
            ))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "patch failed for service `{service_id}` with status {status}: {body}"
        )));
    }

    let payload = response
        .json::<PatchServiceResponse>()
        .await
        .map_err(|err| {
            Error::external(format!(
                "failed to decode patch response for service `{service_id}`: {err}"
            ))
        })?;
    Ok(payload)
}

fn parse_config(raw: &str) -> Result<ClusterConfig> {
    let parsed = json5::from_str(raw)
        .map_err(|err| Error::invalid_config(format!("failed to parse maestro.jsonc: {err}")))?;
    Ok(parsed)
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

fn patch_endpoint(host: &str) -> Result<String> {
    Ok(format!("{}/api/services/patch", normalize_base_url(host)?))
}

fn service_payload(
    service_id: &str,
    service_template: &ServiceTemplate,
) -> Result<PatchServiceRequest> {
    let id = service_id.trim();
    if id.is_empty() {
        return Err(Error::invalid_config("service id cannot be empty"));
    }

    let name = service_template.name.trim();
    if name.is_empty() {
        return Err(Error::invalid_config(format!(
            "service `{service_id}` has empty name"
        )));
    }
    let (build, image) = normalize_build_or_image(&service_template.build, &service_template.image)
        .map_err(|err| Error::invalid_config(format!("service `{service_id}` {err}")))?;

    Ok(PatchServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        build,
        image,
        deploy: service_template.deploy.clone(),
    })
}

fn normalize_build_or_image(
    build: &Option<ServiceBuildConfig>,
    image: &Option<String>,
) -> std::result::Result<(Option<ServiceBuildConfig>, Option<String>), String> {
    match (build, image) {
        (Some(build), None) => {
            if build.repo.trim().is_empty() {
                return Err("has empty build.repo".to_string());
            }
            if build.dockerfile_path.trim().is_empty() {
                return Err("has empty build.dockerfilePath".to_string());
            }
            Ok((Some(build.clone()), None))
        }
        (None, Some(image)) => {
            if image.trim().is_empty() {
                return Err("has empty image".to_string());
            }
            Ok((None, Some(image.clone())))
        }
        (Some(_), Some(_)) => Err("must set either `build` or `image`, not both".to_string()),
        (None, None) => Err("must set either `build` or `image`".to_string()),
    }
}

#[cfg(test)]
#[path = "../tests/cli/rollout.rs"]
mod tests;
