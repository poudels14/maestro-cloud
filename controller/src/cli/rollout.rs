use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::deployment::types::{
    IngressConfig, ServiceBuildConfig, ServiceDeployConfig, ServiceProvider,
};
use crate::error::{Error, Result};

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
    provider: ServiceProvider,
    #[serde(default)]
    build: Option<ServiceBuildConfig>,
    #[serde(default)]
    image: Option<String>,
    deploy: ServiceDeployConfig,
    #[serde(default)]
    ingress: Option<IngressConfig>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceRequest {
    id: String,
    name: String,
    provider: ServiceProvider,
    #[serde(skip_serializing_if = "Option::is_none")]
    build: Option<ServiceBuildConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<String>,
    deploy: ServiceDeployConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingress: Option<IngressConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchServiceResponse {
    queued: bool,
    #[serde(default)]
    replicas: Option<u32>,
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

    let rollout_url = rollout_endpoint(host)?;
    println!("[maestro]: rolling out services via {rollout_url}");

    let client = reqwest::Client::new();

    for (service_id, service_template) in &cluster.services {
        let payload = service_payload(service_id, service_template)?;
        let response = call_rollout_endpoint(&client, &rollout_url, &payload, service_id).await?;

        if response.queued {
            println!(
                "[maestro]: queued service `{}` with version `{}`",
                response.service_id, response.version
            );
        } else if let Some(replicas) = response.replicas {
            println!(
                "[maestro]: scaled service `{}` to {replicas} replicas",
                response.service_id
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

async fn call_rollout_endpoint(
    client: &reqwest::Client,
    endpoint: &str,
    payload: &PatchServiceRequest,
    service_id: &str,
) -> Result<PatchServiceResponse> {
    let response = client
        .post(endpoint)
        .json(payload)
        .send()
        .await
        .map_err(|err| {
            Error::external(format!(
                "failed to call rollout endpoint for service `{service_id}`: {err}"
            ))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "rollout failed for service `{service_id}` with status {status}: {body}"
        )));
    }

    let payload = response
        .json::<PatchServiceResponse>()
        .await
        .map_err(|err| {
            Error::external(format!(
                "failed to decode rollout response for service `{service_id}`: {err}"
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

fn rollout_endpoint(host: &str) -> Result<String> {
    Ok(format!(
        "{}/api/services/rollout",
        normalize_base_url(host)?
    ))
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
    let (build, image, deploy) = validate_service_provider_config(
        service_template.provider,
        &service_template.build,
        &service_template.image,
        &service_template.deploy,
    )
    .map_err(|err| Error::invalid_config(format!("service `{service_id}` {err}")))?;

    Ok(PatchServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        provider: service_template.provider,
        build,
        image,
        deploy,
        ingress: service_template.ingress.clone(),
    })
}

fn validate_build_config(
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

fn validate_service_provider_config(
    provider: ServiceProvider,
    build: &Option<ServiceBuildConfig>,
    image: &Option<String>,
    deploy: &ServiceDeployConfig,
) -> std::result::Result<
    (
        Option<ServiceBuildConfig>,
        Option<String>,
        ServiceDeployConfig,
    ),
    String,
> {
    match provider {
        ServiceProvider::Docker => {
            let (build, image) = validate_build_config(build, image)?;
            Ok((build, image, deploy.clone()))
        }
        ServiceProvider::Shell => {
            if build.is_some() || image.is_some() {
                return Err("must not set `build` or `image` when provider is `shell`".to_string());
            }
            let Some(command) = deploy.command.as_ref() else {
                return Err("must set deploy.command when provider is `shell`".to_string());
            };
            if command.command.trim().is_empty() {
                return Err(
                    "must set non-empty deploy.command.command when provider is `shell`"
                        .to_string(),
                );
            }
            Ok((None, None, deploy.clone()))
        }
    }
}

#[cfg(test)]
#[path = "../tests/cli/rollout.rs"]
mod tests;
