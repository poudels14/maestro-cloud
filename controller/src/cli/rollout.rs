use std::{collections::BTreeMap, io::IsTerminal, path::Path};

use serde::{Deserialize, Serialize};

use crate::deployment::types::{
    IngressConfig, PreviewConfig, ServiceBuildConfig, ServiceDeployConfig,
};
use crate::error::{Error, Result};
use crate::utils::crypto::SecretString;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ClusterConfig {
    pub(super) services: BTreeMap<String, ServiceTemplate>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServiceTemplate {
    name: String,
    #[serde(default)]
    build: Option<ServiceBuildConfig>,
    #[serde(default)]
    image: Option<String>,
    deploy: ServiceDeployConfig,
    #[serde(default)]
    ingress: Option<IngressConfig>,
    #[serde(default)]
    preview: Option<PreviewConfig>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    ingress: Option<IngressConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    preview: Option<PreviewConfig>,
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

pub async fn run_rollout(
    config_path: &Path,
    host: &str,
    apply: bool,
    force: bool,
    filter: &[String],
    yes: bool,
) -> Result<()> {
    let raw = std::fs::read_to_string(config_path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::not_found(format!("{} does not exist", config_path.display()))
        } else {
            Error::invalid_config(format!("failed to read {}: {err}", config_path.display()))
        }
    })?;
    let cluster = parse_cluster_config(&raw)?;

    if cluster.services.is_empty() {
        return Err(Error::invalid_config(format!(
            "no services configured in {}",
            config_path.display()
        )));
    }

    for requested in filter {
        if !cluster.services.contains_key(requested) {
            let available = cluster
                .services
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::invalid_input(format!(
                "service `{requested}` not found in {}. Available: {available}",
                config_path.display()
            )));
        }
    }

    let resolved_filter =
        if filter.is_empty() && std::io::stdin().is_terminal() && std::io::stderr().is_terminal() {
            prompt_service_selection(&cluster)?
        } else {
            filter.to_vec()
        };

    let selected: Vec<(&String, &ServiceTemplate)> = cluster
        .services
        .iter()
        .filter(|(id, _)| {
            resolved_filter.is_empty() || resolved_filter.iter().any(|name| name == *id)
        })
        .collect();

    if selected.is_empty() {
        println!("[maestro]: no services selected; nothing to do");
        return Ok(());
    }

    let base_url = normalize_base_url(host)?;
    let client = crate::cli::contexts::build_http_client()?;

    if !apply {
        let diff_url = format!("{base_url}/api/services/rollout/diff");
        let mut has_changes = false;

        for (service_id, service_template) in &selected {
            let payload = service_payload(service_id, service_template)?;
            let diff = call_diff_endpoint(&client, &diff_url, &payload, service_id).await?;
            print_diff(&diff);
            if !matches!(diff.status.as_str(), "unchanged") {
                has_changes = true;
            }
        }

        if has_changes {
            println!("\nrun with --apply to deploy these changes");
        }
        return Ok(());
    }

    let selected_ids: Vec<&str> = selected.iter().map(|(id, _)| id.as_str()).collect();
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        &format!("About to roll out {} service(s)", selected.len()),
        &[format!("Services: {}", selected_ids.join(", "))],
        yes,
    )
    .await?;
    if !confirmed {
        println!("[maestro]: aborted");
        return Ok(());
    }

    let mut rollout_url = reqwest::Url::parse(&format!("{base_url}/api/services/rollout"))
        .map_err(|err| Error::invalid_input(format!("invalid rollout URL: {err}")))?;
    if force {
        rollout_url.query_pairs_mut().append_pair("force", "true");
    }
    if selected.len() == cluster.services.len() {
        println!("[maestro]: rolling out {} services", selected.len());
    } else {
        println!(
            "[maestro]: rolling out {} service(s): {}",
            selected.len(),
            selected_ids.join(", ")
        );
    }

    for (service_id, service_template) in &selected {
        let payload = service_payload(service_id, service_template)?;
        let response =
            call_rollout_endpoint(&client, rollout_url.as_str(), &payload, service_id).await?;

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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiffResponse {
    service_id: String,
    status: String,
    #[serde(default)]
    changes: Vec<DiffChange>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiffChange {
    field: String,
    from: Option<String>,
    to: Option<String>,
}

async fn call_diff_endpoint(
    client: &reqwest::Client,
    endpoint: &str,
    payload: &PatchServiceRequest,
    service_id: &str,
) -> Result<DiffResponse> {
    let response = client
        .post(endpoint)
        .json(payload)
        .send()
        .await
        .map_err(|err| {
            Error::external(format!(
                "failed to call diff endpoint for service `{service_id}`: {err}"
            ))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "diff failed for service `{service_id}` with status {status}: {body}"
        )));
    }

    let diffs: Vec<DiffResponse> = response.json().await.map_err(|err| {
        Error::external(format!(
            "failed to decode diff response for service `{service_id}`: {err}"
        ))
    })?;

    diffs
        .into_iter()
        .next()
        .ok_or_else(|| Error::external(format!("empty diff response for service `{service_id}`")))
}

fn print_diff(diff: &DiffResponse) {
    match diff.status.as_str() {
        "new" => {
            println!("\n  + {} (new service)", diff.service_id);
        }
        "unchanged" => {
            println!("\n  = {} (no changes)", diff.service_id);
        }
        _ => {
            println!("\n  ~ {} (changed)", diff.service_id);
            for change in &diff.changes {
                match (&change.from, &change.to) {
                    (None, Some(to)) => {
                        println!("    + {}: {to}", change.field);
                    }
                    (Some(from), None) => {
                        println!("    - {}: {from}", change.field);
                    }
                    (Some(from), Some(to)) => {
                        println!("    ~ {}: {from} -> {to}", change.field);
                    }
                    (None, None) => {}
                }
            }
        }
    }
}

async fn call_rollout_endpoint(
    client: &reqwest::Client,
    endpoint: &str,
    payload: &PatchServiceRequest,
    service_id: &str,
) -> Result<PatchServiceResponse> {
    let response = crate::cli::idempotent(client.post(endpoint))
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
        if status == reqwest::StatusCode::CONFLICT && body.contains("frozen") {
            return Err(Error::external(format!(
                "deploy is frozen for service `{service_id}`; use --force to override"
            )));
        }
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

pub(super) fn parse_cluster_config(raw: &str) -> Result<ClusterConfig> {
    parse_cluster_config_with_diagnostics(raw).map(|(config, _)| config)
}

pub(super) fn parse_cluster_config_with_diagnostics(
    raw: &str,
) -> Result<(ClusterConfig, Vec<String>)> {
    let value = json5::from_str(raw)
        .map_err(|err| Error::invalid_config(format!("failed to parse config: {err}")))?;
    crate::config::deserialize_config_value(value)
        .map_err(|err| Error::invalid_config(format!("failed to parse config: {err}")))
}

pub(super) fn validate_service_template(
    service_id: &str,
    template: &ServiceTemplate,
) -> Result<()> {
    let id = service_id.trim();
    crate::validation::validate_user_service_id(id, "service id")
        .map_err(|error| Error::invalid_config(format!("services.{service_id}: {error}")))?;
    if template.name.trim().is_empty() {
        return Err(Error::invalid_config(format!(
            "services.{service_id}.name: cannot be empty"
        )));
    }
    crate::validation::validate_service_provider_config(
        &template.build,
        &template.image,
        &template.deploy,
    )
    .map_err(|error| service_validation_error(service_id, &error))?;
    crate::validation::validate_ingress_config(&template.ingress)
        .map_err(|error| service_validation_error(service_id, &error))?;
    crate::validation::validate_preview_config(
        id,
        &template.preview,
        &template.build,
        &template.ingress,
    )
    .map_err(|error| service_validation_error(service_id, &error))?;
    Ok(())
}

fn service_validation_error(service_id: &str, message: &str) -> Error {
    let relative_path = message
        .split_once([' ', '(', '`'])
        .map(|(path, _)| path)
        .filter(|path| {
            matches!(
                path.split('.').next(),
                Some("build" | "image" | "deploy" | "ingress" | "preview")
            )
        });
    let path = relative_path.map_or_else(
        || format!("services.{service_id}"),
        |path| format!("services.{service_id}.{path}"),
    );
    Error::invalid_config(format!("{path}: {message}"))
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

fn service_payload(
    service_id: &str,
    service_template: &ServiceTemplate,
) -> Result<PatchServiceRequest> {
    let id = service_id.trim();
    crate::validation::validate_user_service_id(id, "service id").map_err(Error::invalid_config)?;

    let name = service_template.name.trim();
    if name.is_empty() {
        return Err(Error::invalid_config(format!(
            "service `{service_id}` has empty name"
        )));
    }
    let (build, image, deploy) = crate::validation::validate_service_provider_config(
        &service_template.build,
        &service_template.image,
        &service_template.deploy,
    )
    .map_err(|err| Error::invalid_config(format!("service `{service_id}` {err}")))?;

    let mut deploy = deploy;
    let mut resolved_env = std::collections::HashMap::new();
    for (key, value) in &deploy.env.items {
        let resolved = expand_env_value(value.as_str()).map_err(|err| {
            Error::invalid_config(format!("service `{service_id}` env `{key}`: {err}"))
        })?;
        resolved_env.insert(key.clone(), SecretString::new(resolved));
    }
    deploy.env.items = resolved_env;
    expand_source(&mut deploy.env.source, service_id, "env.source")?;

    if let Some(secrets) = &mut deploy.secrets {
        let mut resolved_items = std::collections::HashMap::new();
        for (key, value) in &secrets.items {
            let resolved = expand_env_value(value).map_err(|err| {
                Error::invalid_config(format!("service `{service_id}` secret `{key}`: {err}"))
            })?;
            resolved_items.insert(key.clone(), resolved);
        }
        secrets.items = resolved_items;
        expand_source(&mut secrets.source, service_id, "secrets.source")?;
    }

    let build = if let Some(mut build) = build {
        let mut resolved_build_env = std::collections::HashMap::new();
        for (key, value) in &build.env.items {
            let resolved = expand_env_value(value.as_str()).map_err(|err| {
                Error::invalid_config(format!("service `{service_id}` build.env `{key}`: {err}"))
            })?;
            resolved_build_env.insert(key.clone(), SecretString::new(resolved));
        }
        build.env.items = resolved_build_env;
        let mut resolved_build_secrets = std::collections::HashMap::new();
        for (key, value) in &build.secrets.items {
            let resolved = expand_env_value(value.as_str()).map_err(|err| {
                Error::invalid_config(format!(
                    "service `{service_id}` build.secrets `{key}`: {err}"
                ))
            })?;
            resolved_build_secrets.insert(key.clone(), SecretString::new(resolved));
        }
        build.secrets.items = resolved_build_secrets;
        expand_source(&mut build.env.source, service_id, "build.env.source")?;
        expand_source(
            &mut build.secrets.source,
            service_id,
            "build.secrets.source",
        )?;
        Some(build)
    } else {
        None
    };

    let preview = if let Some(mut preview) = service_template.preview.clone() {
        let mut resolved_preview_env = std::collections::HashMap::new();
        for (key, value) in &preview.env.items {
            let resolved = expand_env_value(value.as_str()).map_err(|err| {
                Error::invalid_config(format!("service `{service_id}` preview.env `{key}`: {err}"))
            })?;
            resolved_preview_env.insert(key.clone(), SecretString::new(resolved));
        }
        preview.env.items = resolved_preview_env;
        Some(preview)
    } else {
        None
    };

    Ok(PatchServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        build,
        image,
        deploy,
        ingress: service_template.ingress.clone(),
        preview,
    })
}

fn expand_env_value(value: &str) -> std::result::Result<String, String> {
    shellexpand::env(value)
        .map(|s| s.into_owned())
        .map_err(|err| err.to_string())
}

fn expand_source(
    source: &mut Option<String>,
    service_id: &str,
    field: &str,
) -> std::result::Result<(), Error> {
    if let Some(value) = source {
        let resolved = expand_env_value(value.as_str()).map_err(|err| {
            Error::invalid_config(format!("service `{service_id}` {field}: {err}"))
        })?;
        *value = resolved;
    }
    Ok(())
}

fn prompt_service_selection(cluster: &ClusterConfig) -> Result<Vec<String>> {
    let service_ids: Vec<String> = cluster.services.keys().cloned().collect();
    let defaults: Vec<usize> = (0..service_ids.len()).collect();
    let chosen = inquire::MultiSelect::new(
        "Select services to rollout (space to toggle, enter to confirm)",
        service_ids.clone(),
    )
    .with_default(&defaults)
    .prompt()
    .map_err(|err| Error::external(format!("service selection prompt failed: {err}")))?;
    Ok(chosen)
}

#[cfg(test)]
#[path = "../tests/cli/rollout.rs"]
mod tests;
