use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::deployment::types::{
    IngressConfig, ServiceBuildConfig, ServiceDeployConfig, ServiceProvider,
};
use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServiceManifest {
    pub(super) id: String,
    pub(super) name: String,
    #[serde(default)]
    pub(super) provider: ServiceProvider,
    #[serde(default)]
    pub(super) build: Option<ServiceBuildConfig>,
    #[serde(default)]
    pub(super) image: Option<String>,
    pub(super) deploy: ServiceDeployConfig,
    #[serde(default)]
    pub(super) ingress: Option<IngressConfig>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct UploadSpecPayload {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) provider: ServiceProvider,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) build: Option<ServiceBuildConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) image: Option<String>,
    pub(super) deploy: ServiceDeployConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ingress: Option<IngressConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadResponse {
    service_id: String,
    deployment_id: String,
    version: String,
    name: String,
}

pub async fn run_up(host: &str, config_path: &Path, context_dir: &Path) -> Result<()> {
    let raw = std::fs::read_to_string(config_path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::not_found(format!("{} does not exist", config_path.display()))
        } else {
            Error::invalid_config(format!("failed to read {}: {err}", config_path.display()))
        }
    })?;
    let manifest = parse_service_manifest(&raw)?;
    let payload = build_upload_payload(manifest)?;

    if !context_dir.exists() {
        return Err(Error::invalid_input(format!(
            "context directory {} does not exist",
            context_dir.display()
        )));
    }
    let archive_bytes = crate::utils::archive::pack_context(context_dir)
        .map_err(|err| Error::internal(format!("failed to package context: {err}")))?;
    let archive_size = archive_bytes.len();

    let base_url = crate::cli::contexts::normalize_base_url(host)?;
    let client = crate::cli::contexts::build_http_client()?;
    let endpoint = format!("{base_url}/api/services/up");

    let spec_json = serde_json::to_vec(&payload)
        .map_err(|err| Error::internal(format!("failed to encode spec: {err}")))?;

    let spec_part = reqwest::multipart::Part::bytes(spec_json)
        .file_name("spec.json")
        .mime_str("application/json")
        .map_err(|err| Error::internal(format!("failed to build spec part: {err}")))?;
    let context_part = reqwest::multipart::Part::bytes(archive_bytes)
        .file_name("context.tar.gz")
        .mime_str("application/gzip")
        .map_err(|err| Error::internal(format!("failed to build context part: {err}")))?;
    let form = reqwest::multipart::Form::new()
        .part("spec", spec_part)
        .part("context", context_part);

    println!(
        "[maestro]: uploading context from {} ({} bytes archive)",
        context_dir.display(),
        archive_size,
    );

    let response = client
        .post(&endpoint)
        .multipart(form)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call up endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "up request failed with status {status}: {body}"
        )));
    }

    let result = response
        .json::<UploadResponse>()
        .await
        .map_err(|err| Error::external(format!("failed to decode up response: {err}")))?;

    println!(
        "[maestro]: queued service `{}` (name: `{}`, deployment: `{}`, version: `{}`)",
        result.service_id, result.name, result.deployment_id, result.version,
    );
    Ok(())
}

pub(super) fn parse_service_manifest(raw: &str) -> Result<ServiceManifest> {
    json5::from_str(raw)
        .map_err(|err| Error::invalid_config(format!("failed to parse service manifest: {err}")))
}

pub(super) fn build_upload_payload(manifest: ServiceManifest) -> Result<UploadSpecPayload> {
    let service_id = manifest.id.trim().to_string();
    crate::validation::validate_service_id(&service_id, "id").map_err(Error::invalid_config)?;
    let name = manifest.name.trim().to_string();
    if name.is_empty() {
        return Err(Error::invalid_config(format!(
            "service `{service_id}` has empty name"
        )));
    }
    let (build, image, deploy) = crate::validation::validate_service_provider_config(
        manifest.provider,
        &manifest.build,
        &manifest.image,
        &manifest.deploy,
    )
    .map_err(|err| Error::invalid_config(format!("service `{service_id}` {err}")))?;
    crate::validation::validate_ingress_config(&manifest.ingress)
        .map_err(|err| Error::invalid_config(format!("service `{service_id}` {err}")))?;

    Ok(UploadSpecPayload {
        id: service_id,
        name,
        provider: manifest.provider,
        build,
        image,
        deploy,
        ingress: manifest.ingress,
    })
}

#[cfg(test)]
#[path = "../tests/cli/up.rs"]
mod tests;
