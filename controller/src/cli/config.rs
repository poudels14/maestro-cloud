use std::io::Write;
use std::path::{Path, PathBuf};

use crate::config::StartConfig;
use crate::error::{Error, Result};

const DEFAULT_START_TEMPLATE: &str = include_str!("../templates/maestro.jsonc");
const DEFAULT_CLUSTER_TEMPLATE: &str = include_str!("../templates/cluster.jsonc");
const DEFAULT_START_PATH: &str = "maestro.jsonc";
const DEFAULT_CLUSTER_PATH: &str = "maestro.cluster.jsonc";

pub async fn run_config(host: &str) -> Result<()> {
    let base = normalize_base_url(host)?;
    let endpoint = format!("{base}/api/config");
    let response = reqwest::Client::new()
        .get(&endpoint)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call config endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "config request failed with status {status}: {body}"
        )));
    }

    let body = response
        .json::<serde_json::Value>()
        .await
        .map_err(|err| Error::external(format!("failed to decode config response: {err}")))?;
    let pretty = serde_json::to_string_pretty(&body)
        .map_err(|err| Error::internal(format!("failed to format config: {err}")))?;
    println!("{pretty}");
    Ok(())
}

pub fn run_init() -> Result<()> {
    let kinds = vec![
        "cluster — controller boot/start settings (maestro.jsonc)",
        "services — service definitions to deploy (maestro.cluster.jsonc)",
    ];
    let choice = inquire::Select::new("Which config do you want to create?", kinds.clone())
        .prompt()
        .map_err(|err| Error::external(format!("config selection prompt failed: {err}")))?;
    let index = kinds
        .iter()
        .position(|kind| *kind == choice)
        .expect("selected option must exist in choices");
    if index == 0 {
        write_template(Path::new(DEFAULT_START_PATH), DEFAULT_START_TEMPLATE)
    } else {
        write_template(Path::new(DEFAULT_CLUSTER_PATH), DEFAULT_CLUSTER_TEMPLATE)
    }
}

pub fn run_validate(path: &PathBuf) -> Result<()> {
    let raw = std::fs::read_to_string(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::not_found(format!("{} does not exist", path.display()))
        } else {
            Error::invalid_input(format!("failed to read {}: {err}", path.display()))
        }
    })?;
    let value: serde_json::Value = json5::from_str(&raw)
        .map_err(|err| Error::invalid_config(format!("{}: invalid JSON: {err}", path.display())))?;
    let has_services = value.get("services").is_some();
    let has_cluster = value.get("cluster").is_some();
    let kind = match (has_services, has_cluster) {
        (true, false) => "services",
        (false, true) => "cluster",
        (true, true) => {
            return Err(Error::invalid_config(format!(
                "{}: ambiguous — has both `services` and `cluster` top-level keys",
                path.display()
            )));
        }
        (false, false) => {
            return Err(Error::invalid_config(format!(
                "{}: unrecognized config — expected a top-level `services` or `cluster` key",
                path.display()
            )));
        }
    };
    if kind == "cluster" {
        json5::from_str::<StartConfig>(&raw).map_err(|err| {
            Error::invalid_config(format!("{}: invalid cluster config: {err}", path.display()))
        })?;
    } else {
        let cluster = crate::cli::rollout::parse_cluster_config(&raw)?;
        for (service_id, template) in &cluster.services {
            crate::cli::rollout::validate_service_template(service_id, template)?;
        }
    }
    println!("[maestro]: {} is a valid {kind} config", path.display());
    Ok(())
}

fn write_template(path: &Path, content: &str) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|err| {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                Error::conflict(format!("{} already exists", path.display()))
            } else {
                Error::internal(format!("failed to create {}: {err}", path.display()))
            }
        })?;
    file.write_all(content.as_bytes())
        .map_err(|err| Error::internal(format!("failed to write {}: {err}", path.display())))?;
    println!("created {}", path.display());
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

#[cfg(test)]
#[path = "../tests/cli/config.rs"]
mod tests;
