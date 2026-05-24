use serde::Deserialize;

use crate::cli::contexts;
use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
struct UpgradeSystemResponse {
    accepted: bool,
    system: String,
}

pub async fn run_upgrade_system(host: &str, yes: bool) -> Result<()> {
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        "About to upgrade the host operating system",
        &[],
        yes,
    )
    .await?;
    if !confirmed {
        println!("[maestro]: aborted");
        return Ok(());
    }

    let endpoint = upgrade_system_endpoint(host)?;
    let response = contexts::build_http_client()?
        .post(&endpoint)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call system upgrade endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "system upgrade failed with status {status}: {body}"
        )));
    }

    let payload = response
        .json::<UpgradeSystemResponse>()
        .await
        .map_err(|err| Error::external(format!("failed to decode upgrade response: {err}")))?;

    if payload.accepted {
        println!(
            "[maestro]: system upgrade accepted (system: {}), the system will update and reboot",
            payload.system
        );
    } else {
        println!("[maestro]: system upgrade was not accepted");
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

fn upgrade_system_endpoint(host: &str) -> Result<String> {
    let base = normalize_base_url(host)?;
    Ok(format!("{base}/api/system/upgrade"))
}

pub async fn run_upgrade_cluster(
    host: &str,
    yes: bool,
    target_version: Option<String>,
) -> Result<()> {
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        "About to perform a rolling cluster upgrade",
        &[],
        yes,
    )
    .await?;
    if !confirmed {
        println!("[maestro]: aborted");
        return Ok(());
    }

    let endpoint = format!("{}/api/cluster/upgrade", normalize_base_url(host)?);
    let body = serde_json::json!({
        "targetVersion": target_version.unwrap_or_else(|| "latest".to_string()),
    });
    let response = contexts::build_http_client()?
        .post(&endpoint)
        .json(&body)
        .send()
        .await
        .map_err(|err| {
            Error::external(format!("failed to call cluster upgrade endpoint: {err}"))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "cluster upgrade failed with status {status}: {body}"
        )));
    }

    println!("[maestro]: cluster upgrade started");
    Ok(())
}
