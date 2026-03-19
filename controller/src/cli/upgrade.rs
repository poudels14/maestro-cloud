use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
struct UpgradeSystemResponse {
    accepted: bool,
    system: String,
}

pub async fn run_upgrade_system(host: &str) -> Result<()> {
    let endpoint = upgrade_system_endpoint(host)?;
    let response = reqwest::Client::new()
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
