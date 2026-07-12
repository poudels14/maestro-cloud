use serde::{Deserialize, Serialize};

use crate::cli::contexts;
use crate::error::{Error, Result};

const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Serialize)]
struct UpgradeSystemRequest<'a> {
    version: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpgradeSystemResponse {
    accepted: bool,
    system: String,
    #[serde(default)]
    current_version: Option<String>,
    #[serde(default)]
    target_version: Option<String>,
}

pub async fn run_upgrade_system(host: &str, yes: bool) -> Result<()> {
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        &format!("About to upgrade the host operating system to Maestro {CLIENT_VERSION}"),
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
        .json(&UpgradeSystemRequest {
            version: CLIENT_VERSION,
        })
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
        let target_version = payload.target_version.as_deref().unwrap_or(CLIENT_VERSION);
        if let Some(current_version) = payload.current_version.as_deref() {
            println!(
                "[maestro]: system upgrade accepted ({current_version} -> {target_version}, system: {}), the system will update and reboot",
                payload.system
            );
        } else {
            println!(
                "[maestro]: system upgrade to {target_version} accepted (system: {}), the system will update and reboot",
                payload.system
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_request_uses_the_cargo_package_version() {
        let request = UpgradeSystemRequest {
            version: CLIENT_VERSION,
        };
        let payload = serde_json::to_value(request).expect("serialize upgrade request");

        assert_eq!(payload["version"], env!("CARGO_PKG_VERSION"));
    }
}
