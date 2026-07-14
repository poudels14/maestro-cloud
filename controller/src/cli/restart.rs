use serde::Deserialize;

use crate::cli::contexts;
use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
struct RestartResponse {
    accepted: bool,
}

pub async fn run_restart(host: &str, yes: bool) -> Result<()> {
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        "About to restart the controller (all containers will be stopped)",
        &[],
        yes,
    )
    .await?;
    if !confirmed {
        println!("[maestro]: aborted");
        return Ok(());
    }

    let base = normalize_base_url(host)?;
    let endpoint = format!("{base}/api/system/restart");
    let response = crate::cli::idempotent(contexts::build_http_client()?.post(&endpoint))
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call restart endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "restart request failed with status {status}: {body}"
        )));
    }

    let payload = response
        .json::<RestartResponse>()
        .await
        .map_err(|err| Error::external(format!("failed to decode restart response: {err}")))?;

    if payload.accepted {
        println!(
            "[maestro]: restart accepted; the controller will stop all containers and restart"
        );
    } else {
        println!("[maestro]: restart was not accepted");
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
