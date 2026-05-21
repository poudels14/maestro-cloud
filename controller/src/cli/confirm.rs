use std::io::IsTerminal;

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterInfo {
    cluster_name: String,
    cluster_alias: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ConfigSnapshot {
    #[serde(default)]
    subnet: Option<String>,
}

pub async fn confirm_action(
    host: &str,
    action: &str,
    details: &[String],
    skip: bool,
) -> Result<bool> {
    if skip {
        return Ok(true);
    }
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(true);
    }

    let base = crate::cli::contexts::normalize_base_url(host)?;
    let client = crate::cli::contexts::build_http_client()?;
    let cluster = fetch::<ClusterInfo>(&client, &format!("{base}/api/cluster")).await?;
    let config = fetch::<ConfigSnapshot>(&client, &format!("{base}/api/config"))
        .await
        .ok();

    eprintln!();
    eprintln!("\x1b[1m{action}\x1b[0m");
    eprintln!(
        "  Cluster:  {} ({})",
        cluster.cluster_name, cluster.cluster_alias
    );
    if let Some(subnet) = config.as_ref().and_then(|c| c.subnet.as_deref()) {
        eprintln!("  Subnet:   {subnet}");
    }
    eprintln!("  Host:     {host}");
    for line in details {
        eprintln!("  {line}");
    }
    eprintln!();

    inquire::Confirm::new("Continue?")
        .with_default(false)
        .prompt()
        .map_err(|err| Error::external(format!("confirmation prompt failed: {err}")))
}

async fn fetch<T: serde::de::DeserializeOwned>(client: &reqwest::Client, url: &str) -> Result<T> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call {url}: {err}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "request to {url} failed with status {status}: {body}"
        )));
    }
    response
        .json::<T>()
        .await
        .map_err(|err| Error::external(format!("failed to decode response from {url}: {err}")))
}
