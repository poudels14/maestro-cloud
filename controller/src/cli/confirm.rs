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

struct ConfirmationContext {
    cluster: Option<ClusterInfo>,
    config: Option<ConfigSnapshot>,
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
    let context = fetch_confirmation_context(&client, &base).await;

    eprintln!();
    eprintln!("\x1b[1m{action}\x1b[0m");
    if let Some(cluster) = context.cluster {
        eprintln!(
            "  Cluster:  {} ({})",
            cluster.cluster_name, cluster.cluster_alias
        );
    } else {
        eprintln!("  Cluster:  unavailable");
    }
    if let Some(subnet) = context
        .config
        .as_ref()
        .and_then(|config| config.subnet.as_deref())
    {
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

async fn fetch_confirmation_context(client: &reqwest::Client, base: &str) -> ConfirmationContext {
    let cluster_url = format!("{base}/api/cluster");
    let config_url = format!("{base}/api/config");
    let (cluster, config) = tokio::join!(
        fetch::<ClusterInfo>(client, &cluster_url),
        fetch::<ConfigSnapshot>(client, &config_url),
    );
    ConfirmationContext {
        cluster: cluster.ok(),
        config: config.ok(),
    }
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

#[cfg(test)]
mod tests {
    use axum::{Json, Router, http::StatusCode, routing::get};
    use serde_json::json;

    use super::fetch_confirmation_context;

    #[tokio::test]
    async fn confirmation_tolerates_cluster_status_without_an_elected_leader() {
        let app = Router::new()
            .route(
                "/api/cluster",
                get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "election: no leader") }),
            )
            .route(
                "/api/config",
                get(|| async { Json(json!({ "subnet": "10.100.0.0/16" })) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("read test server address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve test app");
        });

        let context =
            fetch_confirmation_context(&reqwest::Client::new(), &format!("http://{address}")).await;

        assert!(context.cluster.is_none());
        assert_eq!(
            context.config.and_then(|config| config.subnet),
            Some("10.100.0.0/16".to_string())
        );
        server.abort();
    }
}
