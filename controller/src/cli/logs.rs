use std::time::Duration;

use clap::Args;
use serde::Deserialize;

use crate::deployment::types::{DeploymentStatus, DeploymentWithReplicas, ServiceConfig};
use crate::error::{Error, Result};

const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Args)]
pub struct RemoteLogsArgs {
    #[arg(short = 's', long = "service", help = "Service ID to stream")]
    service: Option<String>,
    #[arg(
        long = "deployment",
        requires = "service",
        help = "Deployment ID to stream"
    )]
    deployment: Option<String>,
    #[arg(
        long = "tail",
        default_value_t = 100,
        help = "Number of recent log entries"
    )]
    tail: usize,
    #[arg(long = "no-follow", help = "Print recent logs and exit")]
    no_follow: bool,
    #[arg(long = "include-system", help = "Include Maestro system service logs")]
    include_system: bool,
    #[arg(long = "system", help = "Only stream a Maestro system service")]
    system: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceListItem {
    #[serde(flatten)]
    service: ServiceConfig,
    #[serde(default)]
    system: bool,
}

#[derive(Debug, Deserialize)]
struct RemoteLogEntry {
    #[serde(default)]
    seq: i64,
    ts: i64,
    level: String,
    text: String,
    source: String,
}

#[derive(Debug)]
enum LogTarget {
    Deployment {
        service_id: String,
        deployment_id: String,
    },
    System {
        name: String,
    },
}

struct LogCursor {
    target: LogTarget,
    after: i64,
}

struct TargetEntries {
    index: usize,
    entries: Vec<RemoteLogEntry>,
}

pub async fn run_logs(host: &str, args: RemoteLogsArgs) -> Result<()> {
    let base = normalize_base_url(host)?;
    let client = reqwest::Client::new();
    let mut targets = discover_targets(&client, &base, &args).await?;

    if targets.is_empty() {
        println!("[maestro]: no log targets found");
        return Ok(());
    }

    let initial_entries = fetch_all_targets(&client, &base, &mut targets, args.tail).await?;
    print_entries(initial_entries);

    if args.no_follow {
        return Ok(());
    }

    loop {
        tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        let entries = fetch_all_targets(&client, &base, &mut targets, 500).await?;
        print_entries(entries);
    }
}

async fn discover_targets(
    client: &reqwest::Client,
    base: &str,
    args: &RemoteLogsArgs,
) -> Result<Vec<LogCursor>> {
    if let Some(name) = args.system.as_deref() {
        return Ok(vec![LogCursor {
            target: LogTarget::System {
                name: name.to_string(),
            },
            after: 0,
        }]);
    }

    let services = list_services(client, base).await?;
    let mut targets = Vec::new();

    for item in services {
        if let Some(service_id) = args.service.as_deref() {
            if item.service.id != service_id {
                continue;
            }
        }

        if item.system {
            if args.include_system {
                targets.push(LogCursor {
                    target: LogTarget::System {
                        name: item.service.id,
                    },
                    after: 0,
                });
            }
            continue;
        }

        let deployments = list_deployments(client, base, &item.service.id).await?;
        let deployment = if let Some(deployment_id) = args.deployment.as_deref() {
            deployments
                .into_iter()
                .find(|deployment| deployment.deployment.id == deployment_id)
                .ok_or_else(|| {
                    Error::not_found(format!(
                        "deployment `{deployment_id}` was not found for service `{}`",
                        item.service.id
                    ))
                })?
        } else {
            let Some(deployment) = deployments.into_iter().find(is_streamable_deployment) else {
                continue;
            };
            deployment
        };

        targets.push(LogCursor {
            target: LogTarget::Deployment {
                service_id: item.service.id,
                deployment_id: deployment.deployment.id,
            },
            after: 0,
        });
    }

    if args.service.is_some() && targets.is_empty() {
        return Err(Error::not_found(format!(
            "service `{}` was not found",
            args.service.as_deref().unwrap_or_default()
        )));
    }

    Ok(targets)
}

fn is_streamable_deployment(deployment: &DeploymentWithReplicas) -> bool {
    !matches!(
        deployment.deployment.status,
        DeploymentStatus::Removed | DeploymentStatus::Canceled
    )
}

async fn list_services(client: &reqwest::Client, base: &str) -> Result<Vec<ServiceListItem>> {
    let endpoint = format!("{base}/api/services");
    let response = client
        .get(&endpoint)
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call services endpoint: {err}")))?;

    decode_response(response, "services").await
}

async fn list_deployments(
    client: &reqwest::Client,
    base: &str,
    service_id: &str,
) -> Result<Vec<DeploymentWithReplicas>> {
    let endpoint = format!("{base}/api/services/{service_id}/deployments");
    let response = client.get(&endpoint).send().await.map_err(|err| {
        Error::external(format!(
            "failed to call deployments endpoint for service `{service_id}`: {err}"
        ))
    })?;

    decode_response(response, &format!("deployments for service `{service_id}`")).await
}

async fn fetch_all_targets(
    client: &reqwest::Client,
    base: &str,
    targets: &mut [LogCursor],
    tail: usize,
) -> Result<Vec<RemoteLogEntry>> {
    let mut all_entries = Vec::new();

    for index in 0..targets.len() {
        let entries = fetch_target_logs(client, base, &targets[index], tail).await?;
        all_entries.push(TargetEntries { index, entries });
    }

    let mut merged = Vec::new();
    for TargetEntries { index, entries } in all_entries {
        for entry in entries {
            targets[index].after = targets[index].after.max(entry.seq);
            merged.push(entry);
        }
    }
    merged.sort_by(|a, b| a.seq.cmp(&b.seq).then_with(|| a.ts.cmp(&b.ts)));
    Ok(merged)
}

async fn fetch_target_logs(
    client: &reqwest::Client,
    base: &str,
    cursor: &LogCursor,
    tail: usize,
) -> Result<Vec<RemoteLogEntry>> {
    let endpoint = log_endpoint(base, &cursor.target, cursor.after, tail);
    let response = client.get(&endpoint).send().await.map_err(|err| {
        Error::external(format!("failed to call logs endpoint `{endpoint}`: {err}"))
    })?;

    decode_response(response, "logs").await
}

fn log_endpoint(base: &str, target: &LogTarget, after: i64, tail: usize) -> String {
    let path = match target {
        LogTarget::Deployment {
            service_id,
            deployment_id,
        } => format!("/api/services/{service_id}/deployments/{deployment_id}/logs"),
        LogTarget::System { name } => format!("/api/system/{name}/logs"),
    };

    if after > 0 {
        format!("{base}{path}?after={after}&tail={tail}")
    } else {
        format!("{base}{path}?tail={tail}")
    }
}

async fn decode_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    label: &str,
) -> Result<T> {
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "{label} request failed with status {status}: {body}"
        )));
    }

    response
        .json::<T>()
        .await
        .map_err(|err| Error::external(format!("failed to decode {label} response: {err}")))
}

fn print_entries(entries: Vec<RemoteLogEntry>) {
    for entry in entries {
        println!("{}", format_log_entry(&entry));
    }
}

fn format_log_entry(entry: &RemoteLogEntry) -> String {
    let ts = chrono::DateTime::from_timestamp_millis(entry.ts)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| entry.ts.to_string());
    format!(
        "{ts}  {:<5}  [{}]  {}",
        entry.level.to_uppercase(),
        entry.source,
        entry.text
    )
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
mod tests {
    use super::*;

    #[test]
    fn log_endpoint_uses_tail_until_cursor_is_set() {
        let target = LogTarget::Deployment {
            service_id: "svc".to_string(),
            deployment_id: "dep".to_string(),
        };

        assert_eq!(
            log_endpoint("http://host", &target, 0, 25),
            "http://host/api/services/svc/deployments/dep/logs?tail=25"
        );
        assert_eq!(
            log_endpoint("http://host", &target, 42, 500),
            "http://host/api/services/svc/deployments/dep/logs?after=42&tail=500"
        );
    }

    #[test]
    fn log_endpoint_supports_system_targets() {
        let target = LogTarget::System {
            name: "maestro-admin".to_string(),
        };

        assert_eq!(
            log_endpoint("http://host", &target, 0, 10),
            "http://host/api/system/maestro-admin/logs?tail=10"
        );
    }
}
