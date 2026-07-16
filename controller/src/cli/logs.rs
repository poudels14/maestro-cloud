use std::{collections::BTreeMap, time::Duration};

use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};

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
    #[arg(
        long = "query",
        help = "Datadog-style server-side filter, such as @http.status_code:[500 TO 599]"
    )]
    query: Option<String>,
    #[arg(
        long = "output",
        value_enum,
        default_value = "text",
        help = "Output format; JSON emits one structured log object per line"
    )]
    output: LogOutput,
    #[arg(
        long,
        help = "Include sequence, stream, origin, tags, and raw attributes in JSON output"
    )]
    full: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LogOutput {
    Text,
    Json,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceListItem {
    #[serde(flatten)]
    service: ServiceConfig,
    #[serde(default)]
    system: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoteLogEntry {
    #[serde(default)]
    seq: i64,
    ts: i64,
    level: String,
    text: String,
    source: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    attrs: Vec<(String, String)>,
    #[serde(flatten)]
    details: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct CompactLogEntry<'a> {
    ts: i64,
    level: &'a str,
    message: &'a str,
    source: &'a str,
    attributes: BTreeMap<&'a str, &'a str>,
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
    cursor: i64,
}

pub async fn run_logs(host: &str, args: RemoteLogsArgs) -> Result<()> {
    validate_output_options(args.output, args.full)?;
    let base = normalize_base_url(host)?;
    let client = reqwest::Client::new();
    let targets = discover_targets(&client, &base, &args).await?;
    run_targets(
        &client,
        &base,
        targets,
        args.tail,
        !args.no_follow,
        None,
        args.query.as_deref(),
        args.output,
        args.full,
    )
    .await
}

pub async fn run_daemon_logs(
    host: &str,
    source: Option<String>,
    tail: usize,
    follow: bool,
) -> Result<()> {
    let base = normalize_base_url(host)?;
    let client = reqwest::Client::new();
    let targets = if let Some(source) = source.as_deref() {
        vec![LogCursor {
            target: local_source_target(source)?,
            after: 0,
        }]
    } else {
        discover_targets(
            &client,
            &base,
            &RemoteLogsArgs {
                service: None,
                deployment: None,
                tail,
                no_follow: !follow,
                include_system: true,
                system: None,
                query: None,
                output: LogOutput::Text,
                full: false,
            },
        )
        .await?
    };
    run_targets(
        &client,
        &base,
        targets,
        tail,
        follow,
        source.as_deref(),
        None,
        LogOutput::Text,
        false,
    )
    .await
}

async fn run_targets(
    client: &reqwest::Client,
    base: &str,
    mut targets: Vec<LogCursor>,
    tail: usize,
    follow: bool,
    source_filter: Option<&str>,
    query: Option<&str>,
    output: LogOutput,
    full: bool,
) -> Result<()> {
    if targets.is_empty() {
        if output == LogOutput::Text {
            println!("[maestro]: no log targets found");
        }
        return Ok(());
    }

    let initial_entries =
        fetch_all_targets(client, base, &mut targets, tail, source_filter, query).await?;
    print_entries(initial_entries, output, full)?;

    if !follow {
        return Ok(());
    }

    loop {
        tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        let entries =
            fetch_all_targets(client, base, &mut targets, 500, source_filter, query).await?;
        print_entries(entries, output, full)?;
    }
}

fn local_source_target(source: &str) -> Result<LogTarget> {
    let parts = source.split('/').collect::<Vec<_>>();
    if let [service_id, deployment_id, unit] = parts.as_slice()
        && !service_id.is_empty()
        && !deployment_id.is_empty()
        && !unit.is_empty()
    {
        Ok(LogTarget::Deployment {
            service_id: (*service_id).to_string(),
            deployment_id: (*deployment_id).to_string(),
        })
    } else if let [name] = parts.as_slice()
        && !name.is_empty()
    {
        Ok(LogTarget::System {
            name: source.to_string(),
        })
    } else {
        Err(Error::invalid_input(format!(
            "invalid log source `{source}`; expected a system source name or service/deployment/unit"
        )))
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
        if let Some(service_id) = args.service.as_deref()
            && item.service.id != service_id
        {
            continue;
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
    source_filter: Option<&str>,
    query: Option<&str>,
) -> Result<Vec<RemoteLogEntry>> {
    let mut all_entries = Vec::new();

    for (index, target) in targets.iter().enumerate() {
        let (entries, cursor) = fetch_target_logs(client, base, target, tail, query).await?;
        all_entries.push(TargetEntries {
            index,
            entries,
            cursor,
        });
    }

    let mut merged = Vec::new();
    for TargetEntries {
        index,
        entries,
        cursor,
    } in all_entries
    {
        targets[index].after = targets[index].after.max(cursor);
        for entry in entries {
            targets[index].after = targets[index].after.max(entry.seq);
            if source_filter.is_none_or(|source| entry.source == source) {
                merged.push(entry);
            }
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
    query: Option<&str>,
) -> Result<(Vec<RemoteLogEntry>, i64)> {
    let endpoint = log_endpoint(base, &cursor.target, cursor.after, tail, query)?;
    let response = client.get(&endpoint).send().await.map_err(|err| {
        Error::external(format!("failed to call logs endpoint `{endpoint}`: {err}"))
    })?;

    let cursor = response
        .headers()
        .get("x-maestro-log-cursor")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(cursor.after);
    let entries = decode_response(response, "logs").await?;
    Ok((entries, cursor))
}

fn log_endpoint(
    base: &str,
    target: &LogTarget,
    after: i64,
    tail: usize,
    query: Option<&str>,
) -> Result<String> {
    let path = match target {
        LogTarget::Deployment {
            service_id,
            deployment_id,
        } => format!("/api/services/{service_id}/deployments/{deployment_id}/logs"),
        LogTarget::System { name } => format!("/api/system/{name}/logs"),
    };

    let mut endpoint = reqwest::Url::parse(&format!("{base}{path}"))
        .map_err(|error| Error::invalid_input(format!("invalid logs endpoint: {error}")))?;
    {
        let mut pairs = endpoint.query_pairs_mut();
        if after > 0 {
            pairs.append_pair("after", &after.to_string());
        }
        pairs.append_pair("tail", &tail.to_string());
        if let Some(query) = query.filter(|query| !query.trim().is_empty()) {
            pairs.append_pair("query", query);
        }
    }
    Ok(endpoint.into())
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

fn print_entries(entries: Vec<RemoteLogEntry>, output: LogOutput, full: bool) -> Result<()> {
    for entry in entries {
        match output {
            LogOutput::Text => println!("{}", format_log_entry(&entry)),
            LogOutput::Json => println!("{}", format_log_entry_json(&entry, full)?),
        }
    }
    Ok(())
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

fn format_log_entry_json(entry: &RemoteLogEntry, full: bool) -> Result<String> {
    if full {
        return serde_json::to_string(entry).map_err(|error| {
            Error::internal(format!("failed to encode full log entry as JSON: {error}"))
        });
    }
    let compact = CompactLogEntry {
        ts: entry.ts,
        level: &entry.level,
        message: &entry.text,
        source: &entry.source,
        attributes: entry
            .attrs
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect(),
    };
    serde_json::to_string(&compact)
        .map_err(|error| Error::internal(format!("failed to encode log entry as JSON: {error}")))
}

fn validate_output_options(output: LogOutput, full: bool) -> Result<()> {
    if full && output != LogOutput::Json {
        return Err(Error::invalid_input("--full requires --output json"));
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

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct TestLogsCli {
        #[command(flatten)]
        logs: RemoteLogsArgs,
    }

    #[test]
    fn output_flag_accepts_json_and_defaults_to_text() {
        let default = TestLogsCli::try_parse_from(["test"]).expect("parse default output");
        assert_eq!(default.logs.output, LogOutput::Text);

        let json =
            TestLogsCli::try_parse_from(["test", "--output", "json"]).expect("parse JSON output");
        assert_eq!(json.logs.output, LogOutput::Json);
        assert!(!json.logs.full);
        let full = TestLogsCli::try_parse_from(["test", "--output", "json", "--full"])
            .expect("parse full JSON output");
        assert!(full.logs.full);
        assert!(validate_output_options(full.logs.output, full.logs.full).is_ok());
        assert!(validate_output_options(LogOutput::Text, true).is_err());
        assert!(TestLogsCli::try_parse_from(["test", "--output", "yaml"]).is_err());
    }

    #[test]
    fn json_output_preserves_structured_log_details() {
        let entry: RemoteLogEntry = serde_json::from_value(serde_json::json!({
            "seq": 42,
            "ts": 1_784_167_568_000_i64,
            "level": "info",
            "stream": "stdout",
            "text": "request",
            "source": "agent-server/deployment/replica0",
            "origin": "service",
            "tags": ["service:agent-server"],
            "attrs": [["http.status_code", "404"], ["http.url_details.path", "/missing"]]
        }))
        .expect("decode complete log entry");

        let output = format_log_entry_json(&entry, false).expect("encode compact log entry");
        let output: serde_json::Value = serde_json::from_str(&output).expect("parse JSON output");
        assert_eq!(output["ts"], 1_784_167_568_000_i64);
        assert_eq!(output["level"], "info");
        assert_eq!(output["message"], "request");
        assert_eq!(output["source"], "agent-server/deployment/replica0");
        assert_eq!(output["attributes"]["http.status_code"], "404");
        assert_eq!(output["attributes"]["http.url_details.path"], "/missing");
        assert!(output.get("seq").is_none());
        assert!(output.get("stream").is_none());
        assert!(output.get("origin").is_none());
        assert!(output.get("tags").is_none());

        let full = format_log_entry_json(&entry, true).expect("encode full log entry");
        let full: serde_json::Value = serde_json::from_str(&full).expect("parse full JSON output");
        assert_eq!(full["seq"], 42);
        assert_eq!(full["stream"], "stdout");
        assert_eq!(full["origin"], "service");
        assert_eq!(full["tags"][0], "service:agent-server");
        assert_eq!(
            full["attrs"][0],
            serde_json::json!(["http.status_code", "404"])
        );
    }

    #[test]
    fn log_endpoint_uses_tail_until_cursor_is_set() {
        let target = LogTarget::Deployment {
            service_id: "svc".to_string(),
            deployment_id: "dep".to_string(),
        };

        assert_eq!(
            log_endpoint("http://host", &target, 0, 25, None).expect("endpoint"),
            "http://host/api/services/svc/deployments/dep/logs?tail=25"
        );
        assert_eq!(
            log_endpoint("http://host", &target, 42, 500, None).expect("endpoint"),
            "http://host/api/services/svc/deployments/dep/logs?after=42&tail=500"
        );
    }

    #[test]
    fn log_endpoint_supports_system_targets() {
        let target = LogTarget::System {
            name: "maestro-admin".to_string(),
        };

        assert_eq!(
            log_endpoint("http://host", &target, 0, 10, None).expect("endpoint"),
            "http://host/api/system/maestro-admin/logs?tail=10"
        );
    }

    #[test]
    fn log_endpoint_encodes_search_query() {
        let target = LogTarget::System {
            name: "maestro-ingress".to_string(),
        };
        let endpoint = log_endpoint(
            "http://host",
            &target,
            0,
            100,
            Some("@http.status_code:[500 TO 599]"),
        )
        .expect("endpoint");
        assert_eq!(
            endpoint,
            "http://host/api/system/maestro-ingress/logs?tail=100&query=%40http.status_code%3A%5B500+TO+599%5D"
        );
    }

    #[test]
    fn local_source_routes_packed_service_identity_to_deployment_api() {
        assert!(matches!(
            local_source_target("api/dep/replica0").expect("service source"),
            LogTarget::Deployment {
                service_id,
                deployment_id
            } if service_id == "api" && deployment_id == "dep"
        ));
        assert!(matches!(
            local_source_target("maestro-probe").expect("system source"),
            LogTarget::System { name } if name == "maestro-probe"
        ));
        assert!(local_source_target("api/dep").is_err());
        assert!(local_source_target("api/dep/replica0/extra").is_err());
    }
}
