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
    #[serde(default, rename = "nodeId", skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
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
    #[serde(rename = "nodeId", skip_serializing_if = "Option::is_none")]
    node_id: Option<&'a str>,
    attributes: BTreeMap<&'a str, &'a str>,
}

#[derive(Debug, Clone)]
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
    node_id: Option<String>,
    after: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterNode {
    node_id: String,
    alive: Option<bool>,
}

struct TargetEntries {
    index: usize,
    entries: Vec<RemoteLogEntry>,
    cursor: i64,
}

pub async fn run_logs(host: &str, args: RemoteLogsArgs) -> Result<()> {
    validate_output_options(args.output, args.full)?;
    let base = normalize_base_url(host)?;
    let client = crate::cli::contexts::build_http_client()?;
    let targets = discover_targets(&client, &base, &args).await?;
    let targets = expand_targets_across_cluster(&client, &base, targets).await;
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
            node_id: None,
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
            node_id: None,
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
                    node_id: None,
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
            node_id: None,
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

async fn expand_targets_across_cluster(
    client: &reqwest::Client,
    base: &str,
    targets: Vec<LogCursor>,
) -> Vec<LogCursor> {
    let endpoint = format!("{base}/api/cluster/nodes");
    let Ok(response) = client.get(endpoint).send().await else {
        return targets;
    };
    let Ok(nodes) = decode_response::<Vec<ClusterNode>>(response, "cluster nodes").await else {
        return targets;
    };
    let node_ids = nodes
        .into_iter()
        .filter(|node| node.alive != Some(false))
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    if node_ids.is_empty() {
        return targets;
    }

    targets
        .into_iter()
        .flat_map(|cursor| {
            node_ids.iter().cloned().map(move |node_id| LogCursor {
                target: cursor.target.clone(),
                node_id: Some(node_id),
                after: 0,
            })
        })
        .collect()
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
    merged.sort_by(|a, b| {
        a.ts.cmp(&b.ts)
            .then_with(|| a.node_id.cmp(&b.node_id))
            .then_with(|| a.seq.cmp(&b.seq))
    });
    Ok(merged)
}

async fn fetch_target_logs(
    client: &reqwest::Client,
    base: &str,
    cursor: &LogCursor,
    tail: usize,
    query: Option<&str>,
) -> Result<(Vec<RemoteLogEntry>, i64)> {
    let endpoint = log_endpoint(
        base,
        &cursor.target,
        cursor.node_id.as_deref(),
        cursor.after,
        tail,
        query,
    )?;
    let response = client.get(&endpoint).send().await.map_err(|err| {
        Error::external(format!("failed to call logs endpoint `{endpoint}`: {err}"))
    })?;

    let cursor_seq = response
        .headers()
        .get("x-maestro-log-cursor")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(cursor.after);
    let response_node_id = response
        .headers()
        .get("x-maestro-node-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .or_else(|| cursor.node_id.clone());
    let mut entries: Vec<RemoteLogEntry> = decode_response(response, "logs").await?;
    for entry in &mut entries {
        entry.node_id.clone_from(&response_node_id);
    }
    Ok((entries, cursor_seq))
}

fn log_endpoint(
    base: &str,
    target: &LogTarget,
    node_id: Option<&str>,
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
        if let Some(node_id) = node_id {
            pairs.append_pair("nodeId", node_id);
        }
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
    let source = entry.node_id.as_ref().map_or_else(
        || entry.source.clone(),
        |node_id| format!("{node_id} / {}", entry.source),
    );
    format!(
        "{ts}  {:<5}  [{source}]  {}",
        entry.level.to_uppercase(),
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
        node_id: entry.node_id.as_deref(),
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
    use std::sync::{Arc, Mutex};

    use axum::{
        Json, Router,
        extract::{Query, State},
        http::HeaderValue,
        response::IntoResponse,
        routing::get,
    };
    use clap::Parser;

    use super::*;
    use crate::deployment::types::{ReplicaState, ServiceDeployConfig, ServiceDeployment};

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
            log_endpoint("http://host", &target, None, 0, 25, None).expect("endpoint"),
            "http://host/api/services/svc/deployments/dep/logs?tail=25"
        );
        assert_eq!(
            log_endpoint("http://host", &target, None, 42, 500, None).expect("endpoint"),
            "http://host/api/services/svc/deployments/dep/logs?after=42&tail=500"
        );
    }

    #[test]
    fn log_endpoint_supports_system_targets() {
        let target = LogTarget::System {
            name: "maestro-admin".to_string(),
        };

        assert_eq!(
            log_endpoint("http://host", &target, None, 0, 10, None).expect("endpoint"),
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
            None,
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
    fn log_endpoint_selects_a_cluster_node() {
        let target = LogTarget::Deployment {
            service_id: "svc".to_string(),
            deployment_id: "dep".to_string(),
        };

        assert_eq!(
            log_endpoint("http://host", &target, Some("node-a"), 0, 25, None).expect("endpoint"),
            "http://host/api/services/svc/deployments/dep/logs?tail=25&nodeId=node-a"
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

    fn test_service_config() -> ServiceConfig {
        ServiceConfig {
            id: "svc".to_string(),
            name: "Service".to_string(),
            version: "v1".to_string(),
            build: None,
            image: Some("example/service:latest".to_string()),
            deploy: ServiceDeployConfig {
                flags: Vec::new(),
                expose_ports: Vec::new(),
                command: None,
                healthcheck_path: None,
                healthcheck_interval: 60,
                replicas: 2,
                exec: true,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
                volumes: Vec::new(),
                node_affinity: None,
                egress: Default::default(),
            },
            ingress: None,
            preview: None,
            preview_source: None,
        }
    }

    fn test_deployment() -> DeploymentWithReplicas {
        DeploymentWithReplicas {
            deployment: ServiceDeployment {
                id: "dep".to_string(),
                created_at: 1,
                deployed_at: Some(1),
                drained_at: None,
                status: DeploymentStatus::Ready,
                config: test_service_config(),
                git_commit: None,
                build: None,
                upload_archive: None,
            },
            replicas: ["node-a", "node-b"]
                .into_iter()
                .enumerate()
                .map(|(index, node_id)| ReplicaState {
                    service_id: Some("svc".to_string()),
                    deployment_id: Some("dep".to_string()),
                    replica_index: u32::try_from(index).expect("replica index"),
                    status: DeploymentStatus::Ready,
                    healthcheck_failures: 0,
                    restart_attempts: 0,
                    node_id: Some(node_id.to_string()),
                    assignment_id: Some(format!("assignment-{index}")),
                    endpoint: None,
                    error: None,
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn remote_logs_merge_nodes_with_independent_cursors() {
        type Requests = Arc<Mutex<Vec<(String, i64)>>>;

        async fn logs(
            State(requests): State<Requests>,
            Query(query): Query<BTreeMap<String, String>>,
        ) -> impl IntoResponse {
            let node_id = query.get("nodeId").cloned().unwrap_or_default();
            let after = query
                .get("after")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default();
            requests
                .lock()
                .expect("request lock")
                .push((node_id.clone(), after));
            let (seq, ts) = if node_id == "node-a" {
                (101, 2_000)
            } else {
                (7, 1_000)
            };
            let entries = if after == 0 {
                vec![serde_json::json!({
                    "seq": seq,
                    "ts": ts,
                    "level": "info",
                    "text": format!("from-{node_id}"),
                    "source": "svc/dep/replica0"
                })]
            } else {
                Vec::new()
            };
            let mut response = Json(entries).into_response();
            response.headers_mut().insert(
                "x-maestro-log-cursor",
                HeaderValue::from_str(&seq.to_string()).expect("cursor header"),
            );
            response.headers_mut().insert(
                "x-maestro-node-id",
                HeaderValue::from_str(&node_id).expect("node header"),
            );
            response
        }

        let requests: Requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/api/services",
                get(|| async {
                    Json(vec![serde_json::json!({
                        "id": "svc",
                        "name": "Service",
                        "version": "v1",
                        "image": "example/service:latest",
                        "deploy": { "replicas": 2, "exec": true },
                        "system": false
                    })])
                }),
            )
            .route(
                "/api/services/svc/deployments",
                get(|| async { Json(vec![test_deployment()]) }),
            )
            .route(
                "/api/cluster/nodes",
                get(|| async {
                    Json(serde_json::json!([
                        { "nodeId": "node-a", "alive": true },
                        { "nodeId": "node-b", "alive": true },
                        { "nodeId": "dead-node", "alive": false }
                    ]))
                }),
            )
            .route("/api/services/svc/deployments/dep/logs", get(logs))
            .with_state(requests.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind log test server");
        let address = listener.local_addr().expect("log test address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve log test API");
        });
        let base = format!("http://{address}");
        let client = reqwest::Client::new();
        let args = RemoteLogsArgs {
            service: Some("svc".to_string()),
            deployment: None,
            tail: 100,
            no_follow: true,
            include_system: false,
            system: None,
            query: None,
            output: LogOutput::Json,
            full: true,
        };
        let targets = discover_targets(&client, &base, &args)
            .await
            .expect("discover deployment target");
        let mut targets = expand_targets_across_cluster(&client, &base, targets).await;
        assert_eq!(targets.len(), 2, "dead nodes must not become log targets");

        let entries = fetch_all_targets(&client, &base, &mut targets, 100, None, None)
            .await
            .expect("fetch logs from every node");
        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.node_id.as_deref(), entry.text.as_str()))
                .collect::<Vec<_>>(),
            [
                (Some("node-b"), "from-node-b"),
                (Some("node-a"), "from-node-a")
            ]
        );

        fetch_all_targets(&client, &base, &mut targets, 100, None, None)
            .await
            .expect("poll each node from its own cursor");
        let requests = requests.lock().expect("request lock").clone();
        assert!(requests.contains(&("node-a".to_string(), 101)));
        assert!(requests.contains(&("node-b".to_string(), 7)));
        assert!(!requests.iter().any(|(node, _)| node == "dead-node"));
        server.abort();
    }
}
