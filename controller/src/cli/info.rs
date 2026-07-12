use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterInfo {
    cluster_name: String,
    cluster_alias: String,
    canonical_domain: String,
    alias_domain: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    upgrading: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetricPoint {
    ts: i64,
    #[serde(default)]
    cpu_percent: f64,
    #[serde(default)]
    memory_bytes: i64,
    #[serde(default)]
    memory_limit_bytes: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MaskedConfigView {
    ingress: IngressView,
    #[serde(default)]
    subnet: Option<String>,
    #[serde(default)]
    egress: EgressView,
    #[serde(default)]
    system: Option<String>,
    runtime: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    tailscale: Option<TailscaleView>,
    #[serde(default)]
    datadog: Option<DatadogView>,
    #[serde(default)]
    depot: Option<DepotView>,
    #[serde(default)]
    cloudflare: Option<CloudflareView>,
    #[serde(default)]
    slack: Option<SlackView>,
    #[serde(default)]
    disable_etcd_cert: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct IngressView {
    ports: Vec<u16>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct EgressView {
    #[serde(default)]
    deny: Vec<String>,
    #[serde(default)]
    allow: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct TailscaleView {
    #[serde(default)]
    auth_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DatadogView {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    site: Option<String>,
    #[serde(default)]
    include_ingress_logs: bool,
    #[serde(default)]
    include_tailscale_logs: bool,
    #[serde(default)]
    logs: DatadogLogsView,
    #[serde(default)]
    include_metrics: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DatadogLogsView {
    #[serde(default = "default_true")]
    include_healthcheck: bool,
}

impl Default for DatadogLogsView {
    fn default() -> Self {
        Self {
            include_healthcheck: true,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DepotView {
    #[serde(default)]
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CloudflareView {
    tunnel: CloudflareTunnelView,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CloudflareTunnelView {
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    replicas: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SlackView {
    #[serde(default)]
    webhook_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SlackWebhookView {
    #[serde(default)]
    name: String,
    #[serde(default)]
    categories: Vec<String>,
    #[serde(default)]
    enabled: bool,
}

pub async fn run_info(host: &str) -> Result<()> {
    let base = crate::cli::contexts::normalize_base_url(host)?;
    let client = crate::cli::contexts::build_http_client()?;

    let cluster: ClusterInfo = fetch(&client, &format!("{base}/api/cluster")).await?;
    let config: MaskedConfigView = fetch(&client, &format!("{base}/api/config")).await?;
    let slack_webhooks: Vec<SlackWebhookView> =
        fetch(&client, &format!("{base}/api/webhooks/slack"))
            .await
            .unwrap_or_default();
    let node_metric = fetch::<Vec<MetricPoint>>(&client, &format!("{base}/api/metrics/node"))
        .await
        .ok()
        .and_then(|points| points.into_iter().last());

    println!("Cluster");
    println!("  name              {}", cluster.cluster_name);
    println!("  alias             {}", cluster.cluster_alias);
    println!("  canonical domain  {}", cluster.canonical_domain);
    println!("  alias domain      {}", cluster.alias_domain);
    if let Some(version) = cluster.version.as_deref() {
        println!("  version           {version}");
    }
    if cluster.upgrading {
        println!("  upgrade           in progress");
    }

    if let Some(metric) = node_metric.as_ref() {
        println!("\nMetrics");
        println!("  node cpu          {}", format_cpu(metric.cpu_percent));
        println!(
            "  node memory       {}",
            format_memory(metric.memory_bytes, metric.memory_limit_bytes)
        );
        if let Some(age) = metric_age_label(metric.ts) {
            println!("  as of             {age}");
        }
    }

    println!("\nNetwork");
    println!(
        "  subnet            {}",
        config.subnet.as_deref().unwrap_or("(auto)")
    );
    println!(
        "  ingress ports     {}",
        if config.ingress.ports.is_empty() {
            "(none)".to_string()
        } else {
            config
                .ingress
                .ports
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
    if !config.egress.deny.is_empty() {
        println!("  egress deny       {}", config.egress.deny.join(", "));
    }
    if !config.egress.allow.is_empty() {
        println!("  egress allow      {}", config.egress.allow.join(", "));
    }

    println!("\nRuntime");
    println!("  container         {}", config.runtime);
    if let Some(system) = config.system.as_deref() {
        println!("  host system       {}", system);
    }
    if config.disable_etcd_cert {
        println!("  etcd mTLS         disabled");
    }
    if !config.tags.is_empty() {
        println!("  tags              {}", config.tags.join(", "));
    }

    if !slack_webhooks.is_empty() {
        println!("\nSlack webhooks");
        for webhook in &slack_webhooks {
            let status = if webhook.enabled {
                "enabled"
            } else {
                "disabled"
            };
            let categories = if webhook.categories.is_empty() {
                "(none)".to_string()
            } else {
                webhook.categories.join(",")
            };
            println!(
                "  {:<32} {status:<9} categories: {categories}",
                webhook.name
            );
        }
    }

    println!("\nIntegrations");
    print_integration("tailscale", config.tailscale.as_ref().map(format_tailscale));
    print_integration(
        "cloudflare",
        config.cloudflare.as_ref().map(format_cloudflare),
    );
    print_integration("datadog", config.datadog.as_ref().map(format_datadog));
    print_integration("depot", config.depot.as_ref().map(format_depot));
    print_integration("slack", config.slack.as_ref().map(format_slack));

    Ok(())
}

fn print_integration(name: &str, value: Option<String>) {
    match value {
        Some(detail) => println!("  {name:<17} enabled  ({detail})"),
        None => println!("  {name:<17} off"),
    }
}

fn format_tailscale(view: &TailscaleView) -> String {
    format!(
        "auth-key: {}",
        view.auth_key.as_deref().unwrap_or("(unset)")
    )
}

fn format_cloudflare(view: &CloudflareView) -> String {
    let replicas = view.tunnel.replicas.unwrap_or(2);
    format!(
        "token: {}, replicas: {replicas}",
        view.tunnel.token.as_deref().unwrap_or("(unset)")
    )
}

fn format_datadog(view: &DatadogView) -> String {
    format!(
        "api-key: {}, site: {}, ingress-logs: {}, tailscale-logs: {}, include-healthcheck: {}, metrics: {}",
        view.api_key.as_deref().unwrap_or("(unset)"),
        view.site.as_deref().unwrap_or("(default)"),
        view.include_ingress_logs,
        view.include_tailscale_logs,
        view.logs.include_healthcheck,
        view.include_metrics,
    )
}

fn format_depot(view: &DepotView) -> String {
    format!("token: {}", view.token.as_deref().unwrap_or("(unset)"))
}

fn format_slack(view: &SlackView) -> String {
    format!(
        "webhook-url: {}",
        view.webhook_url.as_deref().unwrap_or("(unset)")
    )
}

fn format_cpu(percent: f64) -> String {
    format!("{:.1}%", percent)
}

fn format_memory(used_bytes: i64, limit_bytes: i64) -> String {
    let used = format_bytes(used_bytes);
    if limit_bytes <= 0 {
        return used;
    }
    let limit = format_bytes(limit_bytes);
    let percent = (used_bytes as f64 / limit_bytes as f64) * 100.0;
    format!("{used} / {limit} ({percent:.1}%)")
}

fn format_bytes(bytes: i64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    const TIB: f64 = GIB * 1024.0;
    let value = bytes as f64;
    if value >= TIB {
        format!("{:.2} TiB", value / TIB)
    } else if value >= GIB {
        format!("{:.2} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.1} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.1} KiB", value / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn metric_age_label(ts_ms: i64) -> Option<String> {
    if ts_ms <= 0 {
        return None;
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    let age_secs = (now_ms - ts_ms) / 1000;
    if age_secs < 0 {
        return Some("just now".to_string());
    }
    if age_secs < 60 {
        return Some(format!("{age_secs}s ago"));
    }
    let minutes = age_secs / 60;
    if minutes < 60 {
        return Some(format!("{minutes}m ago"));
    }
    let hours = minutes / 60;
    Some(format!("{hours}h ago"))
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
