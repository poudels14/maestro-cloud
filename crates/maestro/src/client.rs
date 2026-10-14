use std::path::Path;

use anyhow::Context;
use maestro_core::ServicesFile;
use serde_json::Value;

pub struct Endpoint {
    pub api: String,
    pub token: Option<String>,
}

impl Endpoint {
    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.authorize(reqwest::Client::new().get(format!("{}{path}", self.api)))
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.authorize(reqwest::Client::new().post(format!("{}{path}", self.api)))
    }

    fn authorize(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        }
    }
}

pub async fn rollout(endpoint: &Endpoint, file: &Path, apply: bool) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(file)
        .with_context(|| format!("failed to read {}", file.display()))?;
    let services = ServicesFile::parse(&text)?;
    let plan = request(
        endpoint
            .post(&format!("/api/rollout?apply={apply}"))
            .json(&services),
    )
    .await?;
    print_list("create", &plan["created"]);
    print_list("update", &plan["updated"]);
    print_list("unchanged", &plan["unchanged"]);
    print_list("not in file (left running)", &plan["unmanaged"]);
    match apply {
        true => print_list("queued deployments", &plan["deployments"]),
        false => println!("\nrun again with --apply to roll out these changes"),
    }
    Ok(())
}

pub async fn services(endpoint: &Endpoint) -> anyhow::Result<()> {
    let services = request(endpoint.get("/api/services")).await?;
    println!(
        "{:<24} {:<20} {:<12} IMAGE",
        "SERVICE", "DEPLOYMENT", "STATUS"
    );
    services
        .as_array()
        .into_iter()
        .flatten()
        .for_each(|service| {
            let deployment = &service["latestDeployment"];
            println!(
                "{:<24} {:<20} {:<12} {}",
                text(&service["id"]),
                text(&deployment["id"]),
                text(&deployment["status"]),
                text(&deployment["image"]),
            );
        });
    Ok(())
}

pub async fn deploy(endpoint: &Endpoint, service: &str) -> anyhow::Result<()> {
    let queued = request(endpoint.post(&format!("/api/services/{service}/deployments"))).await?;
    println!("queued deployment {}", text(&queued["deploymentId"]));
    Ok(())
}

pub async fn replicas(endpoint: &Endpoint) -> anyhow::Result<()> {
    let replicas = request(endpoint.get("/api/replicas")).await?;
    println!(
        "{:<28} {:<16} {:<12} {:<16} {:<9} LAST ERROR",
        "REPLICA", "SERVICE", "PHASE", "IP", "RESTARTS"
    );
    replicas
        .as_array()
        .into_iter()
        .flatten()
        .for_each(|replica| {
            println!(
                "{:<28} {:<16} {:<12} {:<16} {:<9} {}",
                text(&replica["replicaId"]),
                text(&replica["serviceId"]),
                text(&replica["phase"]),
                text(&replica["ip"]),
                text(&replica["restarts"]),
                text(&replica["lastError"]),
            );
        });
    Ok(())
}

pub async fn nodes(endpoint: &Endpoint) -> anyhow::Result<()> {
    let nodes = request(endpoint.get("/api/nodes")).await?;
    println!(
        "{:<20} {:<8} {:<16} {:<8} {:<10} STATE",
        "NODE", "ROLE", "SUBNET", "CPU", "MEMORY"
    );
    nodes.as_array().into_iter().flatten().for_each(|node| {
        println!(
            "{:<20} {:<8} {:<16} {:<8} {:<10} {}",
            text(&node["name"]),
            text(&node["role"]),
            text(&node["subnet"]),
            text(&node["capacity"]["cpu"]),
            text(&node["capacity"]["memory"]),
            text(&node["state"]),
        );
    });
    Ok(())
}

pub async fn logs(
    endpoint: &Endpoint,
    service: Option<&str>,
    query: Option<&str>,
    limit: usize,
) -> anyhow::Result<()> {
    let combined = service
        .map(|service| format!("service:{service}"))
        .into_iter()
        .chain(query.map(str::to_string))
        .collect::<Vec<_>>()
        .join(" ");
    let page = request(
        endpoint
            .get("/api/logs")
            .query(&[("query", combined), ("limit", limit.to_string())]),
    )
    .await?;
    let mut records = page["records"].as_array().cloned().unwrap_or_default();
    records.reverse();
    records.iter().for_each(|record| {
        println!(
            "{} {:<16} {:<20} {:<6} {}",
            text(&record["ts"]),
            text(&record["serviceId"]),
            text(&record["replicaId"]),
            text(&record["stream"]),
            text(&record["line"]),
        );
    });
    Ok(())
}

async fn request(builder: reqwest::RequestBuilder) -> anyhow::Result<Value> {
    let response = builder
        .send()
        .await
        .context("failed to reach the maestro API")?;
    let status = response.status();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    match status.is_success() {
        true => Ok(body),
        false => anyhow::bail!("{status}: {}", text(&body["error"])),
    }
}

fn print_list(label: &str, values: &Value) {
    let items = values
        .as_array()
        .into_iter()
        .flatten()
        .map(text)
        .collect::<Vec<_>>();
    if !items.is_empty() {
        println!("{label}: {}", items.join(", "));
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}
