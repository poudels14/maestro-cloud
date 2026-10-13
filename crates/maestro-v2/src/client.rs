use std::path::Path;

use anyhow::Context;
use maestro_core::ServicesFile;
use serde_json::Value;

pub async fn rollout(api: &str, file: &Path, apply: bool) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(file)
        .with_context(|| format!("failed to read {}", file.display()))?;
    let services = ServicesFile::parse(&text)?;
    let plan = request(
        reqwest::Client::new()
            .post(format!("{api}/api/rollout?apply={apply}"))
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

pub async fn services(api: &str) -> anyhow::Result<()> {
    let services = request(reqwest::Client::new().get(format!("{api}/api/services"))).await?;
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

pub async fn deploy(api: &str, service: &str) -> anyhow::Result<()> {
    let queued =
        request(reqwest::Client::new().post(format!("{api}/api/services/{service}/deployments")))
            .await?;
    println!("queued deployment {}", text(&queued["deploymentId"]));
    Ok(())
}

pub async fn replicas(api: &str) -> anyhow::Result<()> {
    let replicas = request(reqwest::Client::new().get(format!("{api}/api/replicas"))).await?;
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

pub async fn nodes(api: &str) -> anyhow::Result<()> {
    let nodes = request(reqwest::Client::new().get(format!("{api}/api/nodes"))).await?;
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
