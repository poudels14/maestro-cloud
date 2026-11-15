use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use clap::Subcommand;
use maestro_core::{EnvSpec, ServiceId, ServiceSpec, ServicesFile, cluster_domain, version_label};

use crate::prompt::{self, Choice};
use crate::uploads::{self, UploadSpec};
use serde_json::{Value, json};

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

    fn patch(&self, path: &str) -> reqwest::RequestBuilder {
        self.authorize(reqwest::Client::new().patch(format!("{}{path}", self.api)))
    }

    fn delete(&self, path: &str) -> reqwest::RequestBuilder {
        self.authorize(reqwest::Client::new().delete(format!("{}{path}", self.api)))
    }

    fn authorize(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        }
    }
}

#[derive(Subcommand)]
pub enum ServiceCommand {
    Freeze {
        service: Option<String>,
    },
    Unfreeze {
        service: Option<String>,
    },
    Redeploy {
        service: Option<String>,
        #[arg(long)]
        force: bool,
    },
    Restart {
        service: Option<String>,
        #[arg(long)]
        force: bool,
    },
    Scale {
        service: Option<String>,
        replicas: Option<u32>,
        #[arg(long, conflicts_with = "replicas")]
        reset: bool,
    },
    Stop {
        service: Option<String>,
        deployment: Option<String>,
    },
    Cancel {
        service: Option<String>,
        deployment: Option<String>,
    },
    Forget {
        service: Option<String>,
        deployment: Option<String>,
    },
    Delete {
        service: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum NodeCommand {
    Drain { node: Option<String> },
    Restore { node: Option<String> },
    Remove { node: Option<String> },
}

#[derive(Subcommand)]
pub enum ClusterCommand {
    Info,
    Upgrade {
        version: Option<String>,
    },
    Restart {
        #[arg(long)]
        node: Option<u64>,
    },
    Unfreeze,
}

pub async fn cluster(endpoint: &Endpoint, command: ClusterCommand) -> anyhow::Result<()> {
    match command {
        ClusterCommand::Info => {
            let info = request(endpoint.get("/api/cluster")).await?;
            let nodes = request(endpoint.get("/api/nodes")).await?;
            let member = |member: &Value| {
                let name = nodes
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|node| node["id"] == member["id"])
                    .map_or_else(|| text(&member["id"]), |node| text(&node["name"]));
                format!("{name} ({})", text(&member["address"]))
            };
            let name = text(&info["name"]);
            println!("{:<12} {name}", "name");
            println!("{:<12} http://{}", "dashboard", cluster_domain(&name));
            println!("{:<12} {}", "version", text(&info["version"]));
            println!("{:<12} {}", "cidr", text(&info["cidr"]));
            let leader = if info["leader"].is_object() {
                member(&info["leader"])
            } else {
                "none".to_string()
            };
            println!("{:<12} {leader}", "leader");
            let voters = info["voters"]
                .as_array()
                .into_iter()
                .flatten()
                .map(member)
                .collect::<Vec<_>>();
            println!("{:<12} {}", "voters", voters.join(", "));
            if info["maintenance"].is_object() {
                let maintenance = &info["maintenance"];
                println!(
                    "{:<12} {} to {} ({})",
                    "maintenance",
                    text(&maintenance["kind"]),
                    text(&maintenance["targetVersion"]),
                    text(&maintenance["phase"])
                );
            }
            println!("components");
            info["versions"]
                .as_array()
                .into_iter()
                .flatten()
                .for_each(|component| {
                    println!(
                        "  {:<12} {}",
                        text(&component["name"]),
                        text(&component["version"])
                    );
                });
            Ok(())
        }
        ClusterCommand::Upgrade { version } => {
            let run = request(
                endpoint
                    .post("/api/cluster/upgrade")
                    .json(&json!({ "targetVersion": version })),
            )
            .await?;
            follow_maintenance(endpoint, run).await
        }
        ClusterCommand::Restart { node } => {
            let run = request(
                endpoint
                    .post("/api/cluster/restart")
                    .json(&json!({ "nodeId": node })),
            )
            .await?;
            follow_maintenance(endpoint, run).await
        }
        ClusterCommand::Unfreeze => {
            request(endpoint.post("/api/cluster/unfreeze")).await?;
            println!("cluster deploys are unfrozen");
            Ok(())
        }
    }
}

pub async fn node(endpoint: &Endpoint, command: NodeCommand) -> anyhow::Result<()> {
    match command {
        NodeCommand::Drain { node } => {
            let node = pick_node(endpoint, node).await?;
            request(endpoint.post(&format!("/api/cluster/nodes/{node}/drain"))).await?;
            println!("draining node {node}; its replicas move once replacements are healthy");
        }
        NodeCommand::Restore { node } => {
            let node = pick_node(endpoint, node).await?;
            request(endpoint.post(&format!("/api/cluster/nodes/{node}/restore"))).await?;
            println!("node {node} accepts new replicas again");
        }
        NodeCommand::Remove { node } => {
            let node = pick_node(endpoint, node).await?;
            let removal = request(endpoint.delete(&format!("/api/cluster/nodes/{node}"))).await?;
            match removal["state"].as_str() {
                Some("removed") => println!("removed node {node}"),
                _ => {
                    println!("node {node} is draining; run this again once its replicas have moved")
                }
            }
        }
    }
    Ok(())
}

pub async fn service(endpoint: &Endpoint, command: ServiceCommand) -> anyhow::Result<()> {
    match command {
        ServiceCommand::Freeze { service } => {
            set_frozen(endpoint, &pick_service(endpoint, service).await?, true).await
        }
        ServiceCommand::Unfreeze { service } => {
            set_frozen(endpoint, &pick_service(endpoint, service).await?, false).await
        }
        ServiceCommand::Redeploy { service, force } => {
            let service = pick_service(endpoint, service).await?;
            queue(
                endpoint,
                &format!("/api/services/{service}/redeploy?force={force}"),
            )
            .await
        }
        ServiceCommand::Restart { service, force } => {
            let service = pick_service(endpoint, service).await?;
            queue(
                endpoint,
                &format!("/api/services/{service}/restart?force={force}"),
            )
            .await
        }
        ServiceCommand::Scale {
            service,
            replicas,
            reset,
        } => {
            let service = pick_service(endpoint, service).await?;
            let path = format!("/api/services/{service}/replicas");
            let response = match reset {
                true => request(endpoint.delete(&path)).await?,
                false => {
                    let replicas = prompt::count("Replicas", replicas)?;
                    request(endpoint.patch(&path).json(&json!({ "replicas": replicas }))).await?
                }
            };
            println!("{service} runs {} replicas", text(&response["replicas"]));
            Ok(())
        }
        ServiceCommand::Stop {
            service,
            deployment,
        } => {
            let service = pick_service(endpoint, service).await?;
            let deployment = pick_deployment(endpoint, &service, deployment).await?;
            request(endpoint.patch(&format!(
                "/api/services/{service}/deployments/{deployment}/remove"
            )))
            .await?;
            println!("stopping deployment {deployment}");
            Ok(())
        }
        ServiceCommand::Cancel {
            service,
            deployment,
        } => {
            let service = pick_service(endpoint, service).await?;
            let deployment = pick_deployment(endpoint, &service, deployment).await?;
            request(endpoint.patch(&format!(
                "/api/services/{service}/deployments/{deployment}/cancel"
            )))
            .await?;
            println!("cancelled deployment {deployment}");
            Ok(())
        }
        ServiceCommand::Forget {
            service,
            deployment,
        } => {
            let service = pick_service(endpoint, service).await?;
            let deployment = pick_deployment(endpoint, &service, deployment).await?;
            request(endpoint.delete(&format!("/api/services/{service}/deployments/{deployment}")))
                .await?;
            println!("deleted deployment record {deployment}");
            Ok(())
        }
        ServiceCommand::Delete { service } => {
            let service = pick_service(endpoint, service).await?;
            request(endpoint.delete(&format!("/api/services/{service}"))).await?;
            println!("deleted service {service}");
            Ok(())
        }
    }
}

pub async fn rollout(
    endpoint: &Endpoint,
    file: &Path,
    apply: bool,
    force: bool,
) -> anyhow::Result<()> {
    let source = std::fs::read_to_string(file)
        .with_context(|| format!("failed to read {}", file.display()))?;
    let cluster = text(&request(endpoint.get("/api/cluster")).await?["name"]);
    let expanded = ServicesFile::parse(&source, &cluster)?
        .services
        .into_iter()
        .map(|(service_id, mut spec)| {
            let expansion = expand_variables(service_id.as_str(), &mut spec);
            (service_id, expansion.map(|_| spec))
        })
        .collect::<BTreeMap<_, _>>();
    let picked = match prompt::interactive() {
        true => pick_services(endpoint, expanded, force).await?,
        false => expanded,
    };
    let services = ServicesFile {
        services: picked
            .into_iter()
            .map(|(service_id, spec)| Ok((service_id, spec?)))
            .collect::<anyhow::Result<_>>()?,
    };
    let plan = request(
        endpoint
            .post(&format!("/api/rollout?apply={apply}&force={force}"))
            .json(&services),
    )
    .await?;
    println!("cluster: {cluster}");
    print_list("create", &plan["created"]);
    print_list("update", &plan["updated"]);
    print_list("unchanged", &plan["unchanged"]);
    print_list("not rolled out (left running)", &plan["unmanaged"]);
    match apply {
        true => print_list("queued deployments", &plan["deployments"]),
        false => println!("\nrun again with --apply to roll out these changes"),
    }
    Ok(())
}

pub async fn services_up(endpoint: &Endpoint, config: &Path, context: &Path) -> anyhow::Result<()> {
    let manifest = std::fs::read_to_string(config)
        .with_context(|| format!("failed to read {}", config.display()))?;
    let mut upload: UploadSpec = json5::from_str(&manifest)
        .with_context(|| format!("{} is not a valid service manifest", config.display()))?;
    expand_variables(upload.id.as_str(), &mut upload.spec)?;
    upload.spec.validate(&upload.id)?;
    let archive = uploads::pack(context)?;
    println!("uploading {} KiB of build context", archive.len() / 1024);
    let form = reqwest::multipart::Form::new()
        .part(
            "spec",
            reqwest::multipart::Part::bytes(serde_json::to_vec(&upload)?)
                .file_name("spec.json")
                .mime_str("application/json")?,
        )
        .part(
            "context",
            reqwest::multipart::Part::bytes(archive)
                .file_name("context.tar.gz")
                .mime_str("application/gzip")?,
        );
    let accepted = request(endpoint.post("/api/services/up").multipart(form)).await?;
    println!(
        "queued deployment {} for {}",
        text(&accepted["deploymentId"]),
        text(&accepted["serviceId"])
    );
    Ok(())
}

pub async fn services(endpoint: &Endpoint) -> anyhow::Result<()> {
    let services = request(endpoint.get("/api/services")).await?;
    println!(
        "{:<24} {:<20} {:<10} {:<12} IMAGE",
        "SERVICE", "DEPLOYMENT", "VERSION", "STATUS"
    );
    services
        .as_array()
        .into_iter()
        .flatten()
        .for_each(|service| {
            let deployment = &service["latestDeployment"];
            println!(
                "{:<24} {:<20} {:<10} {:<12} {}",
                text(&service["id"]),
                text(&deployment["id"]),
                deployment["version"]
                    .as_u64()
                    .map_or_else(|| "-".to_string(), version_label),
                text(&deployment["status"]),
                text(&deployment["image"]),
            );
        });
    Ok(())
}

pub async fn deploy(endpoint: &Endpoint, service: &str, force: bool) -> anyhow::Result<()> {
    queue(
        endpoint,
        &format!("/api/services/{service}/deployments?force={force}"),
    )
    .await
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

fn expand_variables(service: &str, spec: &mut ServiceSpec) -> anyhow::Result<()> {
    let deploy_secrets = spec
        .deploy
        .secrets
        .as_mut()
        .map(|secrets| ("deploy.secrets", &mut secrets.source, &mut secrets.items));
    let (build_env, build_secrets) = match spec.build.as_mut() {
        Some(build) => (Some(&mut build.env), Some(&mut build.secrets)),
        None => (None, None),
    };
    let env_specs = [
        Some(("deploy.env", &mut spec.deploy.env)),
        build_env.map(|env| ("build.env", env)),
        build_secrets.map(|secrets| ("build.secrets", secrets)),
        spec.preview
            .as_mut()
            .map(|preview| ("preview.env", &mut preview.env)),
    ];
    env_specs
        .into_iter()
        .flatten()
        .map(|(field, EnvSpec { source, items, .. })| (field, source, items))
        .chain(deploy_secrets)
        .try_for_each(|(field, source, items)| {
            if let Some(source) = source {
                *source = expand(service, field, "source", source)?;
            }
            items.iter_mut().try_for_each(|(key, value)| {
                *value = expand(service, field, key, value)?;
                Ok(())
            })
        })
}

fn expand(service: &str, field: &str, key: &str, value: &str) -> anyhow::Result<String> {
    shellexpand::env(value)
        .map(|expanded| expanded.into_owned())
        .map_err(|error| anyhow::anyhow!("service `{service}` {field} `{key}`: {error}"))
}

async fn queue(endpoint: &Endpoint, path: &str) -> anyhow::Result<()> {
    let queued = request(endpoint.post(path)).await?;
    println!("queued deployment {}", text(&queued["deploymentId"]));
    Ok(())
}

async fn set_frozen(endpoint: &Endpoint, service: &str, frozen: bool) -> anyhow::Result<()> {
    request(
        endpoint
            .patch(&format!("/api/services/{service}/freeze"))
            .json(&json!({ "frozen": frozen })),
    )
    .await?;
    match frozen {
        true => println!("{service} is frozen; deploys need --force"),
        false => println!("{service} is unfrozen"),
    }
    Ok(())
}

async fn follow_maintenance(endpoint: &Endpoint, run: Value) -> anyhow::Result<()> {
    let run_id = text(&run["runId"]);
    let order = run["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|step| text(&step["nodeName"]))
        .collect::<Vec<_>>();
    println!("started {run_id} on {}", order.join(", "));
    let mut printed = 0;
    let mut latest = run;
    loop {
        let history = latest["history"].as_array().cloned().unwrap_or_default();
        history.iter().skip(printed).for_each(|event| {
            let node = latest["nodes"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|step| step["nodeId"] == event["nodeId"])
                .map(|step| format!(" {}", text(&step["nodeName"])))
                .unwrap_or_default();
            println!(
                "{}{node}: {}",
                text(&event["phase"]),
                text(&event["message"])
            );
        });
        printed = printed.max(history.len());
        match latest["phase"].as_str() {
            Some("succeeded") => break Ok(()),
            Some("failed") => break Err(anyhow::anyhow!("{}", text(&latest["failure"]))),
            _ => {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let current = request(endpoint.get("/api/cluster"))
                    .await
                    .ok()
                    .map(|cluster| cluster["maintenance"].clone())
                    .filter(|maintenance| text(&maintenance["runId"]) == run_id);
                if let Some(current) = current {
                    latest = current;
                }
            }
        }
    }
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

/// Services whose variables can't be expanded are listed unchecked, so a missing variable only
/// matters when its service is picked.
async fn pick_services(
    endpoint: &Endpoint,
    expanded: BTreeMap<ServiceId, anyhow::Result<ServiceSpec>>,
    force: bool,
) -> anyhow::Result<BTreeMap<ServiceId, anyhow::Result<ServiceSpec>>> {
    let previewable = ServicesFile {
        services: expanded
            .iter()
            .filter_map(|(service_id, spec)| {
                Some((service_id.clone(), spec.as_ref().ok()?.clone()))
            })
            .collect(),
    };
    let preview = request(
        endpoint
            .post(&format!("/api/rollout?apply=false&force={force}"))
            .json(&previewable),
    )
    .await?;
    let listed = |key: &str, service_id: &ServiceId| {
        preview[key].as_array().is_some_and(|ids| {
            ids.iter()
                .any(|id| id.as_str() == Some(service_id.as_str()))
        })
    };
    let options = expanded
        .iter()
        .map(|(service_id, spec)| {
            let change = match spec {
                Ok(_) => [("created", "create"), ("updated", "update")]
                    .into_iter()
                    .find(|(key, _)| listed(key, service_id))
                    .map_or("unchanged", |(_, change)| change),
                Err(_) => "missing variables",
            };
            (service_id.clone(), change)
        })
        .collect::<Vec<_>>();
    let changed = options
        .iter()
        .enumerate()
        .filter(|(_, (_, change))| matches!(*change, "create" | "update"))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let picked = inquire::MultiSelect::new(
        "Services to roll out",
        options
            .iter()
            .map(|(service_id, change)| format!("{service_id} ({change})"))
            .collect(),
    )
    .with_default(&changed)
    .raw_prompt()?
    .into_iter()
    .map(|option| options[option.index].0.clone())
    .collect::<Vec<_>>();
    anyhow::ensure!(!picked.is_empty(), "no services selected");
    Ok(expanded
        .into_iter()
        .filter(|(service_id, _)| picked.contains(service_id))
        .collect())
}

pub async fn pick_service(endpoint: &Endpoint, service: Option<String>) -> anyhow::Result<String> {
    prompt::choose("Service", service, async {
        let services = request(endpoint.get("/api/services")).await?;
        Ok(services
            .as_array()
            .into_iter()
            .flatten()
            .map(|service| Choice {
                value: text(&service["id"]),
                label: format!(
                    "{} ({})",
                    text(&service["id"]),
                    text(&service["latestDeployment"]["status"])
                ),
            })
            .collect())
    })
    .await
}

async fn pick_deployment(
    endpoint: &Endpoint,
    service: &str,
    deployment: Option<String>,
) -> anyhow::Result<String> {
    prompt::choose("Deployment", deployment, async {
        let deployments =
            request(endpoint.get(&format!("/api/services/{service}/deployments"))).await?;
        Ok(deployments
            .as_array()
            .into_iter()
            .flatten()
            .map(|deployment| Choice {
                value: text(&deployment["id"]),
                label: format!(
                    "{} {} ({})",
                    text(&deployment["id"]),
                    deployment["version"]
                        .as_u64()
                        .map_or_else(|| "-".to_string(), version_label),
                    text(&deployment["status"])
                ),
            })
            .collect())
    })
    .await
}

async fn pick_node(endpoint: &Endpoint, node: Option<String>) -> anyhow::Result<String> {
    prompt::choose("Node", node, async {
        let nodes = request(endpoint.get("/api/nodes")).await?;
        Ok(nodes
            .as_array()
            .into_iter()
            .flatten()
            .map(|node| Choice {
                value: text(&node["id"]),
                label: format!(
                    "{} ({}, {})",
                    text(&node["name"]),
                    text(&node["endpoint"]),
                    text(&node["state"])
                ),
            })
            .collect())
    })
    .await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_environment_variables_in_env_and_secret_values() {
        let mut spec: ServiceSpec = serde_json::from_value(serde_json::json!({
            "name": "API",
            "image": "api:1",
            "deploy": {
                "env": { "items": { "HOME_DIR": "${HOME}", "PLAIN": "x", "FALLBACK": "${MAESTRO_TEST_UNSET:-dflt}" } },
                "secrets": { "mountPath": "/run/app.env", "items": { "TOKEN": "$HOME" } }
            }
        }))
        .unwrap();
        expand_variables("api", &mut spec).unwrap();
        let home = std::env::var("HOME").unwrap();
        assert_eq!(spec.deploy.env.items["HOME_DIR"], home);
        assert_eq!(spec.deploy.env.items["PLAIN"], "x");
        assert_eq!(spec.deploy.env.items["FALLBACK"], "dflt");
        assert_eq!(spec.deploy.secrets.unwrap().items["TOKEN"], home);
    }

    #[test]
    fn missing_variables_are_errors() {
        let mut spec: ServiceSpec = serde_json::from_value(serde_json::json!({
            "name": "API",
            "image": "api:1",
            "deploy": { "env": { "items": { "KEY": "${MAESTRO_TEST_UNSET}" } } }
        }))
        .unwrap();
        let error = expand_variables("api", &mut spec).unwrap_err().to_string();
        assert!(error.contains("service `api` deploy.env `KEY`"), "{error}");
    }
}
