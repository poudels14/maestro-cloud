use serde::{Deserialize, Serialize};

use crate::cli::contexts;
use crate::error::{Error, Result};

const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Serialize)]
struct UpgradeSystemRequest<'a> {
    version: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpgradeSystemResponse {
    accepted: bool,
    system: String,
    #[serde(default)]
    current_version: Option<String>,
    #[serde(default)]
    target_version: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterUpgradeRequest<'a> {
    target_version: &'a str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterUnfreezeRequest<'a> {
    upgrade_run_id: &'a str,
}

pub async fn run_cluster_upgrade(host: &str, version: Option<&str>, yes: bool) -> Result<()> {
    let version = cluster_target_version(version)?;
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        &format!("About to roll every cluster node to Maestro {version}"),
        &[
            "Deploys will be frozen for the duration of the run".to_string(),
            "Nodes will drain and restart serially".to_string(),
        ],
        yes,
    )
    .await?;
    if !confirmed {
        println!("[maestro]: aborted");
        return Ok(());
    }
    let base = normalize_base_url(host)?;
    let response = crate::cli::idempotent(
        contexts::build_http_client()?.post(format!("{base}/api/cluster/upgrade")),
    )
    .json(&ClusterUpgradeRequest {
        target_version: &version.to_string(),
    })
    .send()
    .await
    .map_err(|error| Error::external(format!("failed to start cluster upgrade: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "cluster upgrade was rejected ({status}): {body}"
        )));
    }
    let run: crate::cluster::UpgradeRun = response
        .json()
        .await
        .map_err(|error| Error::external(format!("invalid cluster upgrade response: {error}")))?;
    println!("[maestro]: cluster upgrade `{}` started", run.run_id);
    stream_cluster_maintenance(&base, run).await
}

fn cluster_target_version(version: Option<&str>) -> Result<semver::Version> {
    let version = version.unwrap_or(CLIENT_VERSION).trim();
    semver::Version::parse(version)
        .map_err(|error| Error::invalid_input(format!("invalid target version: {error}")))
}

pub async fn run_cluster_unfreeze(host: &str, run_id: &str) -> Result<()> {
    let base = normalize_base_url(host)?;
    let response = crate::cli::idempotent(
        contexts::build_http_client()?.post(format!("{base}/api/cluster/upgrade/unfreeze")),
    )
    .json(&ClusterUnfreezeRequest {
        upgrade_run_id: run_id,
    })
    .send()
    .await
    .map_err(|error| Error::external(format!("failed to unfreeze cluster: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "manual unfreeze was rejected ({status}): {body}"
        )));
    }
    let run: crate::cluster::UpgradeRun = response
        .json()
        .await
        .map_err(|error| Error::external(format!("invalid unfreeze response: {error}")))?;
    println!(
        "[maestro]: {} `{}` aborted and cluster deploys unfrozen",
        run.operation_name(),
        run.run_id
    );
    Ok(())
}

pub(crate) async fn stream_cluster_maintenance(
    base: &str,
    mut run: crate::cluster::UpgradeRun,
) -> Result<()> {
    let mut printed = 0_usize;
    let operation = run.operation_name();
    let status_path = match run.kind {
        crate::cluster::ClusterMaintenanceKind::Upgrade => "/api/cluster/upgrade",
        crate::cluster::ClusterMaintenanceKind::Restart => "/api/cluster/restart",
    };
    loop {
        for event in run.history.iter().skip(printed) {
            let node = event
                .node_id
                .as_deref()
                .map(|node| format!(" [{node}]"))
                .unwrap_or_default();
            println!("{}{}: {}", event.at_ms, node, event.message);
        }
        printed = run.history.len();
        if run.phase.is_terminal() {
            return if run.phase == crate::cluster::UpgradePhase::Succeeded {
                match run.kind {
                    crate::cluster::ClusterMaintenanceKind::Upgrade => println!(
                        "[maestro]: cluster upgrade `{}` completed at version {}",
                        run.run_id, run.target_version
                    ),
                    crate::cluster::ClusterMaintenanceKind::Restart => {
                        println!("[maestro]: cluster restart `{}` completed", run.run_id)
                    }
                }
                Ok(())
            } else {
                Err(Error::external(
                    run.failure
                        .unwrap_or_else(|| format!("cluster {operation} failed")),
                ))
            };
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        match contexts::build_http_client()?
            .get(format!("{base}{status_path}"))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                let next: Option<crate::cluster::UpgradeRun> =
                    response.json().await.map_err(|error| {
                        Error::external(format!("invalid cluster {operation} status: {error}"))
                    })?;
                let Some(next) = next else {
                    return Err(Error::external(format!(
                        "cluster {operation} status disappeared"
                    )));
                };
                if next.run_id != run.run_id {
                    return Err(Error::external(format!(
                        "a different cluster maintenance run replaced this {operation}"
                    )));
                }
                run = next;
            }
            Ok(response) => {
                eprintln!(
                    "[maestro]: {operation} status temporarily unavailable ({})",
                    response.status()
                );
            }
            Err(error) => {
                eprintln!("[maestro]: waiting for cluster API after node restart: {error}");
            }
        }
    }
}

pub async fn run_upgrade_system(host: &str, yes: bool) -> Result<()> {
    let confirmed = crate::cli::confirm::confirm_action(
        host,
        &format!("About to upgrade the host operating system to Maestro {CLIENT_VERSION}"),
        &[],
        yes,
    )
    .await?;
    if !confirmed {
        println!("[maestro]: aborted");
        return Ok(());
    }

    let endpoint = upgrade_system_endpoint(host)?;
    let response = crate::cli::idempotent(contexts::build_http_client()?.post(&endpoint))
        .json(&UpgradeSystemRequest {
            version: CLIENT_VERSION,
        })
        .send()
        .await
        .map_err(|err| Error::external(format!("failed to call system upgrade endpoint: {err}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "system upgrade failed with status {status}: {body}"
        )));
    }

    let payload = response
        .json::<UpgradeSystemResponse>()
        .await
        .map_err(|err| Error::external(format!("failed to decode upgrade response: {err}")))?;

    if payload.accepted {
        let target_version = payload.target_version.as_deref().unwrap_or(CLIENT_VERSION);
        if let Some(current_version) = payload.current_version.as_deref() {
            println!(
                "[maestro]: system upgrade accepted ({current_version} -> {target_version}, system: {}), the system will update and reboot",
                payload.system
            );
        } else {
            println!(
                "[maestro]: system upgrade to {target_version} accepted (system: {}), the system will update and reboot",
                payload.system
            );
        }
    } else {
        println!("[maestro]: system upgrade was not accepted");
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

fn upgrade_system_endpoint(host: &str) -> Result<String> {
    let base = normalize_base_url(host)?;
    Ok(format!("{base}/api/system/upgrade"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_request_uses_the_cargo_package_version() {
        let request = UpgradeSystemRequest {
            version: CLIENT_VERSION,
        };
        let payload = serde_json::to_value(request).expect("serialize upgrade request");

        assert_eq!(payload["version"], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn cluster_upgrade_request_uses_camel_case_target() {
        let payload = serde_json::to_value(ClusterUpgradeRequest {
            target_version: "1.2.3",
        })
        .expect("serialize cluster upgrade request");
        assert_eq!(payload["targetVersion"], "1.2.3");
    }

    #[test]
    fn cluster_upgrade_defaults_to_the_cli_version() {
        assert_eq!(
            cluster_target_version(None)
                .expect("resolve target version")
                .to_string(),
            env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    fn cluster_upgrade_accepts_an_explicit_version_override() {
        assert_eq!(
            cluster_target_version(Some(" 1.2.3 "))
                .expect("resolve target version")
                .to_string(),
            "1.2.3"
        );
    }
}
