use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::deployment::store::ClusterStore;
use crate::deployment::types::{Deployment, DeploymentStatus, ReplicaState, ServiceDeployment};

const MAX_HEALTHCHECK_FAILURES: u32 = 10;

/// Tracks whether each replica was healthy on the last check.
/// Keys are "{deployment_id}-replica{replica_index}".
pub type HealthState = HashMap<String, bool>;

pub async fn check_deployments(
    store: &dyn ClusterStore,
    http: &reqwest::Client,
    state: &mut HealthState,
    dns_domain: Option<&str>,
) -> Result<()> {
    let service_ids = store.list_service_ids().await?;

    let mut active_keys = HashSet::new();
    for service_id in service_ids {
        let deployments = store.list_service_deployments(&service_id).await?;

        for deployment in deployments {
            if !matches!(
                deployment.status,
                DeploymentStatus::Ready
                    | DeploymentStatus::Building
                    | DeploymentStatus::PendingReady
            ) {
                continue;
            }

            let replica_states = store
                .list_replica_states(&service_id, &deployment.id)
                .await
                .unwrap_or_default();

            let checkable_replicas: Vec<&ReplicaState> = replica_states
                .iter()
                .filter(|r| {
                    matches!(
                        r.status,
                        DeploymentStatus::PendingReady | DeploymentStatus::Ready
                    )
                })
                .collect();
            if checkable_replicas.is_empty() {
                continue;
            }

            for replica in &checkable_replicas {
                let key = replica_health_key(&deployment.id, replica.replica_index);
                active_keys.insert(key.clone());
            }

            if let Err(err) = check_replicas(
                store,
                http,
                state,
                &service_id,
                &deployment,
                &checkable_replicas,
                dns_domain,
            )
            .await
            {
                eprintln!("error checking {}/{}: {err}", service_id, deployment.id);
            }
        }
    }

    state.retain(|key, _| active_keys.contains(key));
    Ok(())
}

async fn check_replicas(
    store: &dyn ClusterStore,
    http: &reqwest::Client,
    state: &mut HealthState,
    service_id: &str,
    deployment: &ServiceDeployment,
    replicas: &[&ReplicaState],
    dns_domain: Option<&str>,
) -> Result<()> {
    let health_path = deployment
        .config
        .deploy
        .healthcheck_path
        .as_deref()
        .filter(|p| !p.is_empty());

    let Some(health_path) = health_path else {
        return Ok(());
    };

    for replica in replicas {
        let key = replica_health_key(&deployment.id, replica.replica_index);
        let Some(url) = build_health_url_for_replica(
            deployment,
            replica.replica_index,
            health_path,
            dns_domain,
        ) else {
            eprintln!(
                "skipping {}/{}/replica{} healthcheck: ingress.port is not set; marking replica ready",
                service_id, deployment.id, replica.replica_index
            );
            state.remove(&key);
            if replica.status != DeploymentStatus::Ready || replica.healthcheck_failures != 0 {
                store
                    .upsert_replica_state(
                        service_id,
                        &deployment.id,
                        ReplicaState {
                            replica_index: replica.replica_index,
                            status: DeploymentStatus::Ready,
                            healthcheck_failures: 0,
                        },
                    )
                    .await?;
            }
            continue;
        };

        let is_healthy = match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => true,
            Ok(resp) => {
                eprintln!(
                    "healthcheck failed {}/{}/replica{} url={} status={}",
                    service_id,
                    deployment.id,
                    replica.replica_index,
                    url,
                    resp.status()
                );
                false
            }
            Err(err) => {
                eprintln!(
                    "healthcheck error {}/{}/replica{} url={} err={}",
                    service_id, deployment.id, replica.replica_index, url, err
                );
                false
            }
        };

        let was_healthy = state.get(&key).copied();

        if was_healthy != Some(is_healthy) {
            if is_healthy {
                eprintln!(
                    "{}/{}/replica{} became healthy",
                    service_id, deployment.id, replica.replica_index
                );
            } else {
                eprintln!(
                    "{}/{}/replica{} became unhealthy",
                    service_id, deployment.id, replica.replica_index
                );
            }
        }

        state.insert(key, is_healthy);

        let new_status = if is_healthy {
            DeploymentStatus::Ready
        } else {
            DeploymentStatus::PendingReady
        };
        let healthcheck_failures = match is_healthy {
            true => 0,
            false => replica.healthcheck_failures.saturating_add(1),
        };

        if replica.status != new_status || replica.healthcheck_failures != healthcheck_failures {
            store
                .upsert_replica_state(
                    service_id,
                    &deployment.id,
                    ReplicaState {
                        replica_index: replica.replica_index,
                        status: new_status,
                        healthcheck_failures,
                    },
                )
                .await?;
        }

        if !is_healthy && healthcheck_failures >= MAX_HEALTHCHECK_FAILURES {
            eprintln!(
                "stopping deployment {}/{} after {} consecutive healthcheck failures on replica{}",
                service_id, deployment.id, healthcheck_failures, replica.replica_index,
            );
            let _ = store
                .stop_service_deployment(&Deployment {
                    service_id: service_id.to_string(),
                    id: deployment.id.clone(),
                    replica_index: replica.replica_index,
                })
                .await?;
            return Ok(());
        }
    }

    Ok(())
}

fn build_health_url_for_replica(
    deployment: &ServiceDeployment,
    replica_index: u32,
    health_path: &str,
    dns_domain: Option<&str>,
) -> Option<String> {
    let hostname = deployment.hostname_for_replica(replica_index);
    let target = dns_domain
        .map(|domain| format!("{hostname}.{domain}"))
        .unwrap_or(hostname);
    let port = deployment.config.ingress.as_ref().and_then(|i| i.port)?;
    Some(format!("http://{target}:{port}{health_path}"))
}

#[cfg(test)]
#[path = "../tests/probe/healthcheck.rs"]
mod tests;

fn replica_health_key(deployment_id: &str, replica_index: u32) -> String {
    format!("{deployment_id}-replica{replica_index}")
}
