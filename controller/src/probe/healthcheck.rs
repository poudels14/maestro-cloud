use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::deployment::store::ClusterStore;
use crate::deployment::types::{DeploymentStatus, ReplicaState, ServiceDeployment};

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
            if replica.status != DeploymentStatus::Ready {
                store
                    .update_replica_status(
                        service_id,
                        &deployment.id,
                        replica.replica_index,
                        DeploymentStatus::Ready,
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

        if replica.status != new_status {
            store
                .update_replica_status(
                    service_id,
                    &deployment.id,
                    replica.replica_index,
                    new_status,
                )
                .await?;
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
