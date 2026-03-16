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
) -> Result<()> {
    let service_ids = store.list_service_ids().await?;

    let mut active_keys = HashSet::new();
    for service_id in service_ids {
        let deployments = store.list_service_deployments(&service_id).await?;

        for deployment in deployments {
            if deployment.status != DeploymentStatus::Ready {
                continue;
            }

            let replica_states = store
                .list_replica_states(&service_id, &deployment.id)
                .await
                .unwrap_or_default();

            let pending_replicas: Vec<&ReplicaState> = replica_states
                .iter()
                .filter(|r| r.status == DeploymentStatus::PendingReady)
                .collect();
            if pending_replicas.is_empty() {
                continue;
            }

            for replica in &pending_replicas {
                let key = replica_health_key(&deployment.id, replica.replica_index);
                active_keys.insert(key.clone());
            }

            if let Err(err) = check_and_promote_replicas(
                store,
                http,
                state,
                &service_id,
                &deployment,
                &pending_replicas,
            )
            .await
            {
                eprintln!(
                    "[probe] error checking {}/{}: {err}",
                    service_id, deployment.id
                );
            }
        }
    }

    state.retain(|key, _| active_keys.contains(key));
    Ok(())
}

async fn check_and_promote_replicas(
    store: &dyn ClusterStore,
    http: &reqwest::Client,
    state: &mut HealthState,
    service_id: &str,
    deployment: &ServiceDeployment,
    pending_replicas: &[&ReplicaState],
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

    for replica in pending_replicas {
        let Some(url) =
            build_health_url_for_replica(deployment, replica.replica_index, health_path)
        else {
            continue;
        };

        let key = replica_health_key(&deployment.id, replica.replica_index);

        let is_healthy = http
            .get(&url)
            .send()
            .await
            .is_ok_and(|resp| resp.status().is_success());

        let was_healthy = state.get(&key).copied();

        if was_healthy != Some(is_healthy) {
            if is_healthy {
                eprintln!(
                    "[probe] {}/{}/replica{} became healthy",
                    service_id, deployment.id, replica.replica_index
                );
            } else {
                eprintln!(
                    "[probe] {}/{}/replica{} became unhealthy",
                    service_id, deployment.id, replica.replica_index
                );
            }
        }

        state.insert(key, is_healthy);

        if is_healthy {
            store
                .update_replica_status(
                    service_id,
                    &deployment.id,
                    replica.replica_index,
                    DeploymentStatus::Ready,
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
) -> Option<String> {
    let hostname = deployment.hostname_for_replica(replica_index);
    let port = deployment
        .config
        .ingress
        .as_ref()
        .map(|i| i.port.unwrap_or(80))?;
    Some(format!("http://{hostname}:{port}{health_path}"))
}

fn replica_health_key(deployment_id: &str, replica_index: u32) -> String {
    format!("{deployment_id}-replica{replica_index}")
}
