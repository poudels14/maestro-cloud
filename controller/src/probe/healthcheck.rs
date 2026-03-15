use std::collections::HashMap;

use anyhow::Result;

use crate::deployment::store::ClusterStore;
use crate::deployment::types::{Deployment, DeploymentStatus, ServiceDeployment};

/// Tracks whether each deployment was healthy on the last check.
pub type HealthState = HashMap<String, bool>;

pub async fn check_deployments(
    store: &dyn ClusterStore,
    http: &reqwest::Client,
    state: &mut HealthState,
) -> Result<()> {
    let service_ids = store.list_service_ids().await?;

    for service_id in service_ids {
        let deployments = store.list_service_deployments(&service_id).await?;

        for deployment in deployments {
            if deployment.status == DeploymentStatus::PendingReady {
                if let Err(err) =
                    check_and_promote(store, http, state, &service_id, &deployment).await
                {
                    eprintln!("error checking {}/{}: {err}", service_id, deployment.id);
                }
            }
        }
    }

    Ok(())
}

async fn check_and_promote(
    store: &dyn ClusterStore,
    http: &reqwest::Client,
    state: &mut HealthState,
    service_id: &str,
    deployment: &ServiceDeployment,
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

    let Some(url) = build_health_url(deployment, health_path) else {
        return Ok(());
    };

    let is_healthy = http
        .get(&url)
        .send()
        .await
        .is_ok_and(|resp| resp.status().is_success());

    let was_healthy = state.get(&deployment.id).copied();

    if was_healthy != Some(is_healthy) {
        if is_healthy {
            eprintln!("{}/{} became healthy", service_id, deployment.id);
        } else {
            eprintln!("{}/{} became unhealthy", service_id, deployment.id);
        }
    }

    state.insert(deployment.id.clone(), is_healthy);

    if is_healthy {
        let dep = Deployment {
            service_id: service_id.to_string(),
            id: deployment.id.clone(),
        };
        store
            .update_deployment_status(&dep, DeploymentStatus::Ready)
            .await?;
    }

    Ok(())
}

fn build_health_url(deployment: &ServiceDeployment, health_path: &str) -> Option<String> {
    if let Some(first_port) = deployment.config.deploy.ports.first() {
        let host_port = first_port.split(':').next().unwrap_or(first_port);
        Some(format!("http://localhost:{host_port}{health_path}"))
    } else {
        let short_id: String = deployment.id.chars().take(6).collect();
        let container_name = format!("{}-{short_id}", deployment.config.id);
        Some(format!("http://{container_name}{health_path}"))
    }
}
