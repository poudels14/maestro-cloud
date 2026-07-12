use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::deployment::store::ClusterStore;
use crate::deployment::types::{DeploymentStatus, ReplicaState, ServiceDeployment};
use crate::health::ReplicaHealthMonitor;

const UNHEALTHY_RECHECK_SECS: u64 = 5;
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// Tracks whether each replica was healthy on the last check.
/// Keys are "{deployment_id}-replica{replica_index}".
pub type HealthState = HashMap<String, bool>;

pub async fn check_deployments(
    store: &dyn ClusterStore,
    monitor: &dyn ReplicaHealthMonitor,
    http: &reqwest::Client,
    state: &mut HealthState,
    last_polled: &mut HashMap<String, Instant>,
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
                monitor,
                http,
                state,
                last_polled,
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
    last_polled.retain(|key, _| active_keys.contains(key));
    Ok(())
}

async fn check_replicas(
    monitor: &dyn ReplicaHealthMonitor,
    http: &reqwest::Client,
    state: &mut HealthState,
    last_polled: &mut HashMap<String, Instant>,
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

    let healthy_interval =
        Duration::from_secs(deployment.config.deploy.healthcheck_interval as u64);
    let unhealthy_interval = Duration::from_secs(UNHEALTHY_RECHECK_SECS);

    let now = Instant::now();

    for replica in replicas {
        let key = replica_health_key(&deployment.id, replica.replica_index);
        let last_known_healthy = state.get(&key).copied().unwrap_or(false);
        let is_steady_state = replica.status == DeploymentStatus::Ready && last_known_healthy;
        let due_after = if is_steady_state {
            healthy_interval
        } else {
            unhealthy_interval
        };
        if let Some(last) = last_polled.get(&key)
            && now.saturating_duration_since(*last) < due_after
        {
            continue;
        }
        last_polled.insert(key.clone(), now);

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
            let was_healthy = state.insert(key.clone(), true);
            stagger_first_healthy_poll(&key, was_healthy, healthy_interval, now, last_polled);
            if replica_needs_healthy_update(replica) {
                monitor
                    .report_healthy(service_id, &deployment.id, replica.replica_index)
                    .await?;
            }
            continue;
        };

        let (is_healthy, failure_reason) = match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => (true, None),
            Ok(resp) => {
                let status = resp.status();
                eprintln!(
                    "healthcheck failed {}/{}/replica{} url={} status={}",
                    service_id, deployment.id, replica.replica_index, url, status
                );
                (false, Some(format!("HTTP {status}")))
            }
            Err(err) => {
                eprintln!(
                    "healthcheck error {}/{}/replica{} url={} err={}",
                    service_id, deployment.id, replica.replica_index, url, err
                );
                (false, Some(format!("request error: {err}")))
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

        if is_healthy {
            stagger_first_healthy_poll(&key, was_healthy, healthy_interval, now, last_polled);
        }

        state.insert(key, is_healthy);

        if is_healthy {
            if replica_needs_healthy_update(replica) {
                monitor
                    .report_healthy(service_id, &deployment.id, replica.replica_index)
                    .await?;
            }
        } else {
            let reason = format!(
                "healthcheck failed on `{}` (replica{}, {})",
                health_path,
                replica.replica_index,
                failure_reason.as_deref().unwrap_or("unknown"),
            );
            monitor
                .report_unhealthy(service_id, &deployment.id, replica.replica_index, &reason)
                .await?;
        }
    }

    Ok(())
}

fn replica_needs_healthy_update(replica: &ReplicaState) -> bool {
    replica.status != DeploymentStatus::Ready || replica.healthcheck_failures != 0
}

fn stagger_first_healthy_poll(
    key: &str,
    was_healthy: Option<bool>,
    interval: Duration,
    now: Instant,
    last_polled: &mut HashMap<String, Instant>,
) {
    if was_healthy == Some(true) {
        return;
    }

    // A rollout starts replicas together, which would otherwise keep every
    // steady-state health check on the same interval boundary. Move only the
    // first healthy interval onto a stable per-replica phase; subsequent checks
    // retain that phase at the configured frequency.
    let stagger = healthy_check_stagger(key, interval);
    let elapsed = interval.saturating_sub(stagger);
    if let Some(staggered_last_poll) = now.checked_sub(elapsed) {
        last_polled.insert(key.to_string(), staggered_last_poll);
    }
}

fn healthy_check_stagger(key: &str, interval: Duration) -> Duration {
    let tick_secs = super::POLL_TICK_INTERVAL.as_secs().max(1);
    let interval_secs = interval.as_secs().max(tick_secs);
    let slots = (interval_secs / tick_secs).max(1);

    let hash = key.as_bytes().iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    });
    let slot = (hash % slots) + 1;
    Duration::from_secs((slot * tick_secs).min(interval_secs))
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
