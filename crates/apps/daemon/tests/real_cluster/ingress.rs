use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

use kernel_api::{
    Assignment, AssignmentPhase, Deployment, DeploymentPhase, NodeId, ReplicaState, Service,
    assignment_workload_address,
};

use super::workload::list_resources;
use super::workload_fixture::{
    AFFINITY_HEADER, AFFINITY_HOST, AFFINITY_SERVICE_ID, put_affinity_service_and_route,
};
use super::{RETRY_DELAY, RealClusterError, RealProcessCluster};

const AFFINITY_TIMEOUT: Duration = Duration::from_secs(180);

impl RealProcessCluster {
    pub(super) async fn assert_cross_node_affinity(&mut self) -> Result<(), RealClusterError> {
        let store = self
            .connect_store()
            .await
            .map_err(RealClusterError::from_display)?;
        put_affinity_service_and_route(&store, &self.cluster.cluster_id).await?;
        drop(store);

        let deadline = tokio::time::Instant::now() + AFFINITY_TIMEOUT;
        let mut last_observation = "affinity resources were not observed".to_owned();
        loop {
            self.ensure_children_running()?;
            if let Ok(store) = self.connect_store().await {
                let services =
                    list_resources::<Service>(&store, &self.cluster.cluster_id, "Service")
                        .await
                        .map_err(RealClusterError::from_display)?;
                let deployments =
                    list_resources::<Deployment>(&store, &self.cluster.cluster_id, "Deployment")
                        .await
                        .map_err(RealClusterError::from_display)?;
                let assignments =
                    list_resources::<Assignment>(&store, &self.cluster.cluster_id, "Assignment")
                        .await
                        .map_err(RealClusterError::from_display)?;
                let replicas = list_resources::<ReplicaState>(
                    &store,
                    &self.cluster.cluster_id,
                    "ReplicaState",
                )
                .await
                .map_err(RealClusterError::from_display)?;

                let affinity = ready_service_addresses(
                    AFFINITY_SERVICE_ID,
                    &services,
                    &deployments,
                    &assignments,
                    &replicas,
                );
                let traefik = ready_service_addresses(
                    "maestro-system-traefik",
                    &services,
                    &deployments,
                    &assignments,
                    &replicas,
                );
                last_observation =
                    format!("affinity_targets={affinity:?}; traefik_targets={traefik:?}");
                if affinity.len() == 2 && traefik.len() == 2 {
                    let Some((ingress_node, ingress_address)) = traefik.iter().next() else {
                        unreachable!("length checked");
                    };
                    let Some((target_node, _target_address)) = affinity
                        .iter()
                        .find(|(node_id, _)| *node_id != ingress_node)
                    else {
                        last_observation = format!(
                            "no affinity target is remote from ingress node `{ingress_node}`"
                        );
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    };
                    let token =
                        ::ingress::node_affinity_token(&self.cluster.cluster_id, target_node);
                    let Some(gateway_pid) = self.current_tailscale_gateway_pid().await? else {
                        last_observation =
                            "no system workload was available for the affinity probe".to_owned();
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    };
                    let mut failed = None;
                    for _attempt in 0..4 {
                        let output = Command::new("nsenter")
                            .args(["--target", &gateway_pid.to_string(), "--net", "--", "curl"])
                            .args([
                                "--noproxy",
                                "*",
                                "--fail",
                                "--silent",
                                "--show-error",
                                "--connect-timeout",
                                "1",
                                "--max-time",
                                "2",
                                "--resolve",
                                &format!("{AFFINITY_HOST}:8888:{ingress_address}"),
                                "--header",
                                &format!("{AFFINITY_HEADER}: {token}"),
                                &format!("http://{AFFINITY_HOST}:8888/node"),
                            ])
                            .output()
                            .map_err(RealClusterError::from_display)?;
                        let body = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                        if !output.status.success() || body != target_node.as_str() {
                            failed = Some(format!(
                                "Traefik on `{ingress_node}` at `{ingress_address}` returned `{body}` \
                                 instead of remote affinity target `{target_node}`: exit {}; {}",
                                output.status,
                                String::from_utf8_lossy(&output.stderr).trim()
                            ));
                            break;
                        }
                    }
                    if let Some(failed) = failed {
                        last_observation = failed;
                    } else {
                        return Ok(());
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "cross-node ingress affinity did not converge: {last_observation}; logs: {}",
                    self.cluster_logs()
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

fn ready_service_addresses(
    service_id: &str,
    services: &[Service],
    deployments: &[Deployment],
    assignments: &[Assignment],
    replicas: &[ReplicaState],
) -> BTreeMap<NodeId, std::net::IpAddr> {
    let Some(service) = services
        .iter()
        .find(|service| service.meta.id.as_str() == service_id)
    else {
        return BTreeMap::new();
    };
    let Some(deployment_id) = service.status.active_deployment_id.as_ref() else {
        return BTreeMap::new();
    };
    if !deployments.iter().any(|deployment| {
        deployment.meta.id == *deployment_id && deployment.status.phase == DeploymentPhase::Ready
    }) {
        return BTreeMap::new();
    }
    let ready_assignments = replicas
        .iter()
        .filter(|replica| {
            replica.spec.deployment_id == *deployment_id
                && replica.status.phase == DeploymentPhase::Ready
        })
        .map(|replica| replica.spec.assignment_id.clone())
        .collect::<BTreeSet<_>>();
    assignments
        .iter()
        .filter(|assignment| {
            assignment.spec.deployment_id == *deployment_id
                && assignment.status.phase == AssignmentPhase::Running
                && ready_assignments.contains(&assignment.meta.id)
        })
        .filter_map(|assignment| {
            assignment_workload_address(assignment)
                .map(|address| (assignment.spec.node_id.clone(), address))
        })
        .collect()
}
