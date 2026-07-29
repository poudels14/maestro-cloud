use std::collections::BTreeSet;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use clustertest::ClusterSetupCluster;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kernel_api::{
    Assignment, AssignmentPhase, ClusterInfo, Deployment, DeploymentPhase, NodeId, ReplicaState,
    Service,
};
use kernel_store::{Keyspace, Store};
use serde::Serialize;

use super::workload::list_resources;
use super::*;

const SYSTEM_PLANE_TIMEOUT: Duration = Duration::from_secs(180);
const RECURSIVE_DNS_NAME: &str = "recursive.acceptance.test.";

#[tokio::test]
#[ignore = "requires root, containerd, etcd, curl, dig, iproute2, nftables, WireGuard, registry access, and Linux network namespaces"]
async fn real_process_three_node_system_plane_is_reachable()
-> Result<(), Box<dyn std::error::Error>> {
    let mut cluster =
        RealProcessCluster::new_with_operator_access(3, OperatorAccess::Tailscale).await?;
    cluster.bootstrap_seed().await?;
    let nodes = cluster.nodes();
    for node in nodes.iter().skip(1) {
        cluster.join_node(node).await?;
    }
    cluster.await_mesh(&nodes.iter().cloned().collect()).await?;

    let gateway_pid = cluster.await_tailscale_gateway_pid().await?;
    let operator_token = operator_token()?;
    for index in 0..cluster.nodes.len() {
        cluster
            .assert_admin_cluster_endpoint(index, gateway_pid, &operator_token)
            .await?;
        cluster.assert_workload_cannot_reach_system_plane(index)?;
        cluster.assert_recursive_dns(index).await?;
    }
    if !cluster
        .dns_upstream
        .as_ref()
        .is_some_and(LocalDnsUpstream::recursion_observed)
    {
        return Err("local DNS upstream never received a recursive query".into());
    }
    cluster.await_traefik_ready().await?;
    cluster.assert_traefik_provider_root().await?;
    Ok(())
}

impl RealProcessCluster {
    async fn await_traefik_ready(&mut self) -> Result<(), RealClusterError> {
        let deadline = tokio::time::Instant::now() + SYSTEM_PLANE_TIMEOUT;
        let expected_nodes = self
            .cluster
            .nodes
            .iter()
            .filter(|(_, node)| node.role.runs_workloads())
            .map(|(node_id, _)| node_id.clone())
            .collect::<BTreeSet<_>>();
        let mut last_observation = "Traefik resources were not observed".to_owned();
        loop {
            self.ensure_children_running()?;
            if let Ok(store) = self.connect_store().await {
                let services =
                    list_resources::<Service>(&store, &self.cluster.cluster_id, "Service")
                        .await
                        .map_err(RealClusterError::from_display)?;
                if let Some(service) = services
                    .iter()
                    .find(|service| service.meta.id.as_str() == "maestro-system-traefik")
                {
                    if let Some(deployment_id) = service.status.active_deployment_id.as_ref() {
                        let deployments = list_resources::<Deployment>(
                            &store,
                            &self.cluster.cluster_id,
                            "Deployment",
                        )
                        .await
                        .map_err(RealClusterError::from_display)?;
                        let assignments = list_resources::<Assignment>(
                            &store,
                            &self.cluster.cluster_id,
                            "Assignment",
                        )
                        .await
                        .map_err(RealClusterError::from_display)?;
                        let replicas = list_resources::<ReplicaState>(
                            &store,
                            &self.cluster.cluster_id,
                            "ReplicaState",
                        )
                        .await
                        .map_err(RealClusterError::from_display)?;
                        let deployment = deployments
                            .iter()
                            .find(|deployment| deployment.meta.id == *deployment_id);
                        let running_nodes = assignments
                            .iter()
                            .filter(|assignment| {
                                assignment.spec.deployment_id == *deployment_id
                                    && assignment.status.phase == AssignmentPhase::Running
                            })
                            .map(|assignment| assignment.spec.node_id.clone())
                            .collect::<BTreeSet<NodeId>>();
                        let ready_replicas = replicas
                            .iter()
                            .filter(|replica| {
                                replica.spec.deployment_id == *deployment_id
                                    && replica.status.phase == DeploymentPhase::Ready
                            })
                            .count();
                        last_observation = format!(
                            "deployment={:?}; running_nodes={running_nodes:?}; ready_replicas={ready_replicas}",
                            deployment.map(|deployment| deployment.status.phase)
                        );
                        if deployment.is_some_and(|deployment| {
                            deployment.status.phase == DeploymentPhase::Ready
                        }) && running_nodes == expected_nodes
                            && ready_replicas == expected_nodes.len()
                        {
                            return Ok(());
                        }
                    } else {
                        last_observation = "Traefik service had no active deployment".to_owned();
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "Traefik did not become ready on every workload node: {last_observation}; logs: {}",
                    self.cluster_logs()
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn assert_traefik_provider_root(&self) -> Result<(), RealClusterError> {
        let store = self
            .connect_store()
            .await
            .map_err(RealClusterError::from_display)?;
        let provider = store
            .list(&Keyspace::new(&self.cluster.cluster_id).traefik_provider())
            .await
            .map_err(RealClusterError::from_display)?;
        if provider.values.is_empty() {
            Err(RealClusterError::new(
                "Traefik provider compatibility root is empty",
            ))
        } else {
            Ok(())
        }
    }

    async fn await_tailscale_gateway_pid(&mut self) -> Result<u32, RealClusterError> {
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        let mut last_observation = "Tailscale gateway assignment was not observed".to_owned();
        loop {
            self.ensure_children_running()?;
            if let Ok(store) = self.connect_store().await {
                let assignments =
                    list_resources::<Assignment>(&store, &self.cluster.cluster_id, "Assignment")
                        .await
                        .map_err(RealClusterError::from_display)?;
                if let Some(workload_id) = assignments
                    .iter()
                    .find(|assignment| {
                        assignment.spec.service_id.as_str() == "maestro-system-tailscale-gateway"
                            && assignment.status.phase == AssignmentPhase::Running
                    })
                    .and_then(|assignment| assignment.status.workload_id.as_ref())
                {
                    let container_id = format!("maestro-{workload_id}");
                    match self.containerd_task_pid(&container_id)? {
                        Some(pid) => return Ok(pid),
                        None => {
                            last_observation =
                                format!("containerd task `{container_id}` had no running PID");
                        }
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "Tailscale gateway task did not become available: {last_observation}; logs: {}",
                    self.cluster_logs()
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    fn containerd_task_pid(&self, container_id: &str) -> Result<Option<u32>, RealClusterError> {
        let output = Command::new("ctr")
            .args([
                "--address",
                self.containerd_socket.to_string_lossy().as_ref(),
                "--namespace",
                &self.runtime_namespace(),
                "tasks",
                "list",
            ])
            .output()
            .map_err(RealClusterError::from_display)?;
        if !output.status.success() {
            return Err(RealClusterError::new(format!(
                "containerd task listing failed with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next()? == container_id)
                    .then(|| fields.next()?.parse::<u32>().ok())
                    .flatten()
            }))
    }

    async fn assert_admin_cluster_endpoint(
        &mut self,
        index: usize,
        gateway_pid: u32,
        operator_token: &str,
    ) -> Result<(), RealClusterError> {
        let node = self.node(index)?;
        let definition = self
            .cluster
            .nodes
            .get(&node.node_id)
            .ok_or_else(|| RealClusterError::new("node definition is missing"))?;
        let admin_address = definition
            .workload_subnet
            .admin_address()
            .ok_or_else(|| RealClusterError::new("node Admin address is missing"))?;
        let namespace = node.namespace.clone();
        let node_id = node.node_id.clone();
        let url = format!("http://{admin_address}/api/cluster");
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            self.ensure_children_running()?;
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
                    "--header",
                    &format!("Authorization: Bearer {operator_token}"),
                    &url,
                ])
                .output()
                .map_err(RealClusterError::from_display)?;
            let last_observation = if output.status.success() {
                match serde_json::from_slice::<ClusterInfo>(&output.stdout) {
                    Ok(info)
                        if info.cluster_id == self.cluster.cluster_id
                            && info.node_count == 3
                            && info.control_plane_node_count == 3
                            && info.workload_node_count == 2 =>
                    {
                        return Ok(());
                    }
                    Ok(info) => format!("unexpected cluster response: {info:?}"),
                    Err(error) => format!(
                        "invalid Admin response: {error}: {}",
                        String::from_utf8_lossy(&output.stdout).trim()
                    ),
                }
            } else {
                format!(
                    "curl exited with {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                )
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "authenticated Admin endpoint on `{node_id}` was not reachable from the Tailscale gateway at `{url}`: {last_observation}; node: {}; node log: {}",
                    node_namespace_diagnostics(&namespace),
                    read_log(&self.node(index)?.log_path)
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn assert_recursive_dns(&mut self, index: usize) -> Result<(), RealClusterError> {
        let node = self.node(index)?;
        let namespace = node.workload_namespace.clone();
        let gateway = node.workload_gateway.to_string();
        let node_id = node.node_id.clone();
        let expected = self
            .dns_upstream
            .as_ref()
            .ok_or_else(|| RealClusterError::new("local DNS upstream is missing"))?
            .fixture_address()
            .to_string();
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            self.ensure_children_running()?;
            let output = Command::new("ip")
                .args(["netns", "exec", &namespace, "dig"])
                .args([
                    "+time=1",
                    "+tries=1",
                    "+short",
                    &format!("@{gateway}"),
                    RECURSIVE_DNS_NAME,
                    "A",
                ])
                .output()
                .map_err(RealClusterError::from_display)?;
            let answer = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if output.status.success() && answer == expected {
                return Ok(());
            }
            let last_observation = format!(
                "dig exited with {} and answered `{answer}`: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "recursive DNS through the workload bridge on `{node_id}` did not resolve: {last_observation}; node: {}; workload: {}; log: {}",
                    node_namespace_diagnostics(&self.node(index)?.namespace),
                    workload_namespace_diagnostics(&namespace),
                    read_log(&self.node(index)?.log_path)
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    fn assert_workload_cannot_reach_system_plane(
        &self,
        index: usize,
    ) -> Result<(), RealClusterError> {
        let node = self.node(index)?;
        let definition = self
            .cluster
            .nodes
            .get(&node.node_id)
            .ok_or_else(|| RealClusterError::new("node definition is missing"))?;
        let admin_address = definition
            .workload_subnet
            .admin_address()
            .ok_or_else(|| RealClusterError::new("node Admin address is missing"))?;
        let protected = [
            (admin_address, 80_u16, "Admin"),
            (node.host_address, API_PORT, "node API"),
            (node.host_address, STORE_CLIENT_PORT, "etcd client"),
            (node.host_address, STORE_PEER_PORT, "etcd peer"),
        ];
        for (address, port, name) in protected {
            let status = Command::new("ip")
                .args([
                    "netns",
                    "exec",
                    &node.workload_namespace,
                    "bash",
                    "-ceu",
                    "exec 3<>\"/dev/tcp/$1/$2\"",
                    "maestro-system-plane-deny",
                    &address.to_string(),
                    &port.to_string(),
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map_err(RealClusterError::from_display)?;
            if status.success() {
                return Err(RealClusterError::new(format!(
                    "ordinary workload on `{}` opened the protected {name} endpoint `{address}:{port}`",
                    node.node_id
                )));
            }
        }
        Ok(())
    }

    fn cluster_logs(&self) -> String {
        self.nodes
            .iter()
            .map(|node| format!("{}={}", node.node_id, read_log(&node.log_path)))
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

#[derive(Serialize)]
struct OperatorClaims<'a> {
    sub: &'a str,
    scope: &'a str,
    iat: u64,
    exp: u64,
}

fn operator_token() -> Result<String, Box<dyn std::error::Error>> {
    let issued_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    Ok(jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &OperatorClaims {
            sub: "real-cluster-acceptance",
            scope: "operator",
            iat: issued_at,
            exp: issued_at.saturating_add(300),
        },
        &EncodingKey::from_secret(OPERATOR_JWT_SECRET.as_bytes()),
    )?)
}
