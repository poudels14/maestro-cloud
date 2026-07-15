use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::broadcast;

use super::elector::{EtcdLeaderElector, LeaderElector};
use super::executor::RunningReplica;
use super::reconciler::{ReconcileAction, diff_assignments};
use super::scheduler::{ScheduleInput, plan};
use super::types::{
    Assignment, AssignmentManifest, DeploymentGroup, LeadershipState, LeadershipToken, NodeInfo,
    NodeRole, NodeState, SchedulePlan, ServiceScheduleSpec,
};
use crate::deployment::etcd::EtcdStateStore;
use crate::deployment::store::{ClusterMutation, ClusterStore};
use crate::logs::Logger;
use crate::signal::ShutdownEvent;

const TEST_CLUSTER_NAME: &str = "single-host-integration";
const TEST_AFFINITY_HEADER: &str = "X-Session-Affinity";

struct ContainerEtcdCluster {
    runtime_cli: String,
    container_names: Vec<String>,
    endpoints: Vec<String>,
}

impl ContainerEtcdCluster {
    fn start() -> Result<Self> {
        let runtime_cli = test_runtime_cli()?;
        command_output(&runtime_cli, &["info"])
            .context("the distributed test requires a working container daemon")?;

        let nodes = reserve_node_endpoints(3, Ipv4Addr::LOCALHOST)?;
        let members = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                format!(
                    "member{}=http://127.0.0.1:{}",
                    index + 1,
                    node.etcd_peer_port
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let run_id = crate::utils::nanoid::unique_id(12).to_ascii_lowercase();
        let token = format!("maestro-integration-{run_id}");
        let mut cluster = Self {
            runtime_cli,
            container_names: Vec::new(),
            endpoints: Vec::new(),
        };

        for (index, node) in nodes.iter().enumerate() {
            let member = format!("member{}", index + 1);
            let container = format!("maestro-etcd-test-{run_id}-{}", index + 1);
            let client_port = node.etcd_client_port;
            let peer_port = node.etcd_peer_port;
            let mut arguments = vec![
                "run".to_string(),
                "--detach".to_string(),
                "--network".to_string(),
                "host".to_string(),
                "--name".to_string(),
                container.clone(),
                crate::deployment::ETCD_IMAGE_TAG.to_string(),
                "etcd".to_string(),
                format!("--name={member}"),
                "--data-dir=/etcd-data".to_string(),
                format!("--listen-client-urls=http://0.0.0.0:{client_port}"),
                format!("--advertise-client-urls=http://127.0.0.1:{client_port}"),
                format!("--listen-peer-urls=http://0.0.0.0:{peer_port}"),
                format!("--initial-advertise-peer-urls=http://127.0.0.1:{peer_port}"),
                format!("--initial-cluster={members}"),
                "--initial-cluster-state=new".to_string(),
                format!("--initial-cluster-token={token}"),
            ];
            if cluster.runtime_cli == "docker" {
                arguments.insert(2, "--rm".to_string());
            }
            let references = arguments.iter().map(String::as_str).collect::<Vec<_>>();
            cluster.container_names.push(container.clone());
            if let Err(error) = command_output(&cluster.runtime_cli, &references) {
                drop(cluster);
                return Err(error.context(format!("failed to start etcd member `{member}`")));
            }
            cluster
                .endpoints
                .push(format!("http://127.0.0.1:{client_port}"));
        }
        Ok(cluster)
    }

    fn stop_member(&self, index: usize) -> Result<()> {
        command_output(
            &self.runtime_cli,
            &["stop", "--time", "1", &self.container_names[index]],
        )?;
        Ok(())
    }

    async fn wait_until_ready(&self) -> Result<()> {
        let mut last_error = None;
        let readiness = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match self.check_all_members_ready().await {
                    Ok(()) => return,
                    Err(error) => last_error = Some(error.to_string()),
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await;
        if readiness.is_err() {
            bail!(
                "three-member etcd cluster did not become ready: {}",
                last_error.unwrap_or_else(|| "readiness check timed out".to_string())
            );
        }
        Ok(())
    }

    async fn check_all_members_ready(&self) -> Result<()> {
        let mut cluster_leader = None;
        for endpoint in &self.endpoints {
            let mut client = etcd_client::Client::connect(vec![endpoint.clone()], None)
                .await
                .with_context(|| format!("could not connect to etcd member `{endpoint}`"))?;
            let status = client
                .status()
                .await
                .with_context(|| format!("etcd member `{endpoint}` did not answer status"))?;
            if status.leader() == 0 {
                bail!("etcd member `{endpoint}` has not observed a cluster leader");
            }
            if !status.errors().is_empty() {
                bail!(
                    "etcd member `{endpoint}` reported errors: {}",
                    status.errors().join(", ")
                );
            }
            if cluster_leader
                .replace(status.leader())
                .is_some_and(|leader| leader != status.leader())
            {
                bail!("etcd members do not agree on the cluster leader");
            }
            let members = client
                .member_list()
                .await
                .with_context(|| format!("etcd member `{endpoint}` did not list membership"))?;
            if members.members().len() != self.endpoints.len() {
                bail!(
                    "etcd member `{endpoint}` sees {} of {} members",
                    members.members().len(),
                    self.endpoints.len()
                );
            }
        }
        Ok(())
    }
}

struct SingleHostHttpCluster {
    runtime_cli: String,
    run_id: String,
    root: PathBuf,
    host_ip: Ipv4Addr,
    nodes: Vec<super::ClusterNodeEndpoint>,
    public_port: u16,
    networks: Vec<String>,
    container_names: BTreeSet<String>,
}

impl SingleHostHttpCluster {
    fn start() -> Result<Self> {
        let runtime_cli = test_runtime_cli()?;
        command_output(&runtime_cli, &["info"])
            .context("the HTTP cluster test requires a working container daemon")?;
        ensure_image(&runtime_cli, "busybox:1.37")?;
        ensure_image(&runtime_cli, "traefik:v3.6")?;

        let host_ip = test_host_ip()?;
        let nodes = reserve_node_endpoints(3, host_ip)?;
        let reserved_ports = nodes
            .iter()
            .flat_map(|node| {
                [
                    node.api_port,
                    node.gateway_port,
                    node.etcd_client_port,
                    node.etcd_peer_port,
                ]
            })
            .collect::<BTreeSet<_>>();
        let public_port = reserve_port_excluding(host_ip, &reserved_ports)?;
        let run_id = crate::utils::nanoid::unique_id(12).to_ascii_lowercase();
        let root = std::env::temp_dir().join(format!("maestro-http-cluster-{run_id}"));
        std::fs::create_dir_all(&root)?;
        let mut cluster = Self {
            runtime_cli,
            run_id,
            root,
            host_ip,
            nodes,
            public_port,
            networks: Vec::new(),
            container_names: BTreeSet::new(),
        };
        cluster.start_nodes()?;
        cluster.start_public_ingress()?;
        Ok(cluster)
    }

    fn start_nodes(&mut self) -> Result<()> {
        let subnet_base = 10 + usize::from(self.nodes[0].api_port % 220);
        let subnet_second = 180 + usize::from((self.nodes[0].api_port / 220) % 40);
        for index in 0..self.nodes.len() {
            let network = self.network_name(index);
            let subnet = format!(
                "10.{subnet_second}.{}.0/24",
                subnet_base.saturating_add(index)
            );
            self.networks.push(network.clone());
            command_output(
                &self.runtime_cli,
                &["network", "create", "--subnet", &subnet, &network],
            )
            .with_context(|| format!("failed to create logical node {} network", index + 1))?;
            self.start_backend(index)?;
            self.start_gateway(index)?;
        }
        Ok(())
    }

    fn start_backend(&mut self, index: usize) -> Result<()> {
        let name = self.backend_name(index);
        let network = self.network_name(index);
        let body = format!("node-{}", index + 1);
        let command = format!(
            "mkdir -p /www && printf '%s' '{body}' > /www/index.html && exec httpd -f -p 8080 -h /www"
        );
        self.run_container(
            &name,
            &[
                "run",
                "--detach",
                "--network",
                &network,
                "--name",
                &name,
                "busybox:1.37",
                "sh",
                "-c",
                &command,
            ],
        )?;
        Ok(())
    }

    fn start_gateway(&mut self, index: usize) -> Result<()> {
        self.write_gateway_config(index)?;
        let config_path = self.root.join(format!("gateway-{}.yml", index + 1));
        let name = self.gateway_name(index);
        let network = self.network_name(index);
        let publish = format!("{}:{}:8080", self.host_ip, self.nodes[index].gateway_port);
        let mount = format!("{}:/config/dynamic.yml:ro", config_path.display());
        self.run_container(
            &name,
            &[
                "run",
                "--detach",
                "--network",
                &network,
                "--name",
                &name,
                "--publish",
                &publish,
                "--volume",
                &mount,
                "traefik:v3.6",
                "--log.level=ERROR",
                "--api.dashboard=false",
                "--entrypoints.web.address=:8080",
                "--providers.file.filename=/config/dynamic.yml",
                "--providers.file.watch=true",
            ],
        )?;
        Ok(())
    }

    fn write_gateway_config(&self, index: usize) -> Result<()> {
        self.write_gateway_config_for_backends(index, &[self.backend_name(index)])
    }

    fn write_gateway_config_for_backends(
        &self,
        index: usize,
        backend_names: &[String],
    ) -> Result<()> {
        let config_path = self.root.join(format!("gateway-{}.yml", index + 1));
        let config = if backend_names.is_empty() {
            "http:\n  routers: {}\n  middlewares: {}\n  services: {}\n".to_string()
        } else {
            let node_id = format!("node-{}", index + 1);
            let affinity_token =
                super::traefik::affinity_token(TEST_CLUSTER_NAME, node_id.as_str());
            let servers = backend_names
                .iter()
                .map(|name| {
                    self.container_ip(name)
                        .map(|address| format!("          - url: http://{address}:8080"))
                })
                .collect::<Result<Vec<_>>>()?
                .join("\n");
            format!(
                "http:\n  routers:\n    local:\n      entryPoints: [web]\n      rule: PathPrefix(`/`)\n      service: local\n      middlewares: [affinity]\n  middlewares:\n    affinity:\n      headers:\n        customRequestHeaders:\n          {TEST_AFFINITY_HEADER}: \"{affinity_token}\"\n        customResponseHeaders:\n          {TEST_AFFINITY_HEADER}: \"{affinity_token}\"\n  services:\n    local:\n      loadBalancer:\n        sticky:\n          cookie:\n            name: maestro-affinity\n            httpOnly: true\n        servers:\n{servers}\n"
            )
        };
        std::fs::write(&config_path, config)?;
        Ok(())
    }

    fn start_public_ingress(&mut self) -> Result<()> {
        let servers = self
            .nodes
            .iter()
            .map(|node| {
                format!(
                    "          - url: http://{}:{}",
                    self.host_ip, node.gateway_port
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let affinity_routers = self
            .nodes
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let node_id = format!("node-{}", index + 1);
                let token = super::traefik::affinity_token(TEST_CLUSTER_NAME, &node_id);
                format!(
                    "    affinity-{}:\n      entryPoints: [web]\n      rule: Header(`{TEST_AFFINITY_HEADER}`, `{token}`)\n      priority: 100\n      service: affinity-{}",
                    index + 1,
                    index + 1
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let affinity_services = self
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                format!(
                    "    affinity-{}:\n      loadBalancer:\n        healthCheck:\n          path: /\n          interval: 500ms\n          timeout: 300ms\n        servers:\n          - url: http://{}:{}",
                    index + 1,
                    self.host_ip,
                    node.gateway_port
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let config_path = self.root.join("public.yml");
        std::fs::write(
            &config_path,
            format!(
                "http:\n  routers:\n    public:\n      entryPoints: [web]\n      rule: PathPrefix(`/`)\n      service: cluster\n{affinity_routers}\n  services:\n    cluster:\n      loadBalancer:\n        sticky:\n          cookie:\n            name: maestro-node-affinity\n            httpOnly: true\n        healthCheck:\n          path: /\n          interval: 500ms\n          timeout: 300ms\n        servers:\n{servers}\n{affinity_services}\n"
            ),
        )?;
        let name = self.public_name();
        let mount = format!("{}:/config/dynamic.yml:ro", config_path.display());
        let entrypoint = format!("--entrypoints.web.address=:{}", self.public_port);
        self.run_container(
            &name,
            &[
                "run",
                "--detach",
                "--network",
                "host",
                "--name",
                &name,
                "--volume",
                &mount,
                "traefik:v3.6",
                "--log.level=ERROR",
                "--api.dashboard=false",
                &entrypoint,
                "--providers.file.filename=/config/dynamic.yml",
                "--providers.file.watch=false",
            ],
        )?;
        Ok(())
    }

    fn run_container(&mut self, name: &str, arguments: &[&str]) -> Result<()> {
        self.container_names.insert(name.to_string());
        command_output(&self.runtime_cli, arguments)
            .with_context(|| format!("failed to start test container `{name}`"))?;
        Ok(())
    }

    fn stop_backend(&self, index: usize) -> Result<()> {
        self.stop_container(&self.backend_name(index))
    }

    fn restart_backend(&self, index: usize) -> Result<()> {
        command_output(&self.runtime_cli, &["start", &self.backend_name(index)])?;
        self.write_gateway_config(index)?;
        Ok(())
    }

    fn stop_gateway(&self, index: usize) -> Result<()> {
        self.stop_container(&self.gateway_name(index))
    }

    fn restart_gateway(&self, index: usize) -> Result<()> {
        command_output(&self.runtime_cli, &["start", &self.gateway_name(index)])?;
        Ok(())
    }

    fn remove_initial_backends(&self) -> Result<()> {
        for index in 0..self.nodes.len() {
            self.remove_container(&self.backend_name(index))?;
        }
        Ok(())
    }

    fn reconcile_scaled_assignments(
        &mut self,
        actual: &mut BTreeMap<String, RunningReplica>,
        desired: &[Assignment],
    ) -> Result<()> {
        for index in 0..self.nodes.len() {
            let node_id = format!("node-{}", index + 1);
            let node_actual = actual
                .iter()
                .filter(|(_, replica)| replica.assignment.node_id == node_id)
                .map(|(id, replica)| (id.clone(), replica.clone()))
                .collect::<BTreeMap<_, _>>();
            let manifest = AssignmentManifest {
                node_id,
                generation: 1,
                assignments: desired
                    .iter()
                    .filter(|assignment| assignment.node_id == format!("node-{}", index + 1))
                    .cloned()
                    .collect(),
            };
            for action in diff_assignments(&node_actual, &manifest)? {
                match action {
                    ReconcileAction::Stop(assignment_id) => {
                        let replica = actual.remove(&assignment_id).ok_or_else(|| {
                            anyhow!("missing running assignment `{assignment_id}`")
                        })?;
                        self.remove_container(&replica.container_hostname)?;
                    }
                    ReconcileAction::Start(assignment) => {
                        let container_hostname =
                            self.start_assignment_backend(index, &assignment)?;
                        actual.insert(
                            assignment.assignment_id.clone(),
                            RunningReplica {
                                assignment,
                                endpoint: None,
                                container_hostname,
                                handle: None,
                            },
                        );
                    }
                }
            }
        }
        self.write_scaled_gateway_configs(actual)
    }

    fn start_assignment_backend(
        &mut self,
        node_index: usize,
        assignment: &Assignment,
    ) -> Result<String> {
        let name = self.assignment_backend_name(assignment);
        let network = self.network_name(node_index);
        let body = assignment_body(assignment.replica_index);
        let command = format!(
            "mkdir -p /www && printf '%s' '{body}' > /www/index.html && exec httpd -f -p 8080 -h /www"
        );
        self.run_container(
            &name,
            &[
                "run",
                "--detach",
                "--network",
                &network,
                "--name",
                &name,
                "busybox:1.37",
                "sh",
                "-c",
                &command,
            ],
        )?;
        Ok(name)
    }

    fn write_scaled_gateway_configs(
        &self,
        actual: &BTreeMap<String, RunningReplica>,
    ) -> Result<()> {
        for index in 0..self.nodes.len() {
            let node_id = format!("node-{}", index + 1);
            let mut replicas = actual
                .values()
                .filter(|replica| replica.assignment.node_id == node_id)
                .collect::<Vec<_>>();
            replicas.sort_by_key(|replica| replica.assignment.replica_index);
            let names = replicas
                .into_iter()
                .map(|replica| replica.container_hostname.clone())
                .collect::<Vec<_>>();
            self.write_gateway_config_for_backends(index, &names)?;
        }
        Ok(())
    }

    fn remove_container(&self, name: &str) -> Result<()> {
        command_output(&self.runtime_cli, &["rm", "--force", name])?;
        Ok(())
    }

    fn container_exists(&self, name: &str) -> bool {
        command_output(&self.runtime_cli, &["inspect", name]).is_ok()
    }

    fn stop_container(&self, name: &str) -> Result<()> {
        command_output(&self.runtime_cli, &["stop", "--time", "1", name])?;
        Ok(())
    }

    fn container_ip(&self, name: &str) -> Result<Ipv4Addr> {
        let template = if self.runtime_cli == "nerdctl" {
            "{{.NetworkSettings.IPAddress}}"
        } else {
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
        };
        command_output(&self.runtime_cli, &["inspect", "--format", template, name])?
            .parse()
            .with_context(|| format!("container `{name}` has no usable IPv4 address"))
    }

    fn container_diagnostics(&self, name: &str) -> String {
        let state = command_output(
            &self.runtime_cli,
            &["inspect", "--format", "{{json .State}}", name],
        )
        .unwrap_or_else(|error| error.to_string());
        let logs = command_output(&self.runtime_cli, &["logs", name])
            .unwrap_or_else(|error| error.to_string());
        format!("state={state}; logs={logs:?}")
    }

    fn gateway_url(&self, index: usize) -> String {
        format!("http://{}:{}", self.host_ip, self.nodes[index].gateway_port)
    }

    fn public_url(&self) -> String {
        format!("http://{}:{}", self.host_ip, self.public_port)
    }

    fn backend_name(&self, index: usize) -> String {
        format!(
            "maestro-http-replica-{}-node-{}",
            self.run_id, self.nodes[index].api_port
        )
    }

    fn assignment_backend_name(&self, assignment: &Assignment) -> String {
        format!(
            "maestro-http-scale-{}-replica-{}-{}",
            self.run_id,
            assignment.replica_index + 1,
            assignment.assignment_id
        )
    }

    fn gateway_name(&self, index: usize) -> String {
        format!(
            "maestro-http-gateway-{}-node-{}",
            self.run_id, self.nodes[index].api_port
        )
    }

    fn public_name(&self) -> String {
        format!("maestro-http-public-{}", self.run_id)
    }

    fn network_name(&self, index: usize) -> String {
        format!(
            "maestro-http-{}-node-{}",
            self.run_id, self.nodes[index].api_port
        )
    }
}

impl Drop for SingleHostHttpCluster {
    fn drop(&mut self) {
        for name in &self.container_names {
            let _ = Command::new(&self.runtime_cli)
                .args(["rm", "--force", name])
                .output();
        }
        for network in &self.networks {
            let _ = Command::new(&self.runtime_cli)
                .args(["network", "rm", network])
                .output();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Drop for ContainerEtcdCluster {
    fn drop(&mut self) {
        if self.container_names.is_empty() {
            return;
        }
        let mut arguments = vec!["rm", "--force"];
        arguments.extend(self.container_names.iter().map(String::as_str));
        let _ = Command::new(&self.runtime_cli).args(arguments).output();
    }
}

fn reserve_node_endpoints(
    count: usize,
    host_ip: Ipv4Addr,
) -> Result<Vec<super::ClusterNodeEndpoint>> {
    let mut listeners = Vec::with_capacity(count * 4);
    let mut endpoints = Vec::with_capacity(count);
    for _ in 0..count {
        'candidate: loop {
            let api_listener = TcpListener::bind((host_ip, 0))?;
            let api_port = api_listener.local_addr()?.port();
            let Some(peer_port) = api_port.checked_add(3) else {
                continue;
            };
            let mut block = vec![api_listener];
            for port in api_port + 1..=peer_port {
                let Ok(listener) = TcpListener::bind((host_ip, port)) else {
                    continue 'candidate;
                };
                block.push(listener);
            }
            listeners.extend(block);
            endpoints.push(super::ClusterNodeEndpoint {
                host_ip,
                api_port,
                gateway_port: api_port + 1,
                etcd_client_port: api_port + 2,
                etcd_peer_port: peer_port,
                identity_api_port: Some(api_port),
            });
            break;
        }
    }
    drop(listeners);
    Ok(endpoints)
}

fn reserve_port_excluding(host_ip: Ipv4Addr, excluded: &BTreeSet<u16>) -> Result<u16> {
    loop {
        let listener = TcpListener::bind((host_ip, 0))?;
        let port = listener.local_addr()?.port();
        if !excluded.contains(&port) {
            return Ok(port);
        }
    }
}

fn test_runtime_cli() -> Result<String> {
    if let Ok(runtime_cli) = std::env::var("MAESTRO_TEST_RUNTIME") {
        if runtime_cli != "docker" && runtime_cli != "nerdctl" {
            bail!("MAESTRO_TEST_RUNTIME must be `docker` or `nerdctl`");
        }
        command_output(&runtime_cli, &["info"])
            .with_context(|| format!("the requested `{runtime_cli}` daemon is not available"))?;
        return Ok(runtime_cli);
    }

    for runtime_cli in ["nerdctl", "docker"] {
        if Command::new(runtime_cli)
            .arg("info")
            .output()
            .is_ok_and(|output| output.status.success())
        {
            return Ok(runtime_cli.to_string());
        }
    }

    bail!(
        "the multi-node tests require a working nerdctl or Docker daemon; start one or set \
         MAESTRO_TEST_RUNTIME explicitly"
    )
}

fn test_host_ip() -> Result<Ipv4Addr> {
    if let Ok(configured) = std::env::var("MAESTRO_TEST_HOST_IP") {
        let host_ip = configured
            .parse::<Ipv4Addr>()
            .context("MAESTRO_TEST_HOST_IP must be an IPv4 address")?;
        if !host_ip.is_private() || host_ip.is_loopback() || host_ip.is_unspecified() {
            bail!("MAESTRO_TEST_HOST_IP must be a private non-loopback address");
        }
        return Ok(host_ip);
    }
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    socket.connect((Ipv4Addr::new(192, 0, 2, 1), 9))?;
    let std::net::IpAddr::V4(host_ip) = socket.local_addr()?.ip() else {
        bail!("the test host has no routable private IPv4 address");
    };
    if !host_ip.is_private() || host_ip.is_loopback() || host_ip.is_unspecified() {
        bail!("discovered test host IP `{host_ip}` is not private and non-loopback");
    }
    Ok(host_ip)
}

fn ensure_image(runtime_cli: &str, image: &str) -> Result<()> {
    if command_output(runtime_cli, &["image", "inspect", image]).is_err() {
        command_output(runtime_cli, &["pull", image])
            .with_context(|| format!("failed to pull test image `{image}`"))?;
    }
    Ok(())
}

fn command_output(program: &str, arguments: &[&str]) -> Result<String> {
    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        bail!(
            "`{program} {}` failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn wait_for_leader(
    electors: &[Arc<EtcdLeaderElector>],
    timeout: Duration,
) -> Result<(usize, LeadershipToken)> {
    tokio::time::timeout(timeout, async {
        loop {
            let leaders = electors
                .iter()
                .enumerate()
                .filter_map(|(index, elector)| match elector.state() {
                    LeadershipState::Leading(token) => Some((index, token)),
                    LeadershipState::Following(_) | LeadershipState::Unknown => None,
                })
                .collect::<Vec<_>>();
            if let [leader] = leaders.as_slice() {
                return leader.clone();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("exactly one Maestro leader was not elected"))
}

async fn fetch_body(client: &reqwest::Client, url: &str) -> Option<String> {
    let response = client.get(url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    response
        .text()
        .await
        .ok()
        .map(|body| body.trim().to_string())
}

async fn http_observation(client: &reqwest::Client, url: &str) -> String {
    match client.get(url).send().await {
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            format!("status {status}, body {:?}", body.trim())
        }
        Err(error) => format!("request error: {error}"),
    }
}

async fn wait_for_body(
    client: &reqwest::Client,
    url: &str,
    expected: &str,
    timeout: Duration,
) -> Result<()> {
    let result = tokio::time::timeout(timeout, async {
        loop {
            if fetch_body(client, url).await.as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    if result.is_err() {
        bail!(
            "`{url}` did not return `{expected}`: {}",
            http_observation(client, url).await
        );
    }
    Ok(())
}

async fn wait_for_affinity_response(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
) -> Result<reqwest::Response> {
    tokio::time::timeout(timeout, async {
        loop {
            if let Ok(response) = client.get(url).send().await
                && response.status().is_success()
                && response.headers().contains_key(TEST_AFFINITY_HEADER)
            {
                return response;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("`{url}` did not return an affinity-enabled response"))
}

async fn wait_until_unavailable(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            if fetch_body(client, url).await.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("`{url}` remained available"))
}

async fn wait_for_routing_set(
    client: &reqwest::Client,
    url: &str,
    allowed: &BTreeSet<String>,
    required: &BTreeSet<String>,
    timeout: Duration,
) -> Result<()> {
    let mut last_observed = BTreeSet::new();
    tokio::time::timeout(timeout, async {
        let mut observed = BTreeSet::new();
        let mut consecutive = 0_usize;
        loop {
            match fetch_body(client, url).await {
                Some(body) if allowed.contains(&body) => {
                    observed.insert(body);
                    consecutive += 1;
                    last_observed.clone_from(&observed);
                    if required.is_subset(&observed)
                        && consecutive >= required.len().saturating_mul(2).max(4)
                    {
                        return;
                    }
                }
                _ => {
                    observed.clear();
                    consecutive = 0;
                }
            }
            tokio::time::sleep(Duration::from_millis(75)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "`{url}` did not converge to allowed routes {allowed:?}; last observed {last_observed:?}"
        )
    })
}

fn assignment_body(replica_index: u32) -> String {
    format!("replica-{}", replica_index + 1)
}

fn scaling_plan(
    cluster: &SingleHostHttpCluster,
    replicas: u32,
    current: &[Assignment],
    now_ms: i64,
) -> SchedulePlan {
    let nodes = cluster
        .nodes
        .iter()
        .enumerate()
        .map(|(index, endpoint)| NodeInfo {
            node_id: format!("node-{}", index + 1),
            instance_id: format!("instance-{}", index + 1),
            hostname: format!("node-{}", index + 1),
            role: NodeRole::Worker,
            scheduling: true,
            cluster_host_ip: cluster.host_ip,
            cluster_api_port: endpoint.api_port,
            cluster_gateway_port: endpoint.gateway_port,
            subnet: format!("172.30.{}.0/24", index + 1),
            tailscale_ip: None,
            data_plane_ready: true,
            data_plane_checked_at_ms: now_ms,
            data_plane_error: None,
            version: "test".to_string(),
            started_at_ms: 0,
            labels: BTreeMap::new(),
        })
        .collect();
    plan(ScheduleInput {
        cluster_id: "single-host-scaling-test".to_string(),
        services: vec![ServiceScheduleSpec {
            service_id: "scale-test".to_string(),
            groups: vec![DeploymentGroup {
                deployment_id: "deployment-1".to_string(),
                replicas,
            }],
            node_affinity: None,
            unhealthy_slots: BTreeSet::new(),
        }],
        nodes,
        node_states: BTreeMap::<String, NodeState>::new(),
        current: current.to_vec(),
        held: BTreeSet::new(),
        now_ms,
    })
}

fn assignment_counts(assignments: &[Assignment]) -> BTreeMap<String, usize> {
    assignments
        .iter()
        .fold(BTreeMap::new(), |mut counts, assignment| {
            *counts.entry(assignment.node_id.clone()).or_default() += 1;
            counts
        })
}

async fn wait_for_assignment_routes(
    cluster: &SingleHostHttpCluster,
    client: &reqwest::Client,
    assignments: &[Assignment],
) -> Result<()> {
    let expected = assignments
        .iter()
        .map(|assignment| assignment_body(assignment.replica_index))
        .collect::<BTreeSet<_>>();
    wait_for_routing_set(
        client,
        &cluster.public_url(),
        &expected,
        &expected,
        Duration::from_secs(30),
    )
    .await?;
    for index in 0..cluster.nodes.len() {
        let node_id = format!("node-{}", index + 1);
        let local = assignments
            .iter()
            .filter(|assignment| assignment.node_id == node_id)
            .map(|assignment| assignment_body(assignment.replica_index))
            .collect::<BTreeSet<_>>();
        if local.is_empty() {
            wait_until_unavailable(client, &cluster.gateway_url(index), Duration::from_secs(10))
                .await?;
        } else {
            wait_for_routing_set(
                client,
                &cluster.gateway_url(index),
                &local,
                &local,
                Duration::from_secs(20),
            )
            .await?;
        }
    }
    Ok(())
}

/// Exercises the endpoint-port data-plane shape on one physical host: three isolated container
/// subnets, one local replica and gateway per logical node, and a public ingress proxy that
/// health-checks those gateways. It covers initial spreading, replica loss and recovery, and
/// gateway loss, complete ingress loss, and recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn single_host_gateways_route_and_recover_across_logical_nodes() -> Result<()> {
    let cluster = SingleHostHttpCluster::start()?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let all = ["node-1", "node-2", "node-3"]
        .into_iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();

    assert!(
        cluster
            .nodes
            .iter()
            .all(|node| node.host_ip == cluster.host_ip)
    );
    assert_eq!(
        cluster
            .nodes
            .iter()
            .map(|node| node.api_port)
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    for (index, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(node.gateway_port, node.api_port + 1);
        assert_eq!(node.etcd_client_port, node.api_port + 2);
        assert_eq!(node.etcd_peer_port, node.api_port + 3);
        if let Err(error) = wait_for_body(
            &client,
            &cluster.gateway_url(index),
            &format!("node-{}", index + 1),
            Duration::from_secs(20),
        )
        .await
        {
            bail!(
                "{error}; {}",
                cluster.container_diagnostics(&cluster.gateway_name(index))
            );
        }
    }
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &all,
        &all,
        Duration::from_secs(30),
    )
    .await?;

    cluster.stop_backend(1)?;
    wait_until_unavailable(&client, &cluster.gateway_url(1), Duration::from_secs(10)).await?;
    let without_node_two = ["node-1", "node-3"]
        .into_iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &without_node_two,
        &without_node_two,
        Duration::from_secs(30),
    )
    .await?;

    cluster.restart_backend(1)?;
    wait_for_body(
        &client,
        &cluster.gateway_url(1),
        "node-2",
        Duration::from_secs(20),
    )
    .await?;
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &all,
        &all,
        Duration::from_secs(30),
    )
    .await?;

    cluster.stop_gateway(0)?;
    wait_until_unavailable(&client, &cluster.gateway_url(0), Duration::from_secs(10)).await?;
    let without_node_one = ["node-2", "node-3"]
        .into_iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &without_node_one,
        &without_node_one,
        Duration::from_secs(30),
    )
    .await?;

    cluster.stop_gateway(1)?;
    cluster.stop_gateway(2)?;
    wait_until_unavailable(&client, &cluster.public_url(), Duration::from_secs(15)).await?;

    cluster.restart_gateway(1)?;
    cluster.restart_gateway(2)?;
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &without_node_one,
        &without_node_one,
        Duration::from_secs(30),
    )
    .await?;

    cluster.restart_gateway(0)?;
    wait_for_body(
        &client,
        &cluster.gateway_url(0),
        "node-1",
        Duration::from_secs(20),
    )
    .await?;
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &all,
        &all,
        Duration::from_secs(30),
    )
    .await?;
    Ok(())
}

/// Uses the production scheduler and reconciler diff against real HTTP containers and Traefik
/// gateways. It verifies stable placement and externally visible routes while scaling one service
/// from one replica to five and back to two across three logical nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn scheduler_scales_live_replicas_across_logical_nodes() -> Result<()> {
    let mut cluster = SingleHostHttpCluster::start()?;
    cluster.remove_initial_backends()?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let mut actual = BTreeMap::<String, RunningReplica>::new();

    let initial = scaling_plan(&cluster, 1, &[], 1_000);
    assert!(initial.unschedulable.is_empty());
    assert_eq!(initial.assignments.len(), 1);
    assert_eq!(
        assignment_counts(&initial.assignments),
        BTreeMap::from([("node-1".to_string(), 1)])
    );
    cluster.reconcile_scaled_assignments(&mut actual, &initial.assignments)?;
    wait_for_assignment_routes(&cluster, &client, &initial.assignments).await?;

    let scaled_up = scaling_plan(&cluster, 5, &initial.assignments, 2_000);
    assert!(scaled_up.unschedulable.is_empty());
    assert_eq!(scaled_up.assignments.len(), 5);
    assert_eq!(
        assignment_counts(&scaled_up.assignments),
        BTreeMap::from([
            ("node-1".to_string(), 2),
            ("node-2".to_string(), 2),
            ("node-3".to_string(), 1),
        ])
    );
    assert_eq!(
        scaled_up
            .assignments
            .iter()
            .find(|assignment| assignment.replica_index == 0)
            .map(|assignment| assignment.assignment_id.as_str()),
        Some(initial.assignments[0].assignment_id.as_str())
    );
    cluster.reconcile_scaled_assignments(&mut actual, &scaled_up.assignments)?;
    wait_for_assignment_routes(&cluster, &client, &scaled_up.assignments).await?;

    let removed_containers = actual
        .values()
        .filter(|replica| replica.assignment.replica_index >= 2)
        .map(|replica| replica.container_hostname.clone())
        .collect::<Vec<_>>();
    let scaled_down = scaling_plan(&cluster, 2, &scaled_up.assignments, 3_000);
    assert!(scaled_down.unschedulable.is_empty());
    assert_eq!(scaled_down.assignments.len(), 2);
    assert_eq!(
        assignment_counts(&scaled_down.assignments),
        BTreeMap::from([("node-1".to_string(), 1), ("node-2".to_string(), 1),])
    );
    for survivor in &scaled_down.assignments {
        assert_eq!(
            scaled_up
                .assignments
                .iter()
                .find(|assignment| assignment.replica_index == survivor.replica_index)
                .map(|assignment| assignment.assignment_id.as_str()),
            Some(survivor.assignment_id.as_str())
        );
    }
    cluster.reconcile_scaled_assignments(&mut actual, &scaled_down.assignments)?;
    wait_for_assignment_routes(&cluster, &client, &scaled_down.assignments).await?;
    assert!(
        removed_containers
            .iter()
            .all(|name| !cluster.container_exists(name))
    );
    assert_eq!(actual.len(), 2);
    Ok(())
}

/// Verifies production-shaped affinity at both proxy layers. A first request receives opaque
/// affinity identity and both sticky cookies, cookie replay remains on the same replica, and an
/// explicitly replayed response header overrides the cookie to select another logical node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn node_affinity_is_automatic_opaque_and_replayable() -> Result<()> {
    let cluster = SingleHostHttpCluster::start()?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    for index in 0..cluster.nodes.len() {
        wait_for_body(
            &client,
            &cluster.gateway_url(index),
            &format!("node-{}", index + 1),
            Duration::from_secs(20),
        )
        .await?;
    }

    let first =
        wait_for_affinity_response(&client, &cluster.public_url(), Duration::from_secs(20)).await?;
    let response_token = first
        .headers()
        .get(TEST_AFFINITY_HEADER)
        .ok_or_else(|| anyhow!("gateway did not return `{TEST_AFFINITY_HEADER}`"))?
        .to_str()?
        .to_string();
    let cookies = first
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok()?.split(';').next())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let selected_body = first.text().await?.trim().to_string();
    let selected_index = selected_body
        .strip_prefix("node-")
        .and_then(|value| value.parse::<usize>().ok())
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| anyhow!("unexpected affinity response body `{selected_body}`"))?;
    let selected_node_id = format!("node-{}", selected_index + 1);
    assert_eq!(
        response_token,
        super::traefik::affinity_token(TEST_CLUSTER_NAME, &selected_node_id)
    );
    assert!(!response_token.contains(&selected_node_id));
    assert!(
        cookies
            .iter()
            .any(|cookie| cookie.starts_with("maestro-node-affinity="))
    );
    assert!(
        cookies
            .iter()
            .any(|cookie| cookie.starts_with("maestro-affinity="))
    );

    let cookie_header = cookies.join("; ");
    for _ in 0..8 {
        let response = client
            .get(cluster.public_url())
            .header(reqwest::header::COOKIE, &cookie_header)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!(
                "sticky-cookie replay returned {status} with body {body:?}; cookies={cookies:?}; public={}; gateway={}",
                cluster.container_diagnostics(&cluster.public_name()),
                cluster.container_diagnostics(&cluster.gateway_name(selected_index))
            );
        }
        assert_eq!(
            response
                .headers()
                .get(TEST_AFFINITY_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some(response_token.as_str())
        );
        assert_eq!(response.text().await?.trim(), selected_body);
    }

    let target_index = (selected_index + 1) % cluster.nodes.len();
    let target_node_id = format!("node-{}", target_index + 1);
    let target_token = super::traefik::affinity_token(TEST_CLUSTER_NAME, &target_node_id);
    for _ in 0..4 {
        let response = client
            .get(cluster.public_url())
            .header(reqwest::header::COOKIE, &cookie_header)
            .header(TEST_AFFINITY_HEADER, &target_token)
            .send()
            .await?;
        assert!(response.status().is_success());
        assert_eq!(
            response
                .headers()
                .get(TEST_AFFINITY_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some(target_token.as_str())
        );
        assert_eq!(response.text().await?.trim(), target_node_id);
    }
    Ok(())
}

/// This is deliberately ignored in the default unit suite because it starts and destroys three
/// host-networked etcd containers. Run it on an isolated Linux container host with:
///
/// `cargo test-multi-node`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn distributed_election_fencing_and_quorum() -> Result<()> {
    let cluster = ContainerEtcdCluster::start()?;
    cluster.wait_until_ready().await?;

    let elector_a = Arc::new(
        EtcdLeaderElector::connect(&cluster.endpoints, None, "node-a".to_string(), true).await?,
    );
    let elector_b = Arc::new(
        EtcdLeaderElector::connect(&cluster.endpoints, None, "node-b".to_string(), true).await?,
    );
    let electors = [elector_a.clone(), elector_b.clone()];
    let (shutdown_a, _) = broadcast::channel(2);
    let (shutdown_b, _) = broadcast::channel(2);
    let mut handles = [
        elector_a
            .clone()
            .spawn(shutdown_a.subscribe(), Logger::noop()),
        elector_b
            .clone()
            .spawn(shutdown_b.subscribe(), Logger::noop()),
    ];

    let (first_leader, stale_token) = wait_for_leader(&electors, Duration::from_secs(20)).await?;
    let store = EtcdStateStore::new_with_endpoints(
        &cluster.endpoints,
        crate::utils::crypto::derive_key("distributed-integration-test"),
        None,
    )
    .await?;
    store
        .apply_cluster_mutation(
            &stale_token,
            ClusterMutation::ClaimRequest {
                request_id: "before-failover".to_string(),
                fingerprint: "before".to_string(),
                now_ms: 1,
            },
        )
        .await
        .context("the elected leader could not perform a fenced mutation")?;

    let shutdown = [&shutdown_a, &shutdown_b];
    let _ = shutdown[first_leader].send(ShutdownEvent::Graceful);
    for handle in handles[first_leader].drain(..) {
        handle.await?;
    }
    let survivor = 1 - first_leader;
    let (_, live_token) = wait_for_leader(
        std::slice::from_ref(&electors[survivor]),
        Duration::from_secs(20),
    )
    .await?;
    if live_token.info.node_id == stale_token.info.node_id {
        bail!("leadership did not move to the surviving controller");
    }

    let stale_result = store
        .apply_cluster_mutation(
            &stale_token,
            ClusterMutation::ClaimRequest {
                request_id: "stale-after-failover".to_string(),
                fingerprint: "stale".to_string(),
                now_ms: 2,
            },
        )
        .await;
    if stale_result.is_ok() {
        bail!("a stale leader completed a fenced mutation after failover");
    }
    store
        .apply_cluster_mutation(
            &live_token,
            ClusterMutation::ClaimRequest {
                request_id: "live-after-failover".to_string(),
                fingerprint: "live".to_string(),
                now_ms: 3,
            },
        )
        .await
        .context("the successor leader could not perform a fenced mutation")?;

    cluster.stop_member(0)?;
    cluster.stop_member(1)?;
    let quorum_result = tokio::time::timeout(
        Duration::from_secs(10),
        store.apply_cluster_mutation(
            &live_token,
            ClusterMutation::ClaimRequest {
                request_id: "without-quorum".to_string(),
                fingerprint: "quorum".to_string(),
                now_ms: 4,
            },
        ),
    )
    .await;
    if matches!(quorum_result, Ok(Ok(_))) {
        bail!("cluster mutation succeeded after loss of etcd quorum");
    }

    let _ = shutdown[survivor].send(ShutdownEvent::Force);
    for handle in handles[survivor].drain(..) {
        handle.abort();
    }
    Ok(())
}
