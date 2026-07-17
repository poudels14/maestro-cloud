use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use etcd_client::MemberAddOptions;
use tokio::sync::broadcast;

use super::assignment_store::{
    AssignmentStore, EtcdAssignmentStore, InMemoryAssignmentStore, ReplaceOutcome,
};
use super::elector::{EtcdLeaderElector, LeaderElector};
use super::executor::RunningReplica;
use super::reconciler::{ReconcileAction, diff_assignments};
use super::registry::{InMemoryNodeRegistry, NodeRegistry};
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
const ROLLOUT_TEST_IMAGE: &str = "python:3.13-alpine";
const ROLLOUT_SERVER: &str = r#"from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import signal
import threading
import time

body = os.environ["BODY"].encode()
ready = os.environ.get("START_READY") == "1"
draining = False
active = 0
condition = threading.Condition()

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_body(self, status, value):
        self.send_response(status)
        self.send_header("Content-Length", str(len(value)))
        self.end_headers()
        self.wfile.write(value)

    def do_GET(self):
        global active
        if self.path == "/ready":
            with condition:
                available = ready and not draining
            self.send_body(200 if available else 503, b"ready" if available else b"not-ready")
            return

        with condition:
            active += 1
        try:
            if self.path == "/slow":
                open("/tmp/slow-started", "w").close()
                time.sleep(3)
            self.send_body(200, body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with condition:
                active -= 1
                condition.notify_all()

    def log_message(self, format, *args):
        pass

def stop_when_idle():
    with condition:
        condition.wait_for(lambda: active == 0)
    server.shutdown()

def terminate(signum, frame):
    global draining
    with condition:
        draining = True
    threading.Thread(target=stop_when_idle).start()

def become_ready(signum, frame):
    global ready
    with condition:
        ready = True

server = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGUSR1, become_ready)
server.serve_forever()
"#;

#[derive(Clone)]
struct RestartNodeApiState {
    node: NodeInfo,
    registry: Arc<InMemoryNodeRegistry>,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
}

async fn restart_test_node(
    axum::extract::State(state): axum::extract::State<RestartNodeApiState>,
) -> axum::Json<serde_json::Value> {
    state
        .requests
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .push(state.node.node_id.clone());
    let mut restarted = state
        .registry
        .list_nodes()
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|node| node.node_id == state.node.node_id)
        .unwrap_or(state.node);
    restarted.instance_id = format!("{}-restarted", restarted.instance_id);
    restarted.started_at_ms = restarted.started_at_ms.saturating_add(1);
    state.registry.insert_for_test(restarted);
    axum::Json(serde_json::json!({ "accepted": true }))
}

async fn restart_test_healthy() -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}

struct ContainerEtcdCluster {
    runtime_cli: String,
    container_names: Vec<String>,
    endpoints: Vec<String>,
    nodes: Vec<super::ClusterNodeEndpoint>,
    root: PathBuf,
    token: String,
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
        let root = std::env::temp_dir().join(format!("maestro-etcd-cluster-{run_id}"));
        std::fs::create_dir_all(&root)?;
        let mut cluster = Self {
            runtime_cli,
            container_names: Vec::new(),
            endpoints: Vec::new(),
            nodes: nodes.clone(),
            root,
            token: token.clone(),
        };

        for (index, node) in nodes.iter().enumerate() {
            let member = format!("member{}", index + 1);
            let container = format!("maestro-etcd-test-{run_id}-{}", index + 1);
            let client_port = node.etcd_client_port;
            let peer_port = node.etcd_peer_port;
            let data_dir = cluster.root.join(format!("member-{}", index + 1));
            std::fs::create_dir_all(&data_dir)?;
            let arguments = vec![
                "run".to_string(),
                "--detach".to_string(),
                "--network".to_string(),
                "host".to_string(),
                "--name".to_string(),
                container.clone(),
                "--volume".to_string(),
                format!("{}:/etcd-data", data_dir.display()),
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

    fn initial_cluster(&self) -> String {
        self.nodes
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
            .join(",")
    }

    fn wipe_member(&self, index: usize) -> Result<()> {
        command_output(
            &self.runtime_cli,
            &["rm", "--force", &self.container_names[index]],
        )?;
        let data_dir = self.root.join(format!("member-{}", index + 1));
        std::fs::remove_dir_all(&data_dir)?;
        std::fs::create_dir_all(data_dir)?;
        Ok(())
    }

    fn restart_survivor_with_force_new_cluster(&self, index: usize) -> Result<()> {
        command_output(
            &self.runtime_cli,
            &["rm", "--force", &self.container_names[index]],
        )?;
        self.run_member(index, &self.initial_cluster(), true)
    }

    fn start_replacement(&self, index: usize, initial_cluster: &str) -> Result<()> {
        self.run_member(index, initial_cluster, false)
    }

    fn run_member(&self, index: usize, initial_cluster: &str, force_new: bool) -> Result<()> {
        let node = self.nodes[index];
        let member = format!("member{}", index + 1);
        let data_dir = self.root.join(format!("member-{}", index + 1));
        let mut arguments = vec![
            "run".to_string(),
            "--detach".to_string(),
            "--network".to_string(),
            "host".to_string(),
            "--name".to_string(),
            self.container_names[index].clone(),
            "--volume".to_string(),
            format!("{}:/etcd-data", data_dir.display()),
            crate::deployment::ETCD_IMAGE_TAG.to_string(),
            "etcd".to_string(),
            format!("--name={member}"),
            "--data-dir=/etcd-data".to_string(),
            format!(
                "--listen-client-urls=http://0.0.0.0:{}",
                node.etcd_client_port
            ),
            format!(
                "--advertise-client-urls=http://127.0.0.1:{}",
                node.etcd_client_port
            ),
            format!("--listen-peer-urls=http://0.0.0.0:{}", node.etcd_peer_port),
            format!(
                "--initial-advertise-peer-urls=http://127.0.0.1:{}",
                node.etcd_peer_port
            ),
            format!("--initial-cluster={initial_cluster}"),
            "--initial-cluster-state=existing".to_string(),
            format!("--initial-cluster-token={}", self.token),
            "--strict-reconfig-check=true".to_string(),
        ];
        if force_new {
            arguments.push("--force-new-cluster=true".to_string());
        }
        let references = arguments.iter().map(String::as_str).collect::<Vec<_>>();
        command_output(&self.runtime_cli, &references)?;
        Ok(())
    }

    fn stop_member(&self, index: usize) -> Result<()> {
        command_output(
            &self.runtime_cli,
            &["stop", "--time", "1", &self.container_names[index]],
        )?;
        Ok(())
    }

    fn restart_member(&self, index: usize) -> Result<()> {
        command_output(&self.runtime_cli, &["start", &self.container_names[index]])?;
        Ok(())
    }

    async fn wait_for_quorum_write(&self, unavailable: usize, sequence: usize) -> Result<()> {
        let endpoints = self
            .endpoints
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != unavailable)
            .map(|(_, endpoint)| endpoint.clone())
            .collect::<Vec<_>>();
        let mut last_error = None;
        let completed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let result = async {
                    let mut client = etcd_client::Client::connect(endpoints.clone(), None).await?;
                    client
                        .put(
                            format!("/maestro/integration/restart/{sequence}"),
                            format!("member-{unavailable}-offline"),
                            None,
                        )
                        .await?;
                    Result::<(), etcd_client::Error>::Ok(())
                }
                .await;
                match result {
                    Ok(()) => return,
                    Err(error) => last_error = Some(error.to_string()),
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await;
        if completed.is_err() {
            bail!(
                "etcd quorum did not accept a write with member {} offline: {}",
                unavailable + 1,
                last_error.unwrap_or_else(|| "write timed out".to_string())
            );
        }
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

struct FormingEtcdCluster {
    runtime_cli: String,
    run_id: String,
    token: String,
    cluster_id: String,
    root: PathBuf,
    nodes: Vec<super::ClusterNodeEndpoint>,
    container_names: BTreeSet<String>,
    endpoints: Vec<String>,
}

impl FormingEtcdCluster {
    fn start_seed() -> Result<Self> {
        let runtime_cli = test_runtime_cli()?;
        command_output(&runtime_cli, &["info"])
            .context("the formation test requires a working container daemon")?;
        let nodes = reserve_node_endpoints(3, Ipv4Addr::LOCALHOST)?;
        let run_id = crate::utils::nanoid::unique_id(12).to_ascii_lowercase();
        let root = std::env::temp_dir().join(format!("maestro-forming-cluster-{run_id}"));
        std::fs::create_dir_all(&root)?;
        let mut cluster = Self {
            runtime_cli,
            token: format!("maestro-forming-{run_id}"),
            run_id,
            cluster_id: String::new(),
            root,
            nodes,
            container_names: BTreeSet::new(),
            endpoints: Vec::new(),
        };
        let seed_data = cluster.root.join("node-1");
        let config = crate::config::ClusterConfig {
            name: TEST_CLUSTER_NAME.to_string(),
            nodes: cluster
                .nodes
                .iter()
                .enumerate()
                .map(|(index, node)| {
                    (
                        format!("node{}", index + 1),
                        crate::config::ClusterNodeConfig {
                            endpoint: crate::config::ClusterEndpointConfig::Endpoint(
                                format!("{}:{}", node.host_ip, node.api_port)
                                    .parse()
                                    .unwrap(),
                            ),
                            subnet: format!("172.30.{}.0/24", index + 1),
                            role: if index == 0 {
                                NodeRole::Master
                            } else {
                                NodeRole::Voter
                            },
                        },
                    )
                })
                .collect(),
            api_port: cluster.nodes[0].api_port,
            gateway_port: cluster.nodes[0].gateway_port,
            etcd_client_port: cluster.nodes[0].etcd_client_port,
            etcd_peer_port: cluster.nodes[0].etcd_peer_port,
            control_allow_cidrs: vec!["127.0.0.1/32".to_string()],
            shared_registry: Some("registry.invalid/maestro".to_string()),
            join_secret: Some("integration-auto-formation-secret".to_string()),
            selected_node: Some("node1".to_string()),
            ..crate::config::ClusterConfig::default()
        };
        let identity = super::provision::ensure_seed_identity(
            &config,
            NodeRole::Master,
            &seed_data,
            Ipv4Addr::LOCALHOST,
        )?;
        cluster.cluster_id = identity.cluster_id;
        let seed_name = cluster.nodes[0].member_name();
        let seed_peer = cluster.peer_url(0);
        cluster.start_member(0, &seed_name, &format!("{seed_name}={seed_peer}"), "new")?;
        cluster.endpoints.push(cluster.client_url(0));
        Ok(cluster)
    }

    async fn add_and_promote_learner(
        &mut self,
        index: usize,
    ) -> Result<super::bootstrap::JoinInfo> {
        let peer_url = self.peer_url(index);
        let member_name = self.nodes[index].member_name();
        let (member_id, members) = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(mut client) =
                    etcd_client::Client::connect(vec![self.client_url(0)], None).await
                {
                    if let Ok(response) = client.member_list().await
                        && let Some(member) = response
                            .members()
                            .iter()
                            .find(|member| member.peer_urls().iter().any(|url| url == &peer_url))
                    {
                        return (member.id(), response.members().to_vec());
                    }
                    if let Ok(response) = client
                        .member_add(
                            [peer_url.clone()],
                            Some(MemberAddOptions::new().with_is_learner()),
                        )
                        .await
                        && let Some(member) = response.member()
                    {
                        return (member.id(), response.member_list().to_vec());
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("seed did not admit etcd learner `{member_name}`"))?;
        let initial_cluster =
            super::bootstrap::format_initial_cluster(&members, member_id, &member_name, true)?;
        let join_info = super::bootstrap::JoinInfo {
            cluster_id: self.cluster_id.clone(),
            member_id,
            member_name: member_name.clone(),
            peer_url,
            initial_cluster: initial_cluster.clone(),
        };

        self.start_member(index, &member_name, &initial_cluster, "existing")?;
        self.endpoints.push(self.client_url(index));
        self.wait_for_member_status(index).await?;

        let promoted = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(mut client) =
                    etcd_client::Client::connect(vec![self.client_url(0)], None).await
                    && let Ok(members) = client.member_list().await
                {
                    if members
                        .members()
                        .iter()
                        .find(|member| member.id() == member_id)
                        .is_some_and(|member| !member.is_learner())
                    {
                        return;
                    }
                    let _ = client.member_promote(member_id).await;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await;
        if promoted.is_err() {
            bail!("etcd learner `{member_name}` did not catch up and promote");
        }
        Ok(join_info)
    }

    fn start_member(
        &mut self,
        index: usize,
        member_name: &str,
        initial_cluster: &str,
        initial_state: &str,
    ) -> Result<()> {
        let container_name = self.container_name(index);
        let client_port = self.nodes[index].etcd_client_port;
        let peer_port = self.nodes[index].etcd_peer_port;
        let arguments = vec![
            "run".to_string(),
            "--detach".to_string(),
            "--network".to_string(),
            "host".to_string(),
            "--name".to_string(),
            container_name.clone(),
            crate::deployment::ETCD_IMAGE_TAG.to_string(),
            "etcd".to_string(),
            format!("--name={member_name}"),
            "--data-dir=/etcd-data".to_string(),
            format!("--listen-client-urls=http://0.0.0.0:{client_port}"),
            format!("--advertise-client-urls={}", self.client_url(index)),
            format!("--listen-peer-urls=http://0.0.0.0:{peer_port}"),
            format!("--initial-advertise-peer-urls={}", self.peer_url(index)),
            format!("--initial-cluster={initial_cluster}"),
            format!("--initial-cluster-state={initial_state}"),
            format!("--initial-cluster-token={}", self.token),
        ];
        self.container_names.insert(container_name.clone());
        let references = arguments.iter().map(String::as_str).collect::<Vec<_>>();
        command_output(&self.runtime_cli, &references)
            .with_context(|| format!("failed to start etcd member `{member_name}`"))?;
        Ok(())
    }

    async fn wait_for_seed(&self) -> Result<()> {
        self.wait_for_member_status(0).await
    }

    async fn wait_for_member_status(&self, index: usize) -> Result<()> {
        let endpoint = self.client_url(index);
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(mut client) =
                    etcd_client::Client::connect(vec![endpoint.clone()], None).await
                    && client
                        .status()
                        .await
                        .is_ok_and(|status| status.leader() != 0)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("etcd member at `{endpoint}` did not become ready"))
    }

    async fn assert_formed(&self) -> Result<()> {
        let expected_names = self
            .nodes
            .iter()
            .map(|node| node.member_name())
            .collect::<BTreeSet<_>>();
        for endpoint in &self.endpoints {
            let mut client = etcd_client::Client::connect(vec![endpoint.clone()], None).await?;
            let status = client.status().await?;
            if status.leader() == 0 || !status.errors().is_empty() {
                bail!("formed member `{endpoint}` has no healthy leader");
            }
            let members = client.member_list().await?;
            let names = members
                .members()
                .iter()
                .map(|member| member.name().to_string())
                .collect::<BTreeSet<_>>();
            if names != expected_names || members.members().iter().any(|member| member.is_learner())
            {
                bail!("formed membership does not contain three promoted voters: {names:?}");
            }
        }
        let mut client = etcd_client::Client::connect(self.endpoints.clone(), None).await?;
        client
            .put("/maestro/integration/formed", "three-voters", None)
            .await?;
        Ok(())
    }

    fn runtime(&self, index: usize) -> super::ClusterRuntime {
        let node = self.nodes[index];
        super::ClusterRuntime {
            cluster_id: self.cluster_id.clone(),
            node_id: format!("node-{}", index + 1),
            instance_id: format!("instance-{}", index + 1),
            host_ip: node.host_ip,
            role: NodeRole::Voter,
            initial_voters: vec![self.nodes[0]],
            voter_endpoints: self.nodes.clone(),
            subnet: format!("172.30.{}.0/24", index + 1),
            control_allow_cidrs: Vec::new(),
            api_port: node.api_port,
            gateway_port: node.gateway_port,
            etcd_client_port: node.etcd_client_port,
            etcd_peer_port: node.etcd_peer_port,
            shared_registry: None,
            labels: BTreeMap::new(),
            identity_api_port: node.identity_api_port,
        }
    }

    fn client_url(&self, index: usize) -> String {
        format!("http://127.0.0.1:{}", self.nodes[index].etcd_client_port)
    }

    fn peer_url(&self, index: usize) -> String {
        format!("http://127.0.0.1:{}", self.nodes[index].etcd_peer_port)
    }

    fn container_name(&self, index: usize) -> String {
        format!("maestro-forming-etcd-{}-{}", self.run_id, index + 1)
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

    fn start_rollout_backend(
        &mut self,
        index: usize,
        version: &str,
        start_ready: bool,
    ) -> Result<String> {
        let script_path = self.root.join("rollout-server.py");
        if !script_path.exists() {
            std::fs::write(&script_path, ROLLOUT_SERVER)?;
        }
        let name = self.rollout_backend_name(index, version);
        let network = self.network_name(index);
        let mount = format!("{}:/rollout-server.py:ro", script_path.display());
        let body = format!("BODY={version}-node-{}", index + 1);
        let ready = format!("START_READY={}", u8::from(start_ready));
        self.run_container(
            &name,
            &[
                "run",
                "--detach",
                "--network",
                &network,
                "--name",
                &name,
                "--volume",
                &mount,
                "--env",
                &body,
                "--env",
                &ready,
                ROLLOUT_TEST_IMAGE,
                "python",
                "/rollout-server.py",
            ],
        )?;
        Ok(name)
    }

    fn mark_rollout_backend_ready(&self, index: usize, version: &str) -> Result<()> {
        command_output(
            &self.runtime_cli,
            &[
                "exec",
                &self.rollout_backend_name(index, version),
                "sh",
                "-c",
                "kill -USR1 1",
            ],
        )?;
        Ok(())
    }

    fn write_rollout_gateway_configs(&self, version: &str) -> Result<()> {
        for index in 0..self.nodes.len() {
            self.write_gateway_config_for_backends(
                index,
                &[self.rollout_backend_name(index, version)],
            )?;
        }
        Ok(())
    }

    fn rollout_backend_has_status(&self, index: usize, version: &str, expected: u16) -> bool {
        const STATUS_CHECK: &str = r#"import sys, urllib.error, urllib.request
try:
    code = urllib.request.urlopen("http://127.0.0.1:8080/ready").status
except urllib.error.HTTPError as error:
    code = error.code
sys.exit(0 if code == int(sys.argv[1]) else 1)
"#;
        command_output(
            &self.runtime_cli,
            &[
                "exec",
                &self.rollout_backend_name(index, version),
                "python",
                "-c",
                STATUS_CHECK,
                &expected.to_string(),
            ],
        )
        .is_ok()
    }

    fn rollout_marker_exists(&self, index: usize, version: &str, marker: &str) -> bool {
        command_output(
            &self.runtime_cli,
            &[
                "exec",
                &self.rollout_backend_name(index, version),
                "test",
                "-f",
                marker,
            ],
        )
        .is_ok()
    }

    fn stop_rollout_backend(&self, index: usize, version: &str) -> Result<()> {
        command_output(
            &self.runtime_cli,
            &[
                "stop",
                "--time",
                "10",
                &self.rollout_backend_name(index, version),
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
        let mount = format!("{}:/config:ro", self.root.display());
        let provider = format!(
            "--providers.file.filename=/config/{}",
            config_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("gateway config path has no file name"))?
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
                "--publish",
                &publish,
                "--volume",
                &mount,
                "traefik:v3.6",
                "--log.level=ERROR",
                "--api.dashboard=false",
                "--entrypoints.web.address=:8080",
                &provider,
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
        write_atomic(&config_path, config.as_bytes())?;
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
        write_atomic(
            &config_path,
            format!(
                "http:\n  routers:\n    public:\n      entryPoints: [web]\n      rule: PathPrefix(`/`)\n      service: cluster\n{affinity_routers}\n  services:\n    cluster:\n      loadBalancer:\n        sticky:\n          cookie:\n            name: maestro-node-affinity\n            httpOnly: true\n        healthCheck:\n          path: /\n          interval: 500ms\n          timeout: 300ms\n        servers:\n{servers}\n{affinity_services}\n"
            )
            .as_bytes(),
        )?;
        let name = self.public_name();
        let mount = format!("{}:/config:ro", self.root.display());
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
                "--providers.file.filename=/config/public.yml",
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

    fn rollout_backend_name(&self, index: usize, version: &str) -> String {
        format!(
            "maestro-http-rollout-{}-{version}-node-{}",
            self.run_id, self.nodes[index].api_port
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
        if !self.container_names.is_empty() {
            let mut arguments = vec!["rm", "--force"];
            arguments.extend(self.container_names.iter().map(String::as_str));
            let _ = Command::new(&self.runtime_cli).args(arguments).output();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Drop for FormingEtcdCluster {
    fn drop(&mut self) {
        if !self.container_names.is_empty() {
            let mut arguments = vec!["rm", "--force"];
            arguments.extend(self.container_names.iter().map(String::as_str));
            let _ = Command::new(&self.runtime_cli).args(arguments).output();
        }
        let _ = std::fs::remove_dir_all(&self.root);
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

fn write_atomic(path: &std::path::Path, contents: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, contents)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
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

/// Verifies the exact access-log flags used by production against the pinned Traefik image.
/// Included in `cargo test-multi-node` because it requires a container daemon.
#[test]
#[ignore = "requires an isolated Linux container daemon"]
fn production_traefik_access_log_config_starts() -> Result<()> {
    let runtime_cli = test_runtime_cli()?;
    ensure_image(&runtime_cli, crate::deployment::INGRESS_IMAGE_TAG)?;
    let name = format!(
        "maestro-traefik-config-test-{}",
        crate::utils::nanoid::unique_id(10).to_ascii_lowercase()
    );
    let port = reserve_port_excluding(Ipv4Addr::LOCALHOST, &BTreeSet::new())?;
    let publish = format!("127.0.0.1:{port}:8888");
    let mut arguments = vec![
        "run".to_string(),
        "--detach".to_string(),
        "--name".to_string(),
        name.clone(),
        "--publish".to_string(),
        publish,
        crate::deployment::INGRESS_IMAGE_TAG.to_string(),
        "--entrypoints.web.address=:8888".to_string(),
    ];
    arguments.extend(crate::deployment::ingress_access_log_args());
    let references = arguments.iter().map(String::as_str).collect::<Vec<_>>();
    let started = command_output(&runtime_cli, &references);
    let result = started.and_then(|_| {
        std::thread::sleep(Duration::from_secs(1));
        let running = command_output(
            &runtime_cli,
            &["inspect", "--format", "{{.State.Running}}", &name],
        )?;
        if running.trim() != "true" {
            bail!("pinned Traefik exited after parsing production access-log flags");
        }
        let socket = format!("127.0.0.1:{port}").parse()?;
        std::net::TcpStream::connect_timeout(&socket, Duration::from_secs(2))
            .context("the host could not reach Traefik on its published ingress port")?;
        Ok(())
    });
    let _ = command_output(&runtime_cli, &["rm", "--force", &name]);
    result
}

/// Exercises the single-node endpoint from a separate container, where host
/// loopback would point at the caller rather than the etcd container.
#[test]
#[ignore = "requires an isolated Linux container daemon"]
fn single_node_container_etcd_endpoint_is_reachable() -> Result<()> {
    let runtime_cli = test_runtime_cli()?;
    ensure_image(&runtime_cli, crate::deployment::ETCD_IMAGE_TAG)?;
    let suffix = crate::utils::nanoid::unique_id(10).to_ascii_lowercase();
    let network = format!("maestro-etcd-endpoint-test-{suffix}");
    let etcd = format!("maestro-etcd-endpoint-{suffix}");
    command_output(&runtime_cli, &["network", "create", &network])?;
    let started = command_output(
        &runtime_cli,
        &[
            "run",
            "--detach",
            "--network",
            &network,
            "--hostname",
            "maestro-etcd",
            "--name",
            &etcd,
            crate::deployment::ETCD_IMAGE_TAG,
            "etcd",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://maestro-etcd:2379",
        ],
    );
    let result = started.and_then(|_| {
        let endpoint = crate::deployment::container_etcd_endpoints(
            None,
            &["http://127.0.0.1:39999".to_string()],
            true,
        )
        .remove(0);
        for _ in 0..30 {
            let health = command_output(
                &runtime_cli,
                &[
                    "run",
                    "--rm",
                    "--network",
                    &network,
                    crate::deployment::ETCD_IMAGE_TAG,
                    "etcdctl",
                    "--endpoints",
                    &endpoint,
                    "endpoint",
                    "health",
                ],
            );
            if health.is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        bail!("container-local etcd endpoint `{endpoint}` never became healthy")
    });
    let _ = command_output(&runtime_cli, &["rm", "--force", &etcd]);
    let _ = command_output(&runtime_cli, &["network", "rm", &network]);
    result
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

async fn wait_for_rollout_status(
    cluster: &SingleHostHttpCluster,
    index: usize,
    version: &str,
    expected: u16,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            if cluster.rollout_backend_has_status(index, version, expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "rollout backend `{version}` on logical node {} did not return status {expected}",
            index + 1
        )
    })
}

async fn wait_for_rollout_marker(
    cluster: &SingleHostHttpCluster,
    index: usize,
    version: &str,
    marker: &str,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            if cluster.rollout_marker_exists(index, version, marker) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("rollout backend `{version}` did not create marker `{marker}`"))
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

/// Simulates a serial rolling restart across three logical nodes. Each step stops one etcd
/// member, workload, and node gateway; verifies quorum writes and public traffic through the
/// remaining nodes; then waits for every restarted component to rejoin before continuing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn serial_node_restarts_preserve_quorum_and_ingress() -> Result<()> {
    let etcd = ContainerEtcdCluster::start()?;
    etcd.wait_until_ready().await?;
    let http = SingleHostHttpCluster::start()?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let all = ["node-1", "node-2", "node-3"]
        .into_iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();

    wait_for_routing_set(
        &client,
        &http.public_url(),
        &all,
        &all,
        Duration::from_secs(30),
    )
    .await?;

    for index in 0..http.nodes.len() {
        let restarting = format!("node-{}", index + 1);
        etcd.stop_member(index)?;
        http.stop_gateway(index)?;
        http.stop_backend(index)?;

        etcd.wait_for_quorum_write(index, index).await?;
        wait_until_unavailable(&client, &http.gateway_url(index), Duration::from_secs(10)).await?;
        let remaining = all
            .iter()
            .filter(|node| *node != &restarting)
            .cloned()
            .collect::<BTreeSet<_>>();
        wait_for_routing_set(
            &client,
            &http.public_url(),
            &remaining,
            &remaining,
            Duration::from_secs(30),
        )
        .await?;

        etcd.restart_member(index)?;
        http.restart_backend(index)?;
        http.restart_gateway(index)?;

        etcd.wait_until_ready().await?;
        wait_for_body(
            &client,
            &http.gateway_url(index),
            &restarting,
            Duration::from_secs(20),
        )
        .await?;
        wait_for_routing_set(
            &client,
            &http.public_url(),
            &all,
            &all,
            Duration::from_secs(30),
        )
        .await?;
    }
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

/// Exercises a two-version rollout through real node gateways. The new deployment remains outside
/// the routing configuration until every replica reports ready, public requests keep succeeding
/// during the atomic cutover, and SIGTERM waits for an in-flight request on the old deployment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn readiness_gated_rollout_preserves_in_flight_requests() -> Result<()> {
    let mut cluster = SingleHostHttpCluster::start()?;
    ensure_image(&cluster.runtime_cli, ROLLOUT_TEST_IMAGE)?;
    cluster.remove_initial_backends()?;
    for index in 0..cluster.nodes.len() {
        cluster.start_rollout_backend(index, "v1", true)?;
    }
    cluster.write_rollout_gateway_configs("v1")?;

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let v1 = (1..=cluster.nodes.len())
        .map(|index| format!("v1-node-{index}"))
        .collect::<BTreeSet<_>>();
    let v2 = (1..=cluster.nodes.len())
        .map(|index| format!("v2-node-{index}"))
        .collect::<BTreeSet<_>>();
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &v1,
        &v1,
        Duration::from_secs(30),
    )
    .await?;

    for index in 0..cluster.nodes.len() {
        cluster.start_rollout_backend(index, "v2", index + 1 < cluster.nodes.len())?;
    }
    for index in 0..cluster.nodes.len() - 1 {
        wait_for_rollout_status(
            &cluster,
            index,
            "v2",
            reqwest::StatusCode::OK.as_u16(),
            Duration::from_secs(20),
        )
        .await?;
    }
    let delayed_index = cluster.nodes.len() - 1;
    wait_for_rollout_status(
        &cluster,
        delayed_index,
        "v2",
        reqwest::StatusCode::SERVICE_UNAVAILABLE.as_u16(),
        Duration::from_secs(20),
    )
    .await?;
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &v1,
        &v1,
        Duration::from_secs(10),
    )
    .await?;

    cluster.mark_rollout_backend_ready(delayed_index, "v2")?;
    wait_for_rollout_status(
        &cluster,
        delayed_index,
        "v2",
        reqwest::StatusCode::OK.as_u16(),
        Duration::from_secs(10),
    )
    .await?;

    let traffic_client = client.clone();
    let public_url = cluster.public_url();
    let allowed_during_cutover = v1.union(&v2).cloned().collect::<BTreeSet<_>>();
    let (traffic_started, traffic_is_running) = tokio::sync::oneshot::channel();
    let traffic = tokio::spawn(async move {
        let mut observed = BTreeSet::new();
        let mut traffic_started = Some(traffic_started);
        for _ in 0..160 {
            let response = traffic_client.get(&public_url).send().await?;
            if !response.status().is_success() {
                bail!(
                    "public request failed during rollout with {}",
                    response.status()
                );
            }
            let body = response.text().await?.trim().to_string();
            if !allowed_during_cutover.contains(&body) {
                bail!("public request reached unexpected rollout backend `{body}`");
            }
            observed.insert(body);
            if let Some(started) = traffic_started.take() {
                let _ = started.send(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Result::<BTreeSet<String>>::Ok(observed)
    });
    traffic_is_running
        .await
        .map_err(|_| anyhow!("continuous rollout traffic stopped before cutover"))?;

    let slow_client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()?;
    let slow_url = format!("{}/slow", cluster.gateway_url(0));
    let slow_request = tokio::spawn(async move {
        let response = slow_client.get(slow_url).send().await?;
        if !response.status().is_success() {
            bail!("in-flight request returned {}", response.status());
        }
        Result::<String>::Ok(response.text().await?.trim().to_string())
    });
    wait_for_rollout_marker(
        &cluster,
        0,
        "v1",
        "/tmp/slow-started",
        Duration::from_secs(10),
    )
    .await?;

    cluster.write_rollout_gateway_configs("v2")?;
    let runtime_cli = cluster.runtime_cli.clone();
    let old_backend = cluster.rollout_backend_name(0, "v1");
    let old_stop = tokio::task::spawn_blocking(move || {
        command_output(&runtime_cli, &["stop", "--time", "10", &old_backend])?;
        Result::<()>::Ok(())
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !old_stop.is_finished(),
        "the old replica exited before its in-flight request completed"
    );

    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &v2,
        &v2,
        Duration::from_secs(30),
    )
    .await?;
    assert_eq!(slow_request.await??, "v1-node-1");
    old_stop.await??;
    for index in 1..cluster.nodes.len() {
        cluster.stop_rollout_backend(index, "v1")?;
    }
    let observed = traffic.await??;
    assert!(observed.iter().any(|body| v1.contains(body)));
    assert!(observed.iter().any(|body| v2.contains(body)));
    wait_for_routing_set(
        &client,
        &cluster.public_url(),
        &v2,
        &v2,
        Duration::from_secs(20),
    )
    .await?;
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

/// Forms a three-voter cluster from one designated seed and two serial learners. It exercises the
/// production bootstrap state decisions and initial-cluster formatter, proves that a later-listed
/// voter can join while the middle voter is absent, and verifies every learner catches up before
/// promotion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn designated_seed_and_learners_form_one_cluster() -> Result<()> {
    let mut cluster = FormingEtcdCluster::start_seed()?;
    let seed_runtime = cluster.runtime(0);
    let seed_data = cluster.root.join("node-1");
    assert_eq!(
        super::bootstrap::decide(Some(&seed_runtime), &seed_data)?,
        super::bootstrap::BootstrapAction::BootstrapSeed
    );
    super::bootstrap::mark_seed_starting(&seed_data)?;
    assert_eq!(
        super::bootstrap::decide(Some(&seed_runtime), &seed_data)?,
        super::bootstrap::BootstrapAction::WaitForAdmission,
        "a consumed seed permit must wait for authenticated recovery consensus"
    );
    super::bootstrap::mark_seed_joined(&seed_data)?;
    std::fs::create_dir_all(seed_data.join("system/etcd/data/member"))?;
    assert_eq!(
        super::bootstrap::decide(Some(&seed_runtime), &seed_data)?,
        super::bootstrap::BootstrapAction::Restart
    );
    cluster.wait_for_seed().await?;

    for index in [2, 1] {
        let runtime = cluster.runtime(index);
        let data_dir = cluster.root.join(format!("node-{}", index + 1));
        assert_eq!(
            super::bootstrap::decide(Some(&runtime), &data_dir)?,
            super::bootstrap::BootstrapAction::WaitForAdmission
        );
        let join_info = cluster.add_and_promote_learner(index).await?;
        super::bootstrap::persist_join_info(&data_dir, &join_info)?;
        assert_eq!(
            super::bootstrap::decide(Some(&runtime), &data_dir)?,
            super::bootstrap::BootstrapAction::WaitForAdmission
        );
    }

    cluster.assert_formed().await?;
    Ok(())
}

/// Drives the production maintenance state machine against real etcd fencing and two real
/// electors. Simulated node APIs replace their process instance on restart, allowing the test to
/// verify worker/follower/leader order, leadership transfer, health verification, restoration,
/// and removal of the cluster-wide deployment freeze.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn coordinated_restart_drains_verifies_and_restores_nodes_serially() -> Result<()> {
    let etcd = ContainerEtcdCluster::start()?;
    etcd.wait_until_ready().await?;
    let endpoints = reserve_node_endpoints(3, Ipv4Addr::LOCALHOST)?;
    let registry = Arc::new(InMemoryNodeRegistry::new("test-orchestrator".to_string()));
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let roles = [NodeRole::Worker, NodeRole::Voter, NodeRole::Voter];
    let node_ids = ["node-a", "node-b", "node-c"];
    let mut nodes = Vec::new();
    let mut api_handles = Vec::new();
    for (index, endpoint) in endpoints.iter().enumerate() {
        let node = NodeInfo {
            node_id: node_ids[index].to_string(),
            instance_id: format!("instance-{}", node_ids[index]),
            hostname: node_ids[index].to_string(),
            role: roles[index],
            cluster_host_ip: Ipv4Addr::LOCALHOST,
            cluster_api_port: endpoint.api_port,
            cluster_gateway_port: endpoint.gateway_port,
            subnet: format!("172.31.{}.0/24", index + 1),
            tailscale_ip: None,
            data_plane_ready: true,
            data_plane_checked_at_ms: 1,
            data_plane_error: None,
            version: "1.0.0".to_string(),
            started_at_ms: 1,
            labels: BTreeMap::new(),
        };
        registry.insert_for_test(node.clone());
        nodes.push(node.clone());
        let listener =
            tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, endpoint.api_port)).await?;
        let app = axum::Router::new()
            .route(
                "/api/system/restart",
                axum::routing::post(restart_test_node),
            )
            .route("/_healthy", axum::routing::get(restart_test_healthy))
            .with_state(RestartNodeApiState {
                node,
                registry: registry.clone(),
                requests: requests.clone(),
            });
        api_handles.push(tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        }));
    }

    let leader = Arc::new(
        EtcdLeaderElector::connect(&etcd.endpoints, None, "node-c".to_string(), true).await?,
    );
    let follower = Arc::new(
        EtcdLeaderElector::connect(&etcd.endpoints, None, "node-b".to_string(), true).await?,
    );
    let (shutdown_leader, _) = broadcast::channel(2);
    let (shutdown_follower, _) = broadcast::channel(2);
    let mut election_handles = leader
        .clone()
        .spawn(shutdown_leader.subscribe(), Logger::noop());
    let initial_token = leader.wait_until_leading(Duration::from_secs(20)).await?;
    election_handles.extend(
        follower
            .clone()
            .spawn(shutdown_follower.subscribe(), Logger::noop()),
    );

    let store = Arc::new(
        EtcdStateStore::new_with_endpoints(
            &etcd.endpoints,
            crate::utils::crypto::derive_key("coordinated-restart-integration"),
            None,
        )
        .await?,
    );
    let assignments = Arc::new(InMemoryAssignmentStore::default());
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let leader_orchestrator = super::upgrade::ClusterUpgradeOrchestrator::new_for_test(
        "node-c".to_string(),
        leader.clone(),
        registry.clone(),
        assignments.clone(),
        store.clone(),
        http.clone(),
    );
    let follower_orchestrator = super::upgrade::ClusterUpgradeOrchestrator::new_for_test(
        "node-b".to_string(),
        follower.clone(),
        registry.clone(),
        assignments,
        store.clone(),
        http,
    );
    let created = leader_orchestrator
        .create_restart_run(&initial_token, None)
        .await?;
    assert_eq!(created.kind, super::ClusterMaintenanceKind::Restart);
    assert_eq!(
        created
            .nodes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["node-a", "node-b", "node-c"]
    );

    let completed = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let run = store
                .read_cluster_upgrade()
                .await?
                .ok_or_else(|| anyhow!("coordinated restart run disappeared"))?;
            if run.phase.is_terminal() {
                return Result::<super::UpgradeRun>::Ok(run);
            }
            match (leader.state(), follower.state()) {
                (LeadershipState::Leading(token), _) => {
                    leader_orchestrator.tick(&token).await?;
                }
                (_, LeadershipState::Leading(token)) => {
                    follower_orchestrator.tick(&token).await?;
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow!("coordinated restart did not finish"))??;
    assert_eq!(completed.phase, super::UpgradePhase::Succeeded);
    assert_eq!(
        *requests.lock().unwrap_or_else(|error| error.into_inner()),
        vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string()
        ]
    );
    for node in nodes {
        let restarted = registry
            .list_nodes()
            .await?
            .into_iter()
            .find(|candidate| candidate.node_id == node.node_id)
            .ok_or_else(|| anyhow!("restarted node `{}` disappeared", node.node_id))?;
        assert_ne!(restarted.instance_id, node.instance_id);
        assert_eq!(
            registry.get_node_state(&node.node_id).await?,
            NodeState::default()
        );
    }
    assert!(store.read_cluster_freeze().await?.is_none());

    let (active_orchestrator, active_token) = match (leader.state(), follower.state()) {
        (LeadershipState::Leading(token), _) => (&leader_orchestrator, token),
        (_, LeadershipState::Leading(token)) => (&follower_orchestrator, token),
        _ => bail!("cluster had no leader after the all-node restart"),
    };
    let selected = active_orchestrator
        .create_restart_run(&active_token, Some("node-a"))
        .await?;
    assert_eq!(selected.nodes.len(), 1);
    assert_eq!(selected.nodes[0].node_id, "node-a");
    let selected_completed = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let run = store
                .read_cluster_upgrade()
                .await?
                .ok_or_else(|| anyhow!("selected-node restart run disappeared"))?;
            if run.phase.is_terminal() {
                return Result::<super::UpgradeRun>::Ok(run);
            }
            match (leader.state(), follower.state()) {
                (LeadershipState::Leading(token), _) => {
                    leader_orchestrator.tick(&token).await?;
                }
                (_, LeadershipState::Leading(token)) => {
                    follower_orchestrator.tick(&token).await?;
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow!("selected-node restart did not finish"))??;
    assert_eq!(selected_completed.phase, super::UpgradePhase::Succeeded);
    assert_eq!(
        *requests.lock().unwrap_or_else(|error| error.into_inner()),
        vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
            "node-a".to_string()
        ]
    );
    assert!(store.read_cluster_freeze().await?.is_none());

    let _ = shutdown_leader.send(ShutdownEvent::Force);
    let _ = shutdown_follower.send(ShutdownEvent::Force);
    for handle in election_handles {
        handle.abort();
    }
    for handle in api_handles {
        handle.abort();
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
    let assignment_store = EtcdAssignmentStore::connect(&cluster.endpoints, None).await?;
    let first_assignment = Assignment {
        assignment_id: "assignment-before-failover".to_string(),
        placement_epoch: 1,
        service_id: "failover-service".to_string(),
        deployment_id: "deployment-v1".to_string(),
        replica_index: 0,
        node_id: "workload-node".to_string(),
        container_ip: None,
        replaces_assignment_id: None,
        created_at_ms: 1,
    };
    assert_eq!(
        assignment_store
            .replace_for_node(
                &stale_token,
                0,
                AssignmentManifest {
                    node_id: "workload-node".to_string(),
                    generation: 0,
                    assignments: vec![first_assignment.clone()],
                },
            )
            .await?,
        ReplaceOutcome::Applied
    );

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
    let persisted = assignment_store
        .get_for_node(&"workload-node".to_string())
        .await?
        .ok_or_else(|| anyhow!("successor could not load the existing assignment manifest"))?;
    assert_eq!(persisted.generation, 1);
    assert_eq!(persisted.assignments, vec![first_assignment.clone()]);
    let successor_assignment = Assignment {
        assignment_id: "assignment-after-failover".to_string(),
        placement_epoch: 2,
        deployment_id: "deployment-v2".to_string(),
        replaces_assignment_id: Some(first_assignment.assignment_id.clone()),
        created_at_ms: 2,
        ..first_assignment
    };
    let successor_manifest = AssignmentManifest {
        node_id: "workload-node".to_string(),
        generation: persisted.generation,
        assignments: vec![successor_assignment.clone()],
    };
    assert_eq!(
        assignment_store
            .replace_for_node(
                &stale_token,
                persisted.generation,
                successor_manifest.clone(),
            )
            .await?,
        ReplaceOutcome::LeadershipLost
    );
    assert_eq!(
        assignment_store
            .replace_for_node(&live_token, persisted.generation, successor_manifest)
            .await?,
        ReplaceOutcome::Applied
    );
    let resumed = assignment_store
        .get_for_node(&"workload-node".to_string())
        .await?
        .ok_or_else(|| anyhow!("successor assignment manifest disappeared"))?;
    assert_eq!(resumed.generation, 2);
    assert_eq!(resumed.assignments, vec![successor_assignment]);
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

/// Deletes two of three voter data directories, rebuilds membership from the only surviving
/// member with etcd's force-new-cluster recovery, and then adds both empty voters back as
/// learners. The committed key must survive the quorum reconstruction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn surviving_etcd_member_recovers_empty_voters_without_a_backup() -> Result<()> {
    let cluster = ContainerEtcdCluster::start()?;
    cluster.wait_until_ready().await?;
    let mut client = etcd_client::Client::connect(vec![cluster.endpoints[0].clone()], None).await?;
    client
        .put(
            "/maestro/integration/recovery-preserved",
            "before-data-loss",
            None,
        )
        .await?;

    cluster.wipe_member(1)?;
    cluster.wipe_member(2)?;
    cluster.restart_survivor_with_force_new_cluster(0)?;

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(mut candidate) =
                etcd_client::Client::connect(vec![cluster.endpoints[0].clone()], None).await
                && candidate
                    .status()
                    .await
                    .is_ok_and(|status| status.leader() != 0 && status.errors().is_empty())
                && candidate
                    .member_list()
                    .await
                    .is_ok_and(|members| members.members().len() == 1)
                && candidate
                    .put("/maestro/integration/recovery-ready", "yes", None)
                    .await
                    .is_ok()
            {
                client = candidate;
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("surviving etcd member did not establish a one-member quorum"))?;
    let preserved = client
        .get("/maestro/integration/recovery-preserved", None)
        .await?;
    assert_eq!(
        preserved.kvs().first().map(|entry| entry.value()),
        Some(b"before-data-loss".as_slice())
    );

    // A daemon crash after etcd recovered but before the durable recovery marker is cleared may
    // replay the force-new-cluster start. The replay must remain a writable one-member cluster.
    cluster.restart_survivor_with_force_new_cluster(0)?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(mut candidate) =
                etcd_client::Client::connect(vec![cluster.endpoints[0].clone()], None).await
                && candidate
                    .put("/maestro/integration/recovery-replay", "safe", None)
                    .await
                    .is_ok()
            {
                client = candidate;
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("force-new-cluster recovery was not crash-replay safe"))?;

    for index in [1, 2] {
        let peer_url = format!("http://127.0.0.1:{}", cluster.nodes[index].etcd_peer_port);
        let response = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match client
                    .member_add(
                        [peer_url.clone()],
                        Some(MemberAddOptions::new().with_is_learner()),
                    )
                    .await
                {
                    Ok(response) => return response,
                    Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                }
            }
        })
        .await
        .map_err(|_| anyhow!("surviving etcd member did not admit replacement learner"))?;
        let member = response
            .member()
            .ok_or_else(|| anyhow!("etcd omitted replacement learner"))?;
        let member_id = member.id();
        let member_name = format!("member{}", index + 1);
        let initial_cluster = super::bootstrap::format_initial_cluster(
            response.member_list(),
            member_id,
            &member_name,
            true,
        )?;
        cluster.start_replacement(index, &initial_cluster)?;

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(mut replacement) =
                    etcd_client::Client::connect(vec![cluster.endpoints[index].clone()], None).await
                    && replacement.status().await.is_ok()
                    && client.member_promote(member_id).await.is_ok()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("replacement etcd learner {} did not promote", index + 1))?;
    }

    cluster.wait_until_ready().await?;
    let preserved = client
        .get("/maestro/integration/recovery-preserved", None)
        .await?;
    assert_eq!(
        preserved.kvs().first().map(|entry| entry.value()),
        Some(b"before-data-loss".as_slice())
    );
    Ok(())
}
