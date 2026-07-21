use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use clustertest::{
    AffinityCluster, AffinityCookieSet, AffinityObservation, AffinitySession,
    AssignmentManifestSnapshot, AssignmentWriteOutcome, BootstrapDecision, CandidateReadiness,
    ControlPlaneReadiness, CutoverCluster, CutoverObservation, DrainBehavior, ElectionCluster,
    FencedWriteOutcome, FixtureAffinityToken, FixtureControllerName, FixtureInstanceId,
    FixtureMarker, FixtureMutationName, FixtureNodeName, FixtureVersion, FormationCluster,
    FormationMemberRole, FormationSnapshot, IngressConfigurationState, IngressStartupCluster,
    IngressStartupObservation, JoinObservation, LeadershipAgreement, LeadershipSnapshot,
    MaintenanceAttempt, MaintenanceCompletion, MaintenanceFreeze, MaintenanceNodeRole,
    MaintenanceNodeSnapshot, MaintenanceTopology, MembershipAgreement, NodePorts, PeerStoreCluster,
    PeerStoreObservation, QuorumRecoveryCluster, ReadinessProbe, RegistrationCleanup,
    RegistrationObservation, ReplicaCount, ReplicaIndex, ReservationState, ResourceAvailability,
    RestartCluster, RoutingCluster, ScheduledAssignment, SchedulingCluster, SchedulingEligibility,
    SchedulingSnapshot, SecurityRestartState, SeedControlRole, SeedSecurityCluster,
    SeedSecurityObservation, SelectedRestartObservation, TargetRetention, UpgradeCluster,
    UpgradeFault, UpgradeObservation, scenarios,
};
use etcd_client::MemberAddOptions;
use tokio::sync::broadcast;

use super::assignment_store::{
    AssignmentStore, EtcdAssignmentStore, InMemoryAssignmentStore, ReplaceOutcome,
};
use super::elector::{EtcdLeaderElector, LeaderElector};
use super::executor::RunningReplica;
use super::reconciler::{ReconcileAction, diff_assignments};
use super::registry::{EtcdNodeRegistry, InMemoryNodeRegistry, NodeRegistry};
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
struct MaintenanceNodeApiState {
    node: NodeInfo,
    registry: Arc<InMemoryNodeRegistry>,
    restart_requests: Arc<std::sync::Mutex<Vec<String>>>,
    upgrade_observations: Arc<std::sync::Mutex<Vec<NodeUpgradeObservation>>>,
    upgrade_attempts: Arc<std::sync::Mutex<BTreeMap<String, usize>>>,
    fail_first_upgrade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeUpgradeObservation {
    node_id: String,
    drained_nodes: Vec<String>,
}

#[derive(serde::Deserialize)]
struct TestUpgradeRequest {
    version: String,
}

async fn restart_test_node(
    axum::extract::State(state): axum::extract::State<MaintenanceNodeApiState>,
) -> axum::Json<serde_json::Value> {
    state
        .restart_requests
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

async fn upgrade_test_node(
    axum::extract::State(state): axum::extract::State<MaintenanceNodeApiState>,
    axum::Json(request): axum::Json<TestUpgradeRequest>,
) -> std::result::Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
    let nodes = state
        .registry
        .list_nodes()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut drained_nodes = Vec::new();
    for node in nodes {
        let node_state = state
            .registry
            .get_node_state(&node.node_id)
            .await
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
        if node_state.unschedulable {
            drained_nodes.push(node.node_id);
        }
    }
    drained_nodes.sort();
    state
        .upgrade_observations
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .push(NodeUpgradeObservation {
            node_id: state.node.node_id.clone(),
            drained_nodes,
        });
    let attempt = {
        let mut attempts = state
            .upgrade_attempts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let attempt = attempts.entry(state.node.node_id.clone()).or_default();
        *attempt += 1;
        *attempt
    };
    if state.fail_first_upgrade && attempt == 1 {
        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    let mut upgraded = state
        .registry
        .list_nodes()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .find(|node| node.node_id == state.node.node_id)
        .unwrap_or(state.node);
    upgraded.instance_id = format!("{}-upgraded-{attempt}", upgraded.instance_id);
    upgraded.started_at_ms = upgraded.started_at_ms.saturating_add(1);
    upgraded.version = request.version;
    state.registry.insert_for_test(upgraded);
    Ok(axum::Json(serde_json::json!({ "accepted": true })))
}

async fn restart_test_healthy() -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}

struct ContainerEtcdCluster {
    runtime_cli: String,
    container_names: Vec<String>,
    endpoints: Vec<String>,
    root: PathBuf,
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
            root,
        };

        for (index, node) in nodes.iter().enumerate() {
            let member = format!("member{}", index + 1);
            let container = format!("maestro-etcd-test-{run_id}-{}", index + 1);
            let client_port = node.etcd_client_port;
            let peer_port = node.etcd_peer_port;
            let data_dir = cluster.member_data_dir(index);
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

    fn node_data_dir(&self, index: usize) -> PathBuf {
        self.root.join(format!("node-{}", index + 1))
    }

    fn member_data_dir(&self, index: usize) -> PathBuf {
        self.node_data_dir(index).join("system/etcd/data")
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

    fn stop_all_members(&self) -> Result<()> {
        for index in 0..self.container_names.len() {
            self.stop_member(index)?;
        }
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
            super::bootstrap::format_initial_cluster(&members, member_id, &member_name)?;
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

    fn runtime(&self, index: usize) -> super::ClusterRuntime {
        let node = self.nodes[index];
        super::ClusterRuntime {
            cluster_id: self.cluster_id.clone(),
            node_id: format!("node{:08}", index + 1),
            instance_id: format!("instance-{}", index + 1),
            host_ip: node.host_ip,
            role: if index == 0 {
                NodeRole::Master
            } else {
                NodeRole::Voter
            },
            initial_voters: vec![self.nodes[0]],
            voter_endpoints: self.nodes.clone(),
            subnet: format!("172.30.{}.0/24", index + 1),
            control_allow_cidrs: Vec::new(),
            api_port: node.api_port,
            gateway_port: node.gateway_port,
            etcd_client_port: node.etcd_client_port,
            etcd_peer_port: node.etcd_peer_port,
            labels: BTreeMap::new(),
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

struct OldSystemFormationCluster {
    cluster: FormingEtcdCluster,
    registry: Option<EtcdNodeRegistry>,
}

impl OldSystemFormationCluster {
    fn start() -> Result<Self> {
        Ok(Self {
            cluster: FormingEtcdCluster::start_seed()?,
            registry: None,
        })
    }

    fn fixture_node(&self, index: usize) -> FixtureNodeName {
        FixtureNodeName::new(self.cluster.runtime(index).node_id)
    }

    fn node_index(&self, node: &FixtureNodeName) -> Result<usize> {
        (0..self.cluster.nodes.len())
            .find(|index| self.cluster.runtime(*index).node_id == node.as_str())
            .ok_or_else(|| anyhow!("formation node `{}` does not exist", node.as_str()))
    }

    fn fixture_for_member(&self, member_name: &str) -> Result<FixtureNodeName> {
        self.cluster
            .nodes
            .iter()
            .position(|node| node.member_name() == member_name)
            .map(|index| self.fixture_node(index))
            .ok_or_else(|| anyhow!("formed etcd member `{member_name}` is not configured"))
    }

    fn bootstrap_decision(action: super::bootstrap::BootstrapAction) -> Result<BootstrapDecision> {
        match action {
            super::bootstrap::BootstrapAction::BootstrapSeed => {
                Ok(BootstrapDecision::BootstrapSeed)
            }
            super::bootstrap::BootstrapAction::BootstrapSeedResume => {
                Ok(BootstrapDecision::ResumeSeed)
            }
            super::bootstrap::BootstrapAction::Restart => Ok(BootstrapDecision::Restart),
            super::bootstrap::BootstrapAction::JoinExisting(_) => {
                Ok(BootstrapDecision::JoinExisting)
            }
            unexpected => bail!("unexpected formation bootstrap action {unexpected:?}"),
        }
    }

    fn seed_data_dir(&self) -> PathBuf {
        self.cluster.root.join("node-1")
    }

    fn advertised_ports(&self) -> NodePorts {
        let runtime = self.cluster.runtime(0);
        NodePorts {
            api: runtime.api_port,
            gateway: runtime.gateway_port,
            consensus_client: runtime.etcd_client_port,
            consensus_peer: runtime.etcd_peer_port,
        }
    }

    fn reserved_port(reservation: &serde_json::Value, field: &'static str) -> Result<u16> {
        let value = reservation[field]
            .as_u64()
            .ok_or_else(|| anyhow!("control reservation has no numeric `{field}`"))?;
        u16::try_from(value).map_err(|_| anyhow!("control reservation `{field}` is out of range"))
    }
}

#[async_trait]
impl FormationCluster for OldSystemFormationCluster {
    type Error = anyhow::Error;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        (0..self.cluster.nodes.len())
            .map(|index| self.fixture_node(index))
            .collect()
    }

    async fn seed_decision(&mut self) -> Result<BootstrapDecision> {
        let runtime = self.cluster.runtime(0);
        Self::bootstrap_decision(super::bootstrap::decide(
            Some(&runtime),
            &self.seed_data_dir(),
        )?)
    }

    async fn mark_seed_starting(&mut self) -> Result<()> {
        super::bootstrap::mark_seed_starting(&self.seed_data_dir())
    }

    async fn mark_seed_joined(&mut self) -> Result<()> {
        super::bootstrap::mark_seed_joined(&self.seed_data_dir())
    }

    async fn await_seed(&mut self) -> Result<()> {
        self.cluster.wait_for_seed().await
    }

    async fn join_and_promote(&mut self, node: &FixtureNodeName) -> Result<JoinObservation> {
        let index = self.node_index(node)?;
        let runtime = self.cluster.runtime(index);
        let data_dir = self.cluster.root.join(format!("node-{}", index + 1));
        let decision_before_join =
            Self::bootstrap_decision(super::bootstrap::decide(Some(&runtime), &data_dir)?)?;
        let mut join_info = self.cluster.add_and_promote_learner(index).await?;
        let initial_members = join_info
            .initial_cluster
            .split(',')
            .map(|member| {
                member
                    .split_once('=')
                    .map(|(name, _)| name)
                    .ok_or_else(|| anyhow!("invalid initial cluster member `{member}`"))
                    .and_then(|name| self.fixture_for_member(name))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        // The lightweight fixture uses HTTP, while persisted production intent is bound to mTLS.
        join_info.peer_url = runtime.peer_url();
        super::bootstrap::persist_join_info(&data_dir, &join_info)?;
        let decision_after_join =
            Self::bootstrap_decision(super::bootstrap::decide(Some(&runtime), &data_dir)?)?;
        Ok(JoinObservation {
            node: node.clone(),
            decision_before_join,
            decision_after_join,
            initial_members,
            final_role: FormationMemberRole::Voter,
        })
    }

    async fn formation_snapshot(&mut self) -> Result<FormationSnapshot> {
        let mut observed_members = None;
        let mut observed_leader = None;
        let mut membership = MembershipAgreement::Consistent;
        let mut leadership = LeadershipAgreement::Consistent;
        for endpoint in &self.cluster.endpoints {
            let mut client = etcd_client::Client::connect([endpoint.clone()], None).await?;
            let status = client.status().await?;
            if status.leader() == 0 || !status.errors().is_empty() {
                leadership = LeadershipAgreement::Divergent;
            }
            if observed_leader
                .replace(status.leader())
                .is_some_and(|leader| leader != status.leader())
            {
                leadership = LeadershipAgreement::Divergent;
            }
            let members = client
                .member_list()
                .await?
                .members()
                .iter()
                .map(|member| {
                    let role = if member.is_learner() {
                        FormationMemberRole::Learner
                    } else {
                        FormationMemberRole::Voter
                    };
                    self.fixture_for_member(member.name())
                        .map(|node| (node, role))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            if observed_members
                .as_ref()
                .is_some_and(|observed| observed != &members)
            {
                membership = MembershipAgreement::Divergent;
            }
            observed_members = Some(members);
        }
        let mut client = etcd_client::Client::connect(self.cluster.endpoints.clone(), None).await?;
        let writes = match client
            .put("/maestro/integration/formed", "three-voters", None)
            .await
        {
            Ok(_) => ResourceAvailability::Available,
            Err(_) => ResourceAvailability::Unavailable,
        };
        Ok(FormationSnapshot {
            members: observed_members.unwrap_or_default(),
            membership,
            leadership,
            writes,
        })
    }

    async fn register_seed(&mut self) -> Result<RegistrationObservation> {
        let seed_runtime = self.cluster.runtime(0);
        let endpoint = self.cluster.client_url(0);
        let mut client = etcd_client::Client::connect([endpoint.clone()], None).await?;
        super::bootstrap::seed_bootstrap_records(&mut client, &seed_runtime).await?;
        let registry =
            EtcdNodeRegistry::connect(&[endpoint], None, seed_runtime.node_id.clone()).await?;
        let info = NodeInfo {
            node_id: seed_runtime.node_id.clone(),
            instance_id: seed_runtime.instance_id.clone(),
            hostname: "seed-node".to_string(),
            role: seed_runtime.role,
            cluster_host_ip: seed_runtime.host_ip,
            cluster_api_port: seed_runtime.api_port,
            cluster_gateway_port: seed_runtime.gateway_port,
            subnet: seed_runtime.subnet.clone(),
            tailscale_ip: None,
            data_plane_ready: false,
            data_plane_checked_at_ms: 0,
            data_plane_error: None,
            version: "integration-test".to_string(),
            started_at_ms: 1,
            labels: BTreeMap::new(),
        };
        registry.register(info).await?;
        registry.publish_image_holder("api:deployment").await?;
        let registered_nodes = registry
            .list_nodes()
            .await?
            .into_iter()
            .map(|node| FixtureNodeName::new(node.node_id))
            .collect();
        let image_holders = registry
            .list_image_holders("api:deployment")
            .await?
            .into_iter()
            .map(|holder| FixtureNodeName::new(holder.node_id))
            .collect();
        let control_key =
            super::identity::control_reservation_key(seed_runtime.host_ip, seed_runtime.api_port);
        let reservation = client
            .get(control_key, None)
            .await?
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("registered control reservation disappeared"))?
            .value()
            .to_vec();
        let reservation: serde_json::Value = serde_json::from_slice(&reservation)?;
        let reservation_node = FixtureNodeName::new(
            reservation["nodeId"]
                .as_str()
                .ok_or_else(|| anyhow!("control reservation has no node id"))?,
        );
        let reservation_state = if reservation["state"] == "active" {
            ReservationState::Active
        } else {
            ReservationState::Inactive
        };
        let reserved_ports = NodePorts {
            api: Self::reserved_port(&reservation, "apiPort")?,
            gateway: Self::reserved_port(&reservation, "gatewayPort")?,
            consensus_client: Self::reserved_port(&reservation, "etcdClientPort")?,
            consensus_peer: Self::reserved_port(&reservation, "etcdPeerPort")?,
        };
        self.registry = Some(registry);
        Ok(RegistrationObservation {
            registered_nodes,
            image_holders,
            reservation_node,
            reservation_state,
            advertised_ports: self.advertised_ports(),
            reserved_ports,
        })
    }

    async fn deregister_seed(&mut self) -> Result<RegistrationCleanup> {
        let registry = self
            .registry
            .as_ref()
            .ok_or_else(|| anyhow!("formation seed is not registered"))?;
        registry.deregister().await?;
        let registered_nodes = registry
            .list_nodes()
            .await?
            .into_iter()
            .map(|node| FixtureNodeName::new(node.node_id))
            .collect();
        let image_holders = registry
            .list_image_holders("api:deployment")
            .await?
            .into_iter()
            .map(|holder| FixtureNodeName::new(holder.node_id))
            .collect();
        Ok(RegistrationCleanup {
            registered_nodes,
            image_holders,
        })
    }
}

struct OldSystemSeedSecurityCluster {
    cluster: FormingEtcdCluster,
}

impl OldSystemSeedSecurityCluster {
    fn start() -> Result<Self> {
        Ok(Self {
            cluster: FormingEtcdCluster::start_seed()?,
        })
    }
}

#[async_trait]
impl SeedSecurityCluster for OldSystemSeedSecurityCluster {
    type Error = anyhow::Error;

    async fn bootstrap_seed_security(&mut self) -> Result<SeedSecurityObservation> {
        let runtime = self.cluster.runtime(0);
        if runtime.role != NodeRole::Master {
            bail!("designated security seed was not configured as a master");
        }

        let mut unreachable_peers = 0;
        for peer in &self.cluster.nodes[1..] {
            if tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, peer.etcd_peer_port))
                .await
                .is_err()
            {
                unreachable_peers += 1;
            }
        }

        self.cluster.wait_for_seed().await?;
        let endpoint = self.cluster.client_url(0);
        let mut first = etcd_client::Client::connect([endpoint.clone()], None).await?;
        // Production authenticates the no-password root user with its mTLS identity. This
        // plain-HTTP fixture needs a password so it can reconnect after auth is enabled.
        let root_password = "maestro-integration-root";
        first.role_add("root").await?;
        first.user_add("root", root_password, None).await?;
        first.user_grant_role("root", "root").await?;
        super::auth::bootstrap_initial_with_client(&runtime, &mut first).await?;

        let options = etcd_client::ConnectOptions::new().with_user("root", root_password);
        let mut restarted = etcd_client::Client::connect([endpoint], Some(options)).await?;
        super::auth::bootstrap_initial_with_client(&runtime, &mut restarted).await?;
        let gateway_root = restarted
            .get(super::auth::gateway_root_key(&runtime.node_id), None)
            .await?;
        let gateway_root = gateway_root
            .kvs()
            .first()
            .filter(|entry| entry.value().is_empty())
            .map_or(ResourceAvailability::Unavailable, |_| {
                ResourceAvailability::Available
            });
        let store = if restarted.status().await?.leader() == 0 {
            ResourceAvailability::Unavailable
        } else {
            ResourceAvailability::Available
        };

        Ok(SeedSecurityObservation {
            seed_role: SeedControlRole::VotingControlPlane,
            configured_voters: self.cluster.nodes.len(),
            unreachable_peers,
            store,
            security_restart: SecurityRestartState::Preserved,
            gateway_root,
        })
    }
}

/// Starts the configured master as a one-member cluster with both peers offline, enables RBAC,
/// and repeats RBAC initialization through an authenticated client to simulate a daemon restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated Linux container daemon"]
async fn master_bootstrap_and_rbac_restart_do_not_require_reachable_peers() -> Result<()> {
    let mut cluster = OldSystemSeedSecurityCluster::start()?;
    scenarios::isolated_seed_security_restart_is_idempotent(&mut cluster).await?;
    Ok(())
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
                images: Vec::new(),
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

struct OldSystemRoutingCluster {
    cluster: SingleHostHttpCluster,
    client: reqwest::Client,
}

impl OldSystemRoutingCluster {
    fn start() -> Result<Self> {
        let cluster = SingleHostHttpCluster::start()?;
        if !cluster
            .nodes
            .iter()
            .all(|node| node.host_ip == cluster.host_ip)
        {
            bail!("logical nodes do not share the single-host test address");
        }
        if cluster
            .nodes
            .iter()
            .map(|node| node.api_port)
            .collect::<BTreeSet<_>>()
            .len()
            != 3
        {
            bail!("logical nodes do not have three unique API ports");
        }
        for node in &cluster.nodes {
            if node.gateway_port != node.api_port + 1
                || node.etcd_client_port != node.api_port + 2
                || node.etcd_peer_port != node.api_port + 3
            {
                bail!("logical node ports do not use the persisted offset layout");
            }
        }
        Ok(Self {
            cluster,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()?,
        })
    }

    fn node_index(&self, node: &FixtureNodeName) -> Result<usize> {
        self.nodes()
            .iter()
            .position(|candidate| candidate == node)
            .ok_or_else(|| anyhow!("logical node `{}` does not exist", node.as_str()))
    }
}

#[async_trait]
impl RoutingCluster for OldSystemRoutingCluster {
    type Error = anyhow::Error;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        (1..=self.cluster.nodes.len())
            .map(|node_number| FixtureNodeName::new(format!("node-{node_number}")))
            .collect()
    }

    async fn set_workload_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<()> {
        let node_index = self.node_index(node)?;
        match availability {
            ResourceAvailability::Available => {
                self.cluster.restart_backend(node_index)?;
                wait_for_body(
                    &self.client,
                    &self.cluster.gateway_url(node_index),
                    node.as_str(),
                    Duration::from_secs(20),
                )
                .await?;
            }
            ResourceAvailability::Unavailable => {
                self.cluster.stop_backend(node_index)?;
                wait_until_unavailable(
                    &self.client,
                    &self.cluster.gateway_url(node_index),
                    Duration::from_secs(10),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn set_gateway_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<()> {
        let node_index = self.node_index(node)?;
        match availability {
            ResourceAvailability::Available => {
                self.cluster.restart_gateway(node_index)?;
                wait_for_body(
                    &self.client,
                    &self.cluster.gateway_url(node_index),
                    node.as_str(),
                    Duration::from_secs(20),
                )
                .await?;
            }
            ResourceAvailability::Unavailable => {
                self.cluster.stop_gateway(node_index)?;
                wait_until_unavailable(
                    &self.client,
                    &self.cluster.gateway_url(node_index),
                    Duration::from_secs(10),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn await_public_routes(
        &mut self,
        expected: &BTreeSet<FixtureNodeName>,
    ) -> Result<BTreeSet<FixtureNodeName>> {
        let expected_bodies = expected
            .iter()
            .map(|node| node.as_str().to_string())
            .collect::<BTreeSet<_>>();
        wait_for_routing_set(
            &self.client,
            &self.cluster.public_url(),
            &expected_bodies,
            &expected_bodies,
            Duration::from_secs(30),
        )
        .await?;
        Ok(expected.clone())
    }

    async fn await_public_unavailable(&mut self) -> Result<()> {
        wait_until_unavailable(
            &self.client,
            &self.cluster.public_url(),
            Duration::from_secs(15),
        )
        .await
    }
}

struct OldSystemRestartCluster {
    etcd: ContainerEtcdCluster,
    routing: OldSystemRoutingCluster,
    quorum_sequence: usize,
}

impl OldSystemRestartCluster {
    async fn start() -> Result<Self> {
        let etcd = ContainerEtcdCluster::start()?;
        etcd.wait_until_ready().await?;
        Ok(Self {
            etcd,
            routing: OldSystemRoutingCluster::start()?,
            quorum_sequence: 0,
        })
    }
}

#[async_trait]
impl RoutingCluster for OldSystemRestartCluster {
    type Error = anyhow::Error;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.routing.nodes()
    }

    async fn set_workload_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<()> {
        self.routing
            .set_workload_availability(node, availability)
            .await
    }

    async fn set_gateway_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<()> {
        self.routing
            .set_gateway_availability(node, availability)
            .await
    }

    async fn await_public_routes(
        &mut self,
        expected: &BTreeSet<FixtureNodeName>,
    ) -> Result<BTreeSet<FixtureNodeName>> {
        self.routing.await_public_routes(expected).await
    }

    async fn await_public_unavailable(&mut self) -> Result<()> {
        self.routing.await_public_unavailable().await
    }
}

#[async_trait]
impl RestartCluster for OldSystemRestartCluster {
    async fn set_node_availability(
        &mut self,
        node: &FixtureNodeName,
        availability: ResourceAvailability,
    ) -> Result<()> {
        let node_index = self.routing.node_index(node)?;
        match availability {
            ResourceAvailability::Available => {
                self.etcd.restart_member(node_index)?;
                self.routing.cluster.restart_backend(node_index)?;
                self.routing.cluster.restart_gateway(node_index)?;
                wait_for_body(
                    &self.routing.client,
                    &self.routing.cluster.gateway_url(node_index),
                    node.as_str(),
                    Duration::from_secs(20),
                )
                .await?;
            }
            ResourceAvailability::Unavailable => {
                self.etcd.stop_member(node_index)?;
                self.routing.cluster.stop_gateway(node_index)?;
                self.routing.cluster.stop_backend(node_index)?;
                wait_until_unavailable(
                    &self.routing.client,
                    &self.routing.cluster.gateway_url(node_index),
                    Duration::from_secs(10),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn verify_quorum_write(&mut self, unavailable_node: &FixtureNodeName) -> Result<()> {
        let node_index = self.routing.node_index(unavailable_node)?;
        self.etcd
            .wait_for_quorum_write(node_index, self.quorum_sequence)
            .await?;
        self.quorum_sequence = self.quorum_sequence.saturating_add(1);
        Ok(())
    }

    async fn await_control_plane_ready(&mut self) -> Result<()> {
        self.etcd.wait_until_ready().await
    }
}

struct OldSystemSchedulingCluster {
    cluster: SingleHostHttpCluster,
    client: reqwest::Client,
    running: BTreeMap<String, RunningReplica>,
    current: Vec<Assignment>,
    now_ms: i64,
}

impl OldSystemSchedulingCluster {
    fn start() -> Result<Self> {
        let cluster = SingleHostHttpCluster::start()?;
        cluster.remove_initial_backends()?;
        Ok(Self {
            cluster,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()?,
            running: BTreeMap::new(),
            current: Vec::new(),
            now_ms: 1_000,
        })
    }
}

#[async_trait]
impl SchedulingCluster for OldSystemSchedulingCluster {
    type AssignmentId = String;
    type Error = anyhow::Error;

    async fn scale(
        &mut self,
        replicas: ReplicaCount,
    ) -> Result<SchedulingSnapshot<Self::AssignmentId>> {
        let plan = scaling_plan(&self.cluster, replicas.get(), &self.current, self.now_ms);
        self.now_ms = self.now_ms.saturating_add(1_000);
        let desired_ids = plan
            .assignments
            .iter()
            .map(|assignment| assignment.assignment_id.as_str())
            .collect::<BTreeSet<_>>();
        let removed_workloads = self
            .running
            .values()
            .filter(|replica| !desired_ids.contains(replica.assignment.assignment_id.as_str()))
            .map(|replica| replica.container_hostname.clone())
            .collect::<Vec<_>>();
        self.cluster
            .reconcile_scaled_assignments(&mut self.running, &plan.assignments)?;
        wait_for_assignment_routes(&self.cluster, &self.client, &plan.assignments).await?;
        let orphaned_workloads = removed_workloads
            .iter()
            .filter(|workload| self.cluster.container_exists(workload))
            .count();
        let mut assignments = plan
            .assignments
            .iter()
            .map(|assignment| ScheduledAssignment {
                id: assignment.assignment_id.clone(),
                replica_index: ReplicaIndex::new(assignment.replica_index),
                node: FixtureNodeName::new(assignment.node_id.clone()),
            })
            .collect::<Vec<_>>();
        assignments.sort_by_key(|assignment| assignment.replica_index.get());
        let unschedulable_replicas = plan
            .unschedulable
            .iter()
            .map(|replica| ReplicaIndex::new(replica.replica_index))
            .collect();
        self.current = plan.assignments;
        Ok(SchedulingSnapshot {
            assignments,
            unschedulable_replicas,
            orphaned_workloads,
        })
    }
}

struct OldSystemAffinityCluster {
    cluster: SingleHostHttpCluster,
    client: reqwest::Client,
}

impl OldSystemAffinityCluster {
    fn start() -> Result<Self> {
        Ok(Self {
            cluster: SingleHostHttpCluster::start()?,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()?,
        })
    }

    async fn parse_response(
        &self,
        response: reqwest::Response,
    ) -> Result<(AffinityObservation, Vec<String>)> {
        let token = response
            .headers()
            .get(TEST_AFFINITY_HEADER)
            .ok_or_else(|| anyhow!("gateway did not return `{TEST_AFFINITY_HEADER}`"))?
            .to_str()?
            .to_string();
        let cookies = response
            .headers()
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok()?.split(';').next())
            .map(str::to_string)
            .collect::<Vec<_>>();
        let node = FixtureNodeName::new(response.text().await?.trim().to_string());
        let expected = super::traefik::affinity_token(TEST_CLUSTER_NAME, node.as_str());
        if token != expected {
            bail!(
                "gateway returned affinity token `{token}` for `{}`, expected `{expected}`",
                node.as_str()
            );
        }
        let has_node_cookie = cookies
            .iter()
            .any(|cookie| cookie.starts_with("maestro-node-affinity="));
        let has_workload_cookie = cookies
            .iter()
            .any(|cookie| cookie.starts_with("maestro-affinity="));
        let cookie_set = if has_node_cookie && has_workload_cookie {
            AffinityCookieSet::Complete
        } else {
            AffinityCookieSet::Incomplete
        };
        Ok((
            AffinityObservation {
                node,
                token: FixtureAffinityToken::new(token),
                cookies: cookie_set,
            },
            cookies,
        ))
    }
}

#[async_trait]
impl AffinityCluster for OldSystemAffinityCluster {
    type Session = String;
    type Error = anyhow::Error;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        (1..=self.cluster.nodes.len())
            .map(|node_number| FixtureNodeName::new(format!("node-{node_number}")))
            .collect()
    }

    async fn establish_affinity(&mut self) -> Result<AffinitySession<Self::Session>> {
        for (node_index, node) in self.nodes().iter().enumerate() {
            wait_for_body(
                &self.client,
                &self.cluster.gateway_url(node_index),
                node.as_str(),
                Duration::from_secs(20),
            )
            .await?;
        }
        let response = wait_for_affinity_response(
            &self.client,
            &self.cluster.public_url(),
            Duration::from_secs(20),
        )
        .await?;
        let (initial, cookies) = self.parse_response(response).await?;
        Ok(AffinitySession {
            session: cookies.join("; "),
            initial,
        })
    }

    async fn replay_affinity(&mut self, session: &Self::Session) -> Result<AffinityObservation> {
        let response = self
            .client
            .get(self.cluster.public_url())
            .header(reqwest::header::COOKIE, session)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!(
                "sticky-cookie replay returned {status} with body {body:?}; public={}",
                self.cluster
                    .container_diagnostics(&self.cluster.public_name())
            );
        }
        self.parse_response(response)
            .await
            .map(|(observation, _)| observation)
    }

    async fn override_affinity(
        &mut self,
        session: &Self::Session,
        node: &FixtureNodeName,
    ) -> Result<AffinityObservation> {
        let target_token = super::traefik::affinity_token(TEST_CLUSTER_NAME, node.as_str());
        let response = self
            .client
            .get(self.cluster.public_url())
            .header(reqwest::header::COOKIE, session)
            .header(TEST_AFFINITY_HEADER, target_token)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("affinity override returned {}", response.status());
        }
        self.parse_response(response)
            .await
            .map(|(observation, _)| observation)
    }
}

struct OldSystemCutoverCluster {
    cluster: SingleHostHttpCluster,
    client: reqwest::Client,
    routed_version: Option<FixtureVersion>,
    delayed_candidate: Option<(FixtureVersion, usize)>,
}

impl OldSystemCutoverCluster {
    fn start() -> Result<Self> {
        Ok(Self {
            cluster: SingleHostHttpCluster::start()?,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()?,
            routed_version: None,
            delayed_candidate: None,
        })
    }

    fn version_bodies(&self, version: &FixtureVersion) -> BTreeSet<String> {
        (1..=self.cluster.nodes.len())
            .map(|node_number| format!("{}-node-{node_number}", version.as_str()))
            .collect()
    }

    fn observed_version(body: &str) -> Result<FixtureVersion> {
        body.split_once("-node-")
            .map(|(version, _)| FixtureVersion::new(version.to_string()))
            .ok_or_else(|| anyhow!("unexpected rollout response `{body}`"))
    }
}

#[async_trait]
impl CutoverCluster for OldSystemCutoverCluster {
    type Error = anyhow::Error;

    async fn deploy_initial(&mut self, version: FixtureVersion) -> Result<()> {
        ensure_image(&self.cluster.runtime_cli, ROLLOUT_TEST_IMAGE)?;
        self.cluster.remove_initial_backends()?;
        for node_index in 0..self.cluster.nodes.len() {
            self.cluster
                .start_rollout_backend(node_index, version.as_str(), true)?;
        }
        self.cluster
            .write_rollout_gateway_configs(version.as_str())?;
        self.routed_version = Some(version);
        Ok(())
    }

    async fn deploy_candidate(
        &mut self,
        version: FixtureVersion,
        readiness: CandidateReadiness,
    ) -> Result<()> {
        let delayed_index = self.cluster.nodes.len().checked_sub(1);
        for node_index in 0..self.cluster.nodes.len() {
            let starts_ready = match readiness {
                CandidateReadiness::AllReady => true,
                CandidateReadiness::OneDelayed => Some(node_index) != delayed_index,
            };
            self.cluster
                .start_rollout_backend(node_index, version.as_str(), starts_ready)?;
            let expected_status = if starts_ready {
                reqwest::StatusCode::OK.as_u16()
            } else {
                reqwest::StatusCode::SERVICE_UNAVAILABLE.as_u16()
            };
            wait_for_rollout_status(
                &self.cluster,
                node_index,
                version.as_str(),
                expected_status,
                Duration::from_secs(20),
            )
            .await?;
        }
        self.delayed_candidate = match readiness {
            CandidateReadiness::AllReady => None,
            CandidateReadiness::OneDelayed => delayed_index.map(|index| (version, index)),
        };
        Ok(())
    }

    async fn await_routed_versions(&mut self) -> Result<BTreeSet<FixtureVersion>> {
        let version = self
            .routed_version
            .as_ref()
            .ok_or_else(|| anyhow!("rollout fixture has no routed version"))?;
        let expected = self.version_bodies(version);
        wait_for_routing_set(
            &self.client,
            &self.cluster.public_url(),
            &expected,
            &expected,
            Duration::from_secs(30),
        )
        .await?;
        Ok([version.clone()].into_iter().collect())
    }

    async fn make_candidate_ready(&mut self, version: &FixtureVersion) -> Result<()> {
        if let Some((delayed_version, node_index)) = &self.delayed_candidate {
            if delayed_version != version {
                bail!(
                    "delayed candidate is {:?}, not {:?}",
                    delayed_version,
                    version
                );
            }
            self.cluster
                .mark_rollout_backend_ready(*node_index, version.as_str())?;
            wait_for_rollout_status(
                &self.cluster,
                *node_index,
                version.as_str(),
                reqwest::StatusCode::OK.as_u16(),
                Duration::from_secs(10),
            )
            .await?;
        }
        Ok(())
    }

    async fn cutover_with_inflight_request(
        &mut self,
        previous: &FixtureVersion,
        candidate: &FixtureVersion,
    ) -> Result<CutoverObservation> {
        let previous_bodies = self.version_bodies(previous);
        let candidate_bodies = self.version_bodies(candidate);
        let allowed_during_cutover = previous_bodies
            .union(&candidate_bodies)
            .cloned()
            .collect::<BTreeSet<_>>();
        let traffic_client = self.client.clone();
        let public_url = self.cluster.public_url();
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
        let slow_url = format!("{}/slow", self.cluster.gateway_url(0));
        let slow_request = tokio::spawn(async move {
            let response = slow_client.get(slow_url).send().await?;
            if !response.status().is_success() {
                bail!("in-flight request returned {}", response.status());
            }
            Result::<String>::Ok(response.text().await?.trim().to_string())
        });
        wait_for_rollout_marker(
            &self.cluster,
            0,
            previous.as_str(),
            "/tmp/slow-started",
            Duration::from_secs(10),
        )
        .await?;

        self.cluster
            .write_rollout_gateway_configs(candidate.as_str())?;
        self.routed_version = Some(candidate.clone());
        let runtime_cli = self.cluster.runtime_cli.clone();
        let old_backend = self.cluster.rollout_backend_name(0, previous.as_str());
        let old_stop = tokio::task::spawn_blocking(move || {
            command_output(&runtime_cli, &["stop", "--time", "10", &old_backend])?;
            Result::<()>::Ok(())
        });
        tokio::time::sleep(Duration::from_millis(250)).await;
        let drain_behavior = if old_stop.is_finished() {
            DrainBehavior::ExitedEarly
        } else {
            DrainBehavior::WaitedForInflight
        };

        let final_expected = self.version_bodies(candidate);
        wait_for_routing_set(
            &self.client,
            &self.cluster.public_url(),
            &final_expected,
            &final_expected,
            Duration::from_secs(30),
        )
        .await?;
        let in_flight_body = slow_request.await??;
        old_stop.await??;
        for node_index in 1..self.cluster.nodes.len() {
            self.cluster
                .stop_rollout_backend(node_index, previous.as_str())?;
        }
        let traffic_versions = traffic
            .await??
            .into_iter()
            .map(|body| Self::observed_version(&body))
            .collect::<Result<BTreeSet<_>>>()?;
        wait_for_routing_set(
            &self.client,
            &self.cluster.public_url(),
            &final_expected,
            &final_expected,
            Duration::from_secs(20),
        )
        .await?;
        Ok(CutoverObservation {
            traffic_versions,
            public_failures: 0,
            in_flight_version: Self::observed_version(&in_flight_body)?,
            drain_behavior,
            final_routes: [candidate.clone()].into_iter().collect(),
        })
    }
}

struct OldSystemQuorumRecoveryCluster {
    cluster: ContainerEtcdCluster,
}

impl OldSystemQuorumRecoveryCluster {
    async fn start() -> Result<Self> {
        let cluster = ContainerEtcdCluster::start()?;
        cluster.wait_until_ready().await?;
        Ok(Self { cluster })
    }

    fn node_index(&self, node: &FixtureNodeName) -> Result<usize> {
        self.nodes()
            .iter()
            .position(|candidate| candidate == node)
            .ok_or_else(|| anyhow!("persisted voter `{}` does not exist", node.as_str()))
    }
}

#[async_trait]
impl QuorumRecoveryCluster for OldSystemQuorumRecoveryCluster {
    type Error = anyhow::Error;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        (1..=self.cluster.container_names.len())
            .map(|node_number| FixtureNodeName::new(format!("node-{node_number}")))
            .collect()
    }

    async fn write_marker(&mut self, marker: FixtureMarker) -> Result<()> {
        let mut client = etcd_client::Client::connect(self.cluster.endpoints.clone(), None).await?;
        client
            .put(
                "/maestro/integration/etcd-first-restart-preserved",
                marker.as_str(),
                None,
            )
            .await?;
        Ok(())
    }

    async fn stop_all_nodes(&mut self) -> Result<()> {
        self.cluster.stop_all_members()
    }

    async fn start_node(&mut self, node: &FixtureNodeName) -> Result<()> {
        self.cluster.restart_member(self.node_index(node)?)
    }

    async fn probe_readiness(&mut self, probe: ReadinessProbe) -> Result<ControlPlaneReadiness> {
        let endpoint = self
            .cluster
            .endpoints
            .first()
            .ok_or_else(|| anyhow!("quorum fixture has no client endpoint"))?;
        let timeout = match probe {
            ReadinessProbe::Brief => Duration::from_secs(2),
            ReadinessProbe::UntilReady => Duration::from_secs(30),
        };
        let readiness = tokio::time::timeout(
            timeout,
            crate::deployment::await_etcd_quorum(
                std::slice::from_ref(endpoint),
                None,
                &Logger::noop(),
            ),
        )
        .await;
        if readiness.is_ok() {
            Ok(ControlPlaneReadiness::Ready)
        } else {
            Ok(ControlPlaneReadiness::Unavailable)
        }
    }

    async fn read_marker(&mut self) -> Result<Option<FixtureMarker>> {
        let mut client = etcd_client::Client::connect(self.cluster.endpoints.clone(), None).await?;
        let response = client
            .get("/maestro/integration/etcd-first-restart-preserved", None)
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| {
                std::str::from_utf8(entry.value())
                    .map(|value| FixtureMarker::new(value.to_string()))
                    .map_err(anyhow::Error::from)
            })
            .transpose()
    }
}

struct OldSystemElectionCluster {
    cluster: ContainerEtcdCluster,
    controllers: Vec<FixtureControllerName>,
    electors: Vec<Arc<EtcdLeaderElector>>,
    shutdown: Vec<broadcast::Sender<ShutdownEvent>>,
    handles: Vec<Vec<tokio::task::JoinHandle<()>>>,
    active: BTreeSet<usize>,
    store: EtcdStateStore,
    assignments: EtcdAssignmentStore,
    mutation_sequence: i64,
}

impl OldSystemElectionCluster {
    async fn start() -> Result<Self> {
        let cluster = ContainerEtcdCluster::start()?;
        cluster.wait_until_ready().await?;
        let controllers = vec![
            FixtureControllerName::new("node-a"),
            FixtureControllerName::new("node-b"),
        ];
        let mut electors = Vec::new();
        let mut shutdown = Vec::new();
        let mut handles = Vec::new();
        for controller in &controllers {
            let elector = Arc::new(
                EtcdLeaderElector::connect(
                    &cluster.endpoints,
                    None,
                    controller.as_str().to_string(),
                    true,
                )
                .await?,
            );
            let (shutdown_sender, _) = broadcast::channel(2);
            handles.push(
                elector
                    .clone()
                    .spawn(shutdown_sender.subscribe(), Logger::noop()),
            );
            electors.push(elector);
            shutdown.push(shutdown_sender);
        }
        let store = EtcdStateStore::new_with_endpoints(
            &cluster.endpoints,
            crate::utils::crypto::derive_key("distributed-integration-test"),
            None,
        )
        .await?;
        let assignments = EtcdAssignmentStore::connect(&cluster.endpoints, None).await?;
        Ok(Self {
            cluster,
            controllers,
            electors,
            shutdown,
            handles,
            active: [0, 1].into_iter().collect(),
            store,
            assignments,
            mutation_sequence: 0,
        })
    }

    fn controller_index(&self, controller: &FixtureControllerName) -> Result<usize> {
        self.controllers
            .iter()
            .position(|candidate| candidate == controller)
            .ok_or_else(|| anyhow!("controller `{}` does not exist", controller.as_str()))
    }
}

#[async_trait]
impl ElectionCluster for OldSystemElectionCluster {
    type LeadershipToken = LeadershipToken;
    type Error = anyhow::Error;

    async fn await_leader(&mut self) -> Result<LeadershipSnapshot<Self::LeadershipToken>> {
        let active_electors = self
            .active
            .iter()
            .filter_map(|index| self.electors.get(*index).cloned())
            .collect::<Vec<_>>();
        let (_, token) = wait_for_leader(&active_electors, Duration::from_secs(20)).await?;
        Ok(LeadershipSnapshot {
            controller: FixtureControllerName::new(token.info.node_id.clone()),
            token,
        })
    }

    async fn fenced_write(
        &mut self,
        token: &Self::LeadershipToken,
        mutation: FixtureMutationName,
    ) -> Result<FencedWriteOutcome> {
        self.mutation_sequence = self.mutation_sequence.saturating_add(1);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            self.store.apply_cluster_mutation(
                token,
                ClusterMutation::ClaimRequest {
                    request_id: mutation.as_str().to_string(),
                    fingerprint: mutation.as_str().to_string(),
                    now_ms: self.mutation_sequence,
                },
            ),
        )
        .await;
        if matches!(result, Ok(Ok(_))) {
            Ok(FencedWriteOutcome::Applied)
        } else {
            Ok(FencedWriteOutcome::Rejected)
        }
    }

    async fn replace_assignment(
        &mut self,
        token: &Self::LeadershipToken,
        expected_generation: u64,
        version: FixtureVersion,
    ) -> Result<AssignmentWriteOutcome> {
        let assignment = Assignment {
            assignment_id: format!("assignment-{}", version.as_str()),
            placement_epoch: expected_generation.saturating_add(1),
            service_id: "failover-service".to_string(),
            deployment_id: version.as_str().to_string(),
            replica_index: 0,
            node_id: "workload-node".to_string(),
            container_ip: None,
            replaces_assignment_id: None,
            created_at_ms: self.mutation_sequence,
        };
        let outcome = self
            .assignments
            .replace_for_node(
                token,
                expected_generation,
                AssignmentManifest {
                    node_id: "workload-node".to_string(),
                    generation: expected_generation,
                    assignments: vec![assignment],
                    images: Vec::new(),
                },
            )
            .await?;
        Ok(match outcome {
            ReplaceOutcome::Applied => AssignmentWriteOutcome::Applied,
            ReplaceOutcome::GenerationConflict => AssignmentWriteOutcome::GenerationConflict,
            ReplaceOutcome::LeadershipLost => AssignmentWriteOutcome::LeadershipLost,
        })
    }

    async fn assignment(&mut self) -> Result<Option<AssignmentManifestSnapshot>> {
        let manifest = self
            .assignments
            .get_for_node(&"workload-node".to_string())
            .await?;
        manifest
            .map(|manifest| {
                let assignment = manifest
                    .assignments
                    .first()
                    .ok_or_else(|| anyhow!("assignment manifest has no assignments"))?;
                Ok(AssignmentManifestSnapshot {
                    generation: manifest.generation,
                    version: FixtureVersion::new(assignment.deployment_id.clone()),
                })
            })
            .transpose()
    }

    async fn stop_controller(&mut self, controller: &FixtureControllerName) -> Result<()> {
        let controller_index = self.controller_index(controller)?;
        if self.active.remove(&controller_index) {
            let shutdown = self
                .shutdown
                .get(controller_index)
                .ok_or_else(|| anyhow!("controller shutdown channel is missing"))?;
            let _ = shutdown.send(ShutdownEvent::Graceful);
            let handles = self
                .handles
                .get_mut(controller_index)
                .ok_or_else(|| anyhow!("controller task handles are missing"))?;
            for handle in handles.drain(..) {
                handle.await?;
            }
            Ok(())
        } else {
            bail!("controller `{}` is not active", controller.as_str())
        }
    }

    async fn lose_quorum(&mut self) -> Result<()> {
        for member_index in 0..2 {
            self.cluster.stop_member(member_index)?;
        }
        Ok(())
    }
}

impl Drop for OldSystemElectionCluster {
    fn drop(&mut self) {
        for shutdown in &self.shutdown {
            let _ = shutdown.send(ShutdownEvent::Force);
        }
        for handles in &mut self.handles {
            for handle in handles.drain(..) {
                handle.abort();
            }
        }
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
struct OldSystemIngressStartupCluster;

#[async_trait]
impl IngressStartupCluster for OldSystemIngressStartupCluster {
    type Error = anyhow::Error;

    async fn start_ingress(&mut self) -> Result<IngressStartupObservation> {
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
            Ok(IngressStartupObservation {
                configuration: IngressConfigurationState::Accepted,
                endpoint: ResourceAvailability::Available,
            })
        });
        let _ = command_output(&runtime_cli, &["rm", "--force", &name]);
        result
    }
}

#[tokio::test]
#[ignore = "requires an isolated Linux container daemon"]
async fn production_traefik_access_log_config_starts() -> Result<()> {
    scenarios::production_ingress_access_log_configuration_starts(
        &mut OldSystemIngressStartupCluster,
    )
    .await?;
    Ok(())
}

/// Exercises the single-node endpoint from a separate container, where host
/// loopback would point at the caller rather than the etcd container.
struct OldSystemPeerStoreCluster;

#[async_trait]
impl PeerStoreCluster for OldSystemPeerStoreCluster {
    type Error = anyhow::Error;

    async fn probe_store_from_peer(&mut self) -> Result<PeerStoreObservation> {
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
                    return Ok(PeerStoreObservation {
                        endpoint: ResourceAvailability::Available,
                    });
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            bail!("container-local etcd endpoint `{endpoint}` never became healthy")
        });
        let _ = command_output(&runtime_cli, &["rm", "--force", &etcd]);
        let _ = command_output(&runtime_cli, &["network", "rm", &network]);
        result
    }
}

#[tokio::test]
#[ignore = "requires an isolated Linux container daemon"]
async fn single_node_container_etcd_endpoint_is_reachable() -> Result<()> {
    scenarios::single_node_store_endpoint_is_peer_reachable(&mut OldSystemPeerStoreCluster).await?;
    Ok(())
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
            exhausted_slots: BTreeSet::new(),
        }],
        nodes,
        node_states: BTreeMap::<String, NodeState>::new(),
        current: current.to_vec(),
        held: BTreeSet::new(),
        now_ms,
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
    let mut cluster = OldSystemRoutingCluster::start()?;

    scenarios::routing_survives_workload_and_gateway_failures(&mut cluster).await?;
    Ok(())
}

/// Simulates a serial rolling restart across three logical nodes. Each step stops one etcd
/// member, workload, and node gateway; verifies quorum writes and public traffic through the
/// remaining nodes; then waits for every restarted component to rejoin before continuing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn serial_node_restarts_preserve_quorum_and_ingress() -> Result<()> {
    let mut cluster = OldSystemRestartCluster::start().await?;

    scenarios::serial_node_restarts_preserve_quorum_and_routing(&mut cluster).await?;
    Ok(())
}

/// Uses the production scheduler and reconciler diff against real HTTP containers and Traefik
/// gateways. It verifies stable placement and externally visible routes while scaling one service
/// from one replica to five and back to two across three logical nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn scheduler_scales_live_replicas_across_logical_nodes() -> Result<()> {
    let mut cluster = OldSystemSchedulingCluster::start()?;

    scenarios::scheduler_scales_replicas_across_nodes(&mut cluster).await?;
    Ok(())
}

/// Exercises a two-version rollout through real node gateways. The new deployment remains outside
/// the routing configuration until every replica reports ready, public requests keep succeeding
/// during the atomic cutover, and SIGTERM waits for an in-flight request on the old deployment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn readiness_gated_rollout_preserves_in_flight_requests() -> Result<()> {
    let mut cluster = OldSystemCutoverCluster::start()?;

    scenarios::readiness_gated_cutover_preserves_traffic_and_inflight_requests(&mut cluster)
        .await?;
    Ok(())
}

/// Verifies production-shaped affinity at both proxy layers. A first request receives opaque
/// affinity identity and both sticky cookies, cookie replay remains on the same replica, and an
/// explicitly replayed response header overrides the cookie to select another logical node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn node_affinity_is_automatic_opaque_and_replayable() -> Result<()> {
    let mut cluster = OldSystemAffinityCluster::start()?;

    scenarios::affinity_is_opaque_sticky_and_overridable(&mut cluster).await?;
    Ok(())
}

/// Forms a three-voter cluster from one designated seed and two serial learners. It exercises the
/// production bootstrap state decisions and initial-cluster formatter, proves that a later-listed
/// voter can join while the middle voter is absent, verifies every learner catches up before
/// promotion, and registers the seed through the reservation written by bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn designated_seed_and_learners_form_one_cluster() -> Result<()> {
    let mut cluster = OldSystemFormationCluster::start()?;

    scenarios::designated_seed_and_learners_form_registered_cluster(&mut cluster).await?;
    Ok(())
}

struct OldSystemUpgradeCluster {
    _etcd: ContainerEtcdCluster,
    registry: Arc<InMemoryNodeRegistry>,
    restart_requests: Arc<std::sync::Mutex<Vec<String>>>,
    upgrade_observations: Arc<std::sync::Mutex<Vec<NodeUpgradeObservation>>>,
    initial_nodes: Vec<NodeInfo>,
    leader: Arc<EtcdLeaderElector>,
    follower: Arc<EtcdLeaderElector>,
    shutdown_leader: broadcast::Sender<ShutdownEvent>,
    shutdown_follower: broadcast::Sender<ShutdownEvent>,
    election_handles: Vec<tokio::task::JoinHandle<()>>,
    api_handles: Vec<tokio::task::JoinHandle<()>>,
    store: Arc<EtcdStateStore>,
    leader_orchestrator: super::upgrade::ClusterUpgradeOrchestrator,
    follower_orchestrator: super::upgrade::ClusterUpgradeOrchestrator,
}

impl OldSystemUpgradeCluster {
    async fn start() -> Result<Self> {
        let etcd = ContainerEtcdCluster::start()?;
        etcd.wait_until_ready().await?;
        let endpoints = reserve_node_endpoints(4, Ipv4Addr::LOCALHOST)?;
        let registry = Arc::new(InMemoryNodeRegistry::new("test-orchestrator".to_string()));
        let restart_requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let upgrade_observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let upgrade_attempts = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
        let roles = [
            NodeRole::Worker,
            NodeRole::Voter,
            NodeRole::Voter,
            NodeRole::Voter,
        ];
        let node_ids = ["node-a", "node-b", "node-c", "node-d"];
        let mut initial_nodes = Vec::new();
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
            initial_nodes.push(node.clone());
            let listener =
                tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, endpoint.api_port)).await?;
            let app = axum::Router::new()
                .route(
                    "/api/system/upgrade",
                    axum::routing::post(upgrade_test_node),
                )
                .route(
                    "/api/system/restart",
                    axum::routing::post(restart_test_node),
                )
                .route("/_healthy", axum::routing::get(restart_test_healthy))
                .with_state(MaintenanceNodeApiState {
                    node,
                    registry: registry.clone(),
                    restart_requests: restart_requests.clone(),
                    upgrade_observations: upgrade_observations.clone(),
                    upgrade_attempts: upgrade_attempts.clone(),
                    fail_first_upgrade: index == 0,
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
        leader.wait_until_leading(Duration::from_secs(20)).await?;
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
        Ok(Self {
            _etcd: etcd,
            registry,
            restart_requests,
            upgrade_observations,
            initial_nodes,
            leader,
            follower,
            shutdown_leader,
            shutdown_follower,
            election_handles,
            api_handles,
            store,
            leader_orchestrator,
            follower_orchestrator,
        })
    }

    fn active_orchestrator(
        &self,
    ) -> Result<(&super::upgrade::ClusterUpgradeOrchestrator, LeadershipToken)> {
        match (self.leader.state(), self.follower.state()) {
            (LeadershipState::Leading(token), _) => Ok((&self.leader_orchestrator, token)),
            (_, LeadershipState::Leading(token)) => Ok((&self.follower_orchestrator, token)),
            _ => bail!("cluster has no maintenance leader"),
        }
    }

    async fn node_snapshots(&self) -> Result<BTreeMap<FixtureNodeName, MaintenanceNodeSnapshot>> {
        let mut snapshots = BTreeMap::new();
        for node in self.registry.list_nodes().await? {
            let role = if node.role == NodeRole::Worker {
                MaintenanceNodeRole::Worker
            } else if node.role.is_voter() {
                MaintenanceNodeRole::Voter
            } else {
                bail!("maintenance fixture has unsupported role {:?}", node.role);
            };
            let state = self.registry.get_node_state(&node.node_id).await?;
            snapshots.insert(
                FixtureNodeName::new(node.node_id),
                MaintenanceNodeSnapshot {
                    role,
                    version: FixtureVersion::new(node.version),
                    instance_id: FixtureInstanceId::new(node.instance_id),
                    scheduling: if state.unschedulable {
                        SchedulingEligibility::Ineligible
                    } else {
                        SchedulingEligibility::Eligible
                    },
                },
            );
        }
        Ok(snapshots)
    }

    fn completion(phase: super::UpgradePhase) -> MaintenanceCompletion {
        if phase == super::UpgradePhase::Succeeded {
            MaintenanceCompletion::Succeeded
        } else {
            MaintenanceCompletion::Failed
        }
    }

    async fn final_freeze(&self) -> Result<MaintenanceFreeze> {
        if self.store.read_cluster_freeze().await?.is_none() {
            Ok(MaintenanceFreeze::Cleared)
        } else {
            Ok(MaintenanceFreeze::Present)
        }
    }

    async fn run_upgrade(
        &mut self,
        target: FixtureVersion,
        batch: super::UpgradeBatch,
    ) -> Result<UpgradeObservation> {
        self.upgrade_observations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        let (orchestrator, token) = self.active_orchestrator()?;
        let created = orchestrator
            .create_run_with_batch(&token, target.as_str(), batch)
            .await?;
        let planned_nodes = created
            .nodes
            .iter()
            .map(|node| FixtureNodeName::new(node.node_id.clone()))
            .collect();
        let mut target_retention = TargetRetention::RetainedUntilCompletion;
        let completed = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let run = self
                    .store
                    .read_cluster_upgrade()
                    .await?
                    .ok_or_else(|| anyhow!("coordinated upgrade run disappeared"))?;
                if run.phase.is_terminal() {
                    return Result::<super::UpgradeRun>::Ok(run);
                }
                if self.store.read_cluster_freeze().await?.is_none() {
                    target_retention = TargetRetention::ClearedEarly;
                }
                match (self.leader.state(), self.follower.state()) {
                    (LeadershipState::Leading(token), _) => {
                        self.leader_orchestrator.tick(&token).await?;
                    }
                    (_, LeadershipState::Leading(token)) => {
                        self.follower_orchestrator.tick(&token).await?;
                    }
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("coordinated upgrade did not finish"))??;
        let attempts = self
            .upgrade_observations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|observation| MaintenanceAttempt {
                node: FixtureNodeName::new(observation.node_id.clone()),
                drained_nodes: observation
                    .drained_nodes
                    .iter()
                    .cloned()
                    .map(FixtureNodeName::new)
                    .collect(),
            })
            .collect();
        Ok(UpgradeObservation {
            planned_nodes,
            attempts,
            completion: Self::completion(completed.phase),
            target_retention,
            final_freeze: self.final_freeze().await?,
            final_nodes: self.node_snapshots().await?,
        })
    }
}

#[async_trait]
impl UpgradeCluster for OldSystemUpgradeCluster {
    type Error = anyhow::Error;

    async fn topology(&mut self) -> Result<MaintenanceTopology> {
        let leader = match (self.leader.state(), self.follower.state()) {
            (LeadershipState::Leading(token), _) => token.info.node_id,
            (_, LeadershipState::Leading(token)) => token.info.node_id,
            _ => bail!("cluster has no leader before maintenance"),
        };
        Ok(MaintenanceTopology {
            nodes: self.node_snapshots().await?,
            leader: FixtureNodeName::new(leader),
        })
    }

    async fn rolling_upgrade(
        &mut self,
        target: FixtureVersion,
        fault: UpgradeFault,
    ) -> Result<UpgradeObservation> {
        let UpgradeFault::FailFirstAttempt { node: failed_node } = fault;
        let configured_failure = self
            .initial_nodes
            .first()
            .ok_or_else(|| anyhow!("maintenance fixture has no worker"))?;
        if configured_failure.node_id != failed_node.as_str() {
            bail!(
                "legacy fixture injects the first failure on `{}`, not `{}`",
                configured_failure.node_id,
                failed_node.as_str()
            );
        }
        self.run_upgrade(target, super::UpgradeBatch::Rolling).await
    }

    async fn all_node_upgrade(&mut self, target: FixtureVersion) -> Result<UpgradeObservation> {
        self.run_upgrade(target, super::UpgradeBatch::All).await
    }

    async fn restart_node(&mut self, node: &FixtureNodeName) -> Result<SelectedRestartObservation> {
        let (orchestrator, token) = self.active_orchestrator()?;
        let selected = orchestrator
            .create_restart_run(&token, Some(node.as_str()))
            .await?;
        let planned_nodes = selected
            .nodes
            .iter()
            .map(|node| FixtureNodeName::new(node.node_id.clone()))
            .collect();
        let completed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let run = self
                    .store
                    .read_cluster_upgrade()
                    .await?
                    .ok_or_else(|| anyhow!("selected-node restart run disappeared"))?;
                if run.phase.is_terminal() {
                    return Result::<super::UpgradeRun>::Ok(run);
                }
                match (self.leader.state(), self.follower.state()) {
                    (LeadershipState::Leading(token), _) => {
                        self.leader_orchestrator.tick(&token).await?;
                    }
                    (_, LeadershipState::Leading(token)) => {
                        self.follower_orchestrator.tick(&token).await?;
                    }
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        })
        .await
        .map_err(|_| anyhow!("selected-node restart did not finish"))??;
        let requested_nodes = self
            .restart_requests
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .cloned()
            .map(FixtureNodeName::new)
            .collect();
        Ok(SelectedRestartObservation {
            planned_nodes,
            requested_nodes,
            completion: Self::completion(completed.phase),
            final_freeze: self.final_freeze().await?,
        })
    }
}

impl Drop for OldSystemUpgradeCluster {
    fn drop(&mut self) {
        let _ = self.shutdown_leader.send(ShutdownEvent::Force);
        let _ = self.shutdown_follower.send(ShutdownEvent::Force);
        for handle in &self.election_handles {
            handle.abort();
        }
        for handle in &self.api_handles {
            handle.abort();
        }
    }
}

/// Drives both production upgrade parameterizations against real etcd fencing and two real
/// electors. Simulated node APIs restart their controller at the requested version instead of
/// mutating the host NixOS system. The rolling run also injects one retryable worker failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn multinode_rolling_upgrade_retries_and_restores_nodes_serially() -> Result<()> {
    let mut cluster = OldSystemUpgradeCluster::start().await?;

    scenarios::rolling_upgrade_retries_and_restores_nodes_serially(&mut cluster).await?;
    scenarios::all_node_upgrade_restores_nodes_as_one_batch(&mut cluster).await?;
    Ok(())
}

/// This is deliberately ignored in the default unit suite because it starts and destroys three
/// host-networked etcd containers. Run it on an isolated Linux container host with:
///
/// `cargo test-multi-node`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn distributed_election_fencing_and_quorum() -> Result<()> {
    let mut cluster = OldSystemElectionCluster::start().await?;

    scenarios::leader_failover_fences_stale_writes(&mut cluster).await?;
    Ok(())
}

/// Stops every member of a real three-voter etcd cluster and proves the production readiness gate
/// remains blocked with one member, then completes when all persisted members restart. This is the
/// controller-restart equivalent of an all-node system upgrade without mutating the host OS.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Linux container daemon"]
async fn all_voter_restart_waits_for_etcd_quorum_and_preserves_data() -> Result<()> {
    let mut cluster = OldSystemQuorumRecoveryCluster::start().await?;

    scenarios::all_voter_restart_waits_for_quorum_and_preserves_state(&mut cluster).await?;
    Ok(())
}
