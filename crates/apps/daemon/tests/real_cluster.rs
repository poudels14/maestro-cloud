#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use cluster::{
    CertificateValidity, ClusterCertificateAuthority, ClusterConfig, ClusterPorts,
    EmbeddedEtcdProvider, EmbeddedEtcdSettings, Ipv4Cidr, JoinPayload, JoinPrivateKey, JoinRequest,
    JoinResponseStatus, MemberState, NodeCertificateBundle, NodeDefinition, NodeEndpoint,
    StoreJoinTicket, StoreMember, StoreProvider, StoreProviderConfig, StoreProviderError,
    admit_join_request, decrypt_join_response, encrypt_join_response, sign_join_request,
};
use clustertest::{
    ClusterSetupCluster, FixtureNodeName, scenarios::cluster_bootstraps_joins_meshes_and_recovers,
};
use daemon::{DaemonLaunchConfig, StoreLaunchMode};
use kernel_api::{
    ClusterId, ConditionState, NodeId, NodeInstanceId, NodeNetwork, NodeRole, ResourceKind,
    ResourceName, SecretValue,
};
use kernel_store::{
    CasOutcome, EtcdStore, EtcdTlsConfig, ExpectedVersion, Keyspace, PutRequest, Store, TokioClock,
};
use time::{Duration as TimeDuration, OffsetDateTime};

const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const RETRY_DELAY: Duration = Duration::from_millis(100);
const STORE_CLIENT_PORT: u16 = 34_379;
const STORE_PEER_PORT: u16 = 34_380;
const WIREGUARD_PORT: u16 = 34_820;
const GATEWAY_PORT: u16 = 34_001;
const API_PORT: u16 = 35_000;

static NEXT_NETWORK: AtomicU16 = AtomicU16::new(1);

#[tokio::test]
#[ignore = "requires root, iproute2, ping, WireGuard, MAESTRO_ETCD_BIN, and Linux network namespaces"]
async fn real_process_one_node_cluster_setup() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = RealProcessCluster::new(1)?;
    cluster_bootstraps_joins_meshes_and_recovers(&mut cluster).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires root, iproute2, ping, WireGuard, MAESTRO_ETCD_BIN, and Linux network namespaces"]
async fn real_process_three_node_cluster_setup() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = RealProcessCluster::new(3)?;
    cluster_bootstraps_joins_meshes_and_recovers(&mut cluster).await?;
    Ok(())
}

struct RealProcessCluster {
    root: tempfile::TempDir,
    daemon_binary: PathBuf,
    etcd_binary: PathBuf,
    bridge: String,
    cluster: ClusterConfig,
    authority: ClusterCertificateAuthority,
    nodes: Vec<RealNode>,
    securities: BTreeMap<NodeId, NodeCertificateBundle>,
    write_sequence: u32,
}

impl RealProcessCluster {
    fn new(node_count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        if !matches!(node_count, 1 | 3) {
            return Err("real-process cluster requires one or three nodes".into());
        }
        require_command("ip")?;
        require_command("ping")?;
        let etcd_binary = PathBuf::from(std::env::var("MAESTRO_ETCD_BIN")?);
        let daemon_binary = PathBuf::from(env!("CARGO_BIN_EXE_daemon"));
        let root = tempfile::tempdir()?;
        let allocation = NEXT_NETWORK.fetch_add(1, Ordering::Relaxed);
        let segment = u8::try_from((allocation % 200).saturating_add(20))?;
        let token = format!("{:x}{allocation:x}", std::process::id());
        let bridge = shortened_interface_name("mb", &token, 0);
        let cluster = topology(node_count, segment, &token)?;
        let authority = ClusterCertificateAuthority::generate(
            &cluster.name,
            CertificateValidity::new(
                OffsetDateTime::now_utc() - TimeDuration::hours(1),
                OffsetDateTime::now_utc() + TimeDuration::days(365),
            )?,
        )?;
        let validity = CertificateValidity::new(
            OffsetDateTime::now_utc() - TimeDuration::hours(1),
            OffsetDateTime::now_utc() + TimeDuration::days(365),
        )?;
        let securities = cluster
            .nodes
            .iter()
            .map(|(node_id, node)| {
                Ok((
                    node_id.clone(),
                    authority.issue_node_certificate(
                        node_id,
                        &node.hostname,
                        node.endpoint.host_address,
                        node.role,
                        validity,
                    )?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, cluster::CertificateError>>()?;
        let nodes = cluster
            .nodes
            .iter()
            .enumerate()
            .map(|(index, (node_id, node))| RealNode {
                fixture: FixtureNodeName::new(node_id.as_str()),
                node_id: node_id.clone(),
                namespace: format!("maestro-{token}-{index}"),
                host_veth: shortened_interface_name("mh", &token, index),
                namespace_veth: format!("mn{index}"),
                host_address: node.endpoint.host_address,
                workload_address: Ipv4Addr::new(
                    node.workload_subnet.network_address().octets()[0],
                    node.workload_subnet.network_address().octets()[1],
                    node.workload_subnet.network_address().octets()[2],
                    17,
                ),
                data_directory: root.path().join(node_id.as_str()),
                config_path: root.path().join(format!("{node_id}.launch.json")),
                log_path: root.path().join(format!("{node_id}.log")),
                child: None,
                launch_sequence: 0,
            })
            .collect();
        let mut real = Self {
            root,
            daemon_binary,
            etcd_binary,
            bridge,
            cluster,
            authority,
            nodes,
            securities,
            write_sequence: 0,
        };
        real.create_network(segment)?;
        Ok(real)
    }

    fn create_network(&mut self, segment: u8) -> Result<(), Box<dyn std::error::Error>> {
        run_checked("ip", ["link", "add", &self.bridge, "type", "bridge"])?;
        run_checked(
            "ip",
            [
                "addr",
                "add",
                &format!("10.203.{segment}.1/24"),
                "dev",
                &self.bridge,
            ],
        )?;
        run_checked("ip", ["link", "set", &self.bridge, "up"])?;
        for node in &self.nodes {
            run_checked("ip", ["netns", "add", &node.namespace])?;
            run_checked(
                "ip",
                [
                    "link",
                    "add",
                    &node.host_veth,
                    "type",
                    "veth",
                    "peer",
                    "name",
                    &node.namespace_veth,
                ],
            )?;
            run_checked(
                "ip",
                [
                    "link",
                    "set",
                    &node.namespace_veth,
                    "netns",
                    &node.namespace,
                ],
            )?;
            run_checked(
                "ip",
                ["link", "set", &node.host_veth, "master", &self.bridge],
            )?;
            run_checked("ip", ["link", "set", &node.host_veth, "up"])?;
            run_checked("ip", ["-n", &node.namespace, "link", "set", "lo", "up"])?;
            run_checked(
                "ip",
                [
                    "-n",
                    &node.namespace,
                    "addr",
                    "add",
                    &format!("{}/24", node.host_address),
                    "dev",
                    &node.namespace_veth,
                ],
            )?;
            run_checked(
                "ip",
                [
                    "-n",
                    &node.namespace,
                    "link",
                    "set",
                    &node.namespace_veth,
                    "up",
                ],
            )?;
            run_checked(
                "ip",
                [
                    "-n",
                    &node.namespace,
                    "link",
                    "add",
                    "workload0",
                    "type",
                    "dummy",
                ],
            )?;
            run_checked(
                "ip",
                [
                    "-n",
                    &node.namespace,
                    "addr",
                    "add",
                    &format!("{}/24", node.workload_address),
                    "dev",
                    "workload0",
                ],
            )?;
            run_checked(
                "ip",
                ["-n", &node.namespace, "link", "set", "workload0", "up"],
            )?;
        }
        Ok(())
    }

    async fn launch_node(
        &mut self,
        index: usize,
        store_mode: StoreLaunchMode,
    ) -> Result<(), RealClusterError> {
        if self.node(index)?.child.is_some() {
            return Err(RealClusterError::new("node process is already running"));
        }
        let launch_sequence = {
            let node = self.node_mut(index)?;
            node.launch_sequence = node.launch_sequence.saturating_add(1);
            node.launch_sequence
        };
        let node = self.node(index)?;
        let security = self
            .securities
            .get(&node.node_id)
            .cloned()
            .ok_or_else(|| RealClusterError::new("node security is missing"))?;
        let config = DaemonLaunchConfig {
            cluster: self.cluster.clone(),
            node_id: node.node_id.clone(),
            data_directory: node.data_directory.clone(),
            etcd_binary: Some(self.etcd_binary.clone()),
            store_mode,
            security,
            operator_jwt_secret: kernel_api::SecretValue::new(
                "real-cluster-operator-secret-with-32-characters",
            ),
            instance_id: Some(
                NodeInstanceId::new(format!("{}-process-{launch_sequence}", node.node_id))
                    .map_err(RealClusterError::from_display)?,
            ),
            datadog: None,
            log_backup: None,
            preview: None,
            nixos_upgrade: None,
        };
        write_private_json(&node.config_path, &config)?;
        let log = append_file(&node.log_path)?;
        let error_log = log.try_clone().map_err(RealClusterError::from_display)?;
        let child = Command::new("ip")
            .args(["netns", "exec", &node.namespace])
            .arg(&self.daemon_binary)
            .arg(&node.config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log))
            .spawn()
            .map_err(RealClusterError::from_display)?;
        self.node_mut(index)?.child = Some(child);
        Ok(())
    }

    async fn wait_store(&mut self) -> Result<EtcdStore, RealClusterError> {
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            self.ensure_children_running()?;
            match self.connect_store().await {
                Ok(store) => return Ok(store),
                Err(error) if tokio::time::Instant::now() < deadline => {
                    let _detail = error;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(error) => {
                    return Err(RealClusterError::new(format!(
                        "store readiness deadline elapsed: {error}"
                    )));
                }
            }
        }
    }

    async fn connect_store(&self) -> Result<EtcdStore, kernel_store::StoreError> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.child.is_some())
            .ok_or_else(|| kernel_store::StoreError::Unavailable {
                message: "no running node is available for store access".to_owned(),
            })?;
        let security = self.securities.get(&node.node_id).ok_or_else(|| {
            kernel_store::StoreError::Contract {
                message: "running node security is missing".to_owned(),
            }
        })?;
        let tls = EtcdTlsConfig::new(
            node.host_address.to_string(),
            security.trust_root_pem.as_bytes().to_vec(),
            security.identity.certificate_pem.as_bytes().to_vec(),
            security
                .identity
                .private_key_pem
                .expose()
                .as_bytes()
                .to_vec(),
        );
        EtcdStore::connect_with_tls(
            [format!(
                "https://{}:{}",
                node.host_address, STORE_CLIENT_PORT
            )],
            tls,
        )
        .await
    }

    fn leader_provider(&self) -> Result<EmbeddedEtcdProvider, RealClusterError> {
        let config = self.provider_config(0)?;
        EmbeddedEtcdProvider::new(
            config,
            self.etcd_binary.clone(),
            Arc::new(TokioClock::new()),
            EmbeddedEtcdSettings::default(),
        )
        .map_err(RealClusterError::from_display)
    }

    fn provider_config(&self, index: usize) -> Result<StoreProviderConfig, RealClusterError> {
        let node = self.node(index)?;
        let known = self
            .nodes
            .iter()
            .map(|member| {
                (
                    member.node_id.clone(),
                    StoreMember {
                        node_id: member.node_id.clone(),
                        host_address: member.host_address,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let local = known
            .get(&node.node_id)
            .cloned()
            .ok_or_else(|| RealClusterError::new("local provider member is missing"))?;
        let security = self
            .securities
            .get(&node.node_id)
            .cloned()
            .ok_or_else(|| RealClusterError::new("provider security is missing"))?;
        StoreProviderConfig::new(
            self.cluster.cluster_id.clone(),
            local,
            known,
            self.cluster.ports,
            node.data_directory.join("store"),
            security,
        )
        .map_err(RealClusterError::from_display)
    }

    async fn stage_member(&mut self, index: usize) -> Result<StoreJoinTicket, RealClusterError> {
        let provider = self.leader_provider()?;
        let node = self.node(index)?;
        let member = StoreMember {
            node_id: node.node_id.clone(),
            host_address: node.host_address,
        };
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            match provider.stage_member(member.clone()).await {
                Ok((ticket, activation)) if activation.state == MemberState::Staged => {
                    return Ok(ticket);
                }
                Ok(_) | Err(StoreProviderError::Unavailable { .. })
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(error) => return Err(RealClusterError::from_display(error)),
                Ok(_) => return Err(RealClusterError::new("member staging did not converge")),
            }
        }
    }

    async fn admit_and_stage_member(
        &mut self,
        index: usize,
    ) -> Result<StoreJoinTicket, RealClusterError> {
        let node = self.node(index)?;
        let node_id = node.node_id.clone();
        let source_address = node.host_address;
        let certificates = self
            .securities
            .get(&node_id)
            .cloned()
            .ok_or_else(|| RealClusterError::new("joiner security is missing"))?;
        let join_key = JoinPrivateKey::generate();
        let now_unix_ms =
            i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / i128::from(1_000_000))
                .map_err(RealClusterError::from_display)?;
        let request = JoinRequest::from_config(&join_key, &self.cluster, &node_id, now_unix_ms)
            .map_err(RealClusterError::from_display)?;
        let signature = sign_join_request(&self.cluster.join_secret, &request)
            .map_err(RealClusterError::from_display)?;
        let admission = admit_join_request(
            &self.cluster,
            &request,
            &signature,
            source_address,
            now_unix_ms,
        )
        .map_err(RealClusterError::from_display)?;
        if admission.node_id != node_id {
            return Err(RealClusterError::new(
                "join admission returned a different node identity",
            ));
        }

        let ticket = self.stage_member(index).await?;
        let payload = JoinPayload {
            cluster_id: self.cluster.cluster_id.clone(),
            cluster_name: self.cluster.name.clone(),
            nodes: self.cluster.nodes.clone(),
            ports: self.cluster.ports,
            certificates,
            store_join_ticket: Some(ticket),
            certificate_issuer: Some(self.authority.clone()),
        };
        let envelope = encrypt_join_response(
            &self.cluster.join_secret,
            &request,
            &payload,
            JoinResponseStatus::ACCEPTED,
        )
        .map_err(RealClusterError::from_display)?;
        let admitted = decrypt_join_response(
            &self.cluster.join_secret,
            &join_key,
            &request,
            &envelope,
            JoinResponseStatus::ACCEPTED,
        )
        .map_err(RealClusterError::from_display)?;
        self.securities.insert(node_id, admitted.certificates);
        admitted
            .store_join_ticket
            .ok_or_else(|| RealClusterError::new("join response omitted the store ticket"))
    }

    async fn activate_member(&mut self, ticket: &StoreJoinTicket) -> Result<(), RealClusterError> {
        let provider = self.leader_provider()?;
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            match provider.activate_member(ticket).await {
                Ok(activation) if activation.state == MemberState::Active => return Ok(()),
                Ok(_) | Err(StoreProviderError::MemberNotReady { .. })
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(error) => return Err(RealClusterError::from_display(error)),
                Ok(_) => return Err(RealClusterError::new("member activation did not converge")),
            }
        }
    }

    async fn stop_process(&mut self, index: usize) -> Result<(), RealClusterError> {
        let node = self.node_mut(index)?;
        let Some(mut child) = node.child.take() else {
            return Err(RealClusterError::new("node process is not running"));
        };
        let status = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .map_err(RealClusterError::from_display)?;
        if !status.success() {
            return Err(RealClusterError::new("failed to signal daemon process"));
        }
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            if child
                .try_wait()
                .map_err(RealClusterError::from_display)?
                .is_some()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                child.kill().map_err(RealClusterError::from_display)?;
                child.wait().map_err(RealClusterError::from_display)?;
                return Err(RealClusterError::new(
                    "daemon did not stop before the shutdown deadline",
                ));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    fn ensure_children_running(&mut self) -> Result<(), RealClusterError> {
        for node in &mut self.nodes {
            if let Some(child) = node.child.as_mut()
                && let Some(status) = child.try_wait().map_err(RealClusterError::from_display)?
            {
                return Err(RealClusterError::new(format!(
                    "daemon `{}` exited early with {status}; log: {}",
                    node.node_id,
                    read_log(&node.log_path)
                )));
            }
        }
        Ok(())
    }

    fn node(&self, index: usize) -> Result<&RealNode, RealClusterError> {
        self.nodes
            .get(index)
            .ok_or_else(|| RealClusterError::new(format!("node index {index} is out of range")))
    }

    fn node_mut(&mut self, index: usize) -> Result<&mut RealNode, RealClusterError> {
        self.nodes
            .get_mut(index)
            .ok_or_else(|| RealClusterError::new(format!("node index {index} is out of range")))
    }

    fn index_for(&self, fixture: &FixtureNodeName) -> Result<usize, RealClusterError> {
        self.nodes
            .iter()
            .position(|node| node.fixture == *fixture)
            .ok_or_else(|| RealClusterError::new(format!("unknown node `{}`", fixture.as_str())))
    }
}

#[async_trait]
impl ClusterSetupCluster for RealProcessCluster {
    type Error = RealClusterError;

    fn nodes(&self) -> Vec<FixtureNodeName> {
        self.nodes.iter().map(|node| node.fixture.clone()).collect()
    }

    async fn bootstrap_seed(&mut self) -> Result<(), Self::Error> {
        self.launch_node(0, StoreLaunchMode::Bootstrap).await?;
        self.wait_store().await?;
        Ok(())
    }

    async fn join_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        let index = self.index_for(node)?;
        let ticket = self.admit_and_stage_member(index).await?;
        self.launch_node(
            index,
            StoreLaunchMode::Join {
                ticket: ticket.clone(),
            },
        )
        .await?;
        self.activate_member(&ticket).await?;
        self.wait_store().await?;
        Ok(())
    }

    async fn await_mesh(
        &mut self,
        expected: &BTreeSet<FixtureNodeName>,
    ) -> Result<BTreeSet<FixtureNodeName>, Self::Error> {
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        let prefix = Keyspace::new(&self.cluster.cluster_id).resource_kind(
            &ResourceKind::new("NodeNetwork").map_err(RealClusterError::from_display)?,
        );
        loop {
            self.ensure_children_running()?;
            if let Ok(store) = self.connect_store().await
                && let Ok(snapshot) = store.list(&prefix).await
            {
                let resources = snapshot
                    .values
                    .iter()
                    .filter_map(|stored| serde_json::from_slice::<NodeNetwork>(&stored.value).ok())
                    .filter(|resource| {
                        resource.status.applied_generation == resource.meta.generation
                            && resource.status.conditions.iter().any(|condition| {
                                condition.state == ConditionState::True
                                    && condition.condition_type.0 == "MeshReady"
                            })
                    })
                    .map(|resource| FixtureNodeName::new(resource.spec.node_id.as_str()))
                    .collect::<BTreeSet<_>>();
                if resources == *expected {
                    return Ok(resources);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "mesh did not converge before deadline; logs: {}",
                    self.nodes
                        .iter()
                        .map(|node| format!("{}={}", node.node_id, read_log(&node.log_path)))
                        .collect::<Vec<_>>()
                        .join(" | ")
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn ping_workload(
        &mut self,
        source: &FixtureNodeName,
        target: &FixtureNodeName,
    ) -> Result<(), Self::Error> {
        let source_index = self.index_for(source)?;
        let target_index = self.index_for(target)?;
        let source_namespace = self.node(source_index)?.namespace.clone();
        let target_address = self.node(target_index)?.workload_address;
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            self.ensure_children_running()?;
            let status = Command::new("ip")
                .args([
                    "netns",
                    "exec",
                    &source_namespace,
                    "ping",
                    "-c",
                    "1",
                    "-W",
                    "1",
                ])
                .arg(target_address.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map_err(RealClusterError::from_display)?;
            if status.success() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "workload ping from `{}` to `{}` did not converge",
                    source.as_str(),
                    target.as_str(),
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn stop_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        let index = self.index_for(node)?;
        self.stop_process(index).await
    }

    async fn restart_node(&mut self, node: &FixtureNodeName) -> Result<(), Self::Error> {
        let index = self.index_for(node)?;
        self.launch_node(index, StoreLaunchMode::Restart).await?;
        self.wait_store().await?;
        Ok(())
    }

    async fn verify_store_write(&mut self) -> Result<(), Self::Error> {
        self.write_sequence = self.write_sequence.saturating_add(1);
        let store = self.wait_store().await?;
        let key = Keyspace::new(&self.cluster.cluster_id).resource(
            &ResourceKind::new("SetupProbe").map_err(RealClusterError::from_display)?,
            &ResourceName::new(format!("write-{}", self.write_sequence))
                .map_err(RealClusterError::from_display)?,
        );
        let value = format!("quorum-write-{}", self.write_sequence).into_bytes();
        let outcome = store
            .put_cas(PutRequest {
                key: key.clone(),
                value: value.clone(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await
            .map_err(RealClusterError::from_display)?;
        if !matches!(outcome, CasOutcome::Applied(_)) {
            return Err(RealClusterError::new("setup probe write conflicted"));
        }
        let observed = store
            .get(&key)
            .await
            .map_err(RealClusterError::from_display)?
            .map(|stored| stored.value);
        if observed == Some(value) {
            Ok(())
        } else {
            Err(RealClusterError::new(
                "setup probe read did not match write",
            ))
        }
    }
}

impl Drop for RealProcessCluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                let _ = Command::new("kill")
                    .args(["-TERM", &child.id().to_string()])
                    .status();
                for _attempt in 0..50 {
                    if child.try_wait().ok().flatten().is_some() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                if child.try_wait().ok().flatten().is_none() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
            kill_namespace_processes(&node.namespace);
            let _ = Command::new("ip")
                .args(["netns", "delete", &node.namespace])
                .status();
        }
        let _ = Command::new("ip")
            .args(["link", "delete", &self.bridge])
            .status();
        let _root_path = self.root.path();
    }
}

struct RealNode {
    fixture: FixtureNodeName,
    node_id: NodeId,
    namespace: String,
    host_veth: String,
    namespace_veth: String,
    host_address: Ipv4Addr,
    workload_address: Ipv4Addr,
    data_directory: PathBuf,
    config_path: PathBuf,
    log_path: PathBuf,
    child: Option<Child>,
    launch_sequence: u32,
}

#[derive(Debug, thiserror::Error)]
#[error("real-process cluster failed: {detail}")]
struct RealClusterError {
    detail: String,
}

impl RealClusterError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    fn from_display(error: impl std::fmt::Display) -> Self {
        Self::new(error.to_string())
    }
}

fn topology(
    node_count: usize,
    segment: u8,
    token: &str,
) -> Result<ClusterConfig, Box<dyn std::error::Error>> {
    let roles = if node_count == 1 {
        vec![NodeRole::Master]
    } else {
        vec![NodeRole::Master, NodeRole::Hybrid, NodeRole::ControlPlane]
    };
    let nodes = roles
        .into_iter()
        .enumerate()
        .map(|(index, role)| {
            let number = u8::try_from(index)?.saturating_add(1);
            let node_id = NodeId::new(format!("node-{number}"))?;
            Ok((
                node_id.clone(),
                NodeDefinition {
                    hostname: format!("{node_id}.internal"),
                    endpoint: NodeEndpoint {
                        host_address: Ipv4Addr::new(10, 203, segment, number.saturating_add(10)),
                        api_port: API_PORT,
                    },
                    workload_subnet: format!("172.22.{number}.0/24").parse::<Ipv4Cidr>()?,
                    role,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;
    let name = format!("real-{}", token.to_ascii_lowercase());
    Ok(ClusterConfig {
        cluster_id: ClusterId::new(name.clone())?,
        name,
        nodes,
        control_allow_cidrs: vec![format!("10.203.{segment}.0/24").parse()?],
        ports: ClusterPorts::new(
            GATEWAY_PORT,
            STORE_CLIENT_PORT,
            STORE_PEER_PORT,
            WIREGUARD_PORT,
        )?,
        join_secret: SecretValue::new("real-process-join-secret-with-at-least-32-characters"),
    })
}

fn write_private_json(path: &Path, value: &impl serde::Serialize) -> Result<(), RealClusterError> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path).map_err(RealClusterError::from_display)?;
    serde_json::to_writer_pretty(file, value).map_err(RealClusterError::from_display)
}

fn append_file(path: &Path) -> Result<File, RealClusterError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(RealClusterError::from_display)
}

fn run_checked<I, S>(program: &str, arguments: I) -> Result<(), Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program).args(arguments).output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "`{program}` failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

fn require_command(command: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new(command)
        .arg("-Version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() || command == "ping" {
        Ok(())
    } else {
        Err(format!("required command `{command}` is unavailable").into())
    }
}

fn shortened_interface_name(prefix: &str, token: &str, index: usize) -> String {
    let available = 15_usize.saturating_sub(prefix.len() + index.to_string().len());
    let shortened = token.chars().take(available).collect::<String>();
    format!("{prefix}{shortened}{index}")
}

fn kill_namespace_processes(namespace: &str) {
    let Ok(output) = Command::new("ip")
        .args(["netns", "pids", namespace])
        .output()
    else {
        return;
    };
    for pid in String::from_utf8_lossy(&output.stdout).split_whitespace() {
        let _ = Command::new("kill").args(["-KILL", pid]).status();
    }
}

fn read_log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| format!("[unavailable: {error}]"))
}
