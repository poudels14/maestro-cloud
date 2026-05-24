//! Integration tests against a real 3-node etcd cluster spun up in containers.
//!
//! Marked `#[ignore]` so `cargo test` doesn't try to run them — they need a
//! container runtime (docker or nerdctl) on PATH plus the ability to bind to
//! a handful of localhost ports. Run with:
//!
//! ```text
//! cargo test --bin maestro --ignored etcd_chaos
//! ```
//!
//! These cover the integration gap our in-memory tests can't reach: actual
//! Raft consensus, peer URL advertising, lease keep-alives over the wire, and
//! recovery when an etcd node disappears mid-election.

use std::sync::Arc;
use std::time::Duration;

use etcd_client::Client as EtcdClient;
use tokio::sync::Mutex;

use crate::cluster::adapters::{EtcdPortAllocator, EtcdTraefikSink, StoreServiceCatalog};
use crate::cluster::assignment_store::AssignmentStore;
use crate::cluster::elector::LeaderElector;
use crate::cluster::etcd_assignment_store::EtcdAssignmentStore;
use crate::cluster::etcd_elector::EtcdLeaderElector;
use crate::cluster::etcd_registry::EtcdNodeRegistry;
use crate::cluster::leader_loop::LeaderLoop;
use crate::cluster::registry::NodeRegistry;
use crate::cluster::scheduler::DefaultScheduler;
use crate::cluster::types::{LeadershipState, NodeInfo, NodeRole};
use crate::deployment::etcd::EtcdStateStore;
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{
    Command, DeploymentStatus, IngressConfig, ReplicaState, ServiceConfig, ServiceDeployConfig,
    ServiceDeployment,
};
use crate::utils::crypto::derive_key;

const ETCD_IMAGE: &str = "quay.io/coreos/etcd:v3.6.8";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(15);
const FAILOVER_TIMEOUT: Duration = Duration::from_secs(45);

#[tokio::test]
#[ignore = "needs docker/nerdctl + open ports; run with --ignored"]
async fn three_node_cluster_forms_and_elects_one_leader() {
    let Some(runtime) = ContainerRuntime::detect() else {
        eprintln!("skipping: no container runtime found on PATH");
        return;
    };
    let cluster = match EtcdTestCluster::start(&runtime, 3).await {
        Ok(cluster) => cluster,
        Err(err) => panic!("failed to start etcd cluster: {err}"),
    };

    let electors = cluster.electors(["node-a", "node-b", "node-c"]).await;
    let registries = cluster.registries(["node-a", "node-b", "node-c"]).await;

    for (node_id, registry) in ["node-a", "node-b", "node-c"].iter().zip(&registries) {
        let info = test_node_info(node_id);
        registry.register(&info).await.expect("register");
    }

    let campaign_handles: Vec<_> = electors
        .iter()
        .cloned()
        .map(|elector| tokio::spawn(async move { elector.campaign().await }))
        .collect();

    let winner = wait_for_any_leader(&electors, ELECTION_TIMEOUT).await;
    for handle in campaign_handles {
        handle.abort();
    }

    let winner_idx = winner.expect("a node should have won the election within timeout");
    let leader_count = electors
        .iter()
        .filter(|elector| matches!(elector.state(), LeadershipState::Leading(_)))
        .count();
    assert_eq!(
        leader_count, 1,
        "exactly one node should be leader; saw {leader_count} (winner_idx={winner_idx})"
    );

    let all_nodes = registries[0].list_nodes().await.expect("list nodes");
    assert_eq!(all_nodes.len(), 3, "all 3 nodes should be registered");

    cluster.cleanup().await;
}

#[tokio::test]
#[ignore = "needs docker/nerdctl + open ports; run with --ignored"]
async fn killing_leader_node_triggers_failover() {
    let Some(runtime) = ContainerRuntime::detect() else {
        eprintln!("skipping: no container runtime found on PATH");
        return;
    };
    let cluster = match EtcdTestCluster::start(&runtime, 3).await {
        Ok(cluster) => cluster,
        Err(err) => panic!("failed to start etcd cluster: {err}"),
    };

    let electors = cluster.electors(["node-a", "node-b", "node-c"]).await;
    let registries = cluster.registries(["node-a", "node-b", "node-c"]).await;
    for (node_id, registry) in ["node-a", "node-b", "node-c"].iter().zip(&registries) {
        registry
            .register(&test_node_info(node_id))
            .await
            .expect("register");
    }

    let campaign_handles: Vec<_> = electors
        .iter()
        .cloned()
        .map(|elector| tokio::spawn(async move { elector.campaign().await }))
        .collect();

    let initial_leader = wait_for_any_leader(&electors, ELECTION_TIMEOUT)
        .await
        .expect("initial election should complete");

    cluster
        .kill_member(initial_leader)
        .await
        .expect("kill leader's etcd container");

    let new_leader = wait_for_different_leader(&electors, initial_leader, FAILOVER_TIMEOUT)
        .await
        .expect("a surviving node should win a new election");
    assert_ne!(new_leader, initial_leader);

    for handle in campaign_handles {
        handle.abort();
    }
    cluster.cleanup().await;
}

#[tokio::test]
#[ignore = "needs docker/nerdctl + open ports; run with --ignored"]
async fn graceful_full_cluster_restart_preserves_state() {
    let Some(runtime) = ContainerRuntime::detect() else {
        eprintln!("skipping: no container runtime found on PATH");
        return;
    };
    let cluster = match EtcdTestCluster::start(&runtime, 3).await {
        Ok(cluster) => cluster,
        Err(err) => panic!("failed to start etcd cluster: {err}"),
    };

    let endpoint = format!("http://127.0.0.1:{}", cluster.client_port(0));
    let mut client = EtcdClient::connect([&endpoint], None).await.unwrap();
    client
        .put("chaos/persistent-key", "before-restart", None)
        .await
        .expect("initial put");

    for index in 0..cluster.member_count() {
        cluster.stop_member(index).await.expect("graceful stop");
    }

    for index in 0..cluster.member_count() {
        cluster
            .start_member_existing(index)
            .await
            .expect("restart with existing data dir");
    }
    cluster
        .await_ready()
        .await
        .expect("cluster ready after restart");

    let mut client = EtcdClient::connect([&endpoint], None).await.unwrap();
    let response = client
        .get("chaos/persistent-key", None)
        .await
        .expect("get after restart");
    let kv = response.kvs().first().expect("key should survive restart");
    assert_eq!(kv.value_str().unwrap(), "before-restart");

    cluster.cleanup().await;
}

#[tokio::test]
#[ignore = "needs docker/nerdctl + open ports; run with --ignored"]
async fn losing_quorum_makes_writes_fail_then_recover() {
    let Some(runtime) = ContainerRuntime::detect() else {
        eprintln!("skipping: no container runtime found on PATH");
        return;
    };
    let cluster = match EtcdTestCluster::start(&runtime, 3).await {
        Ok(cluster) => cluster,
        Err(err) => panic!("failed to start etcd cluster: {err}"),
    };
    let endpoint = format!("http://127.0.0.1:{}", cluster.members[0].client_port);
    let mut client = EtcdClient::connect([&endpoint], None).await.unwrap();
    client.put("chaos/before", "ok", None).await.unwrap();

    cluster.kill_member(1).await.unwrap();
    cluster.kill_member(2).await.unwrap();

    let write_during_partition = tokio::time::timeout(
        Duration::from_secs(5),
        client.put("chaos/during", "should-fail", None),
    )
    .await;
    assert!(
        matches!(write_during_partition, Err(_) | Ok(Err(_))),
        "writes should fail/timeout when 2/3 members are gone (no quorum)"
    );

    cluster.cleanup().await;
}

#[tokio::test]
#[ignore = "needs docker/nerdctl + open ports; run with --ignored"]
async fn unhealthy_replica_is_rescheduled_to_different_node() {
    let Some(runtime) = ContainerRuntime::detect() else {
        eprintln!("skipping: no container runtime found on PATH");
        return;
    };
    let cluster = match EtcdTestCluster::start(&runtime, 3).await {
        Ok(cluster) => cluster,
        Err(err) => panic!("failed to start etcd cluster: {err}"),
    };

    let endpoint = format!("http://127.0.0.1:{}", cluster.client_port(0));
    let store: Arc<dyn ClusterStore> = Arc::new(
        EtcdStateStore::new(&endpoint, derive_key("test-key"), None)
            .await
            .expect("connect state store"),
    );
    let cluster_client = {
        let raw = EtcdClient::connect([&endpoint], None).await.unwrap();
        Arc::new(Mutex::new(raw))
    };

    let registry = Arc::new(EtcdNodeRegistry::new(
        cluster_client.clone(),
        "node-a".to_string(),
    ));
    let registry_b = Arc::new(EtcdNodeRegistry::new(
        cluster_client.clone(),
        "node-b".to_string(),
    ));
    let registry_c = Arc::new(EtcdNodeRegistry::new(
        cluster_client.clone(),
        "node-c".to_string(),
    ));
    registry.register(&test_node_info("node-a")).await.unwrap();
    registry_b
        .register(&test_node_info("node-b"))
        .await
        .unwrap();
    registry_c
        .register(&test_node_info("node-c"))
        .await
        .unwrap();

    let elector = Arc::new(EtcdLeaderElector::new(
        cluster_client.clone(),
        "node-a".to_string(),
    ));
    elector.spawn_observer().await;
    let elector_for_campaign = elector.clone();
    let campaign_task = tokio::spawn(async move { elector_for_campaign.campaign().await });
    wait_for_any_leader(&[elector.clone()], ELECTION_TIMEOUT)
        .await
        .expect("node-a should win the election");

    let service_id = "chaos-web".to_string();
    let deployment = ServiceDeployment {
        id: "d-001".to_string(),
        created_at: 1,
        deployed_at: None,
        drained_at: None,
        status: DeploymentStatus::Queued,
        config: ServiceConfig {
            id: service_id.clone(),
            name: "Chaos Web".to_string(),
            version: "v1".to_string(),
            build: None,
            image: Some("nginx:1.27".to_string()),
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports: vec![],
                command: Some(Command {
                    command: "nginx".to_string(),
                    args: vec!["-g".to_string(), "daemon off;".to_string()],
                }),
                healthcheck_path: None,
                healthcheck_interval: 60,
                replicas: 1,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
                volumes: vec![],
                node_affinity: None,
            },
            ingress: Some(IngressConfig {
                host: Some("chaos.example.com".to_string()),
                hosts: vec![],
                port: None,
            }),
        },
        git_commit: None,
        build: None,
        upload_archive: None,
    };
    store
        .queue_deployment(deployment.clone())
        .await
        .expect("queue deployment");
    // Walk the deployment state machine to Ready so unhealthy_slots will see
    // it as an active deployment: Queued → Building → PendingReady → Ready.
    let queued = store
        .list_queued_deployments()
        .await
        .expect("list queued")
        .into_iter()
        .find(|q| q.service_id == service_id)
        .expect("our queued deployment should be listed");
    store
        .claim_deployment_building(&queued)
        .await
        .expect("claim building");
    let dep_ref = crate::deployment::types::Deployment {
        id: deployment.id.clone(),
        service_id: service_id.clone(),
        replica_index: 0,
    };
    store
        .update_deployment_status(&dep_ref, DeploymentStatus::PendingReady)
        .await
        .expect("→ PendingReady");
    store
        .update_deployment_status(&dep_ref, DeploymentStatus::Ready)
        .await
        .expect("→ Ready");

    let leader_loop = LeaderLoop {
        catalog: Arc::new(StoreServiceCatalog::new(store.clone())),
        registry: registry.clone() as Arc<dyn NodeRegistry>,
        assignments: Arc::new(EtcdAssignmentStore::new(cluster_client.clone()))
            as Arc<dyn AssignmentStore>,
        port_allocator: Arc::new(EtcdPortAllocator::new(cluster_client.clone())),
        scheduler: Arc::new(DefaultScheduler::new()),
        traefik_sink: Arc::new(EtcdTraefikSink::new(cluster_client.clone())),
        elector: elector.clone() as Arc<dyn LeaderElector>,
        tick_interval: Duration::from_secs(1),
        default_entry_point: "web".to_string(),
        on_plan_applied: None,
        metrics: None,
    };
    let plan = leader_loop.run_once(1_000).await.expect("first plan");
    assert_eq!(plan.assignments.len(), 1, "single replica should be placed");
    let initial_node = plan.assignments[0].node_id.clone();

    // Simulate a healthcheck failure storm: mark the replica as Crashed in
    // the persisted replica state. `StoreServiceCatalog.unhealthy_slots` will
    // pick this up on the next tick.
    store
        .upsert_replica_state(
            &service_id,
            &deployment.id,
            ReplicaState {
                replica_index: 0,
                status: DeploymentStatus::Crashed,
                healthcheck_failures: 10,
                restart_attempts: 0,
            },
        )
        .await
        .expect("upsert replica state");

    let next_plan = leader_loop.run_once(2_000).await.expect("rescheduled plan");
    assert_eq!(next_plan.assignments.len(), 1);
    assert_ne!(
        next_plan.assignments[0].node_id, initial_node,
        "unhealthy replica should be rescheduled to a different node (was on {initial_node})"
    );

    campaign_task.abort();
    cluster.cleanup().await;
}

async fn wait_for_any_leader(
    electors: &[Arc<EtcdLeaderElector>],
    timeout: Duration,
) -> Option<usize> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for (idx, elector) in electors.iter().enumerate() {
            if matches!(elector.state(), LeadershipState::Leading(_)) {
                return Some(idx);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

async fn wait_for_different_leader(
    electors: &[Arc<EtcdLeaderElector>],
    avoid: usize,
    timeout: Duration,
) -> Option<usize> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for (idx, elector) in electors.iter().enumerate() {
            if idx == avoid {
                continue;
            }
            if matches!(elector.state(), LeadershipState::Leading(_)) {
                return Some(idx);
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

fn test_node_info(node_id: &str) -> NodeInfo {
    NodeInfo {
        node_id: node_id.to_string(),
        hostname: format!("{node_id}.test"),
        role: NodeRole::Both,
        tailscale_ip: None,
        api_port: 3001,
        version: "test".to_string(),
        started_at_ms: 0,
        labels: Default::default(),
        unschedulable: false,
    }
}

#[derive(Clone)]
struct ContainerRuntime {
    binary: String,
}

impl ContainerRuntime {
    fn detect() -> Option<Self> {
        for candidate in ["docker", "nerdctl", "podman"] {
            if std::process::Command::new(candidate)
                .arg("--version")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
            {
                return Some(Self {
                    binary: candidate.to_string(),
                });
            }
        }
        None
    }

    async fn run_detached(&self, name: &str, args: &[&str]) -> anyhow::Result<()> {
        let mut cmd = tokio::process::Command::new(&self.binary);
        cmd.arg("run").arg("-d").arg("--name").arg(name);
        for arg in args {
            cmd.arg(arg);
        }
        let output = cmd.output().await?;
        if !output.status.success() {
            anyhow::bail!(
                "container start failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    async fn stop(&self, name: &str) {
        let _ = tokio::process::Command::new(&self.binary)
            .arg("stop")
            .arg("--time")
            .arg("1")
            .arg(name)
            .output()
            .await;
        let _ = tokio::process::Command::new(&self.binary)
            .arg("rm")
            .arg("-f")
            .arg(name)
            .output()
            .await;
    }
}

struct EtcdMember {
    container_name: String,
    client_port: u16,
    peer_port: u16,
    data_dir: std::path::PathBuf,
    run_args: Vec<String>,
}

struct EtcdTestCluster {
    runtime: ContainerRuntime,
    members: Vec<EtcdMember>,
    network_name: String,
    base_dir: std::path::PathBuf,
}

impl EtcdTestCluster {
    async fn start(runtime: &ContainerRuntime, size: usize) -> anyhow::Result<Self> {
        let suffix = nanoid::unique_id(6);
        let network_name = format!("maestro-test-{suffix}");
        let _ = tokio::process::Command::new(&runtime.binary)
            .args(["network", "create", &network_name])
            .output()
            .await;

        let base_dir = std::env::temp_dir().join(format!("maestro-etcd-test-{suffix}"));
        std::fs::create_dir_all(&base_dir)?;

        let mut members = Vec::with_capacity(size);
        let base_client_port = pick_port_base()?;
        for index in 0..size {
            let client_port = base_client_port + (index as u16) * 2;
            let peer_port = client_port + 1;
            let container_name = format!("etcd-test-{suffix}-{index}");
            let data_dir = base_dir.join(format!("member-{index}"));
            std::fs::create_dir_all(&data_dir)?;
            let initial_cluster: Vec<String> = (0..size)
                .map(|other| {
                    let other_peer = base_client_port + (other as u16) * 2 + 1;
                    format!(
                        "etcd-test-{suffix}-{other}=http://etcd-test-{suffix}-{other}:{other_peer}"
                    )
                })
                .collect();
            let cluster_token = format!("maestro-test-{suffix}");
            let run_args = vec![
                "--network".to_string(),
                network_name.clone(),
                "-p".to_string(),
                format!("127.0.0.1:{client_port}:{client_port}"),
                "-v".to_string(),
                format!("{}:/data", data_dir.display()),
                ETCD_IMAGE.to_string(),
                "etcd".to_string(),
                "--name".to_string(),
                container_name.clone(),
                "--data-dir".to_string(),
                "/data".to_string(),
                "--listen-client-urls".to_string(),
                format!("http://0.0.0.0:{client_port}"),
                "--advertise-client-urls".to_string(),
                format!("http://127.0.0.1:{client_port}"),
                "--listen-peer-urls".to_string(),
                format!("http://0.0.0.0:{peer_port}"),
                "--initial-advertise-peer-urls".to_string(),
                format!("http://{container_name}:{peer_port}"),
                "--initial-cluster".to_string(),
                initial_cluster.join(","),
                "--initial-cluster-state".to_string(),
                "new".to_string(),
                "--initial-cluster-token".to_string(),
                cluster_token,
            ];
            let run_args_refs: Vec<&str> = run_args.iter().map(String::as_str).collect();
            runtime
                .run_detached(&container_name, &run_args_refs)
                .await?;
            members.push(EtcdMember {
                container_name,
                client_port,
                peer_port,
                data_dir,
                run_args,
            });
        }
        let cluster = Self {
            runtime: runtime.clone(),
            members,
            network_name,
            base_dir,
        };
        cluster.await_ready().await?;
        Ok(cluster)
    }

    /// Stop the container without removing the data dir. Use with [`start_member_existing`]
    /// to test graceful restart semantics.
    async fn stop_member(&self, index: usize) -> anyhow::Result<()> {
        let name = &self
            .members
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("invalid member index"))?
            .container_name;
        let stop = tokio::process::Command::new(&self.runtime.binary)
            .args(["stop", "--time", "5", name])
            .output()
            .await?;
        if !stop.status.success() {
            anyhow::bail!("stop failed: {}", String::from_utf8_lossy(&stop.stderr));
        }
        let rm = tokio::process::Command::new(&self.runtime.binary)
            .args(["rm", name])
            .output()
            .await?;
        if !rm.status.success() {
            anyhow::bail!("rm failed: {}", String::from_utf8_lossy(&rm.stderr));
        }
        Ok(())
    }

    /// Restart a previously-stopped member with the same data dir and identity.
    /// The `--initial-cluster-state` is flipped to `existing` to match what etcd
    /// expects on rejoin.
    async fn start_member_existing(&self, index: usize) -> anyhow::Result<()> {
        let member = self
            .members
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("invalid member index"))?;
        let mut args: Vec<String> = member
            .run_args
            .iter()
            .map(|arg| {
                if arg == "new" {
                    "existing".to_string()
                } else {
                    arg.clone()
                }
            })
            .collect();
        let args_refs: Vec<&str> = args.iter_mut().map(|arg| arg.as_str()).collect();
        self.runtime
            .run_detached(&member.container_name, &args_refs)
            .await
    }

    async fn await_ready(&self) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            let mut all_ready = true;
            for member in &self.members {
                let endpoint = format!("http://127.0.0.1:{}", member.client_port);
                let result = EtcdClient::connect([&endpoint], None).await;
                if let Ok(mut client) = result {
                    if client.status().await.is_ok() {
                        continue;
                    }
                }
                all_ready = false;
                break;
            }
            if all_ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("etcd cluster did not become ready within {STARTUP_TIMEOUT:?}");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn shared_client(&self, member_index: usize) -> Arc<Mutex<EtcdClient>> {
        let endpoint = format!(
            "http://127.0.0.1:{}",
            self.members[member_index].client_port
        );
        let client = EtcdClient::connect([&endpoint], None)
            .await
            .expect("connect to etcd");
        Arc::new(Mutex::new(client))
    }

    async fn electors<const N: usize>(&self, node_ids: [&str; N]) -> Vec<Arc<EtcdLeaderElector>> {
        let mut electors = Vec::with_capacity(N);
        for (index, node_id) in node_ids.iter().enumerate() {
            let client = self.shared_client(index % self.members.len()).await;
            let elector = Arc::new(EtcdLeaderElector::new(client, node_id.to_string()));
            elector.spawn_observer().await;
            electors.push(elector);
        }
        electors
    }

    async fn registries<const N: usize>(&self, node_ids: [&str; N]) -> Vec<Arc<EtcdNodeRegistry>> {
        let mut registries = Vec::with_capacity(N);
        for (index, node_id) in node_ids.iter().enumerate() {
            let client = self.shared_client(index % self.members.len()).await;
            registries.push(Arc::new(EtcdNodeRegistry::new(client, node_id.to_string())));
        }
        registries
    }

    async fn kill_member(&self, index: usize) -> anyhow::Result<()> {
        let name = &self
            .members
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("invalid member index {index}"))?
            .container_name;
        let output = tokio::process::Command::new(&self.runtime.binary)
            .args(["kill", name])
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "failed to kill {name}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    async fn cleanup(self) {
        for member in &self.members {
            self.runtime.stop(&member.container_name).await;
        }
        let _ = tokio::process::Command::new(&self.runtime.binary)
            .args(["network", "rm", &self.network_name])
            .output()
            .await;
        let _ = std::fs::remove_dir_all(&self.base_dir);
    }

    fn member_count(&self) -> usize {
        self.members.len()
    }

    fn client_port(&self, index: usize) -> u16 {
        self.members[index].client_port
    }
}

fn pick_port_base() -> anyhow::Result<u16> {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port.max(20_000))
}

use crate::utils::nanoid;
