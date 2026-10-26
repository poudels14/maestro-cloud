#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use maestro_core::{DeploySpec, NodeId, ServiceId, ServiceSpec};
use maestro_proto::{CertRole, ClusterCa, NodeCredentials};
use maestro_store::{Consensus, ConsensusSettings, Mutation};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const LEADER_TIMEOUT: Duration = Duration::from_secs(10);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(10);

pub struct TestNode {
    pub consensus: Arc<Consensus>,
    pub address: String,
    pub server: JoinHandle<()>,
    pub directory: TempDir,
}

pub async fn start_cluster(
    ca: &ClusterCa,
    node_ids: &[u64],
    snapshot_every_logs: Option<u64>,
) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for node_id in node_ids {
        nodes.push(start_node(ca, *node_id, snapshot_every_logs).await);
    }
    let leader = &nodes[0].consensus;
    leader.bootstrap_single_voter().await.unwrap();
    leader.wait_until_leader(LEADER_TIMEOUT).await.unwrap();
    for node in &nodes[1..] {
        leader
            .add_learner(node.consensus.metrics().node_id, &node.address)
            .await
            .unwrap();
    }
    let promoted = nodes[1..]
        .iter()
        .map(|node| node.consensus.metrics().node_id)
        .collect();
    leader.promote(promoted).await.unwrap();
    nodes
}

pub async fn start_node(
    ca: &ClusterCa,
    node_id: u64,
    snapshot_every_logs: Option<u64>,
) -> TestNode {
    let credentials: NodeCredentials = ca
        .issue_node(node_id, CertRole::Voter, &["127.0.0.1".parse().unwrap()])
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let directory = tempfile::tempdir().unwrap();
    let mut settings = ConsensusSettings::new(NodeId(node_id), &address, directory.path());
    if let Some(threshold) = snapshot_every_logs {
        settings.snapshot_every_logs = threshold;
        settings.logs_kept_after_snapshot = 0;
    }
    let consensus = Arc::new(
        Consensus::open(settings, Some(credentials.clone()))
            .await
            .unwrap(),
    );
    let service = consensus.raft_service();
    let tls = credentials.server_tls();
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    TestNode {
        consensus,
        address,
        server,
        directory,
    }
}

pub async fn wait_for_new_leader(nodes: &[TestNode], previous: NodeId) -> &TestNode {
    let deadline = tokio::time::Instant::now() + LEADER_TIMEOUT;
    loop {
        let leader = nodes.iter().find(|node| {
            node.consensus.is_leader() && node.consensus.metrics().node_id != previous
        });
        match leader {
            Some(leader) => break leader,
            None if tokio::time::Instant::now() > deadline => panic!("no new leader elected"),
            None => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

pub async fn wait_for_service(node: &TestNode, service: &str) {
    let service_id = ServiceId::from(service);
    wait_until(|| {
        node.consensus
            .state()
            .service(&service_id)
            .unwrap()
            .is_some()
    })
    .await;
}

pub async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(REPLICATION_TIMEOUT, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("condition not reached in time");
}

pub async fn shutdown(nodes: &[TestNode]) {
    for node in nodes {
        node.consensus.shutdown().await.ok();
        node.server.abort();
    }
}

pub fn put_service(id: &str) -> Mutation {
    Mutation::PutService {
        id: ServiceId::from(id),
        spec: ServiceSpec {
            name: id.to_string(),
            image: Some("nginx:1.27".to_string()),
            build: None,
            deploy: DeploySpec::default(),
            ingress: None,
            preview: None,
        },
    }
}
