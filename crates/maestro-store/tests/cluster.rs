use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::{DeploySpec, NodeId, ServiceId, ServiceSpec};
use maestro_proto::pb::Envelope;
use maestro_proto::pb::raft_client::RaftClient;
use maestro_proto::{ClusterCa, NODE_SERVER_NAME, NodeCredentials};
use maestro_store::{Consensus, ConsensusError, ConsensusSettings, Mutation};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Server};

const LEADER_TIMEOUT: Duration = Duration::from_secs(10);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(10);

struct TestNode {
    consensus: Arc<Consensus>,
    address: String,
    server: JoinHandle<()>,
    _directory: TempDir,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_writes_and_survives_leader_loss() {
    let ca = ClusterCa::generate("test").unwrap();
    let nodes = start_cluster(&ca, &[1, 2, 3], None).await;

    nodes[0]
        .consensus
        .write(vec![put_service("api")])
        .await
        .unwrap();
    for node in &nodes {
        wait_for_service(node, "api").await;
    }

    let leader = &nodes[0];
    leader.consensus.shutdown().await.unwrap();
    leader.server.abort();
    let survivors = &nodes[1..];
    let new_leader = wait_for_new_leader(survivors, NodeId(1)).await;
    assert_ne!(new_leader.consensus.metrics().node_id, NodeId(1));

    new_leader
        .consensus
        .write(vec![put_service("web")])
        .await
        .unwrap();
    for node in survivors {
        wait_for_service(node, "web").await;
    }
    shutdown(survivors).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_writes_point_at_the_leader() {
    let ca = ClusterCa::generate("test").unwrap();
    let nodes = start_cluster(&ca, &[1, 2, 3], None).await;
    let leader = &nodes[0];
    let follower = &nodes[1];
    wait_until(|| follower.consensus.current_leader().is_some()).await;

    assert_eq!(
        follower.consensus.current_leader(),
        Some((NodeId(1), leader.address.clone()))
    );
    let rejected = follower.consensus.write(vec![put_service("api")]).await;
    match rejected {
        Err(ConsensusError::ForwardToLeader {
            leader_id,
            leader_address,
        }) => {
            assert_eq!(leader_id, Some(NodeId(1)));
            assert_eq!(leader_address, Some(leader.address.clone()));
        }
        other => panic!("expected ForwardToLeader, got {other:?}"),
    }
    assert!(!follower.consensus.is_leader());
    let metrics = follower.consensus.metrics();
    assert_eq!(
        metrics.voters.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([NodeId(1), NodeId(2), NodeId(3)])
    );
    assert!(metrics.learners.is_empty());
    shutdown(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_learner_catches_up_from_a_snapshot() {
    let ca = ClusterCa::generate("test").unwrap();
    let nodes = start_cluster(&ca, &[1, 2, 3], Some(5)).await;
    let leader = &nodes[0].consensus;
    for index in 0..20 {
        leader
            .write(vec![put_service(&format!("service-{index}"))])
            .await
            .unwrap();
    }
    wait_until(|| {
        leader
            .metrics()
            .snapshot_index
            .is_some_and(|index| index >= 15)
    })
    .await;

    let late = start_node(&ca, 4, None).await;
    leader.add_learner(NodeId(4), &late.address).await.unwrap();
    wait_for_service(&late, "service-19").await;
    assert_eq!(late.consensus.state().services().unwrap().len(), 20);
    assert!(late.consensus.metrics().snapshot_index.is_some());

    leader.promote(BTreeSet::from([NodeId(4)])).await.unwrap();
    leader.remove_node(NodeId(3)).await.unwrap();
    let metrics = leader.metrics();
    assert_eq!(
        metrics.voters.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([NodeId(1), NodeId(2), NodeId(4)])
    );
    shutdown(&nodes).await;
    shutdown(std::slice::from_ref(&late)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_peers_without_a_client_certificate() {
    let ca = ClusterCa::generate("test").unwrap();
    let node = start_node(&ca, 1, None).await;
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca.cert_pem()))
        .domain_name(NODE_SERVER_NAME);
    let channel = Channel::from_shared(format!("https://{}", node.address))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect()
        .await
        .unwrap();

    let status = RaftClient::new(channel)
        .vote(Envelope {
            json: b"{}".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    shutdown(std::slice::from_ref(&node)).await;
}

async fn start_cluster(
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

async fn start_node(ca: &ClusterCa, node_id: u64, snapshot_every_logs: Option<u64>) -> TestNode {
    let credentials: NodeCredentials = ca
        .issue_node(node_id, &["127.0.0.1".parse().unwrap()])
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
        _directory: directory,
    }
}

async fn wait_for_new_leader(nodes: &[TestNode], previous: NodeId) -> &TestNode {
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

async fn wait_for_service(node: &TestNode, service: &str) {
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

async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(REPLICATION_TIMEOUT, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("condition not reached in time");
}

async fn shutdown(nodes: &[TestNode]) {
    for node in nodes {
        node.consensus.shutdown().await.ok();
        node.server.abort();
    }
}

fn put_service(id: &str) -> Mutation {
    Mutation::PutService {
        id: ServiceId::from(id),
        spec: ServiceSpec {
            name: id.to_string(),
            image: Some("nginx:1.27".to_string()),
            build: None,
            deploy: DeploySpec::default(),
            ingress: None,
        },
    }
}
