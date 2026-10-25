mod support;

use std::collections::BTreeSet;

use maestro_core::NodeId;
use maestro_proto::pb::Envelope;
use maestro_proto::pb::raft_client::RaftClient;
use maestro_proto::{ClusterCa, NODE_SERVER_NAME};
use maestro_store::ConsensusError;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use support::{
    put_service, shutdown, start_cluster, start_node, wait_for_new_leader, wait_for_service,
    wait_until,
};

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
