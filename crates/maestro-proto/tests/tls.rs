use std::net::SocketAddr;

use maestro_proto::pb::Envelope;
use maestro_proto::pb::raft_client::RaftClient;
use maestro_proto::pb::raft_server::{Raft, RaftServer};
use maestro_proto::{CertRole, ClusterCa, NodeCredentials, connect_pinned};
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

struct Echo;

#[tonic::async_trait]
impl Raft for Echo {
    async fn append_entries(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let authenticated = request.peer_certs().is_some_and(|certs| !certs.is_empty());
        let mut reply = request.into_inner();
        reply.json.extend_from_slice(match authenticated {
            true => b"+mtls",
            false => b"+anonymous",
        });
        Ok(Response::new(reply))
    }

    async fn vote(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Ok(Response::new(request.into_inner()))
    }

    async fn install_snapshot(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        Ok(Response::new(request.into_inner()))
    }
}

async fn serve(credentials: &NodeCredentials) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let tls = credentials.server_tls();
    tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(RaftServer::new(Echo))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    address
}

async fn call(channel: Channel) -> Result<String, Status> {
    let reply = RaftClient::new(channel)
        .append_entries(Envelope {
            json: b"ping".to_vec(),
        })
        .await?;
    Ok(String::from_utf8(reply.into_inner().json).unwrap())
}

async fn mtls_channel(
    address: SocketAddr,
    credentials: &NodeCredentials,
) -> Result<Channel, tonic::transport::Error> {
    Channel::from_shared(format!("https://{address}"))
        .unwrap()
        .tls_config(credentials.client_tls())?
        .connect()
        .await
}

#[tokio::test]
async fn nodes_authenticate_each_other_with_cluster_certificates() {
    let ca = ClusterCa::generate("test").unwrap();
    let server = ca
        .issue_node(1, CertRole::Voter, &["127.0.0.1".parse().unwrap()])
        .unwrap();
    let client = ca.issue_node(2, CertRole::Voter, &[]).unwrap();
    let address = serve(&server).await;

    let channel = mtls_channel(address, &client).await.unwrap();
    assert_eq!(call(channel).await.unwrap(), "ping+mtls");
}

#[tokio::test]
async fn joining_nodes_pin_the_ca_fingerprint() {
    let ca = ClusterCa::generate("test").unwrap();
    let server = ca.issue_node(1, CertRole::Voter, &[]).unwrap();
    let address = serve(&server).await;

    let channel = connect_pinned(&address.to_string(), &ca.fingerprint().unwrap())
        .await
        .unwrap();
    assert_eq!(call(channel).await.unwrap(), "ping+anonymous");

    let impostor = ClusterCa::generate("other").unwrap();
    let rejected = connect_pinned(&address.to_string(), &impostor.fingerprint().unwrap()).await;
    let failed = match rejected {
        Ok(channel) => call(channel).await.is_err(),
        Err(_) => true,
    };
    assert!(failed);
}

#[tokio::test]
async fn rejects_certificates_from_another_cluster() {
    let ca = ClusterCa::generate("test").unwrap();
    let server = ca.issue_node(1, CertRole::Voter, &[]).unwrap();
    let address = serve(&server).await;
    let foreign = ClusterCa::generate("other")
        .unwrap()
        .issue_node(9, CertRole::Voter, &[])
        .unwrap();

    let connected = mtls_channel(address, &foreign).await;
    let failed = match connected {
        Ok(channel) => call(channel).await.is_err(),
        Err(_) => true,
    };
    assert!(failed);
}

#[test]
fn persists_ca_and_node_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let ca = ClusterCa::generate("test").unwrap();
    ca.save(directory.path()).unwrap();
    let loaded = ClusterCa::load(directory.path()).unwrap();
    assert_eq!(loaded.fingerprint().unwrap(), ca.fingerprint().unwrap());
    let credentials = loaded.issue_node(3, CertRole::Voter, &[]).unwrap();
    credentials.save(&directory.path().join("node")).unwrap();
    assert_eq!(
        NodeCredentials::load(&directory.path().join("node")).unwrap(),
        credentials
    );
}

#[test]
fn node_certificates_carry_their_identity() {
    let ca = ClusterCa::generate("prod").unwrap();
    let voter = ca.issue_node(7, CertRole::Voter, &[]).unwrap();
    let worker = ca.issue_node(9, CertRole::Worker, &[]).unwrap();
    assert_eq!(
        voter.identity(),
        Some(maestro_proto::PeerIdentity {
            node_id: 7,
            role: Some(CertRole::Voter),
        })
    );
    assert_eq!(
        worker.identity().map(|identity| identity.role),
        Some(Some(CertRole::Worker))
    );
}

#[tokio::test]
async fn control_plane_clients_only_accept_voter_servers() {
    let ca = ClusterCa::generate("test").unwrap();
    let client = ca.issue_node(2, CertRole::Worker, &[]).unwrap();
    let voter = serve(&ca.issue_node(1, CertRole::Voter, &[]).unwrap()).await;
    let impostor = serve(&ca.issue_node(3, CertRole::Worker, &[]).unwrap()).await;
    let connect = |address: SocketAddr| {
        let tls = client.voter_client_tls();
        async move {
            let channel = Channel::from_shared(format!("https://{address}"))
                .unwrap()
                .tls_config(tls)?
                .connect()
                .await?;
            Ok::<_, Box<dyn std::error::Error>>(call(channel).await?)
        }
    };
    assert_eq!(connect(voter).await.unwrap(), "ping+mtls");
    assert!(connect(impostor).await.is_err());
}
