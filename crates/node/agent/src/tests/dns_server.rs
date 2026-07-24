use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use hickory_server::proto::op::{Message, Query, ResponseCode};
use hickory_server::proto::rr::rdata::A;
use hickory_server::proto::rr::{Name, RData, RecordType};
use kernel_api::{
    DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, DnsRecordValue, Generation, ObjectMeta,
    ResourceRevision,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::watch;

use crate::{AuthoritativeDnsResolver, BoundDnsServer, DnsServerError, DnsServerSettings};

#[tokio::test]
async fn hickory_server_answers_udp_and_tcp_without_recursion()
-> Result<(), Box<dyn std::error::Error>> {
    let resolver = AuthoritativeDnsResolver::new()?;
    resolver
        .replace(&[record(
            "api",
            "api.maestro.internal.",
            Ipv4Addr::new(10, 42, 1, 11),
        )])
        .await?;
    let server = BoundDnsServer::bind_address((Ipv4Addr::LOCALHOST, 0).into(), resolver).await?;
    let address = server.local_address();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server_task = tokio::spawn(server.serve(shutdown_rx));

    let udp_response = udp_query(
        address,
        query_message("api.maestro.internal.", RecordType::A)?,
    )
    .await?;
    assert_eq!(udp_response.metadata.response_code, ResponseCode::NoError);
    assert!(udp_response.metadata.authoritative);
    assert!(udp_response.metadata.recursion_desired);
    assert!(!udp_response.metadata.recursion_available);
    let udp_answer = udp_response.answers.first().ok_or("UDP answer missing")?;
    assert_eq!(udp_answer.ttl, 30);
    assert_eq!(&udp_answer.data, &RData::A(A(Ipv4Addr::new(10, 42, 1, 11))));

    let missing = tcp_query(
        address,
        query_message("missing.maestro.internal.", RecordType::A)?,
    )
    .await?;
    assert_eq!(missing.metadata.response_code, ResponseCode::NXDomain);
    assert!(missing.metadata.authoritative);
    assert!(!missing.metadata.recursion_available);

    let refused = udp_query(address, query_message("example.com.", RecordType::A)?).await?;
    assert_eq!(refused.metadata.response_code, ResponseCode::Refused);
    assert!(!refused.metadata.authoritative);
    assert!(refused.answers.is_empty());

    shutdown_tx.send(true)?;
    server_task.await??;
    Ok(())
}

#[test]
fn dns_server_settings_require_a_concrete_bridge_address() -> Result<(), Box<dyn std::error::Error>>
{
    for address in [
        SocketAddr::from(([0, 0, 0, 0], 53)),
        SocketAddr::from(([127, 0, 0, 1], 53)),
        SocketAddr::from(([224, 0, 0, 1], 53)),
        SocketAddr::from(([255, 255, 255, 255], 53)),
    ] {
        assert!(matches!(
            DnsServerSettings::bridge(address),
            Err(DnsServerError::UnsafeBindAddress { .. })
        ));
    }
    assert!(matches!(
        DnsServerSettings::bridge(SocketAddr::from(([10, 42, 0, 1], 0))),
        Err(DnsServerError::ZeroPort)
    ));
    let valid = DnsServerSettings::bridge(SocketAddr::from(([10, 42, 0, 1], 53)))?;
    assert_eq!(valid.bind_address(), SocketAddr::from(([10, 42, 0, 1], 53)));
    let container = DnsServerSettings::container(53)?;
    assert_eq!(
        container.bind_address(),
        SocketAddr::from(([0, 0, 0, 0], 53))
    );
    assert!(matches!(
        DnsServerSettings::container(0),
        Err(DnsServerError::ZeroPort)
    ));
    Ok(())
}

async fn udp_query(
    address: SocketAddr,
    request: Vec<u8>,
) -> Result<Message, Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(3), async move {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        socket.send_to(&request, address).await?;
        let mut response = vec![0_u8; 4096];
        let (length, source) = socket.recv_from(&mut response).await?;
        if source != address {
            return Err(format!("unexpected DNS response source {source}").into());
        }
        response.truncate(length);
        Ok(Message::from_vec(&response)?)
    })
    .await?
}

async fn tcp_query(
    address: SocketAddr,
    request: Vec<u8>,
) -> Result<Message, Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(3), async move {
        let mut stream = TcpStream::connect(address).await?;
        let request_length = u16::try_from(request.len())?;
        stream.write_all(&request_length.to_be_bytes()).await?;
        stream.write_all(&request).await?;
        let response_length = stream.read_u16().await?;
        let mut response = vec![0_u8; usize::from(response_length)];
        stream.read_exact(&mut response).await?;
        Ok(Message::from_vec(&response)?)
    })
    .await?
}

fn query_message(
    name: &str,
    record_type: RecordType,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut message = Message::query();
    message.metadata.id = 42;
    message.metadata.recursion_desired = true;
    message.add_query(Query::query(Name::from_ascii(name)?, record_type));
    Ok(message.to_vec()?)
}

fn record(id: &str, name: &str, address: Ipv4Addr) -> DnsRecord {
    DnsRecord {
        meta: ObjectMeta {
            id: DnsRecordId::new(id).expect("dns record id"),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: DnsRecordSpec {
            name: name.to_owned(),
            values: vec![DnsRecordValue::A(address)],
            ttl_secs: 30,
        },
        status: DnsRecordStatus {
            applied_generation: Generation::default(),
            published_nodes: Vec::new(),
            conditions: Vec::new(),
        },
    }
}
