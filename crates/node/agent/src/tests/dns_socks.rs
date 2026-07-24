use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use hickory_server::proto::op::Message;
use hickory_server::proto::rr::rdata::A;
use hickory_server::proto::rr::{Name, RData, Record};
use kernel_api::DnsRecordValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::DnsQueryType;
use crate::dns_socks::{DnsForwardClient, Socks5DnsForwardClient};

#[tokio::test]
async fn socks_client_connects_to_the_declared_nameserver_and_decodes_dns_tcp()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let proxy = listener.local_addr()?;
    let nameserver = SocketAddr::from((Ipv4Addr::new(172, 23, 1, 1), 53));
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut greeting = [0_u8; 3];
        stream.read_exact(&mut greeting).await?;
        if greeting != [5, 1, 0] {
            return Err("unexpected SOCKS greeting".into());
        }
        stream.write_all(&[5, 0]).await?;

        let mut connect = [0_u8; 10];
        stream.read_exact(&mut connect).await?;
        let expected = [5, 1, 0, 1, 172, 23, 1, 1, 0, 53];
        if connect != expected {
            return Err("unexpected SOCKS target".into());
        }
        stream.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 53]).await?;

        let request_length = stream.read_u16().await?;
        let mut request = vec![0_u8; usize::from(request_length)];
        stream.read_exact(&mut request).await?;
        let mut response = Message::from_vec(&request)?.into_response();
        response.metadata.authoritative = true;
        response.add_answer(Record::from_rdata(
            Name::from_ascii("api.remote.maestro.internal.")?,
            30,
            RData::A(A(Ipv4Addr::new(172, 23, 1, 44))),
        ));
        let response = response.to_vec()?;
        stream
            .write_all(&u16::try_from(response.len())?.to_be_bytes())
            .await?;
        stream.write_all(&response).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let lookup = Socks5DnsForwardClient::default()
        .lookup(
            proxy,
            nameserver,
            "api.remote.maestro.internal.",
            DnsQueryType::A,
            Duration::from_secs(3),
        )
        .await?;
    assert!(lookup.authoritative);
    assert_eq!(
        lookup.answers.first().ok_or("answer missing")?.value,
        DnsRecordValue::A(Ipv4Addr::new(172, 23, 1, 44))
    );
    server.await??;
    Ok(())
}
