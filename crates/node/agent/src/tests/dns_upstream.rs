use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use hickory_server::proto::op::Message;
use hickory_server::proto::rr::rdata::A;
use hickory_server::proto::rr::{Name, RData, Record};
use kernel_api::DnsRecordValue;
use tokio::net::UdpSocket;

use crate::dns_upstream::{
    DirectDnsForwardClient, SystemDnsPluginSettings, SystemDnsResolverPlugin,
};
use crate::{AuthoritativeDnsResolver, DnsQueryType, DnsResolverPlugin, DnsResponseCode};

#[test]
fn system_dns_settings_require_safe_nameservers() {
    assert!(
        SystemDnsPluginSettings::from_resolv_conf(
            "search example.test\nnameserver 10.1.0.2\nnameserver 10.1.0.2\n",
            Duration::from_secs(3),
        )
        .is_ok()
    );
    assert!(
        SystemDnsPluginSettings::from_resolv_conf("search example.test\n", Duration::from_secs(3))
            .is_err()
    );
    assert!(
        SystemDnsPluginSettings::from_resolv_conf("nameserver 0.0.0.0\n", Duration::from_secs(3),)
            .is_err()
    );
}

#[tokio::test]
async fn system_dns_forwards_external_names_without_leaking_maestro_names()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let upstream = socket.local_addr()?;
    let server = tokio::spawn(async move {
        let mut request = vec![0_u8; 4_096];
        let (length, source) = socket.recv_from(&mut request).await?;
        request.truncate(length);
        let request = Message::from_vec(&request)?;
        if !request.metadata.recursion_desired {
            return Err("forwarded system DNS query did not request recursion".into());
        }
        let mut response = request.into_response();
        response.metadata.recursion_available = true;
        response.add_answer(Record::from_rdata(
            Name::from_ascii("example.com.")?,
            30,
            RData::A(A(Ipv4Addr::new(203, 0, 113, 10))),
        ));
        socket.send_to(&response.to_vec()?, source).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let plugin = SystemDnsResolverPlugin::with_client(
        vec![upstream],
        Duration::from_secs(3),
        Arc::new(DirectDnsForwardClient::default()),
    );
    let resolver = AuthoritativeDnsResolver::new()?
        .with_plugin(Arc::new(plugin) as Arc<dyn DnsResolverPlugin>);

    let missing = resolver
        .lookup("missing.maestro.internal.", DnsQueryType::A)
        .await?;
    assert_eq!(missing.response_code, DnsResponseCode::NameError);

    let external = resolver.lookup("example.com.", DnsQueryType::A).await?;
    assert!(!external.authoritative);
    assert!(external.recursion_available);
    assert_eq!(
        external.answers.first().ok_or("answer missing")?.value,
        DnsRecordValue::A(Ipv4Addr::new(203, 0, 113, 10))
    );
    server.await??;
    Ok(())
}
