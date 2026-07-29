use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, DnsRecordValue, Generation,
    ObjectMeta, ResourceRevision,
};

use crate::dns_plugin::TailscaleDnsResolverPlugin;
use crate::dns_socks::DnsForwardClient;
use crate::{
    AuthoritativeDnsResolver, DnsAnswer, DnsLookup, DnsQueryType, DnsResolverPluginError,
    DnsResponseCode, TailscaleDnsRoute,
};

#[derive(Default)]
struct RecordingClient {
    calls: Mutex<Vec<(SocketAddr, SocketAddr, String, DnsQueryType)>>,
}

#[async_trait]
impl DnsForwardClient for RecordingClient {
    async fn lookup(
        &self,
        proxy: SocketAddr,
        nameserver: SocketAddr,
        name: &str,
        query_type: DnsQueryType,
        _timeout: Duration,
    ) -> Result<DnsLookup, DnsResolverPluginError> {
        self.calls
            .lock()
            .map_err(|_| DnsResolverPluginError::new("recording client lock poisoned"))?
            .push((proxy, nameserver, name.to_owned(), query_type));
        Ok(DnsLookup {
            authoritative: true,
            recursion_available: false,
            response_code: DnsResponseCode::NoError,
            answers: vec![DnsAnswer {
                name: name.to_owned(),
                ttl_secs: 20,
                value: DnsRecordValue::A(Ipv4Addr::new(172, 23, 1, 44)),
            }],
        })
    }
}

#[tokio::test]
async fn plugin_forwards_only_declared_remote_cluster_suffixes()
-> Result<(), Box<dyn std::error::Error>> {
    let base = AuthoritativeDnsResolver::new()?;
    base.replace(&[record(
        "gateway",
        "maestro-system-tailscale-gateway.local.maestro.internal.",
        Ipv4Addr::new(172, 22, 1, 8),
    )])
    .await?;
    let client = Arc::new(RecordingClient::default());
    let plugin = TailscaleDnsResolverPlugin::with_client(
        base.zone_reader(),
        &ClusterId::new("local")?,
        vec![TailscaleDnsRoute::new(
            ClusterId::new("remote")?,
            vec![Ipv4Addr::new(172, 23, 1, 1)],
        )],
        Duration::from_secs(3),
        client.clone(),
    );
    let resolver = base.with_plugin(Arc::new(plugin));

    let remote = resolver
        .lookup("api.remote.maestro.internal.", DnsQueryType::A)
        .await?;
    assert_eq!(remote.response_code, DnsResponseCode::NoError);
    assert_eq!(
        remote.answers.first().ok_or("remote answer missing")?.value,
        DnsRecordValue::A(Ipv4Addr::new(172, 23, 1, 44))
    );
    let missing_local = resolver
        .lookup("missing.local.maestro.internal.", DnsQueryType::A)
        .await?;
    assert_eq!(missing_local.response_code, DnsResponseCode::NameError);
    let outside = resolver.lookup("example.com.", DnsQueryType::A).await?;
    assert_eq!(outside.response_code, DnsResponseCode::Refused);

    let calls = client.calls.lock().map_err(|_| "client lock poisoned")?;
    assert_eq!(
        calls.as_slice(),
        [(
            SocketAddr::from((Ipv4Addr::new(172, 22, 1, 8), 1_055)),
            SocketAddr::from((Ipv4Addr::new(172, 23, 1, 1), 53)),
            "api.remote.maestro.internal.".to_owned(),
            DnsQueryType::A,
        )]
    );
    Ok(())
}

#[tokio::test]
async fn plugin_requires_a_ready_managed_gateway() -> Result<(), Box<dyn std::error::Error>> {
    let base = AuthoritativeDnsResolver::new()?;
    let plugin = TailscaleDnsResolverPlugin::with_client(
        base.zone_reader(),
        &ClusterId::new("local")?,
        vec![TailscaleDnsRoute::new(
            ClusterId::new("remote")?,
            vec![Ipv4Addr::new(172, 23, 1, 1)],
        )],
        Duration::from_secs(3),
        Arc::new(RecordingClient::default()),
    );
    let resolver = base.with_plugin(Arc::new(plugin));

    let error = resolver
        .lookup("api.remote.maestro.internal.", DnsQueryType::A)
        .await
        .expect_err("remote query must fail without a ready Tailscale gateway");
    assert!(
        error
            .to_string()
            .contains("no ready managed Tailscale gateway")
    );
    Ok(())
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
