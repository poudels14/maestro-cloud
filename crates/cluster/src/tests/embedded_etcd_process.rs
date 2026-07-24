use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use kernel_store::{Clock, TokioClock};

use crate::embedded_etcd_plan::EtcdReadiness;
use crate::embedded_etcd_process::probe_readiness_before;
use crate::{ClusterCertificateAuthority, ClusterPorts, StoreMember, StoreProviderConfig};

use super::fixtures::validity;

#[tokio::test]
async fn readiness_probe_cannot_outlive_its_deadline() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("https://{}", listener.local_addr()?);
    let accepted = tokio::spawn(async move {
        if let Ok((connection, _peer)) = listener.accept().await {
            let _connection = connection;
            std::future::pending::<()>().await;
        }
    });
    let directory = tempfile::tempdir()?;
    let config = provider_config(directory.path())?;
    let clock = TokioClock::new();
    let deadline = clock.now().saturating_add(Duration::from_millis(100));

    let error = probe_readiness_before(
        &config,
        &endpoint,
        EtcdReadiness::WritableQuorum,
        Duration::from_secs(30),
        deadline,
        &clock,
    )
    .await
    .expect_err("a silent TLS endpoint must miss the deadline");
    accepted.abort();

    assert_eq!(error, "readiness probe deadline elapsed");
    Ok(())
}

fn provider_config(
    root: &std::path::Path,
) -> Result<StoreProviderConfig, Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("readiness-deadline-test")?;
    let node_id = NodeId::new("node-1")?;
    let member = StoreMember {
        node_id: node_id.clone(),
        host_address: Ipv4Addr::LOCALHOST,
    };
    let authority = ClusterCertificateAuthority::generate(cluster_id.as_str(), validity()?)?;
    let security = authority.issue_node_certificate(
        &node_id,
        "node-1.internal",
        member.host_address,
        NodeRole::Master,
        validity()?,
    )?;
    Ok(StoreProviderConfig::new_with_address_validator(
        cluster_id,
        member.clone(),
        BTreeMap::from([(node_id, member)]),
        ClusterPorts::new(30_000, 30_001, 30_002, 30_003)?,
        root.join("store"),
        SecretValue::new("readiness-deadline-secret-with-32-characters"),
        security,
        |address| address.is_loopback(),
    )?)
}
