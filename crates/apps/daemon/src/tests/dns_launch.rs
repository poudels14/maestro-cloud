use std::path::PathBuf;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId};

use crate::{DnsResolverLaunchConfig, DnsResolverLaunchError};

#[test]
fn delegated_dns_launch_requires_secure_complete_coordinates()
-> Result<(), Box<dyn std::error::Error>> {
    let valid = config()?;
    valid.validate()?;

    let mut empty_endpoints = valid.clone();
    empty_endpoints.endpoints.clear();
    assert!(matches!(
        empty_endpoints.validate(),
        Err(DnsResolverLaunchError::InvalidConfiguration { .. })
    ));

    let mut plaintext_endpoint = valid.clone();
    plaintext_endpoint.endpoints = vec!["http://127.0.0.1:2379".to_owned()];
    assert!(matches!(
        plaintext_endpoint.validate(),
        Err(DnsResolverLaunchError::InvalidConfiguration { .. })
    ));

    let mut relative_secret = valid.clone();
    relative_secret.store_encryption_secret = PathBuf::from("store-key");
    assert!(matches!(
        relative_secret.validate(),
        Err(DnsResolverLaunchError::InvalidConfiguration { .. })
    ));

    let mut zero_port = valid.clone();
    zero_port.port = 0;
    assert!(matches!(
        zero_port.validate(),
        Err(DnsResolverLaunchError::Server(
            node_agent::DnsServerError::ZeroPort
        ))
    ));

    let mut zero_resync = valid;
    zero_resync.resync_interval = Duration::ZERO;
    assert!(matches!(
        zero_resync.validate(),
        Err(DnsResolverLaunchError::InvalidConfiguration { .. })
    ));
    Ok(())
}

fn config() -> Result<DnsResolverLaunchConfig, kernel_api::InvalidIdentifier> {
    Ok(DnsResolverLaunchConfig {
        cluster_id: ClusterId::new("cluster-a")?,
        node_id: NodeId::new("node-a")?,
        endpoints: vec!["https://10.0.0.10:2379".to_owned()],
        certificate_authority: PathBuf::from("/run/secrets/etcd/ca.pem"),
        client_certificate: PathBuf::from("/run/secrets/etcd/client.pem"),
        client_private_key: PathBuf::from("/run/secrets/etcd/client-key.pem"),
        store_encryption_secret: PathBuf::from("/run/secrets/etcd/store-key"),
        port: 53,
        resync_interval: Duration::from_secs(30),
        dns_plugin_settings: None,
    })
}
