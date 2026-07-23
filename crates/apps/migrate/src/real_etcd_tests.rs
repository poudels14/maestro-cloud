use std::collections::BTreeMap;
use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cluster::{
    CertificateValidity, ClusterCertificateAuthority, ClusterPorts, EmbeddedEtcdProvider,
    EmbeddedEtcdSettings, StoreMember, StoreProvider, StoreProviderConfig, StoreShutdown,
    StoreStartMode,
};
use etcd_client::{Certificate, Client, ConnectOptions, Identity, TlsOptions};
use kernel_api::{ClusterId, NodeId, NodeRole, ResourceKind, ResourceName, SecretValue};
use kernel_store::{Keyspace, Store, TokioClock};
use serde_json::json;
use time::{Duration as CertificateDuration, OffsetDateTime};

use crate::legacy_fixtures::CLUSTER_ID;
use crate::legacy_tests::{
    MASTER_SECRET, cutover_service_snapshot, encrypted as encrypted_fixture,
};
use crate::{
    CutoverEtcdConnection, CutoverMigration, LegacyEtcdSource, MigrationError, MigrationOutcome,
    MigrationVerification, plan_legacy_snapshot,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires MAESTRO_ETCD_BIN and MAESTRO_ETCD_TEST_IP"]
async fn real_cutover_fences_and_commits_encrypted_destinations() -> TestResult {
    let binary = PathBuf::from(std::env::var("MAESTRO_ETCD_BIN")?);
    let host_address = std::env::var("MAESTRO_ETCD_TEST_IP")?.parse::<Ipv4Addr>()?;
    if !host_address.is_private() || host_address.is_loopback() {
        return Err("MAESTRO_ETCD_TEST_IP must be a private non-loopback address".into());
    }

    let directory = tempfile::tempdir()?;
    let ports = allocate_ports(host_address)?;
    let config = provider_config(directory.path(), host_address, ports)?;
    let endpoint = format!("https://{host_address}:{}", ports.store_client);
    let connection = cutover_connection(&config, endpoint.clone())?;
    let provider = EmbeddedEtcdProvider::new(
        config.clone(),
        binary,
        Arc::new(TokioClock::new()),
        EmbeddedEtcdSettings::new(
            Duration::from_secs(30),
            Duration::from_secs(2),
            Duration::from_millis(100),
            Duration::from_secs(1),
        )?,
    )?;
    let runtime = provider.start(StoreStartMode::Bootstrap).await?;
    let mut raw = raw_client(&config, &endpoint).await?;

    let snapshot = cutover_service_snapshot(vec![
        encrypted_fixture(
            "/maetro/services/api/deploy-1/deploy/env",
            json!({"PUBLIC_NAME": "api"}),
        )?,
        encrypted_fixture(
            "/maetro/services/api/deploy-1/deploy/secrets",
            json!({"TOKEN": "secret"}),
        )?,
    ])?;
    for entry in snapshot.entries() {
        raw.put(entry.key(), entry.value(), None).await?;
    }

    let mut source = connection.legacy_source().await?;
    let captured = source.capture().await?;
    assert_eq!(captured.snapshot(), &snapshot);
    let plan = plan_legacy_snapshot(captured.snapshot(), MASTER_SECRET)?;
    let keyspace = Keyspace::new(plan.cluster_id());
    let store: Arc<dyn Store> = Arc::new(connection.destination_store(MASTER_SECRET).await?);
    let migration = CutoverMigration::new(
        store.clone(),
        keyspace.clone(),
        ResourceName::new("real-etcd-rehearsal")?,
    );
    let expected_digest = snapshot.digest();
    let outcome = migration
        .apply_guarded(&plan, || source_matches(&mut source, expected_digest))
        .await?;
    assert_eq!(
        outcome,
        MigrationOutcome::Applied {
            resources: plan.writes().len(),
            request_claims: plan.request_claims().len(),
            written: plan.writes().len() + plan.request_claims().len(),
            reused: 0,
        }
    );
    assert_eq!(
        migration.verify(&plan).await?,
        MigrationVerification::Verified {
            resources: plan.writes().len(),
            request_claims: plan.request_claims().len(),
            source_sha256: hex::encode(expected_digest),
        }
    );
    for write in plan.writes() {
        let key = keyspace.resource(&ResourceKind::new(write.kind().as_str())?, write.id());
        assert_eq!(
            store.get(&key).await?.map(|stored| stored.value),
            Some(write.value().to_vec())
        );
    }

    raw.put(
        "/maetro/services/api/deployments/history-next-index",
        "2",
        None,
    )
    .await?;
    assert!(matches!(
        migration
            .apply_guarded(&plan, || source_matches(&mut source, expected_digest))
            .await,
        Err(MigrationError::SourceFence { .. })
    ));

    runtime.shutdown(StoreShutdown::Immediate).await?;
    Ok(())
}

async fn source_matches(
    source: &mut LegacyEtcdSource,
    expected_digest: [u8; 32],
) -> Result<(), String> {
    let captured = source.capture().await.map_err(|error| error.to_string())?;
    if captured.snapshot().digest() == expected_digest {
        Ok(())
    } else {
        Err("legacy digest changed".to_owned())
    }
}

fn provider_config(
    root: &Path,
    host_address: Ipv4Addr,
    ports: ClusterPorts,
) -> Result<StoreProviderConfig, Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new(CLUSTER_ID)?;
    let node_id = NodeId::new("node-a")?;
    let member = StoreMember {
        node_id: node_id.clone(),
        host_address,
    };
    let authority = ClusterCertificateAuthority::generate(CLUSTER_ID, certificate_validity()?)?;
    let security = authority.issue_node_certificate(
        &node_id,
        "node-a.internal",
        host_address,
        NodeRole::Master,
        certificate_validity()?,
    )?;
    Ok(StoreProviderConfig::new(
        cluster_id,
        member.clone(),
        BTreeMap::from([(node_id, member)]),
        ports,
        root.join("store"),
        SecretValue::new("provider-only-encryption-secret-with-32-characters"),
        security,
    )?)
}

fn cutover_connection(
    config: &StoreProviderConfig,
    endpoint: String,
) -> Result<CutoverEtcdConnection, Box<dyn std::error::Error>> {
    let security = config.security();
    Ok(CutoverEtcdConnection::new(
        vec![endpoint],
        security.trust_root_pem.as_bytes().to_vec(),
        security.identity.certificate_pem.as_bytes().to_vec(),
        security
            .identity
            .private_key_pem
            .expose()
            .as_bytes()
            .to_vec(),
    )?)
}

async fn raw_client(
    config: &StoreProviderConfig,
    endpoint: &str,
) -> Result<Client, etcd_client::Error> {
    let security = config.security();
    let tls = TlsOptions::new()
        .ca_certificate(Certificate::from_pem(security.trust_root_pem.as_bytes()))
        .identity(Identity::from_pem(
            security.identity.certificate_pem.as_bytes(),
            security.identity.private_key_pem.expose().as_bytes(),
        ));
    Client::connect([endpoint], Some(ConnectOptions::new().with_tls(tls))).await
}

fn allocate_ports(host_address: Ipv4Addr) -> Result<ClusterPorts, Box<dyn std::error::Error>> {
    let mut listeners = Vec::new();
    let mut ports = Vec::new();
    while ports.len() < 4 {
        let listener = TcpListener::bind((host_address, 0))?;
        let port = listener.local_addr()?.port();
        if !ports.contains(&port) {
            ports.push(port);
            listeners.push(listener);
        }
    }
    let [gateway, store_client, store_peer, wireguard]: [u16; 4] = ports
        .try_into()
        .map_err(|_| "failed to allocate rehearsal ports")?;
    drop(listeners);
    Ok(ClusterPorts::new(
        gateway,
        store_client,
        store_peer,
        wireguard,
    )?)
}

fn certificate_validity() -> Result<CertificateValidity, Box<dyn std::error::Error>> {
    let now = OffsetDateTime::now_utc();
    Ok(CertificateValidity::new(
        now - CertificateDuration::days(1),
        now + CertificateDuration::days(3_650),
    )?)
}
