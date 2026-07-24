use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cluster::{CertificateKeyPair, NodeCertificateBundle};
use kernel_api::{
    ArtifactTemplate, ClusterId, HealthProbe, NodeApiAccess, NodeId, NodeInstanceId, NodeRole,
    ResourceKind, SecretMountSpec, SecretValue, Service, Timestamp,
};
use kernel_controller::{FencedStore, LeaderIdentity, LeadershipToken};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};
use semver::Version;

use crate::dns_reconciler::DnsResolverResourceReconciler;
use crate::dns_resources::{DNS_RESOLVER_SERVICE_ID, DnsResolverSystemResources};

use super::cluster_with_nodes;

#[test]
fn builds_store_authenticated_delegated_dns_service() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    let node_id = NodeId::new("master")?;
    let resources = DnsResolverSystemResources::for_docker_node(
        &cluster,
        &node_id,
        &security(),
        &SecretValue::new("store-encryption-secret-with-32-characters"),
        &Version::parse("1.2.3")?,
    )?;
    let service = &resources.service;
    assert_eq!(service.meta.id.as_str(), DNS_RESOLVER_SERVICE_ID);
    assert_eq!(service.spec.version, "maestro-dns-1.2.3");
    assert_eq!(service.spec.replicas, 1);
    assert_eq!(service.spec.node_api, NodeApiAccess::Disabled);
    assert_eq!(service.spec.placement.node_id, Some(node_id));
    assert!(matches!(
        &service.spec.artifact,
        ArtifactTemplate::Image { reference } if reference == "maestro-daemon:1.2.3"
    ));
    assert!(matches!(
        service.spec.health_check.as_ref().map(|check| &check.probe),
        Some(HealthProbe::Tcp { port: 53 })
    ));
    let command = service
        .spec
        .command
        .as_ref()
        .ok_or("DNS resolver command is missing")?;
    assert_eq!(command.executable, "/bin/maestro-daemon");
    assert_eq!(
        command.arguments,
        [
            "dns",
            "--cluster-id",
            "daemon-test",
            "--node-id",
            "master",
            "--endpoint",
            "https://10.20.0.11:2379",
            "--certificate-authority",
            "/run/secrets/dns/ca.pem",
            "--client-certificate",
            "/run/secrets/dns/client.pem",
            "--client-private-key",
            "/run/secrets/dns/client-key.pem",
            "--store-encryption-secret",
            "/run/secrets/dns/store-key",
        ]
    );
    assert!(
        !command
            .arguments
            .iter()
            .any(|argument| argument.contains("32-characters"))
    );
    let Some(SecretMountSpec::Files { mount_path, files }) = &service.spec.secrets else {
        return Err("DNS resolver credentials are not a file set".into());
    };
    assert_eq!(mount_path, "/run/secrets/dns");
    assert_eq!(
        files.get("store-key").map(SecretValue::expose),
        Some("store-encryption-secret-with-32-characters")
    );
    Ok(())
}

#[tokio::test]
async fn reconciles_dns_service_create_update_and_removal() -> Result<(), Box<dyn std::error::Error>>
{
    let cluster_id = ClusterId::new("dns-reconcile")?;
    let (store, fenced, _session) = fenced_store(&cluster_id).await?;
    let mut cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    cluster.cluster_id = cluster_id.clone();
    let mut desired = DnsResolverSystemResources::for_docker_node(
        &cluster,
        &NodeId::new("master")?,
        &security(),
        &SecretValue::new("store-encryption-secret-with-32-characters"),
        &Version::parse("1.2.3")?,
    )?;

    DnsResolverResourceReconciler::new(&cluster_id, Some(desired.clone()))?
        .reconcile(&fenced, Timestamp(10_000))
        .await?;
    let service = read_service(&store, &cluster_id).await?;
    assert_eq!(service.meta.generation.0, 1);
    assert_eq!(service.spec.version, "maestro-dns-1.2.3");

    desired.service.spec.version = "maestro-dns-1.2.4".to_owned();
    DnsResolverResourceReconciler::new(&cluster_id, Some(desired))?
        .reconcile(&fenced, Timestamp(20_000))
        .await?;
    let service = read_service(&store, &cluster_id).await?;
    assert_eq!(service.meta.generation.0, 2);
    assert_eq!(service.spec.version, "maestro-dns-1.2.4");

    DnsResolverResourceReconciler::new(&cluster_id, None)?
        .reconcile(&fenced, Timestamp(30_000))
        .await?;
    assert_eq!(
        read_service(&store, &cluster_id)
            .await?
            .meta
            .deletion_timestamp,
        Some(Timestamp(30_000))
    );
    Ok(())
}

fn security() -> NodeCertificateBundle {
    NodeCertificateBundle {
        trust_root_pem: "test-ca".to_owned(),
        identity: CertificateKeyPair {
            certificate_pem: "test-client".to_owned(),
            private_key_pem: SecretValue::new("test-client-key"),
        },
    }
}

async fn fenced_store(
    cluster_id: &ClusterId,
) -> Result<(Arc<InMemoryStore>, FencedStore, Box<dyn Session>), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
    let keys = Keyspace::new(cluster_id);
    let session = store.session(Duration::from_secs(30)).await?;
    let outcome = store
        .put_cas(PutRequest {
            key: keys.leader(),
            value: b"dns-reconciler".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await?;
    let CasOutcome::Applied(leader) = outcome else {
        return Err("leader campaign conflicted".into());
    };
    let fenced = FencedStore::new(
        store.clone(),
        keys.leader(),
        LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: NodeId::new("master")?,
                instance_id: NodeInstanceId::new("dns-reconciler")?,
            },
            session.id(),
            leader.version,
        ),
    );
    Ok((store, fenced, session))
}

async fn read_service(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
) -> Result<Service, Box<dyn std::error::Error>> {
    let values = store
        .list(&Keyspace::new(cluster_id).resource_kind(&ResourceKind::new("Service")?))
        .await?
        .values;
    let [stored] = values.as_slice() else {
        return Err(format!("expected one DNS resolver Service, found {}", values.len()).into());
    };
    Ok(serde_json::from_slice(&stored.value)?)
}

struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}
