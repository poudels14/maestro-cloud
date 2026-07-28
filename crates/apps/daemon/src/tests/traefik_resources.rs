use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cluster::{CertificateKeyPair, NodeCertificateBundle};
use kernel_api::{
    ArtifactTemplate, ClusterId, HealthProbe, NodeApiAccess, NodeId, NodeInstanceId, NodeRole,
    ReplicaSpread, ResourceKind, SecretMountSpec, SecretValue, Service, ServiceId, Timestamp,
    WorkloadUserSpec,
};
use kernel_controller::{FencedStore, LeaderIdentity, LeadershipToken};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};
use runtime::{HostPortPublication, PortProtocol};

use crate::system_service_reconciler::SystemServiceReconciler;
use crate::traefik_resources::{
    TRAEFIK_IMAGE, TRAEFIK_MANAGED_OWNER, TRAEFIK_SERVICE_ID, TraefikSystemResources,
};

use super::cluster_with_nodes;

#[test]
fn builds_cluster_wide_mtls_ingress_service_and_host_publications()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster = cluster_with_nodes(&[
        ("master", NodeRole::Master),
        ("worker", NodeRole::Worker),
        ("control", NodeRole::ControlPlane),
    ])?;
    let resources = TraefikSystemResources::for_cluster(&cluster, &security())?;
    let service = &resources.service;
    assert_eq!(service.meta.id.as_str(), TRAEFIK_SERVICE_ID);
    assert_eq!(service.spec.name, "Traefik");
    assert_eq!(service.spec.replicas, 2);
    assert_eq!(service.spec.node_api, NodeApiAccess::IdentityAndTelemetry);
    assert_eq!(
        service.spec.user,
        Some(WorkloadUserSpec {
            user_id: 0,
            group_id: 0
        })
    );
    assert_eq!(
        service.spec.placement.replica_spread,
        ReplicaSpread::BestEffort
    );
    assert!(matches!(
        &service.spec.artifact,
        ArtifactTemplate::Image { reference } if reference == TRAEFIK_IMAGE
    ));
    let command = service
        .spec
        .command
        .as_ref()
        .ok_or("Traefik command is missing")?;
    assert_eq!(command.executable, "/usr/local/bin/traefik");
    for expected in [
        "--providers.etcd=true",
        "--providers.etcd.rootKey=maestro/clusters/daemon-test/integrations/traefik",
        "--providers.etcd.tls.ca=/run/secrets/etcd/ca.pem",
        "--providers.etcd.tls.cert=/run/secrets/etcd/client.pem",
        "--providers.etcd.tls.key=/run/secrets/etcd/client-key.pem",
        "--entrypoints.web.address=:80",
        "--entrypoints.websecure.address=:443",
        "--accesslog.format=json",
    ] {
        assert!(
            command
                .arguments
                .iter()
                .any(|argument| argument == expected)
        );
    }
    let endpoints = command
        .arguments
        .iter()
        .find(|argument| argument.starts_with("--providers.etcd.endpoints="))
        .ok_or("Traefik etcd endpoints are missing")?;
    assert!(endpoints.contains("10.20.0.11:2379"));
    assert!(endpoints.contains("10.20.0.13:2379"));
    assert!(!endpoints.contains("10.20.0.12:2379"));
    assert!(
        !command
            .arguments
            .iter()
            .any(|argument| argument.contains("metrics.prometheus"))
    );
    assert!(matches!(
        service.spec.health_check.as_ref().map(|check| &check.probe),
        Some(HealthProbe::Http { port: 80, path }) if path == "/ping"
    ));
    let Some(SecretMountSpec::Files { mount_path, files }) = &service.spec.secrets else {
        return Err("Traefik etcd credentials are not a file set".into());
    };
    assert_eq!(mount_path, "/run/secrets/etcd");
    assert_eq!(
        files.get("ca.pem").map(SecretValue::expose),
        Some("test-ca")
    );
    assert_eq!(
        files.get("client.pem").map(SecretValue::expose),
        Some("test-client")
    );
    assert_eq!(
        files.get("client-key.pem").map(SecretValue::expose),
        Some("test-client-key")
    );
    assert_eq!(
        resources.host_port_grants(),
        std::collections::BTreeMap::from([(
            ServiceId::new(TRAEFIK_SERVICE_ID)?,
            vec![publication(80), publication(443)]
        )])
    );
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    assert_eq!(
        resources.firewall_routes(),
        vec![
            firewall::HostPortRoute {
                service_id: ServiceId::new(TRAEFIK_SERVICE_ID)?,
                host_port: 80,
                workload_port: 80,
                protocol: firewall::HostPortProtocol::Tcp,
            },
            firewall::HostPortRoute {
                service_id: ServiceId::new(TRAEFIK_SERVICE_ID)?,
                host_port: 443,
                workload_port: 443,
                protocol: firewall::HostPortProtocol::Tcp,
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn reconciles_create_update_and_removal_under_one_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("traefik-reconcile")?;
    let (store, fenced, _session) = fenced_store(&cluster_id).await?;
    let mut cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    cluster.cluster_id = cluster_id.clone();
    let mut desired = TraefikSystemResources::for_cluster(&cluster, &security())?;

    SystemServiceReconciler::new(
        &cluster_id,
        "Traefik",
        TRAEFIK_SERVICE_ID,
        TRAEFIK_MANAGED_OWNER,
        Some(desired.service.clone()),
    )?
    .reconcile(&fenced, Timestamp(10_000))
    .await?;
    let service = read_service(&store, &cluster_id).await?;
    assert_eq!(service.meta.generation.0, 1);
    assert_eq!(service.spec.version, "traefik-3.6.23");

    desired.service.spec.version = "traefik-test-update".to_owned();
    SystemServiceReconciler::new(
        &cluster_id,
        "Traefik",
        TRAEFIK_SERVICE_ID,
        TRAEFIK_MANAGED_OWNER,
        Some(desired.service),
    )?
    .reconcile(&fenced, Timestamp(20_000))
    .await?;
    let service = read_service(&store, &cluster_id).await?;
    assert_eq!(service.meta.generation.0, 2);
    assert_eq!(service.spec.version, "traefik-test-update");

    SystemServiceReconciler::new(
        &cluster_id,
        "Traefik",
        TRAEFIK_SERVICE_ID,
        TRAEFIK_MANAGED_OWNER,
        None,
    )?
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

fn publication(port: u16) -> HostPortPublication {
    HostPortPublication {
        container_port: port,
        host_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        host_port: port,
        protocol: PortProtocol::Tcp,
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
            value: b"traefik-reconciler".to_vec(),
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
                instance_id: NodeInstanceId::new("traefik-reconciler")?,
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
        return Err(format!("expected one Traefik Service, found {}", values.len()).into());
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
