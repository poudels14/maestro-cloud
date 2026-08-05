use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cluster::TailscaleAuthKeyRecord;
use kernel_api::{
    ArtifactTemplate, ClusterId, ExecPolicy, FirewallPolicy, HealthProbe, NodeId, NodeInstanceId,
    NodeRole, ReplicaSpread, ResourceKind, ResourceName, SecretValue, Service,
    TAILSCALE_GATEWAY_SERVICE_ID, Timestamp, VolumeSource, WorkloadUserSpec,
};
use kernel_controller::{
    ControllerError, FencedStore, LeaderIdentity, LeadershipToken, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};
use tokio::sync::watch;

use crate::tailscale_reconciler::{TailscaleReconcileError, TailscaleResourceReconciler};
use crate::tailscale_resources::{AUTH_SCRIPT, TAILSCALE_IMAGE, TailscaleSystemResources};

use super::cluster_with_nodes;

#[test]
fn builds_pinned_gateway_resources() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster =
        cluster_with_nodes(&[("node-a", NodeRole::Master), ("node-b", NodeRole::Worker)])?;
    cluster.tailscale = Some(cluster::TailscaleGatewayConfig {
        auth_key: SecretValue::new("tskey-auth-reusable-test-secret"),
        advertise_routes: None,
        replicas: 2,
        tags: vec!["tag:maestro".to_owned()],
        cross_cluster_dns: Vec::new(),
    });

    let resources = required(
        TailscaleSystemResources::from_cluster(&cluster)?,
        "Tailscale resources",
    )?;
    assert_eq!(
        resources.system_host_access.service_id.as_str(),
        TAILSCALE_GATEWAY_SERVICE_ID
    );
    assert_eq!(
        resources.system_host_access.trusted_source_cidrs,
        ["100.64.0.0/10"]
    );
    assert!(resources.system_host_access.host_ports.is_empty());
    assert_eq!(
        resources.system_host_access.endpoints,
        [
            firewall::SystemHostEndpoint {
                address: "172.22.0.250".parse()?,
                public_port: 80,
                listener_port: 3_011,
            },
            firewall::SystemHostEndpoint {
                address: "172.22.1.250".parse()?,
                public_port: 80,
                listener_port: 3_012,
            },
        ]
    );
    let service = resources.service;
    assert_eq!(service.spec.name, "Tailscale Gateway");
    assert_eq!(service.spec.replicas, 2);
    assert_eq!(
        service.spec.placement.replica_spread,
        ReplicaSpread::BestEffort
    );
    assert_eq!(service.spec.exec, ExecPolicy::Denied);
    assert_eq!(service.spec.user, Some(WorkloadUserSpec::UNPRIVILEGED));
    assert_eq!(service.spec.exposed_ports, vec![1_055, 9_002]);
    assert_eq!(
        service.spec.environment.get("TS_SOCKS5_SERVER"),
        Some(&":1055".to_owned())
    );
    assert_eq!(
        service.spec.environment.get("TS_LOCAL_ADDR_PORT"),
        Some(&"0.0.0.0:9002".to_owned())
    );
    assert!(
        service
            .spec
            .environment
            .get("TS_EXTRA_ARGS")
            .is_some_and(|value| value.contains("--accept-routes"))
    );
    assert!(matches!(
        service.spec.artifact,
        ArtifactTemplate::Image { reference } if reference == TAILSCALE_IMAGE
    ));
    let command = required(service.spec.command, "gateway command")?;
    assert_eq!(command.executable, "/bin/sh");
    assert_eq!(command.arguments, vec!["-ceu", AUTH_SCRIPT]);
    assert!(
        AUTH_SCRIPT.starts_with("export PATH=/usr/local/bin:/usr/bin:/bin\n"),
        "containerboot must be able to locate tailscaled in the image"
    );
    assert!(
        AUTH_SCRIPT.contains(". /run/secrets/tailscale.env\n"),
        "every launch must load the auth key even when an unauthenticated state file exists"
    );
    assert!(
        AUTH_SCRIPT.contains("TS_HOSTNAME=\"${MAESTRO_TAILSCALE_HOSTNAME_PREFIX}-${replica}\""),
        "gateway identities must include the cluster name and replica ordinal"
    );
    assert!(
        AUTH_SCRIPT.contains("serve --bg --yes --http=80"),
        "each gateway must expose the node API over its encrypted tailnet transport without \
         depending on Tailscale certificate issuance"
    );
    assert!(
        AUTH_SCRIPT.contains("set \\\n  --hostname=\"$TS_HOSTNAME\""),
        "persisted gateway identities must adopt the configured cluster hostname"
    );
    assert!(
        AUTH_SCRIPT.contains("awk '$1 == \"nameserver\" { print $2; exit }' /etc/resolv.conf"),
        "the gateway must use Maestro's mounted resolver address without requiring iproute2"
    );
    assert!(
        !AUTH_SCRIPT.contains("ip -4 route"),
        "the pinned Tailscale image does not include the ip utility"
    );
    assert!(
        AUTH_SCRIPT.contains("while [ \"$admin_attempt\" -lt 60 ]"),
        "gateway startup must tolerate Admin and firewall convergence"
    );
    assert!(
        AUTH_SCRIPT.contains("serve reset"),
        "stale Serve listeners must not survive a gateway hostname change"
    );
    assert!(
        AUTH_SCRIPT.contains("\"http://${admin}:80\""),
        "the direct tailnet endpoint must proxy the fixed bridge-only Admin listener"
    );
    assert!(
        !AUTH_SCRIPT.contains("tailscaled.state"),
        "an unauthenticated state file must not suppress auth-key loading"
    );
    assert_eq!(
        service
            .spec
            .environment
            .get("TS_ROUTES")
            .map(String::as_str),
        Some("172.22.0.0/24,172.22.1.0/24")
    );
    assert_eq!(
        service
            .spec
            .environment
            .get("MAESTRO_TAILSCALE_HOSTNAME_PREFIX")
            .map(String::as_str),
        Some("maestro-daemon-test-gateway")
    );
    assert!(
        !service
            .spec
            .environment
            .contains_key("MAESTRO_NODE_API_PORTS")
    );
    assert_eq!(
        service
            .spec
            .environment
            .get("TS_EXTRA_ARGS")
            .map(String::as_str),
        Some("--accept-routes --advertise-tags=tag:maestro")
    );
    assert!(!service.spec.environment.contains_key("TS_AUTHKEY"));
    let secrets = required(service.spec.secrets, "gateway secret mount")?;
    let kernel_api::SecretMountSpec::Dotenv {
        mount_path, items, ..
    } = secrets
    else {
        return Err("gateway secret mount is not dotenv".into());
    };
    assert_eq!(mount_path, "/run/secrets/tailscale.env");
    assert_eq!(
        items.get("TS_AUTHKEY").map(SecretValue::expose),
        Some("tskey-auth-reusable-test-secret")
    );
    assert!(matches!(
        service.spec.volumes.as_slice(),
        [kernel_api::VolumeMountSpec {
            source: VolumeSource::ReplicaManaged { name },
            target,
            ..
        }] if name == "tailscale-state" && target == "/state"
    ));
    assert!(matches!(
        required(service.spec.health_check, "gateway health check")?.probe,
        HealthProbe::Http { port: 9_002, ref path } if path == "/healthz"
    ));
    Ok(())
}

#[test]
fn untagged_gateway_omits_the_advertise_tags_argument() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster =
        cluster_with_nodes(&[("node-a", NodeRole::Master), ("node-b", NodeRole::Worker)])?;
    cluster.tailscale = Some(cluster::TailscaleGatewayConfig {
        auth_key: SecretValue::new("tskey-auth-reusable-test-secret"),
        advertise_routes: None,
        replicas: 2,
        tags: Vec::new(),
        cross_cluster_dns: Vec::new(),
    });

    let resources = required(
        TailscaleSystemResources::from_cluster(&cluster)?,
        "Tailscale resources",
    )?;
    assert_eq!(
        resources
            .service
            .spec
            .environment
            .get("TS_EXTRA_ARGS")
            .map(String::as_str),
        Some("--accept-routes")
    );
    Ok(())
}

#[test]
fn omitting_tailscale_omits_gateway_resources() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = cluster_with_nodes(&[("node-a", NodeRole::Master)])?;
    assert!(TailscaleSystemResources::from_cluster(&cluster)?.is_none());
    Ok(())
}

#[tokio::test]
async fn reconciles_enable_update_and_removal_as_fenced_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("tailscale-reconcile")?;
    let (store, fenced, _session) = fenced_store(&cluster_id).await?;
    let mut cluster =
        cluster_with_nodes(&[("node-a", NodeRole::Master), ("node-b", NodeRole::Worker)])?;
    cluster.cluster_id = cluster_id.clone();
    cluster.tailscale = Some(cluster::TailscaleGatewayConfig {
        auth_key: SecretValue::new("tskey-auth-first-reusable-secret"),
        advertise_routes: None,
        replicas: 2,
        tags: vec!["tag:maestro".to_owned()],
        cross_cluster_dns: Vec::new(),
    });
    let desired = required(
        TailscaleSystemResources::from_cluster(&cluster)?,
        "Tailscale resources",
    )?;

    let reconciler = TailscaleResourceReconciler::new(&cluster_id, Some(desired))?;
    reconciler.reconcile(&fenced, Timestamp(10_000)).await?;
    let service: Service = read(&store, &cluster_id, "Service").await?;
    assert_eq!(service.meta.generation.0, 1);
    assert!(
        list::<FirewallPolicy>(&store, &cluster_id, "FirewallPolicy")
            .await?
            .is_empty()
    );

    put_auth_key(
        &store,
        &cluster_id,
        "tskey-auth-live-rotation-secret",
        ExpectedVersion::Missing,
    )
    .await?;
    reconciler.reconcile(&fenced, Timestamp(15_000)).await?;
    let service: Service = read(&store, &cluster_id, "Service").await?;
    assert_eq!(service.meta.generation.0, 2);
    assert_eq!(
        gateway_auth_key(&service),
        "tskey-auth-live-rotation-secret"
    );

    let config = cluster
        .tailscale
        .as_mut()
        .ok_or_else(|| std::io::Error::other("Tailscale config is missing"))?;
    config.replicas = 1;
    config.auth_key = SecretValue::new("tskey-auth-rotated-reusable-secret");
    let desired = required(
        TailscaleSystemResources::from_cluster(&cluster)?,
        "Tailscale resources",
    )?;
    TailscaleResourceReconciler::new(&cluster_id, Some(desired))?
        .reconcile(&fenced, Timestamp(20_000))
        .await?;
    let service: Service = read(&store, &cluster_id, "Service").await?;
    assert_eq!(service.meta.generation.0, 3);
    assert_eq!(service.spec.replicas, 1);
    assert_eq!(
        gateway_auth_key(&service),
        "tskey-auth-live-rotation-secret"
    );
    assert!(
        list::<FirewallPolicy>(&store, &cluster_id, "FirewallPolicy")
            .await?
            .is_empty()
    );

    TailscaleResourceReconciler::new(&cluster_id, None)?
        .reconcile(&fenced, Timestamp(30_000))
        .await?;
    let service: Service = read(&store, &cluster_id, "Service").await?;
    assert_eq!(service.meta.deletion_timestamp, Some(Timestamp(30_000)));
    assert!(
        list::<FirewallPolicy>(&store, &cluster_id, "FirewallPolicy")
            .await?
            .is_empty()
    );
    Ok(())
}

async fn put_auth_key(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    auth_key: &str,
    expected: ExpectedVersion,
) -> Result<(), Box<dyn std::error::Error>> {
    let record = TailscaleAuthKeyRecord::new(SecretValue::new(auth_key))?;
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(cluster_id).tailscale_auth_key(),
            value: serde_json::to_vec(&record)?,
            expected,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("Tailscale auth-key write conflicted".into())
    }
}

#[tokio::test]
async fn refuses_a_reserved_id_collision_without_creating_the_other_resource()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("tailscale-collision")?;
    let (store, fenced, _session) = fenced_store(&cluster_id).await?;
    let mut cluster = cluster_with_nodes(&[("node-a", NodeRole::Master)])?;
    cluster.cluster_id = cluster_id.clone();
    cluster.tailscale = Some(cluster::TailscaleGatewayConfig {
        auth_key: SecretValue::new("tskey-auth-reusable-test-secret"),
        advertise_routes: None,
        replicas: 1,
        tags: vec!["tag:maestro".to_owned()],
        cross_cluster_dns: Vec::new(),
    });
    let desired = required(
        TailscaleSystemResources::from_cluster(&cluster)?,
        "Tailscale resources",
    )?;
    let mut collision = desired.service.clone();
    collision.meta.annotations.clear();
    put(&store, &cluster_id, "Service", &collision).await?;

    assert!(matches!(
        TailscaleResourceReconciler::new(&cluster_id, Some(desired))?
            .reconcile(&fenced, Timestamp(10_000))
            .await,
        Err(TailscaleReconcileError::ResourceCollision {
            kind: "Service",
            ..
        })
    ));
    assert!(
        list::<FirewallPolicy>(&store, &cluster_id, "FirewallPolicy")
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn stale_leadership_cannot_create_gateway_resources() -> Result<(), Box<dyn std::error::Error>>
{
    let cluster_id = ClusterId::new("tailscale-stale")?;
    let (store, fenced, session) = fenced_store(&cluster_id).await?;
    let mut cluster = cluster_with_nodes(&[("node-a", NodeRole::Master)])?;
    cluster.cluster_id = cluster_id.clone();
    cluster.tailscale = Some(cluster::TailscaleGatewayConfig {
        auth_key: SecretValue::new("tskey-auth-reusable-test-secret"),
        advertise_routes: None,
        replicas: 1,
        tags: vec!["tag:maestro".to_owned()],
        cross_cluster_dns: Vec::new(),
    });
    let desired = required(
        TailscaleSystemResources::from_cluster(&cluster)?,
        "Tailscale resources",
    )?;
    session.close().await?;

    assert!(matches!(
        TailscaleResourceReconciler::new(&cluster_id, Some(desired))?
            .reconcile(&fenced, Timestamp(10_000))
            .await,
        Err(TailscaleReconcileError::Controller(
            ControllerError::LeadershipLost
        ))
    ));
    assert!(
        list::<Service>(&store, &cluster_id, "Service")
            .await?
            .is_empty()
    );
    assert!(
        list::<FirewallPolicy>(&store, &cluster_id, "FirewallPolicy")
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn live_auth_key_changes_trigger_fenced_gateway_reconciliation()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("tailscale-live-rotation")?;
    let (store, fenced, _session) = fenced_store(&cluster_id).await?;
    let mut cluster = cluster_with_nodes(&[("node-a", NodeRole::Master)])?;
    cluster.cluster_id = cluster_id.clone();
    cluster.tailscale = Some(cluster::TailscaleGatewayConfig {
        auth_key: SecretValue::new("tskey-auth-launch-document-secret"),
        advertise_routes: None,
        replicas: 1,
        tags: vec!["tag:maestro".to_owned()],
        cross_cluster_dns: Vec::new(),
    });
    let desired = required(
        TailscaleSystemResources::from_cluster(&cluster)?,
        "Tailscale resources",
    )?;
    let reconciler = TailscaleResourceReconciler::new(&cluster_id, Some(desired))?;
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let runner = tokio::spawn(async move {
        reconciler
            .run(
                Arc::new(fenced),
                Arc::new(FixedTimestampClock),
                shutdown_receiver,
            )
            .await
    });

    await_gateway_auth_key(&store, &cluster_id, "tskey-auth-launch-document-secret").await?;
    put_auth_key(
        &store,
        &cluster_id,
        "tskey-auth-live-watched-secret",
        ExpectedVersion::Missing,
    )
    .await?;
    await_gateway_auth_key(&store, &cluster_id, "tskey-auth-live-watched-secret").await?;

    shutdown.send(true)?;
    runner.await??;
    Ok(())
}

async fn fenced_store(
    cluster_id: &ClusterId,
) -> Result<(Arc<InMemoryStore>, FencedStore, Box<dyn Session>), Box<dyn std::error::Error>> {
    let clock: Arc<dyn Clock> = Arc::new(NoopClock);
    let store = Arc::new(InMemoryStore::new(clock));
    let keys = Keyspace::new(cluster_id);
    let session = store.session(Duration::from_secs(30)).await?;
    let leader = store
        .put_cas(PutRequest {
            key: keys.leader(),
            value: b"tailscale-reconciler".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await?;
    let CasOutcome::Applied(leader) = leader else {
        return Err("leader campaign conflicted".into());
    };
    let fenced = FencedStore::new(
        store.clone(),
        keys.leader(),
        LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: NodeId::new("node-a")?,
                instance_id: NodeInstanceId::new("tailscale-reconciler")?,
            },
            session.id(),
            leader.version,
        ),
    );
    Ok((store, fenced, session))
}

async fn put<Resource>(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    kind: &str,
    resource: &Resource,
) -> Result<(), Box<dyn std::error::Error>>
where
    Resource: ResourceIdentity + serde::Serialize,
{
    let key = Keyspace::new(cluster_id).resource(
        &ResourceKind::new(kind)?,
        &ResourceName::new(resource.resource_id())?,
    );
    let outcome = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(resource)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err(format!("{kind} create conflicted").into())
    }
}

async fn read<Resource: serde::de::DeserializeOwned>(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    kind: &str,
) -> Result<Resource, Box<dyn std::error::Error>> {
    let mut resources = list(store, cluster_id, kind).await?;
    if resources.len() != 1 {
        return Err(format!("expected one {kind}, found {}", resources.len()).into());
    }
    Ok(resources.remove(0))
}

async fn list<Resource: serde::de::DeserializeOwned>(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    kind: &str,
) -> Result<Vec<Resource>, Box<dyn std::error::Error>> {
    let values = store
        .list(&Keyspace::new(cluster_id).resource_kind(&ResourceKind::new(kind)?))
        .await?
        .values;
    values
        .into_iter()
        .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
        .collect()
}

async fn await_gateway_auth_key(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let services = list::<Service>(store, cluster_id, "Service").await?;
            if services
                .first()
                .is_some_and(|service| gateway_auth_key(service) == expected)
            {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "timed out waiting for Tailscale gateway reconciliation")?
}

trait ResourceIdentity {
    fn resource_id(&self) -> &str;
}

fn gateway_auth_key(service: &Service) -> &str {
    service
        .spec
        .secrets
        .as_ref()
        .and_then(|secrets| match secrets {
            kernel_api::SecretMountSpec::Dotenv { items, .. } => items.get("TS_AUTHKEY"),
            kernel_api::SecretMountSpec::Files { .. } => None,
        })
        .map(SecretValue::expose)
        .unwrap_or("")
}

fn required<Value>(value: Option<Value>, name: &str) -> Result<Value, std::io::Error> {
    value.ok_or_else(|| std::io::Error::other(format!("{name} are missing")))
}

impl ResourceIdentity for Service {
    fn resource_id(&self) -> &str {
        self.meta.id.as_str()
    }
}

struct NoopClock;

struct FixedTimestampClock;

impl TimestampClock for FixedTimestampClock {
    fn now(&self) -> Timestamp {
        Timestamp(10_000)
    }
}

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}
