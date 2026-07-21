use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use ingress::{BackendChange, IngressBackend, IngressBackendError};
use kernel_api::{
    ArtifactTemplate, ClusterId, Deployment, ExecPolicy, Generation, NodeApiAccess, NodeFirewall,
    NodeId, NodeInstanceId, NodeNetwork, NodeNetworkId, NodeNetworkSpec, NodeNetworkStatus,
    NodeRole, Object, ObjectMeta, PlacementConstraint, ResourceKind, ResourceName,
    ResourceRevision, RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp,
};
use kernel_controller::{
    Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    SessionBinding, Store,
};

use crate::{OperatorBackends, OperatorSettings, OperatorSuite};

use super::cluster_with_nodes;

#[tokio::test]
async fn suite_composes_service_operators_and_zero_policy_firewall_baseline()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("cluster-1")?;
    let keys = Keyspace::new(&cluster_id);
    let monotonic: Arc<dyn Clock> = Arc::new(NoopClock);
    let store = Arc::new(InMemoryStore::new(monotonic.clone()));
    let session = store.session(Duration::from_secs(30)).await?;
    let leader = store
        .put_cas(PutRequest {
            key: keys.leader(),
            value: b"operator-suite".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await?;
    let CasOutcome::Applied(leader) = leader else {
        return Err("leader campaign conflicted".into());
    };
    let fenced = Arc::new(FencedStore::new(
        store.clone(),
        keys.leader(),
        LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: NodeId::new("node-1")?,
                instance_id: NodeInstanceId::new("operator-suite")?,
            },
            session.id(),
            leader.version,
        ),
    ));
    put(&store, &keys, "Service", &service()?).await?;
    put(&store, &keys, "NodeNetwork", &network()?).await?;
    let ingress = Arc::new(RecordingIngress::default());
    let suite = OperatorSuite::new(
        cluster_id,
        fenced,
        monotonic,
        Arc::new(FixedTimestampClock),
        settings()?,
        OperatorBackends {
            ingress: ingress.clone(),
        },
    )?;

    let first = suite.reconcile_snapshot().await?;
    assert_eq!(first.firewall_baselines, 1);
    assert_eq!(first.deployment, 0);
    let stored = one::<Service>(&store, &keys, "Service").await?;
    assert_eq!(stored.meta.finalizers.len(), 4);

    let second = suite.reconcile_snapshot().await?;
    assert_eq!(
        (
            second.deployment,
            second.scheduler,
            second.ingress,
            second.dns,
            second.firewall_baselines,
        ),
        (1, 1, 1, 1, 1)
    );
    assert_eq!(
        list::<Deployment>(&store, &keys, "Deployment").await?.len(),
        1
    );
    let desired_firewall = one::<NodeFirewall>(&store, &keys, "NodeFirewall").await?;
    assert!(desired_firewall.spec.script.contains("tcp dport 53 accept"));
    let ingress_changes = ingress
        .changes
        .lock()
        .map_err(|_| "ingress change lock poisoned")?;
    assert_eq!(ingress_changes.len(), 1);
    assert!(
        ingress_changes
            .first()
            .is_some_and(|change| change.active.is_none())
    );
    Ok(())
}

#[test]
fn production_settings_derive_host_firewall_boundaries_from_the_cluster()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    let settings = OperatorSettings::production(&cluster)?;
    let mut expected_ports = vec![
        cluster.ports.gateway,
        cluster.ports.store_client,
        cluster.ports.store_peer,
    ];
    expected_ports.extend(cluster.nodes.values().map(|node| node.endpoint.api_port));
    expected_ports.sort_unstable();
    expected_ports.dedup();
    let mut expected_cidrs = cluster
        .control_allow_cidrs
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    expected_cidrs.extend(
        cluster
            .nodes
            .values()
            .filter(|node| {
                cluster
                    .control_allow_cidrs
                    .iter()
                    .all(|network| !network.contains(node.endpoint.host_address))
            })
            .map(|node| format!("{}/32", node.endpoint.host_address)),
    );

    assert_eq!(settings.firewall.protected_host_ports, expected_ports);
    assert_eq!(
        settings.firewall.control_allow_cidrs,
        expected_cidrs.into_iter().collect::<Vec<_>>()
    );
    assert_eq!(settings.firewall.workload_interface, "maestro0");
    assert_eq!(settings.firewall.dns_port, 53);

    let mut cluster_without_allowlist = cluster;
    cluster_without_allowlist.control_allow_cidrs.clear();
    let fallback = OperatorSettings::production(&cluster_without_allowlist)?;
    assert_eq!(
        fallback.firewall.control_allow_cidrs,
        cluster_without_allowlist
            .nodes
            .values()
            .map(|node| format!("{}/32", node.endpoint.host_address))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[derive(Default)]
struct RecordingIngress {
    changes: Mutex<Vec<BackendChange>>,
}

#[async_trait]
impl IngressBackend for RecordingIngress {
    async fn apply(&self, change: &BackendChange) -> Result<(), IngressBackendError> {
        self.changes
            .lock()
            .map_err(|_| IngressBackendError::new("ingress change lock poisoned"))?
            .push(change.clone());
        Ok(())
    }
}

fn settings() -> Result<OperatorSettings, Box<dyn std::error::Error>> {
    Ok(OperatorSettings {
        runtime: RuntimeConfig::new(
            Duration::from_secs(30),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
        scheduler: scheduler::SchedulerSettings {
            replacement_grace: Duration::from_secs(30),
            deployment_drain_grace: Duration::from_secs(30),
        },
        deployment: deployment::LifecycleSettings {
            drain_grace: Duration::from_secs(30),
        },
        ingress: ingress::IngressSettings {
            retirement_grace: Duration::from_secs(30),
        },
        dns: dns::DnsSettings { ttl_secs: 5 },
        firewall: firewall::FirewallSettings {
            table_name: "maestro_firewall".to_string(),
            workload_interface: "maestro0".to_string(),
            dns_port: 53,
            protected_host_ports: vec![3000, 3001],
            control_allow_cidrs: vec!["10.0.0.0/8".to_string()],
            system_services: BTreeSet::new(),
        },
    })
}

fn service() -> Result<Service, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(ServiceId::new("api")?),
        spec: ServiceSpec {
            name: "API".to_string(),
            version: "1.0.0".to_string(),
            artifact: ArtifactTemplate::Image {
                reference: "registry.test/api:latest".to_string(),
            },
            command: None,
            replicas: 1,
            exposed_ports: vec![8080],
            health_check: None,
            max_restarts: Some(3),
            environment: BTreeMap::new(),
            user: None,
            node_api: NodeApiAccess::Disabled,
            secrets: None,
            volumes: Vec::new(),
            placement: PlacementConstraint::default(),
            exec: ExecPolicy::Allowed,
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout: RolloutState::Active,
            conditions: Vec::new(),
        },
    })
}

fn network() -> Result<NodeNetwork, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(NodeNetworkId::new("node-1")?),
        spec: NodeNetworkSpec {
            node_id: NodeId::new("node-1")?,
            public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            endpoint: SocketAddr::from(([10, 0, 0, 1], 51_820)),
            workload_subnet: "10.42.1.0/24".to_string(),
            mtu_bytes: 1_420,
        },
        status: NodeNetworkStatus {
            applied_generation: Generation(1),
            conditions: Vec::new(),
        },
    })
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}

async fn put<Resource, Id>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
    resource: &Object<Id, Resource, impl serde::Serialize>,
) -> Result<(), Box<dyn std::error::Error>>
where
    Id: Clone + Into<ResourceName> + serde::Serialize,
    Resource: serde::Serialize,
{
    let outcome = store
        .put_cas(PutRequest {
            key: keys.resource(&ResourceKind::new(kind)?, &resource.meta.id.clone().into()),
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

async fn list<Resource: serde::de::DeserializeOwned>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
) -> Result<Vec<Resource>, Box<dyn std::error::Error>> {
    store
        .list(&keys.resource_kind(&ResourceKind::new(kind)?))
        .await?
        .values
        .into_iter()
        .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
        .collect()
}

async fn one<Resource: serde::de::DeserializeOwned>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
) -> Result<Resource, Box<dyn std::error::Error>> {
    let mut resources = list(store, keys, kind).await?;
    if resources.len() != 1 {
        return Err(format!("expected one {kind}, found {}", resources.len()).into());
    }
    Ok(resources.remove(0))
}

struct FixedTimestampClock;

impl TimestampClock for FixedTimestampClock {
    fn now(&self) -> Timestamp {
        Timestamp(10_000)
    }
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
