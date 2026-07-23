use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, BuildSource, BuildTemplate, ClusterId, Deployment, DeploymentGoal,
    DeploymentId, DeploymentPhase, DeploymentSpec, DeploymentStatus, ExecPolicy, Generation, Node,
    NodeApiAccess, NodeId, NodeInstanceId, NodeRole, NodeSpec, NodeStatus, ObjectMeta,
    PlacementConstraint, ResourceKind, ResourceName, ResourceRevision, ServiceId, ServiceSpec,
    Timestamp,
};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, SessionBinding, Store,
    TokioClock,
};
use runtime::{
    ArtifactBuildRequest, ArtifactByteStream, ArtifactDigest, ArtifactPrunePolicy,
    ArtifactPruneReport, ArtifactReference, ArtifactStore, ArtifactStoreError,
};

use crate::artifact_retention::{preserved_digests, retained_digests};
use crate::{
    ArtifactHolderRegistry, ArtifactPeerSource, ArtifactPeerSourceError, ArtifactReplicationAgent,
    ArtifactReplicationSettings,
};

#[tokio::test]
async fn replication_imports_from_a_live_holder_and_publishes_the_copy()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(TokioClock::new());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let cluster_id = ClusterId::new("replication-test")?;
    let source_node = NodeId::new("node-source")?;
    let target_node = NodeId::new("node-target")?;
    let source_session = store.session(Duration::from_secs(30)).await?;
    let target_session = store.session(Duration::from_secs(30)).await?;
    let digest = ArtifactDigest::new("sha256:abc")?;
    let source_holders = ArtifactHolderRegistry::new(
        store.clone(),
        &cluster_id,
        source_node.clone(),
        source_session.id(),
    );
    source_holders.publish(&digest).await?;
    put_resource(
        &store,
        &cluster_id,
        "Deployment",
        "deployment-1",
        &deployment(
            "deployment-1",
            1,
            DeploymentPhase::Ready,
            Some("sha256:abc"),
        )?,
    )
    .await?;
    put_resource(
        &store,
        &cluster_id,
        "Node",
        target_node.as_str(),
        &node(target_node.clone())?,
    )
    .await?;
    put_liveness(&store, &cluster_id, &target_node, target_session.id()).await?;
    let artifacts = Arc::new(TestArtifacts::default());
    let stale = ArtifactDigest::new("sha256:stale")?;
    lock(&artifacts.local).insert(stale.clone());
    let holders = ArtifactHolderRegistry::new(
        store.clone(),
        &cluster_id,
        target_node.clone(),
        target_session.id(),
    );
    let agent = ArtifactReplicationAgent::new(
        store.clone(),
        artifacts.clone(),
        Arc::new(TestPeers),
        holders,
        ArtifactReplicationSettings {
            cluster_id: cluster_id.clone(),
            node_id: target_node.clone(),
            resync_interval: Duration::from_secs(30),
        },
        clock,
        Arc::new(FixedStatusClock),
    )?;

    let report = agent.reconcile_once().await?;
    assert_eq!(report.retained, 1);
    assert_eq!(report.eligible_nodes, 1);
    assert_eq!(report.imported, 1);
    assert_eq!(report.pruned, 1);
    assert_eq!(report.prune_failure, None);
    assert!(report.drain_ready);
    assert_eq!(report.missing_peer_copies, 0);
    assert!(report.failures.is_empty());
    assert_eq!(
        lock(&artifacts.prune_policies).as_slice(),
        &[ArtifactPrunePolicy::Preserve(vec![digest.clone()])]
    );
    assert!(artifacts.contains(&digest).await?);
    assert!(!artifacts.contains(&stale).await?);
    let readers = ArtifactHolderRegistry::new(store, &cluster_id, target_node, target_session.id());
    assert_eq!(readers.holders(&digest).await?.len(), 2);
    Ok(())
}

#[test]
fn retention_keeps_active_and_latest_registry_free_builds() -> Result<(), Box<dyn std::error::Error>>
{
    let deployments = vec![
        deployment("active", 1, DeploymentPhase::Ready, Some("sha256:active"))?,
        deployment(
            "older",
            2,
            DeploymentPhase::Terminated,
            Some("sha256:older"),
        )?,
        deployment(
            "latest",
            3,
            DeploymentPhase::Terminated,
            Some("sha256:latest"),
        )?,
        image_deployment("registry", 4, "registry.test/api@sha256:external")?,
    ];
    assert_eq!(
        retained_digests(&deployments)?,
        BTreeSet::from([
            ArtifactDigest::new("sha256:active")?,
            ArtifactDigest::new("sha256:latest")?,
        ])
    );
    assert_eq!(
        preserved_digests(&deployments)?,
        BTreeSet::from([
            ArtifactDigest::new("sha256:active")?,
            ArtifactDigest::new("sha256:latest")?,
            ArtifactDigest::new("registry.test/api@sha256:external")?,
        ])
    );
    Ok(())
}

#[derive(Default)]
struct TestArtifacts {
    local: Mutex<BTreeSet<ArtifactDigest>>,
    prune_policies: Mutex<Vec<ArtifactPrunePolicy>>,
}

#[async_trait]
impl ArtifactStore for TestArtifacts {
    async fn build(
        &self,
        _request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("build"))
    }

    async fn pull(
        &self,
        _reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("pull"))
    }

    async fn push(
        &self,
        _digest: &ArtifactDigest,
        _destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError> {
        Err(unused("push"))
    }

    async fn resolve_digest(
        &self,
        _reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("resolve"))
    }

    async fn contains(&self, digest: &ArtifactDigest) -> Result<bool, ArtifactStoreError> {
        Ok(lock(&self.local).contains(digest))
    }

    async fn export(
        &self,
        _digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError> {
        Err(unused("export"))
    }

    async fn import(
        &self,
        mut source: Box<dyn ArtifactByteStream>,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = source.next().await? {
            bytes.extend_from_slice(&chunk);
        }
        let digest = ArtifactDigest::new(String::from_utf8(bytes).map_err(|error| {
            ArtifactStoreError::Rejected {
                message: error.to_string(),
            }
        })?)?;
        lock(&self.local).insert(digest.clone());
        Ok(digest)
    }

    async fn prune(
        &self,
        policy: &ArtifactPrunePolicy,
    ) -> Result<ArtifactPruneReport, ArtifactStoreError> {
        lock(&self.prune_policies).push(policy.clone());
        let ArtifactPrunePolicy::Preserve(preserved) = policy;
        let preserved = preserved.iter().collect::<BTreeSet<_>>();
        let mut local = lock(&self.local);
        let removed = local
            .iter()
            .filter(|digest| !preserved.contains(digest))
            .cloned()
            .collect::<Vec<_>>();
        local.retain(|digest| preserved.contains(digest));
        Ok(ArtifactPruneReport { removed })
    }
}

struct TestPeers;

#[async_trait]
impl ArtifactPeerSource for TestPeers {
    async fn export(
        &self,
        _node_id: &NodeId,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactPeerSourceError> {
        Ok(Box::new(TestStream(Some(
            digest.as_str().as_bytes().to_vec(),
        ))))
    }
}

struct TestStream(Option<Vec<u8>>);

#[async_trait]
impl ArtifactByteStream for TestStream {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, ArtifactStoreError> {
        Ok(self.0.take())
    }
}

fn deployment(
    id: &str,
    created_at: i64,
    phase: DeploymentPhase,
    image_digest: Option<&str>,
) -> Result<Deployment, kernel_api::InvalidIdentifier> {
    Ok(Deployment {
        meta: metadata(DeploymentId::new(id)?),
        spec: DeploymentSpec {
            service_id: ServiceId::new("api")?,
            service_generation: Generation(1),
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            service: service(ArtifactTemplate::Build {
                template: BuildTemplate {
                    source: BuildSource::Git {
                        repository: "https://example.test/api.git".to_owned(),
                        revision: "main".to_owned(),
                    },
                    dockerfile: "Dockerfile".to_owned(),
                    watch: false,
                    environment: BTreeMap::new(),
                    secrets: BTreeMap::new(),
                },
            })?,
            goal: DeploymentGoal::Run,
            build_id: None,
        },
        status: DeploymentStatus {
            phase,
            created_at: Timestamp(created_at),
            ready_at: None,
            draining_at: None,
            image_digest: image_digest.map(str::to_owned),
            conditions: Vec::new(),
        },
    })
}

fn image_deployment(
    id: &str,
    created_at: i64,
    digest: &str,
) -> Result<Deployment, kernel_api::InvalidIdentifier> {
    let mut deployment = deployment(id, created_at, DeploymentPhase::Ready, Some(digest))?;
    deployment.spec.service = service(ArtifactTemplate::Image {
        reference: digest.to_owned(),
    })?;
    Ok(deployment)
}

fn service(artifact: ArtifactTemplate) -> Result<ServiceSpec, kernel_api::InvalidIdentifier> {
    Ok(ServiceSpec {
        name: "API".to_owned(),
        version: "1.0.0".to_owned(),
        artifact,
        preview: None,
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
        exec: ExecPolicy::Denied,
    })
}

fn node(node_id: NodeId) -> Result<Node, kernel_api::InvalidIdentifier> {
    Ok(Node {
        meta: metadata(node_id),
        spec: NodeSpec {
            hostname: "node-target.internal".to_owned(),
            host_address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            role: NodeRole::Worker,
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new("instance-target")?,
            version: "1.0.0".to_owned(),
            last_seen: Timestamp(1),
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

async fn put_resource<Resource: serde::Serialize>(
    store: &Arc<InMemoryStore>,
    cluster_id: &ClusterId,
    kind: &str,
    id: &str,
    resource: &Resource,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(cluster_id)
                .resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?),
            value: serde_json::to_vec(resource)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("resource seed conflicted".into())
    }
}

async fn put_liveness(
    store: &Arc<InMemoryStore>,
    cluster_id: &ClusterId,
    node_id: &NodeId,
    session_id: kernel_store::SessionId,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key: Keyspace::new(cluster_id).node_liveness(node_id),
            value: b"instance-target".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding { session_id }),
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("liveness seed conflicted".into())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct FixedStatusClock;

impl crate::StatusClock for FixedStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(20_000)
    }
}

fn unused(operation: &str) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: format!("test artifact store does not support {operation}"),
    }
}
