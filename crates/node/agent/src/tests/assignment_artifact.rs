use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentPhase, BuildSource, BuildTemplate, Deployment,
    ResourceKind, ResourceName, Timestamp,
};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock,
};
use runtime::{
    ArtifactBuildRequest, ArtifactByteStream, ArtifactDigest, ArtifactPrunePolicy,
    ArtifactPruneReport, ArtifactReference, ArtifactStore, ArtifactStoreError, FakeNetworkProvider,
    FakeRuntime, NetworkCidr, NetworkProvider, WorkloadRuntime,
};

use super::assignment::{assignment, cluster_id, deployment, node_id};
use crate::{
    ArtifactHolderRegistry, ArtifactPeerSource, ArtifactPeerSourceError, ArtifactReplicationAgent,
    ArtifactReplicationSettings, AssignmentAgent, AssignmentAgentSettings, StatusClock,
};

#[tokio::test]
async fn registry_free_assignment_waits_for_a_verified_local_artifact()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(TokioClock::new());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let session = store.session(Duration::from_secs(30)).await?;
    let artifacts = Arc::new(AvailabilityArtifacts::default());
    let holders = ArtifactHolderRegistry::new(
        store.clone(),
        &cluster_id(),
        node_id("node-1"),
        session.id(),
    );
    let replication = Arc::new(ArtifactReplicationAgent::new(
        store.clone(),
        artifacts.clone(),
        Arc::new(NoPeers),
        holders,
        ArtifactReplicationSettings {
            cluster_id: cluster_id(),
            node_id: node_id("node-1"),
            resync_interval: Duration::from_secs(30),
        },
        clock.clone(),
    )?);
    let runtime = Arc::new(FakeRuntime::new());
    let network = Arc::new(FakeNetworkProvider::default());
    let secrets = tempfile::tempdir()?;
    let node_api = tempfile::tempdir()?;
    let agent = AssignmentAgent::new(
        store.clone(),
        runtime.clone(),
        network as Arc<dyn NetworkProvider>,
        AssignmentAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id("node-1"),
            network: runtime::NetworkSpec {
                name: "maestro-node-1".to_owned(),
                range: NetworkCidr::new(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 0)), 24)?,
                gateway: IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1)),
                mtu_bytes: 1_420,
            },
            stop_timeout: Duration::from_secs(5),
            resync_interval: Duration::from_secs(30),
            restart_backoff_base: Duration::from_secs(5),
            restart_backoff_max: Duration::from_secs(60),
            reconcile_timeout: Duration::from_secs(10),
            secrets_root: secrets.path().to_path_buf(),
            node_api_root: node_api.path().join("mounts"),
        },
        #[cfg(unix)]
        None,
        clock,
        Arc::new(FixedStatusClock),
    )?
    .with_artifact_replication(replication);
    let mut desired_deployment = deployment();
    desired_deployment.spec.service.artifact = ArtifactTemplate::Build {
        template: BuildTemplate {
            source: BuildSource::Git {
                repository: "https://example.test/api.git".to_owned(),
                revision: "main".to_owned(),
            },
            dockerfile: "Dockerfile".to_owned(),
            watch: false,
            environment: Default::default(),
            secrets: Default::default(),
        },
    };
    desired_deployment.status.image_digest = Some("sha256:local-build".to_owned());
    seed(&store, &desired_deployment, &assignment()).await?;

    let waiting = agent.reconcile_once().await?;
    assert_eq!(waiting.unresolved, 1);
    assert!(
        runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .is_empty()
    );
    let pending = load_assignment(&store).await?;
    assert_eq!(pending.status.phase, AssignmentPhase::Pending);
    assert_eq!(
        pending
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("ArtifactReplicationUnavailable")
    );

    artifacts.available.store(true, Ordering::SeqCst);
    let running = agent.reconcile_once().await?;
    assert_eq!(running.running, 1);
    assert_eq!(
        runtime.list(&cluster_id(), &node_id("node-1")).await?.len(),
        1
    );
    let readers =
        ArtifactHolderRegistry::new(store, &cluster_id(), node_id("node-1"), session.id());
    assert_eq!(
        readers
            .holders(&ArtifactDigest::new("sha256:local-build")?)
            .await?
            .len(),
        1
    );
    Ok(())
}

async fn seed(
    store: &Arc<InMemoryStore>,
    deployment: &Deployment,
    assignment: &Assignment,
) -> Result<(), Box<dyn std::error::Error>> {
    let keyspace = Keyspace::new(&cluster_id());
    put(
        store,
        keyspace.resource(
            &ResourceKind::new("Deployment")?,
            &ResourceName::new(deployment.meta.id.as_str())?,
        ),
        serde_json::to_vec(deployment)?,
    )
    .await?;
    put(
        store,
        keyspace.resource(
            &ResourceKind::new("Assignment")?,
            &ResourceName::new(assignment.meta.id.as_str())?,
        ),
        serde_json::to_vec(assignment)?,
    )
    .await
}

async fn put(
    store: &Arc<InMemoryStore>,
    key: kernel_store::StoreKey,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    if matches!(
        store
            .put_cas(PutRequest {
                key,
                value,
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ) {
        Ok(())
    } else {
        Err("test resource seed conflicted".into())
    }
}

async fn load_assignment(
    store: &Arc<InMemoryStore>,
) -> Result<Assignment, Box<dyn std::error::Error>> {
    let key = Keyspace::new(&cluster_id()).resource(
        &ResourceKind::new("Assignment")?,
        &ResourceName::new("assignment-1")?,
    );
    let stored = store.get(&key).await?.ok_or("assignment missing")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

#[derive(Default)]
struct AvailabilityArtifacts {
    available: AtomicBool,
}

#[async_trait]
impl ArtifactStore for AvailabilityArtifacts {
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

    async fn contains(&self, _digest: &ArtifactDigest) -> Result<bool, ArtifactStoreError> {
        Ok(self.available.load(Ordering::SeqCst))
    }

    async fn export(
        &self,
        _digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError> {
        Err(unused("export"))
    }

    async fn import(
        &self,
        _source: Box<dyn ArtifactByteStream>,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("import"))
    }

    async fn prune(
        &self,
        _policy: &ArtifactPrunePolicy,
    ) -> Result<ArtifactPruneReport, ArtifactStoreError> {
        Err(unused("prune"))
    }
}

struct NoPeers;

#[async_trait]
impl ArtifactPeerSource for NoPeers {
    async fn export(
        &self,
        _node_id: &kernel_api::NodeId,
        _digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactPeerSourceError> {
        Err(ArtifactPeerSourceError::Unavailable {
            message: "test has no peers".to_owned(),
        })
    }
}

struct FixedStatusClock;

impl StatusClock for FixedStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(1_750_000_000_000)
    }
}

fn unused(operation: &str) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: format!("test artifact store does not support {operation}"),
    }
}
