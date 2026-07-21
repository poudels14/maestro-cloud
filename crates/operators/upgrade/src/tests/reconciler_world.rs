use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, Generation, Node, NodeId, NodeInstanceId, NodeRole, NodeSpec, NodeStatus, Object,
    ObjectMeta, ResourceKind, ResourceName, ResourceRevision, Timestamp, UpgradeMode, UpgradePhase,
    UpgradeRun, UpgradeRunId, UpgradeRunSpec, UpgradeRunStatus,
};
use kernel_controller::{
    Backoff, ControllerError, ControllerRuntime, FencedStore, LeaderIdentity, LeadershipToken,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};

use crate::{
    NodeUpgradeBackend, NodeUpgradeBackendError, NodeUpgradeRequest, UpgradeReconciler,
    UpgradeSettings,
};

pub(super) struct World {
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    runtime: ControllerRuntime<UpgradeReconciler>,
    backend: Arc<RecordingBackend>,
    _session: Box<dyn Session>,
}

impl World {
    pub(super) async fn new(
        mutate_on_first_dispatch: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_mode(mutate_on_first_dispatch, UpgradeMode::AllNodes).await
    }

    pub(super) async fn new_with_mode(
        mutate_dispatches: bool,
        mode: UpgradeMode,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("upgrade-test")?;
        let keys = Keyspace::new(&cluster_id);
        let clock: Arc<dyn Clock> = Arc::new(NoopClock);
        let store = Arc::new(InMemoryStore::new(clock.clone()));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"upgrade-suite".to_vec(),
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
                    instance_id: NodeInstanceId::new("upgrade-suite")?,
                },
                session.id(),
                leader.version,
            ),
        ));
        for (id, role) in [
            ("node-1", NodeRole::Master),
            ("node-2", NodeRole::Hybrid),
            ("node-3", NodeRole::ControlPlane),
        ] {
            let node = node(id, role)?;
            put_resource(&store, &keys, "Node", &node).await?;
            put_key(&store, keys.node_liveness(&node.meta.id), b"live".to_vec()).await?;
        }
        put_resource(&store, &keys, "UpgradeRun", &run(mode)?).await?;
        let backend = Arc::new(RecordingBackend {
            store: store.clone(),
            keys: keys.clone(),
            mutate_dispatches,
            accepted: Mutex::new(BTreeSet::new()),
            requests: Mutex::new(Vec::new()),
        });
        let runtime = runtime(cluster_id, clock, fenced, backend.clone())?;
        Ok(Self {
            keys,
            store,
            runtime,
            backend,
            _session: session,
        })
    }

    pub(super) async fn handoff(
        &self,
        node_id: &str,
    ) -> Result<(ControllerRuntime<UpgradeReconciler>, Box<dyn Session>), Box<dyn std::error::Error>>
    {
        let session = self.store.session(Duration::from_secs(30)).await?;
        let current = self
            .store
            .get(&self.keys.leader())
            .await?
            .ok_or("leader missing")?;
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self.keys.leader(),
                value: format!("successor-{node_id}").into_bytes(),
                expected: ExpectedVersion::Exact(current.version),
                session: Some(SessionBinding {
                    session_id: session.id(),
                }),
            })
            .await?;
        let CasOutcome::Applied(successor) = outcome else {
            return Err("leadership handoff conflicted".into());
        };
        let fenced = Arc::new(FencedStore::new(
            self.store.clone(),
            self.keys.leader(),
            LeadershipToken::from_campaign(
                LeaderIdentity {
                    node_id: NodeId::new(node_id)?,
                    instance_id: NodeInstanceId::new(format!("successor-{node_id}"))?,
                },
                session.id(),
                successor.version,
            ),
        ));
        Ok((
            runtime(
                ClusterId::new("upgrade-test")?,
                Arc::new(NoopClock),
                fenced,
                self.backend.clone(),
            )?,
            session,
        ))
    }

    pub(super) async fn pass(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.runtime.reconcile_snapshot().await?;
        Ok(())
    }

    pub(super) async fn stale_pass(&self) -> Result<usize, ControllerError> {
        self.runtime.reconcile_snapshot().await
    }

    pub(super) async fn run(&self) -> Result<UpgradeRun, Box<dyn std::error::Error>> {
        self.run_optional()
            .await?
            .ok_or_else(|| "run missing".into())
    }

    pub(super) async fn run_optional(
        &self,
    ) -> Result<Option<UpgradeRun>, Box<dyn std::error::Error>> {
        let key = self.keys.resource(
            &ResourceKind::new("UpgradeRun")?,
            &ResourceName::new("upgrade-1")?,
        );
        self.store
            .get(&key)
            .await?
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .transpose()
    }

    pub(super) async fn maintained_nodes(
        &self,
    ) -> Result<BTreeSet<NodeId>, Box<dyn std::error::Error>> {
        Ok(list_resources::<Node>(&self.store, &self.keys, "Node")
            .await?
            .into_iter()
            .filter(|node| {
                node.status.conditions.iter().any(|condition| {
                    condition.condition_type.0 == "Maintenance"
                        && condition.state == kernel_api::ConditionState::True
                })
            })
            .map(|node| node.meta.id)
            .collect())
    }

    pub(super) async fn mark_run_deleting(&self) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.resource(
            &ResourceKind::new("UpgradeRun")?,
            &ResourceName::new("upgrade-1")?,
        );
        let stored = self.store.get(&key).await?.ok_or("run missing")?;
        let mut run: UpgradeRun = serde_json::from_slice(&stored.value)?;
        run.meta.deletion_timestamp = Some(Timestamp(10_000));
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&run)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("run deletion conflicted".into())
        }
    }

    pub(super) fn requests(&self) -> Vec<NodeUpgradeRequest> {
        self.backend.requests()
    }
}

struct RecordingBackend {
    store: Arc<InMemoryStore>,
    keys: Keyspace,
    mutate_dispatches: bool,
    accepted: Mutex<BTreeSet<String>>,
    requests: Mutex<Vec<NodeUpgradeRequest>>,
}

impl RecordingBackend {
    fn requests(&self) -> Vec<NodeUpgradeRequest> {
        lock(&self.requests).clone()
    }

    async fn publish_upgraded_nodes(
        &self,
        request: &NodeUpgradeRequest,
    ) -> Result<(), NodeUpgradeBackendError> {
        for target in &request.targets {
            let key = self.keys.resource(
                &ResourceKind::new("Node").map_err(rejected)?,
                &ResourceName::from(target.node_id.clone()),
            );
            let stored = self
                .store
                .get(&key)
                .await
                .map_err(unavailable)?
                .ok_or_else(|| NodeUpgradeBackendError::Rejected {
                    message: format!("node `{}` disappeared", target.node_id),
                })?;
            let mut node: Node = serde_json::from_slice(&stored.value).map_err(rejected)?;
            node.status.instance_id =
                NodeInstanceId::new(format!("{}-upgraded", target.previous_instance_id))
                    .map_err(rejected)?;
            node.status.version.clone_from(&request.target_version);
            let outcome = self
                .store
                .put_cas(PutRequest {
                    key,
                    value: serde_json::to_vec(&node).map_err(rejected)?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await
                .map_err(unavailable)?;
            if !matches!(outcome, CasOutcome::Applied(_)) {
                return Err(NodeUpgradeBackendError::Unavailable {
                    message: format!("node `{}` update conflicted", target.node_id),
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NodeUpgradeBackend for RecordingBackend {
    async fn apply(&self, request: &NodeUpgradeRequest) -> Result<(), NodeUpgradeBackendError> {
        lock(&self.requests).push(request.clone());
        let request_key = format!(
            "{}:{}",
            request.run_id,
            request
                .targets
                .iter()
                .map(|target| target.node_id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
        let first = lock(&self.accepted).insert(request_key);
        if first && self.mutate_dispatches {
            self.publish_upgraded_nodes(request).await?;
        }
        Ok(())
    }
}

fn node(id: &str, role: NodeRole) -> Result<Node, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(NodeId::new(id)?),
        spec: NodeSpec {
            hostname: format!("{id}.internal"),
            host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            role,
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new(format!("instance-{id}"))?,
            version: "1.0.0".to_string(),
            last_seen: Timestamp(10_000),
            conditions: Vec::new(),
        },
    })
}

fn run(mode: UpgradeMode) -> Result<UpgradeRun, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(UpgradeRunId::new("upgrade-1")?),
        spec: UpgradeRunSpec {
            target_version: "2.0.0".to_string(),
            mode,
            node_ids: Vec::new(),
        },
        status: UpgradeRunStatus {
            phase: UpgradePhase::Pending,
            nodes: Vec::new(),
            conditions: Vec::new(),
        },
    })
}

fn runtime(
    cluster_id: ClusterId,
    clock: Arc<dyn Clock>,
    store: Arc<FencedStore>,
    backend: Arc<RecordingBackend>,
) -> Result<ControllerRuntime<UpgradeReconciler>, Box<dyn std::error::Error>> {
    Ok(Arc::new(UpgradeReconciler::new(
        cluster_id,
        clock.clone(),
        Arc::new(FixedTimestampClock),
        UpgradeSettings::new(Duration::from_secs(5), Duration::from_secs(1), 3)?,
        backend,
    )?)
    .runtime(
        store,
        RuntimeConfig::new(
            Duration::from_secs(30),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
    ))
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

async fn put_resource<Id, Spec, Status>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
    resource: &Object<Id, Spec, Status>,
) -> Result<(), Box<dyn std::error::Error>>
where
    Id: Clone + Into<ResourceName> + serde::Serialize,
    Spec: serde::Serialize,
    Status: serde::Serialize,
{
    put_key(
        store,
        keys.resource(&ResourceKind::new(kind)?, &resource.meta.id.clone().into()),
        serde_json::to_vec(resource)?,
    )
    .await
}

async fn put_key(
    store: &InMemoryStore,
    key: kernel_store::StoreKey,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key,
            value,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("resource create conflicted".into())
    }
}

async fn list_resources<Resource: serde::de::DeserializeOwned>(
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

fn rejected(error: impl std::fmt::Display) -> NodeUpgradeBackendError {
    NodeUpgradeBackendError::Rejected {
        message: error.to_string(),
    }
}

fn unavailable(error: impl std::fmt::Display) -> NodeUpgradeBackendError {
    NodeUpgradeBackendError::Unavailable {
        message: error.to_string(),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
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
