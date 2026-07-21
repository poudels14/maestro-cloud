use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    Generation, Node, NodeId, NodeInstanceId, NodeRole, NodeSpec, NodeStatus, Object, Preview,
    PreviewPhase, ResourceKind, ResourceName, RolloutState, Service, Timestamp,
};
use kernel_controller::{
    Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store, StoreKey,
};

use crate::{
    PreviewSourceReconciler, PreviewSourceSettings, PullRequest, PullRequestApi,
    PullRequestApiError, PullRequestReadiness,
};

use super::support::{base_service, metadata};

#[tokio::test]
async fn source_reconciler_creates_pushes_closes_and_reopens_one_stable_preview()
-> Result<(), Box<dyn std::error::Error>> {
    let api = Arc::new(FakePullRequests::new(vec![pull_request("first")]));
    let world = World::new(api.clone()).await?;
    world.put("Node", &node()).await?;
    let mut base = base_service();
    base.status.rollout = RolloutState::Active;
    world.put("Service", &base).await?;

    assert_eq!(world.runtime.reconcile_snapshot().await?, 1);
    let created = world.preview().await?;
    assert_eq!(created.meta.id.as_str(), "api-pr-42");
    assert_eq!(created.spec.head_revision, "first");
    assert_eq!(created.status.phase, PreviewPhase::Pending);

    api.set_open(vec![pull_request("second")]);
    world.runtime.reconcile_snapshot().await?;
    let pushed = world.preview().await?;
    assert_eq!(pushed.meta.id, created.meta.id);
    assert_eq!(pushed.spec.service_id, created.spec.service_id);
    assert_eq!(pushed.spec.head_revision, "second");

    api.set_open(Vec::new());
    world.clock.set_millis(20_000);
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(
        world.preview().await?.meta.deletion_timestamp,
        Some(Timestamp(20_000))
    );

    api.set_open(vec![pull_request("third")]);
    world.runtime.reconcile_snapshot().await?;
    let reopened = world.preview().await?;
    assert_eq!(reopened.meta.deletion_timestamp, None);
    assert_eq!(reopened.status.phase, PreviewPhase::Pending);
    assert_eq!(reopened.spec.head_revision, "third");

    let comments = api.comments();
    assert_eq!(comments.len(), 4);
    assert!(comments.iter().all(|comment| comment.3 == "preview-api"));
    assert!(
        comments
            .iter()
            .any(|comment| comment.4.contains("building"))
    );
    assert!(
        comments
            .iter()
            .any(|comment| comment.4.contains("updating"))
    );
    assert!(comments.iter().any(|comment| comment.4.contains("removal")));
    assert!(
        comments
            .iter()
            .any(|comment| comment.4.contains("restored"))
    );
    Ok(())
}

#[tokio::test]
async fn repository_rate_limit_suppresses_calls_until_the_injected_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let api = Arc::new(FakePullRequests::new(vec![pull_request("first")]));
    api.enqueue_list(Err(PullRequestApiError::RateLimited {
        retry_after: Duration::from_secs(30),
    }));
    let world = World::new(api.clone()).await?;
    world.put("Node", &node()).await?;
    let mut base = base_service();
    base.status.rollout = RolloutState::Active;
    world.put("Service", &base).await?;

    world.runtime.reconcile_snapshot().await?;
    assert_eq!(api.list_calls(), 1);
    assert!(world.previews().await?.is_empty());

    world.clock.set_millis(29_999);
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(api.list_calls(), 1);
    assert!(world.previews().await?.is_empty());

    world.clock.set_millis(30_000);
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(api.list_calls(), 2);
    assert_eq!(world.previews().await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn failed_sticky_comment_is_nonfatal_and_retried_from_level_state()
-> Result<(), Box<dyn std::error::Error>> {
    let api = Arc::new(FakePullRequests::new(vec![pull_request("first")]));
    api.enqueue_comment(Err(PullRequestApiError::Unavailable {
        message: "comments unavailable".to_string(),
    }));
    let world = World::new(api.clone()).await?;
    world.put("Node", &node()).await?;
    let mut base = base_service();
    base.status.rollout = RolloutState::Active;
    world.put("Service", &base).await?;

    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.previews().await?.len(), 1);
    assert!(api.comments().is_empty());

    world.clock.set_millis(5_000);
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(api.comments().len(), 1);
    assert!(api.comments().first().unwrap().4.contains("updating"));
    Ok(())
}

#[tokio::test]
async fn concurrent_service_edit_conflicts_without_preview_or_feedback()
-> Result<(), Box<dyn std::error::Error>> {
    let api = Arc::new(FakePullRequests::new(vec![pull_request("first")]));
    let world = World::new(api.clone()).await?;
    world.put("Node", &node()).await?;
    let mut base = base_service();
    base.status.rollout = RolloutState::Active;
    world.put("Service", &base).await?;
    api.conflict_once(
        world.store.clone(),
        world
            .keys
            .resource(&ResourceKind::new("Service")?, &ResourceName::new("api")?),
    );

    world.runtime.reconcile_snapshot().await?;
    assert!(world.previews().await?.is_empty());
    assert!(api.comments().is_empty());

    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.previews().await?.len(), 1);
    assert_eq!(api.comments().len(), 1);
    Ok(())
}

type ListResult = Result<Vec<PullRequest>, PullRequestApiError>;
type Comment = (String, String, u64, String, String);

struct FakePullRequests {
    open: Mutex<Vec<PullRequest>>,
    list_results: Mutex<VecDeque<ListResult>>,
    comment_results: Mutex<VecDeque<Result<(), PullRequestApiError>>>,
    list_calls: AtomicUsize,
    comments: Mutex<Vec<Comment>>,
    conflict: Mutex<Option<(Arc<InMemoryStore>, StoreKey)>>,
}

impl FakePullRequests {
    fn new(open: Vec<PullRequest>) -> Self {
        Self {
            open: Mutex::new(open),
            list_results: Mutex::new(VecDeque::new()),
            comment_results: Mutex::new(VecDeque::new()),
            list_calls: AtomicUsize::new(0),
            comments: Mutex::new(Vec::new()),
            conflict: Mutex::new(None),
        }
    }

    fn set_open(&self, open: Vec<PullRequest>) {
        *self.open.lock().unwrap() = open;
    }

    fn enqueue_list(&self, result: ListResult) {
        self.list_results.lock().unwrap().push_back(result);
    }

    fn enqueue_comment(&self, result: Result<(), PullRequestApiError>) {
        self.comment_results.lock().unwrap().push_back(result);
    }

    fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }

    fn comments(&self) -> Vec<Comment> {
        self.comments.lock().unwrap().clone()
    }

    fn conflict_once(&self, store: Arc<InMemoryStore>, key: StoreKey) {
        *self.conflict.lock().unwrap() = Some((store, key));
    }
}

#[async_trait]
impl PullRequestApi for FakePullRequests {
    async fn list_open(
        &self,
        _owner: &str,
        _repository: &str,
    ) -> Result<Vec<PullRequest>, PullRequestApiError> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        let conflict = self.conflict.lock().unwrap().take();
        if let Some((store, key)) = conflict {
            let stored = store
                .get(&key)
                .await
                .map_err(|error| PullRequestApiError::Unavailable {
                    message: error.to_string(),
                })?
                .ok_or_else(|| PullRequestApiError::Unavailable {
                    message: "conflict target disappeared".to_string(),
                })?;
            let mut service: Service = serde_json::from_slice(&stored.value).map_err(|error| {
                PullRequestApiError::Unavailable {
                    message: error.to_string(),
                }
            })?;
            service.spec.version = "concurrent-edit".to_string();
            service.meta.generation = Generation(service.meta.generation.0.saturating_add(1));
            let outcome = store
                .put_cas(PutRequest {
                    key,
                    value: serde_json::to_vec(&service).map_err(|error| {
                        PullRequestApiError::Unavailable {
                            message: error.to_string(),
                        }
                    })?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await
                .map_err(|error| PullRequestApiError::Unavailable {
                    message: error.to_string(),
                })?;
            if !matches!(outcome, CasOutcome::Applied(_)) {
                return Err(PullRequestApiError::Unavailable {
                    message: "conflict injection lost its compare".to_string(),
                });
            }
        }
        if let Some(result) = self.list_results.lock().unwrap().pop_front() {
            result
        } else {
            Ok(self.open.lock().unwrap().clone())
        }
    }

    async fn upsert_comment(
        &self,
        owner: &str,
        repository: &str,
        pull_request_number: u64,
        comment_key: &str,
        body: &str,
    ) -> Result<(), PullRequestApiError> {
        if let Some(result) = self.comment_results.lock().unwrap().pop_front() {
            result?;
        }
        self.comments.lock().unwrap().push((
            owner.to_string(),
            repository.to_string(),
            pull_request_number,
            comment_key.to_string(),
            body.to_string(),
        ));
        Ok(())
    }
}

struct World {
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    runtime: kernel_controller::ControllerRuntime<PreviewSourceReconciler>,
    clock: Arc<ManualClock>,
    _session: Box<dyn Session>,
}

impl World {
    async fn new(api: Arc<dyn PullRequestApi>) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = kernel_api::ClusterId::new("preview-source-test")?;
        let keys = Keyspace::new(&cluster_id);
        let clock = Arc::new(ManualClock::new(0));
        let store = Arc::new(InMemoryStore::new(clock.clone()));
        let session = store.session(Duration::from_secs(300)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"preview-source-test".to_vec(),
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
                    instance_id: NodeInstanceId::new("preview-source-test")?,
                },
                session.id(),
                leader.version,
            ),
        ));
        let reconciler = Arc::new(PreviewSourceReconciler::new(
            cluster_id,
            api,
            PreviewSourceSettings {
                poll_interval: Duration::from_secs(60),
                max_concurrent_previews: 3,
                initial_backoff: Duration::from_secs(5),
                max_backoff: Duration::from_secs(60),
            },
            clock.clone(),
            clock.clone(),
        )?);
        let runtime = reconciler.runtime(
            fenced,
            RuntimeConfig::new(
                Duration::from_secs(30),
                Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
            )?,
        );
        Ok(Self {
            keys,
            store,
            runtime,
            clock,
            _session: session,
        })
    }

    async fn put<Id, Spec, Status>(
        &self,
        kind: &str,
        resource: &kernel_api::Object<Id, Spec, Status>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Id: Clone + Into<ResourceName> + serde::Serialize,
        Spec: serde::Serialize,
        Status: serde::Serialize,
    {
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self
                    .keys
                    .resource(&ResourceKind::new(kind)?, &resource.meta.id.clone().into()),
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

    async fn preview(&self) -> Result<Preview, Box<dyn std::error::Error>> {
        self.previews()
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| "preview missing".into())
    }

    async fn previews(&self) -> Result<Vec<Preview>, Box<dyn std::error::Error>> {
        let prefix = self.keys.resource_kind(&ResourceKind::new("Preview")?);
        self.store
            .list(&prefix)
            .await?
            .values
            .into_iter()
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .collect()
    }
}

struct ManualClock {
    milliseconds: AtomicI64,
}

impl ManualClock {
    fn new(milliseconds: i64) -> Self {
        Self {
            milliseconds: AtomicI64::new(milliseconds),
        }
    }

    fn set_millis(&self, milliseconds: i64) {
        self.milliseconds.store(milliseconds, Ordering::SeqCst);
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        let milliseconds = self.milliseconds.load(Ordering::SeqCst).max(0);
        MonotonicTime::from_duration(Duration::from_millis(
            u64::try_from(milliseconds).unwrap_or_default(),
        ))
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}

impl TimestampClock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.milliseconds.load(Ordering::SeqCst))
    }
}

fn node() -> Node {
    Object {
        meta: metadata(NodeId::new("node-1").unwrap()),
        spec: NodeSpec {
            hostname: "node-1".to_string(),
            host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            role: NodeRole::Master,
            scheduling_labels: Default::default(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new("node-instance-1").unwrap(),
            last_seen: Timestamp(0),
            conditions: Vec::new(),
        },
    }
}

fn pull_request(revision: &str) -> PullRequest {
    PullRequest {
        number: 42,
        title: "Preview this".to_string(),
        readiness: PullRequestReadiness::Ready,
        created_at: Timestamp(0),
        head_reference: "feature".to_string(),
        head_revision: revision.to_string(),
        head_repository: Some("acme/api".to_string()),
    }
}
