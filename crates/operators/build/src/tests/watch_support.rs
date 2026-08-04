use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, Build, BuildId, BuildPhase, BuildSource, BuildSpec, BuildStatus,
    BuildTemplate, ClusterId, Deployment, DeploymentGoal, DeploymentId, DeploymentPhase,
    DeploymentSpec, DeploymentStatus, ExecPolicy, Generation, NodeApiAccess, NodeId,
    NodeInstanceId, Object, ObjectMeta, PlacementConstraint, ResourceKind, ResourceName,
    RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp,
};
use kernel_controller::{Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store, StoredValue,
};

use crate::{BuildRevisionResolver, BuildSourceError, BuildWatchReconciler, BuildWatchSettings};

pub(super) type WatchTestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(super) struct WatchWorld {
    cluster_id: ClusterId,
    keys: Keyspace,
    pub(super) store: Arc<InMemoryStore>,
    fenced: Arc<FencedStore>,
    _session: Box<dyn Session>,
}

impl WatchWorld {
    pub(super) async fn new() -> WatchTestResult<Self> {
        let cluster_id = ClusterId::new("cluster-1")?;
        let keys = Keyspace::new(&cluster_id);
        let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"build-watch-controller".to_vec(),
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
                    instance_id: NodeInstanceId::new("build-watch-controller")?,
                },
                session.id(),
                leader.version,
            ),
        ));
        Ok(Self {
            cluster_id,
            keys,
            store,
            fenced,
            _session: session,
        })
    }

    pub(super) async fn seed(
        &self,
        service: &Service,
        deployment: &Deployment,
        build: &Build,
    ) -> WatchTestResult {
        self.put("Service", &service.meta.id, service).await?;
        self.put("Deployment", &deployment.meta.id, deployment)
            .await?;
        self.put("Build", &build.meta.id, build).await
    }

    pub(super) fn runtime(
        &self,
        resolver: Arc<dyn BuildRevisionResolver>,
    ) -> WatchTestResult<kernel_controller::ControllerRuntime<BuildWatchReconciler>> {
        let reconciler = Arc::new(BuildWatchReconciler::new(
            self.cluster_id.clone(),
            resolver,
            BuildWatchSettings {
                poll_interval: Duration::from_secs(30),
            },
        )?);
        Ok(reconciler.runtime(
            self.fenced.clone(),
            Arc::new(NoopClock),
            RuntimeConfig::new(
                Duration::from_secs(30),
                Backoff::new(Duration::from_secs(30), Duration::from_secs(5 * 60))?,
            )?,
        ))
    }

    pub(super) async fn stored_service(&self) -> WatchTestResult<StoredValue> {
        self.store
            .get(&self.service_key()?)
            .await?
            .ok_or_else(|| "Service disappeared".into())
    }

    pub(super) fn service_key(&self) -> WatchTestResult<kernel_store::StoreKey> {
        self.key("Service", "api")
    }

    pub(super) fn deployment_key(&self) -> WatchTestResult<kernel_store::StoreKey> {
        self.key("Deployment", "deployment-1")
    }

    pub(super) async fn service(&self) -> WatchTestResult<Service> {
        Ok(serde_json::from_slice(&self.stored_service().await?.value)?)
    }

    async fn put<Id: Clone + Into<ResourceName>>(
        &self,
        kind: &str,
        id: &Id,
        resource: &impl serde::Serialize,
    ) -> WatchTestResult {
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self
                    .keys
                    .resource(&ResourceKind::new(kind)?, &id.clone().into()),
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

    fn key(&self, kind: &str, id: &str) -> WatchTestResult<kernel_store::StoreKey> {
        Ok(self
            .keys
            .resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?))
    }
}

pub(super) fn fixture(
    phase: DeploymentPhase,
    source_revision: Option<&str>,
    captured_revision: &str,
) -> WatchTestResult<(Service, Deployment, Build)> {
    let service_id = ServiceId::new("api")?;
    let deployment_id = DeploymentId::new("deployment-1")?;
    let build_id = BuildId::new("build-1")?;
    let spec = service_spec(captured_revision);
    let service = Object {
        meta: metadata(service_id.clone(), Generation(2)),
        spec: spec.clone(),
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout: RolloutState::Active,
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    };
    let deployment = Object {
        meta: metadata(deployment_id.clone(), Generation(1)),
        spec: DeploymentSpec {
            service_id: service_id.clone(),
            service_generation: Generation(2),
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            service: spec.clone(),
            goal: DeploymentGoal::Run,
            build_id: Some(build_id.clone()),
        },
        status: DeploymentStatus {
            phase,
            created_at: Timestamp(10_000),
            ready_at: None,
            draining_at: None,
            image_digest: None,
            git_commit: None,
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    };
    let ArtifactTemplate::Build { template } = spec.artifact else {
        return Err("fixture service is not build-backed".into());
    };
    let build = Object {
        meta: metadata(build_id, Generation(1)),
        spec: BuildSpec {
            service_id,
            deployment_id,
            template,
        },
        status: BuildStatus {
            phase: if source_revision.is_some() {
                BuildPhase::Succeeded
            } else {
                BuildPhase::Failed
            },
            image_digest: source_revision.map(|_| "sha256:image".to_string()),
            source_revision: source_revision.map(str::to_string),
            source_title: None,
            conditions: Vec::new(),
        },
    };
    Ok((service, deployment, build))
}

fn service_spec(revision: &str) -> ServiceSpec {
    ServiceSpec {
        name: "api".to_string(),
        version: "1".to_string(),
        artifact: ArtifactTemplate::Build {
            template: BuildTemplate {
                source: BuildSource::Git {
                    repository: "https://github.com/acme/api.git".to_string(),
                    revision: revision.to_string(),
                },
                dockerfile: "Dockerfile".to_string(),
                watch: true,
                registry: None,
                depot: None,
                environment: BTreeMap::new(),
                environment_source: None,
                secrets: BTreeMap::from([(
                    "GH_TOKEN".to_owned(),
                    kernel_api::SecretValue::new("github-watch-secret"),
                )]),
                secrets_source: None,
            },
        },
        preview: None,
        command: None,
        replicas: 1,
        exposed_ports: vec![8080],
        health_check: None,
        max_restarts: Some(3),
        environment: BTreeMap::new(),
        environment_sources: Vec::new(),
        user: None,
        node_api: NodeApiAccess::Disabled,
        secrets: None,
        volumes: Vec::new(),
        placement: PlacementConstraint::default(),
        exec: ExecPolicy::Denied,
    }
}

fn metadata<Id>(id: Id, generation: Generation) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: Default::default(),
        generation,
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}

pub(super) struct FakeRevisionResolver {
    results: Mutex<VecDeque<Result<Option<String>, BuildSourceError>>>,
    calls: Mutex<Vec<BuildSource>>,
    github_tokens: Mutex<Vec<Option<String>>>,
}

impl FakeRevisionResolver {
    pub(super) fn new(results: Vec<Result<Option<String>, BuildSourceError>>) -> Self {
        Self {
            results: Mutex::new(results.into()),
            calls: Mutex::new(Vec::new()),
            github_tokens: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn fixed(revision: &str) -> Self {
        Self::new(vec![Ok(Some(revision.to_string()))])
    }

    pub(super) fn calls(&self) -> Vec<BuildSource> {
        lock(&self.calls).clone()
    }

    pub(super) fn github_tokens(&self) -> Vec<Option<String>> {
        lock(&self.github_tokens).clone()
    }
}

#[async_trait]
impl BuildRevisionResolver for FakeRevisionResolver {
    async fn resolve_revision(
        &self,
        source: &BuildSource,
        github_token: Option<&kernel_api::SecretValue>,
    ) -> Result<Option<String>, BuildSourceError> {
        lock(&self.calls).push(source.clone());
        lock(&self.github_tokens).push(github_token.map(|token| token.expose().to_owned()));
        let mut results = lock(&self.results);
        if results.len() > 1 {
            match results.pop_front() {
                Some(result) => result,
                None => Err(BuildSourceError::rejected("fake resolver has no result")),
            }
        } else {
            match results.front() {
                Some(result) => result.clone(),
                None => Err(BuildSourceError::rejected("fake resolver has no result")),
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
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
