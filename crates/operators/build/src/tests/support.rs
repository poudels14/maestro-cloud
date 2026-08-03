use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    Build, BuildId, BuildPhase, BuildSource, BuildSpec, BuildStatus, BuildTemplate, ClusterId,
    DeploymentId, Generation, NodeId, NodeInstanceId, Object, ObjectMeta, ResourceKind,
    ResourceName, ServiceId, Timestamp,
};
use kernel_controller::{
    Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};
use runtime::{
    ArtifactBuildOutputSink, ArtifactBuildOutputStream, ArtifactBuildRequest, ArtifactByteStream,
    ArtifactDigest, ArtifactPrunePolicy, ArtifactPruneReport, ArtifactReference, ArtifactSource,
    ArtifactStore, ArtifactStoreError, ValueSourceResolver,
};

use crate::{
    BuildReconciler, BuildSourceError, BuildSourceProvider, DepotBuildBackend, PreparedBuildSource,
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(super) struct TestWorld {
    cluster_id: ClusterId,
    keys: Keyspace,
    pub(super) store: Arc<InMemoryStore>,
    pub(super) logs: Arc<logs::InMemoryLogStore>,
    fenced: Arc<FencedStore>,
    _session: Box<dyn Session>,
}

impl TestWorld {
    pub(super) async fn new() -> TestResult<Self> {
        let cluster_id = ClusterId::new("cluster-1")?;
        let keys = Keyspace::new(&cluster_id);
        let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"build-controller".to_vec(),
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
                    instance_id: NodeInstanceId::new("build-controller")?,
                },
                session.id(),
                leader.version,
            ),
        ));
        Ok(Self {
            cluster_id,
            keys,
            store,
            logs: Arc::new(logs::InMemoryLogStore::new()),
            fenced,
            _session: session,
        })
    }

    pub(super) async fn seed(&self, build: &Build) -> TestResult {
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self.build_key()?,
                value: serde_json::to_vec(build)?,
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("build create conflicted".into())
        }
    }

    pub(super) fn runtime(
        &self,
        source: Arc<dyn BuildSourceProvider>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> TestResult<kernel_controller::ControllerRuntime<BuildReconciler>> {
        self.runtime_with_depot(source, artifacts, None)
    }

    pub(super) fn runtime_with_depot(
        &self,
        source: Arc<dyn BuildSourceProvider>,
        artifacts: Arc<dyn ArtifactStore>,
        depot: Option<Arc<dyn DepotBuildBackend>>,
    ) -> TestResult<kernel_controller::ControllerRuntime<BuildReconciler>> {
        self.runtime_with_backends(source, artifacts, depot, None)
    }

    pub(super) fn runtime_with_value_sources(
        &self,
        source: Arc<dyn BuildSourceProvider>,
        artifacts: Arc<dyn ArtifactStore>,
        value_sources: Arc<dyn ValueSourceResolver>,
    ) -> TestResult<kernel_controller::ControllerRuntime<BuildReconciler>> {
        self.runtime_with_backends(source, artifacts, None, Some(value_sources))
    }

    fn runtime_with_backends(
        &self,
        source: Arc<dyn BuildSourceProvider>,
        artifacts: Arc<dyn ArtifactStore>,
        depot: Option<Arc<dyn DepotBuildBackend>>,
        value_sources: Option<Arc<dyn ValueSourceResolver>>,
    ) -> TestResult<kernel_controller::ControllerRuntime<BuildReconciler>> {
        let reconciler = Arc::new(
            BuildReconciler::new(
                self.cluster_id.clone(),
                source,
                artifacts,
                self.logs.clone(),
                Arc::new(FixedTimestampClock),
            )?
            .with_depot_backend(depot)
            .with_value_source_resolver(value_sources),
        );
        Ok(reconciler.runtime(
            self.fenced.clone(),
            Arc::new(NoopClock),
            RuntimeConfig::new(
                Duration::from_secs(30),
                Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
            )?,
        ))
    }

    pub(super) async fn build(&self) -> TestResult<Build> {
        let stored = self
            .store
            .get(&self.build_key()?)
            .await?
            .ok_or("build disappeared")?;
        Ok(serde_json::from_slice(&stored.value)?)
    }

    pub(super) async fn mark_deleting(&self) -> TestResult {
        let key = self.build_key()?;
        let stored = self.store.get(&key).await?.ok_or("build disappeared")?;
        let mut build: Build = serde_json::from_slice(&stored.value)?;
        build.meta.deletion_timestamp = Some(Timestamp(20_000));
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&build)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("build deletion marker conflicted".into())
        }
    }

    pub(super) async fn build_exists(&self) -> TestResult<bool> {
        Ok(self.store.get(&self.build_key()?).await?.is_some())
    }

    pub(super) fn build_key(&self) -> TestResult<kernel_store::StoreKey> {
        Ok(self
            .keys
            .resource(&ResourceKind::new("Build")?, &ResourceName::new("build-1")?))
    }
}

pub(super) fn queued_build(dockerfile: &str) -> TestResult<Build> {
    Ok(Object {
        meta: ObjectMeta {
            id: BuildId::new("build-1")?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: Default::default(),
            generation: Generation(3),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: BuildSpec {
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("deployment-1")?,
            template: BuildTemplate {
                source: BuildSource::Git {
                    repository: "https://github.com/acme/api.git".to_string(),
                    revision: "main".to_string(),
                },
                dockerfile: dockerfile.to_string(),
                watch: false,
                registry: None,
                depot: None,
                environment: BTreeMap::from([("PROFILE".to_string(), "release".to_string())]),
                environment_source: None,
                secrets: BTreeMap::from([
                    (
                        "GH_TOKEN".to_string(),
                        kernel_api::SecretValue::new("github-secret"),
                    ),
                    (
                        "TOKEN".to_string(),
                        kernel_api::SecretValue::new("secret-value"),
                    ),
                ]),
                secrets_source: None,
            },
        },
        status: BuildStatus {
            phase: BuildPhase::Queued,
            image_digest: None,
            source_revision: None,
            conditions: Vec::new(),
        },
    })
}

pub(super) struct RecordingSource {
    results: Mutex<VecDeque<Result<PreparedBuildSource, BuildSourceError>>>,
    calls: Mutex<Vec<Option<String>>>,
    github_tokens: Mutex<Vec<Option<String>>>,
    cleanup_calls: Mutex<Vec<BuildId>>,
}

impl RecordingSource {
    pub(super) fn successful(revision: &str) -> Self {
        Self::new(vec![Ok(prepared(revision))])
    }

    pub(super) fn new(results: Vec<Result<PreparedBuildSource, BuildSourceError>>) -> Self {
        Self {
            results: Mutex::new(results.into()),
            calls: Mutex::new(Vec::new()),
            github_tokens: Mutex::new(Vec::new()),
            cleanup_calls: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn calls(&self) -> Vec<Option<String>> {
        lock(&self.calls).clone()
    }

    pub(super) fn cleanup_calls(&self) -> Vec<BuildId> {
        lock(&self.cleanup_calls).clone()
    }

    pub(super) fn github_tokens(&self) -> Vec<Option<String>> {
        lock(&self.github_tokens).clone()
    }
}

#[async_trait]
impl BuildSourceProvider for RecordingSource {
    async fn prepare(
        &self,
        _build_id: &BuildId,
        _source: &BuildSource,
        resolved_revision: Option<&str>,
        github_token: Option<&kernel_api::SecretValue>,
    ) -> Result<PreparedBuildSource, BuildSourceError> {
        lock(&self.calls).push(resolved_revision.map(str::to_string));
        lock(&self.github_tokens).push(github_token.map(|token| token.expose().to_owned()));
        let mut results = lock(&self.results);
        if results.len() > 1 {
            match results.pop_front() {
                Some(result) => result,
                None => Err(BuildSourceError::rejected("fake source has no result")),
            }
        } else {
            match results.front() {
                Some(result) => result.clone(),
                None => Err(BuildSourceError::rejected("fake source has no result")),
            }
        }
    }

    async fn cleanup(&self, build_id: &BuildId) -> Result<(), BuildSourceError> {
        lock(&self.cleanup_calls).push(build_id.clone());
        Ok(())
    }
}

pub(super) fn prepared(revision: &str) -> PreparedBuildSource {
    PreparedBuildSource {
        artifact_source: ArtifactSource::Directory {
            root: PathBuf::from("/var/lib/maestro/builds/build-1"),
            definition: PathBuf::new(),
        },
        revision: revision.to_string(),
    }
}

pub(super) struct RecordingArtifacts {
    result: Result<ArtifactDigest, ArtifactStoreError>,
    build_output: Vec<(ArtifactBuildOutputStream, Vec<u8>)>,
    calls: Mutex<Vec<ArtifactBuildRequest>>,
    publishes: Mutex<Vec<(ArtifactDigest, ArtifactReference)>>,
    race: Mutex<Option<(Arc<InMemoryStore>, kernel_store::StoreKey)>>,
}

impl RecordingArtifacts {
    pub(super) fn successful() -> TestResult<Self> {
        Ok(Self {
            result: Ok(ArtifactDigest::new("sha256:abc123")?),
            build_output: Vec::new(),
            calls: Mutex::new(Vec::new()),
            publishes: Mutex::new(Vec::new()),
            race: Mutex::new(None),
        })
    }

    pub(super) fn with_build_output(
        mut self,
        output: impl IntoIterator<Item = (ArtifactBuildOutputStream, Vec<u8>)>,
    ) -> Self {
        self.build_output = output.into_iter().collect();
        self
    }

    pub(super) fn calls(&self) -> Vec<ArtifactBuildRequest> {
        lock(&self.calls).clone()
    }

    pub(super) fn publishes(&self) -> Vec<(ArtifactDigest, ArtifactReference)> {
        lock(&self.publishes).clone()
    }

    pub(super) fn race_with_cancellation(
        &self,
        store: Arc<InMemoryStore>,
        key: kernel_store::StoreKey,
    ) {
        *lock(&self.race) = Some((store, key));
    }
}

#[async_trait]
impl ArtifactStore for RecordingArtifacts {
    async fn build(
        &self,
        request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        lock(&self.calls).push(request.clone());
        let race = lock(&self.race).take();
        if let Some((store, key)) = race {
            let stored = store.get(&key).await.map_err(rejected)?.ok_or_else(|| {
                ArtifactStoreError::Rejected {
                    message: "racing Build disappeared".to_string(),
                }
            })?;
            let mut build: Build = serde_json::from_slice(&stored.value).map_err(rejected)?;
            build.status.phase = BuildPhase::Canceled;
            let outcome = store
                .put_cas(PutRequest {
                    key,
                    value: serde_json::to_vec(&build).map_err(rejected)?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await
                .map_err(rejected)?;
            if !matches!(outcome, CasOutcome::Applied(_)) {
                return Err(ArtifactStoreError::Rejected {
                    message: "injected cancellation conflicted".to_string(),
                });
            }
        }
        self.result.clone()
    }

    async fn build_with_output(
        &self,
        request: &ArtifactBuildRequest,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        for (stream, frame) in &self.build_output {
            output.write(*stream, frame.clone()).await;
        }
        self.build(request).await
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

    async fn publish(
        &self,
        digest: &ArtifactDigest,
        destination: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        lock(&self.publishes).push((digest.clone(), destination.clone()));
        digest.for_reference(destination)
    }

    async fn resolve_digest(
        &self,
        _reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("resolve_digest"))
    }

    async fn contains(&self, _digest: &ArtifactDigest) -> Result<bool, ArtifactStoreError> {
        Ok(false)
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

fn rejected(error: impl std::fmt::Display) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: error.to_string(),
    }
}

fn unused(operation: &str) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: format!("unexpected {operation} call"),
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
        Timestamp(12_345)
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
