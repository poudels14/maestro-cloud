use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    Build, BuildId, BuildPhase, BuildSpec, BuildStatus, Condition, ConditionReason, ConditionState,
    ConditionType, Object, ResourceKind,
};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{Clock, Keyspace};
use logs::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore, LogStream, OriginCursor,
};
use runtime::{
    ArtifactBuildOutputSink, ArtifactBuildOutputStream, ArtifactBuildRequest, ArtifactDigest,
    ArtifactReference, ArtifactStore, ArtifactStoreError, ValueSourceError, ValueSourceResolver,
};
use sha2::{Digest, Sha256};

use crate::DepotBuildBackend;
use crate::source::{BuildSourceError, BuildSourceProvider, PreparedBuildSource};
use crate::writer::{BuildStatusWriter, BuildWriteError};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);
/// Watch-driven Build resource reconciler.
pub struct BuildReconciler {
    cluster_id: kernel_api::ClusterId,
    source: Arc<dyn BuildSourceProvider>,
    artifacts: Arc<dyn ArtifactStore>,
    logs: Arc<dyn LogStore>,
    depot: Option<Arc<dyn DepotBuildBackend>>,
    value_sources: Option<Arc<dyn ValueSourceResolver>>,
    timestamp_clock: Arc<dyn TimestampClock>,
    writer: BuildStatusWriter,
    prefix: kernel_store::StorePrefix,
}

impl BuildReconciler {
    /// Creates a build reconciler without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        source: Arc<dyn BuildSourceProvider>,
        artifacts: Arc<dyn ArtifactStore>,
        logs: Arc<dyn LogStore>,
        timestamp_clock: Arc<dyn TimestampClock>,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        let keyspace = Keyspace::new(&cluster_id);
        let kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        let writer = BuildStatusWriter::new(&cluster_id)?;
        Ok(Self {
            cluster_id,
            source,
            artifacts,
            logs,
            depot: None,
            value_sources: None,
            timestamp_clock,
            writer,
            prefix: keyspace.resource_kind(&kind),
        })
    }

    /// Selects the protected Depot backend for templates that name a Depot project.
    pub fn with_depot_backend(mut self, depot: Option<Arc<dyn DepotBuildBackend>>) -> Self {
        self.depot = depot;
        self
    }

    /// Resolves external build value references only when a build consumes them.
    pub fn with_value_source_resolver(
        mut self,
        resolver: Option<Arc<dyn ValueSourceResolver>>,
    ) -> Self {
        self.value_sources = resolver;
        self
    }

    /// Wraps this operator in the shared watch, resync, and retry runtime.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        monotonic_clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new(
            self.clone(),
            self.prefix.clone(),
            store,
            monotonic_clock,
            config,
        )
    }

    async fn advance(
        &self,
        mut build: Build,
        context: &ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        build.meta.revision = context.observed_version().resource_revision();
        match build.status.phase {
            BuildPhase::Queued => {
                build.status.phase = BuildPhase::Preparing;
                build.status.image_digest = None;
                build.status.source_revision = None;
                build.status.source_title = None;
                self.set_condition(
                    &mut build,
                    ConditionState::False,
                    "Preparing",
                    "source preparation queued",
                );
                self.persist(context, &build, Action::Requeue(Duration::ZERO))
                    .await
            }
            BuildPhase::Preparing => self.prepare(build, context).await,
            BuildPhase::Building => self.build(build, context).await,
            BuildPhase::Succeeded | BuildPhase::Failed | BuildPhase::Canceled => Ok(Action::Done),
        }
    }

    async fn prepare(
        &self,
        mut build: Build,
        context: &ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        if let Err(message) = validate_definition(&build.spec.template.dockerfile) {
            return self
                .fail(build, context, "InvalidBuildDefinition", message)
                .await;
        }
        let secrets = match self.resolve_secrets(&build).await {
            Ok(secrets) => secrets,
            Err(ValueSourceError::Unavailable { message }) => {
                return self
                    .fail(build, context, "ExternalValueSourceUnavailable", message)
                    .await;
            }
            Err(ValueSourceError::Rejected { message }) => {
                return self
                    .fail(build, context, "ExternalValueSourceRejected", message)
                    .await;
            }
        };
        match self
            .source
            .prepare(
                &build.meta.id,
                &build.spec.template.source,
                None,
                github_token(&secrets),
            )
            .await
        {
            Ok(prepared) => {
                if prepared.revision.trim().is_empty() {
                    return self
                        .fail(
                            build,
                            context,
                            "InvalidSourceRevision",
                            "source backend returned an empty immutable revision".to_string(),
                        )
                        .await;
                }
                build.status.phase = BuildPhase::Building;
                build.status.source_revision = Some(prepared.revision);
                build.status.source_title = prepared.title;
                self.set_condition(
                    &mut build,
                    ConditionState::False,
                    "Building",
                    "artifact build is running",
                );
                self.persist(context, &build, Action::Requeue(Duration::ZERO))
                    .await
            }
            Err(BuildSourceError::Unavailable { message }) => {
                self.fail(build, context, "SourceUnavailable", message)
                    .await
            }
            Err(BuildSourceError::Rejected { message }) => {
                self.fail(build, context, "SourceRejected", message).await
            }
        }
    }

    async fn build(
        &self,
        build: Build,
        context: &ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        let Some(revision) = build.status.source_revision.clone() else {
            return self
                .fail(
                    build,
                    context,
                    "MissingSourceRevision",
                    "building phase has no resolved source revision".to_string(),
                )
                .await;
        };
        let secrets = match self.resolve_secrets(&build).await {
            Ok(secrets) => secrets,
            Err(ValueSourceError::Unavailable { message }) => {
                return self
                    .fail(build, context, "ExternalValueSourceUnavailable", message)
                    .await;
            }
            Err(ValueSourceError::Rejected { message }) => {
                return self
                    .fail(build, context, "ExternalValueSourceRejected", message)
                    .await;
            }
        };
        let prepared = match self
            .source
            .prepare(
                &build.meta.id,
                &build.spec.template.source,
                Some(&revision),
                github_token(&secrets),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(BuildSourceError::Unavailable { message }) => {
                return self
                    .fail(build, context, "SourceUnavailable", message)
                    .await;
            }
            Err(BuildSourceError::Rejected { message }) => {
                return self.fail(build, context, "SourceRejected", message).await;
            }
        };
        if prepared.revision != revision {
            return self
                .fail(
                    build,
                    context,
                    "SourceRevisionChanged",
                    format!(
                        "source backend returned revision `{}` while `{revision}` was pinned",
                        prepared.revision
                    ),
                )
                .await;
        }
        let environment = match self.resolve_environment(&build).await {
            Ok(environment) => environment,
            Err(ValueSourceError::Unavailable { message }) => {
                return self
                    .fail(build, context, "ExternalValueSourceUnavailable", message)
                    .await;
            }
            Err(ValueSourceError::Rejected { message }) => {
                return self
                    .fail(build, context, "ExternalValueSourceRejected", message)
                    .await;
            }
        };
        self.run_artifact_build(build, context, prepared, environment, secrets)
            .await
    }

    async fn run_artifact_build(
        &self,
        mut build: Build,
        context: &ReconcileContext,
        prepared: PreparedBuildSource,
        arguments: BTreeMap<String, kernel_api::SecretValue>,
        secrets: BTreeMap<String, kernel_api::SecretValue>,
    ) -> Result<Action, ReconcileError> {
        let definition = PathBuf::from(&build.spec.template.dockerfile);
        let request = ArtifactBuildRequest {
            source: with_definition(prepared, definition),
            arguments,
            secrets,
            tags: Vec::new(),
        };
        let output = BuildLogOutput::new(
            self.cluster_id.clone(),
            context.store().token().identity().node_id.clone(),
            build.meta.id.clone(),
            self.logs.clone(),
            self.timestamp_clock.clone(),
        );
        match self.build_and_publish(&build, &request, &output).await {
            Ok(digest) => {
                build.status.phase = BuildPhase::Succeeded;
                build.status.image_digest = Some(digest.as_str().to_string());
                self.set_condition(
                    &mut build,
                    ConditionState::True,
                    "BuildSucceeded",
                    "artifact is available",
                );
                self.persist(context, &build, Action::Done).await
            }
            Err(
                error
                @ (ArtifactStoreError::Unavailable { .. } | ArtifactStoreError::Stream { .. }),
            ) => {
                self.fail(
                    build,
                    context,
                    "ArtifactBuildUnavailable",
                    error.to_string(),
                )
                .await
            }
            Err(error) => {
                self.fail(build, context, "ArtifactBuildRejected", error.to_string())
                    .await
            }
        }
    }

    async fn resolve_environment(
        &self,
        build: &Build,
    ) -> Result<BTreeMap<String, kernel_api::SecretValue>, ValueSourceError> {
        let mut values = self
            .resolve_source(build.spec.template.environment_source.as_deref())
            .await?;
        values.extend(
            build
                .spec
                .template
                .environment
                .iter()
                .map(|(key, value)| (key.clone(), kernel_api::SecretValue::new(value.clone()))),
        );
        Ok(values)
    }

    async fn resolve_secrets(
        &self,
        build: &Build,
    ) -> Result<BTreeMap<String, kernel_api::SecretValue>, ValueSourceError> {
        let mut values = self
            .resolve_source(build.spec.template.secrets_source.as_deref())
            .await?;
        values.extend(build.spec.template.secrets.clone());
        Ok(values)
    }

    async fn resolve_source(
        &self,
        source: Option<&str>,
    ) -> Result<BTreeMap<String, kernel_api::SecretValue>, ValueSourceError> {
        let Some(source) = source else {
            return Ok(BTreeMap::new());
        };
        let resolver = self
            .value_sources
            .as_ref()
            .ok_or_else(|| ValueSourceError::Rejected {
                message: "build contains an external value source but no resolver is configured"
                    .to_owned(),
            })?;
        resolver.resolve(source).await
    }

    async fn build_and_publish(
        &self,
        build: &Build,
        request: &ArtifactBuildRequest,
        output: &BuildLogOutput,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let destination = build
            .spec
            .template
            .registry
            .as_ref()
            .map(|registry| {
                let repository = build
                    .spec
                    .template
                    .registry_repository
                    .as_ref()
                    .unwrap_or(&build.spec.service_id);
                ArtifactReference::new(format!(
                    "{registry}/{repository}:{deployment}",
                    registry = registry.trim_end_matches('/'),
                    deployment = build.spec.deployment_id,
                ))
            })
            .transpose()?;
        match &build.spec.template.depot {
            Some(depot) => {
                let backend = self
                    .depot
                    .as_ref()
                    .ok_or_else(|| ArtifactStoreError::Rejected {
                        message: "build selects Depot but this cluster has no Depot token"
                            .to_owned(),
                    })?;
                match destination.as_ref() {
                    Some(destination) => {
                        output
                            .event(
                                "publishing",
                                "info",
                                LogStream::Stdout,
                                format!(
                                    "Building and publishing image to {}",
                                    destination.as_str()
                                ),
                            )
                            .await;
                        let digest = backend
                            .build_and_publish_with_output(
                                request,
                                &depot.project,
                                destination,
                                output,
                            )
                            .await?;
                        output
                            .event(
                                "publishing",
                                "info",
                                LogStream::Stdout,
                                format!("Published image {digest}"),
                            )
                            .await;
                        Ok(digest)
                    }
                    None if backend.registry_enabled() => {
                        output
                            .event(
                                "publishing",
                                "info",
                                LogStream::Stdout,
                                "Publishing image to the Depot registry".to_owned(),
                            )
                            .await;
                        let digest = backend
                            .build_and_save_with_output(
                                request,
                                &depot.project,
                                build.spec.deployment_id.as_str(),
                                output,
                            )
                            .await?;
                        output
                            .event(
                                "publishing",
                                "info",
                                LogStream::Stdout,
                                format!("Published image {digest}"),
                            )
                            .await;
                        Ok(digest)
                    }
                    None => {
                        let digest = backend
                            .build_with_output(request, &depot.project, output)
                            .await?;
                        output
                            .event(
                                "publishing",
                                "info",
                                LogStream::Stdout,
                                "Build artifact is ready; publishing it to assigned nodes"
                                    .to_owned(),
                            )
                            .await;
                        Ok(digest)
                    }
                }
            }
            None => {
                let digest = self.artifacts.build_with_output(request, output).await?;
                match destination.as_ref() {
                    Some(destination) => {
                        output
                            .event(
                                "publishing",
                                "info",
                                LogStream::Stdout,
                                format!("Publishing image to {}", destination.as_str()),
                            )
                            .await;
                        let published = match self
                            .artifacts
                            .publish_with_output(&digest, destination, &PublishingLogOutput(output))
                            .await
                        {
                            Ok(published) => published,
                            Err(error) => {
                                output
                                    .event(
                                        "publishing",
                                        "error",
                                        LogStream::Stderr,
                                        format!("Image publication failed: {error}"),
                                    )
                                    .await;
                                return Err(error);
                            }
                        };
                        output
                            .event(
                                "publishing",
                                "info",
                                LogStream::Stdout,
                                format!("Published image {published}"),
                            )
                            .await;
                        Ok(published)
                    }
                    None => {
                        output
                            .event(
                                "publishing",
                                "info",
                                LogStream::Stdout,
                                "Build artifact is ready; publishing it to assigned nodes"
                                    .to_owned(),
                            )
                            .await;
                        Ok(digest)
                    }
                }
            }
        }
    }

    async fn fail(
        &self,
        mut build: Build,
        context: &ReconcileContext,
        reason: &str,
        message: String,
    ) -> Result<Action, ReconcileError> {
        let log_result = self.append_error(&build, context, reason, &message).await;
        build.status.phase = BuildPhase::Failed;
        build.status.image_digest = None;
        self.set_condition(&mut build, ConditionState::False, reason, &message);
        let action = self.persist(context, &build, Action::Done).await?;
        log_result?;
        Ok(action)
    }

    async fn append_error(
        &self,
        build: &Build,
        context: &ReconcileContext,
        reason: &str,
        message: &str,
    ) -> Result<(), ReconcileError> {
        let phase = build_phase_name(build.status.phase);
        let timestamp = build
            .status
            .conditions
            .iter()
            .find(|condition| condition.condition_type == ConditionType::Ready)
            .map_or_else(
                || self.timestamp_clock.now(),
                |condition| condition.last_transition_time,
            );
        let node_id = context.store().token().identity().node_id.clone();
        let cursor = build_error_cursor(phase, reason, message, timestamp);
        let entry = IngestLogEntry {
            id: LogRecordId {
                node_id: node_id.clone(),
                producer: LogProducer::Build(build.meta.id.clone()),
                cursor: OriginCursor::new(cursor),
            },
            observed_at: timestamp,
            event_at: timestamp,
            severity: "error".to_owned(),
            stream: LogStream::Stderr,
            origin: LogOrigin::Build {
                cluster_id: self.cluster_id.clone(),
                node_id,
                build_id: build.meta.id.clone(),
            },
            body: LogBody::Text(message.to_owned()),
            attributes: BTreeMap::from([
                ("maestro.build.phase".to_owned(), phase.to_owned()),
                ("maestro.build.reason".to_owned(), reason.to_owned()),
                ("maestro.build.retryable".to_owned(), "false".to_owned()),
            ]),
        };
        self.logs
            .append(&[entry])
            .await
            .map(|_| ())
            .map_err(|error| ReconcileError::Retryable {
                message: format!(
                    "failed to record build error: {error}; original build error: {message}"
                ),
            })
    }

    async fn persist(
        &self,
        context: &ReconcileContext,
        build: &Build,
        applied: Action,
    ) -> Result<Action, ReconcileError> {
        match self
            .writer
            .replace(context.store(), context.observed_version(), build)
            .await
        {
            Ok(true) => Ok(applied),
            Ok(false) => Ok(Action::Requeue(CONFLICT_RETRY)),
            Err(BuildWriteError::Controller(error)) => Err(ReconcileError::Infrastructure(error)),
            Err(error) => Err(ReconcileError::Terminal {
                reason: "BuildStatusWriteFailed".to_string(),
                message: error.to_string(),
            }),
        }
    }

    fn set_condition(&self, build: &mut Build, state: ConditionState, reason: &str, message: &str) {
        let previous = build
            .status
            .conditions
            .iter()
            .find(|condition| condition.condition_type == ConditionType::Ready);
        let transitioned_at = previous
            .filter(|condition| condition.state == state)
            .map_or_else(
                || self.timestamp_clock.now(),
                |condition| condition.last_transition_time,
            );
        let condition = Condition {
            condition_type: ConditionType::Ready,
            state,
            reason: ConditionReason(reason.to_string()),
            message: message.to_string(),
            observed_generation: build.meta.generation,
            last_transition_time: transitioned_at,
        };
        build
            .status
            .conditions
            .retain(|existing| existing.condition_type != ConditionType::Ready);
        build.status.conditions.push(condition);
    }
}

struct BuildLogOutput {
    cluster_id: kernel_api::ClusterId,
    node_id: kernel_api::NodeId,
    build_id: BuildId,
    logs: Arc<dyn LogStore>,
    timestamp_clock: Arc<dyn TimestampClock>,
    attempt: String,
    sequence: AtomicU64,
}

impl BuildLogOutput {
    fn new(
        cluster_id: kernel_api::ClusterId,
        node_id: kernel_api::NodeId,
        build_id: BuildId,
        logs: Arc<dyn LogStore>,
        timestamp_clock: Arc<dyn TimestampClock>,
    ) -> Self {
        Self {
            cluster_id,
            node_id,
            build_id,
            logs,
            timestamp_clock,
            attempt: uuid::Uuid::new_v4().simple().to_string(),
            sequence: AtomicU64::new(0),
        }
    }

    async fn event(&self, phase: &str, severity: &str, stream: LogStream, message: String) {
        self.append(phase, severity, stream, "maestro", message.into_bytes())
            .await;
    }

    async fn append(
        &self,
        phase: &str,
        severity: &str,
        stream: LogStream,
        output_kind: &str,
        output: Vec<u8>,
    ) {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let timestamp = self.timestamp_clock.now();
        let entry = IngestLogEntry {
            id: LogRecordId {
                node_id: self.node_id.clone(),
                producer: LogProducer::Build(self.build_id.clone()),
                cursor: OriginCursor::new(format!("build-output:{}:{sequence:020}", self.attempt)),
            },
            observed_at: timestamp,
            event_at: timestamp,
            severity: severity.to_owned(),
            stream,
            origin: LogOrigin::Build {
                cluster_id: self.cluster_id.clone(),
                node_id: self.node_id.clone(),
                build_id: self.build_id.clone(),
            },
            body: LogBody::Text(String::from_utf8_lossy(&output).into_owned()),
            attributes: BTreeMap::from([
                ("maestro.build.phase".to_owned(), phase.to_owned()),
                ("maestro.build.output".to_owned(), output_kind.to_owned()),
            ]),
        };
        let _ignored = self.logs.append(&[entry]).await;
    }
}

#[async_trait]
impl ArtifactBuildOutputSink for BuildLogOutput {
    async fn write(&self, stream: ArtifactBuildOutputStream, output: Vec<u8>) {
        let stream = match stream {
            ArtifactBuildOutputStream::Stdout => LogStream::Stdout,
            ArtifactBuildOutputStream::Stderr => LogStream::Stderr,
        };
        self.append("building", "info", stream, "backend", output)
            .await;
    }
}

struct PublishingLogOutput<'a>(&'a BuildLogOutput);

#[async_trait]
impl ArtifactBuildOutputSink for PublishingLogOutput<'_> {
    async fn write(&self, stream: ArtifactBuildOutputStream, output: Vec<u8>) {
        let stream = match stream {
            ArtifactBuildOutputStream::Stdout => LogStream::Stdout,
            ArtifactBuildOutputStream::Stderr => LogStream::Stderr,
        };
        self.0
            .append("publishing", "info", stream, "backend", output)
            .await;
    }
}

fn build_phase_name(phase: BuildPhase) -> &'static str {
    match phase {
        BuildPhase::Queued => "queued",
        BuildPhase::Preparing => "preparing",
        BuildPhase::Building => "building",
        BuildPhase::Succeeded => "succeeded",
        BuildPhase::Failed => "failed",
        BuildPhase::Canceled => "canceled",
    }
}

fn build_error_cursor(
    phase: &str,
    reason: &str,
    message: &str,
    timestamp: kernel_api::Timestamp,
) -> String {
    let mut digest = Sha256::new();
    for component in [phase, reason, "terminal", message] {
        let component_len = u64::try_from(component.len()).unwrap_or(u64::MAX);
        digest.update(component_len.to_be_bytes());
        digest.update(component.as_bytes());
    }
    digest.update(timestamp.0.to_be_bytes());
    format!("controller-error:{}", hex::encode(digest.finalize()))
}

fn github_token(
    secrets: &BTreeMap<String, kernel_api::SecretValue>,
) -> Option<&kernel_api::SecretValue> {
    secrets.get("GH_TOKEN")
}

#[async_trait]
impl Reconciler for BuildReconciler {
    type Id = BuildId;
    type Spec = BuildSpec;
    type Status = BuildStatus;

    const KIND: &'static str = "Build";
    const FINALIZER: Option<&'static str> = Some("build.maestro.dev/source");

    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.advance(resource, &context).await
    }

    async fn finalize(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        _context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        match self.source.cleanup(&resource.meta.id).await {
            Ok(()) => Ok(Action::Done),
            Err(BuildSourceError::Unavailable { message }) => {
                Err(ReconcileError::Retryable { message })
            }
            Err(BuildSourceError::Rejected { message }) => Err(ReconcileError::Terminal {
                reason: "BuildSourceCleanupRejected".to_string(),
                message,
            }),
        }
    }
}

fn validate_definition(value: &str) -> Result<(), String> {
    let path = Path::new(value);
    if value.trim().is_empty() {
        return Err("build definition path cannot be empty".to_string());
    }
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(format!(
            "build definition `{value}` must be a relative path inside source"
        ));
    }
    Ok(())
}

fn with_definition(prepared: PreparedBuildSource, definition: PathBuf) -> runtime::ArtifactSource {
    match prepared.artifact_source {
        runtime::ArtifactSource::Directory { root, .. } => {
            runtime::ArtifactSource::Directory { root, definition }
        }
        runtime::ArtifactSource::Archive { path, .. } => {
            runtime::ArtifactSource::Archive { path, definition }
        }
    }
}
