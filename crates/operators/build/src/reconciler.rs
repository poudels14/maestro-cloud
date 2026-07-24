use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
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
use runtime::{
    ArtifactBuildRequest, ArtifactDigest, ArtifactReference, ArtifactStore, ArtifactStoreError,
};

use crate::DepotBuildBackend;
use crate::source::{BuildSourceError, BuildSourceProvider, PreparedBuildSource};
use crate::writer::{BuildStatusWriter, BuildWriteError};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);
const READY_CONDITION: &str = "Ready";

/// Watch-driven Build resource reconciler.
pub struct BuildReconciler {
    source: Arc<dyn BuildSourceProvider>,
    artifacts: Arc<dyn ArtifactStore>,
    depot: Option<Arc<dyn DepotBuildBackend>>,
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
        timestamp_clock: Arc<dyn TimestampClock>,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        let keyspace = Keyspace::new(&cluster_id);
        let kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        Ok(Self {
            source,
            artifacts,
            depot: None,
            timestamp_clock,
            writer: BuildStatusWriter::new(&cluster_id)?,
            prefix: keyspace.resource_kind(&kind),
        })
    }

    /// Selects the protected Depot backend for templates that name a Depot project.
    pub fn with_depot_backend(mut self, depot: Option<Arc<dyn DepotBuildBackend>>) -> Self {
        self.depot = depot;
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
        match self
            .source
            .prepare(
                &build.meta.id,
                &build.spec.template.source,
                None,
                github_token(&build),
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
                Err(ReconcileError::Retryable { message })
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
        let prepared = match self
            .source
            .prepare(
                &build.meta.id,
                &build.spec.template.source,
                Some(&revision),
                github_token(&build),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(BuildSourceError::Unavailable { message }) => {
                return Err(ReconcileError::Retryable { message });
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
        self.run_artifact_build(build, context, prepared).await
    }

    async fn run_artifact_build(
        &self,
        mut build: Build,
        context: &ReconcileContext,
        prepared: PreparedBuildSource,
    ) -> Result<Action, ReconcileError> {
        let definition = PathBuf::from(&build.spec.template.dockerfile);
        let request = ArtifactBuildRequest {
            source: with_definition(prepared, definition),
            arguments: build.spec.template.environment.clone(),
            secrets: build.spec.template.secrets.clone(),
            tags: Vec::new(),
        };
        match self.build_and_publish(&build, &request).await {
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
            ) => Err(ReconcileError::Retryable {
                message: error.to_string(),
            }),
            Err(error) => {
                self.fail(build, context, "ArtifactBuildRejected", error.to_string())
                    .await
            }
        }
    }

    async fn build_and_publish(
        &self,
        build: &Build,
        request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let digest = match &build.spec.template.depot {
            Some(depot) => {
                let backend = self
                    .depot
                    .as_ref()
                    .ok_or_else(|| ArtifactStoreError::Rejected {
                        message: "build selects Depot but this cluster has no Depot token"
                            .to_owned(),
                    })?;
                backend.build(request, &depot.project).await?
            }
            None => self.artifacts.build(request).await?,
        };
        let Some(registry) = &build.spec.template.registry else {
            return Ok(digest);
        };
        let destination = ArtifactReference::new(format!(
            "{}/{service}:{deployment}",
            registry.trim_end_matches('/'),
            service = build.spec.service_id,
            deployment = build.spec.deployment_id,
        ))?;
        self.artifacts.publish(&digest, &destination).await
    }

    async fn fail(
        &self,
        mut build: Build,
        context: &ReconcileContext,
        reason: &str,
        message: String,
    ) -> Result<Action, ReconcileError> {
        build.status.phase = BuildPhase::Failed;
        build.status.image_digest = None;
        self.set_condition(&mut build, ConditionState::False, reason, &message);
        self.persist(context, &build, Action::Done).await
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
            .find(|condition| condition.condition_type.0 == READY_CONDITION);
        let transitioned_at = previous
            .filter(|condition| condition.state == state)
            .map_or_else(
                || self.timestamp_clock.now(),
                |condition| condition.last_transition_time,
            );
        let condition = Condition {
            condition_type: ConditionType(READY_CONDITION.to_string()),
            state,
            reason: ConditionReason(reason.to_string()),
            message: message.to_string(),
            observed_generation: build.meta.generation,
            last_transition_time: transitioned_at,
        };
        build
            .status
            .conditions
            .retain(|existing| existing.condition_type.0 != READY_CONDITION);
        build.status.conditions.push(condition);
    }
}

fn github_token(build: &Build) -> Option<&kernel_api::SecretValue> {
    build.spec.template.secrets.get("GH_TOKEN")
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
