use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    AnnotationKey, ArtifactTemplate, BUILD_WATCH_REVISION_ANNOTATION, Build, BuildId, BuildSource,
    Deployment, DeploymentPhase, Object, ResourceKind, RolloutState, Service, ServiceId,
    ServiceSpec, ServiceStatus,
};
use kernel_controller::{
    Action, ControllerError, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError,
    Reconciler, RuntimeConfig,
};
use kernel_store::{Clock, Keyspace, StorePrefix, StoredValue};

use crate::source::{BuildRevisionResolver, BuildSourceError};
use crate::watch_writer::{BuildWatchWriteError, BuildWatchWriter};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);

/// Poll cadence for watched service build sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildWatchSettings {
    /// Normal interval between remote branch checks.
    pub poll_interval: Duration,
}

/// Build-watch construction failure.
#[derive(Debug, thiserror::Error)]
pub enum BuildWatchError {
    /// Polling at zero duration would create a hot loop.
    #[error("build-watch poll interval must be greater than zero")]
    ZeroPollInterval,
    /// A built-in resource kind was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
}

/// Service reconciler that advances watched Git revisions after prior builds settle.
pub struct BuildWatchReconciler {
    resolver: Arc<dyn BuildRevisionResolver>,
    settings: BuildWatchSettings,
    keyspace: Keyspace,
    service_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
    deployment_prefix: StorePrefix,
    build_kind: ResourceKind,
    writer: BuildWatchWriter,
}

impl BuildWatchReconciler {
    /// Creates a watcher without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        resolver: Arc<dyn BuildRevisionResolver>,
        settings: BuildWatchSettings,
    ) -> Result<Self, BuildWatchError> {
        if settings.poll_interval.is_zero() {
            return Err(BuildWatchError::ZeroPollInterval);
        }
        let keyspace = Keyspace::new(&cluster_id);
        let service_kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        let deployment_kind = ResourceKind::new("Deployment")?;
        let build_kind = ResourceKind::new("Build")?;
        Ok(Self {
            resolver,
            settings,
            service_prefix: keyspace.resource_kind(&service_kind),
            trigger_prefix: keyspace.cluster(),
            deployment_prefix: keyspace.resource_kind(&deployment_kind),
            build_kind,
            writer: BuildWatchWriter::new(&cluster_id)?,
            keyspace,
        })
    }

    /// Wraps this watcher in the shared per-resource backoff runtime.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        monotonic_clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.service_prefix.clone(),
            self.trigger_prefix.clone(),
            store,
            monotonic_clock,
            config,
        )
    }

    async fn poll(
        &self,
        mut service: Service,
        context: &ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        let Some((source, github_token)) = watched_source(&service)? else {
            return Ok(Action::Done);
        };
        let requeue = Action::Requeue(self.settings.poll_interval);
        if service.status.rollout == RolloutState::Frozen {
            return Ok(requeue);
        }
        let Some(latest) = self
            .latest_deployment(context.store(), &service.meta.id)
            .await?
        else {
            return Ok(requeue);
        };
        let desired = desired_revision(&service);
        if desired.is_some_and(|desired| captured_revision(&latest) != Some(desired)) {
            return Ok(requeue);
        }
        if matches!(
            latest.status.phase,
            DeploymentPhase::Queued | DeploymentPhase::Building
        ) {
            return Ok(requeue);
        }
        let Some(build_id) = latest.spec.build_id.as_ref() else {
            return Ok(requeue);
        };
        let Some(build) = self.load_build(context.store(), build_id).await? else {
            return Ok(requeue);
        };
        let Some(current) = build.status.source_revision.as_deref() else {
            return Ok(requeue);
        };
        let remote = self
            .resolver
            .resolve_revision(source, github_token)
            .await
            .map_err(retry_source)?
            .ok_or_else(|| ReconcileError::Terminal {
                reason: "UnsupportedBuildWatchSource".to_string(),
                message: "build revision resolver did not return a Git revision".to_string(),
            })?;
        if remote == current || desired == Some(remote.as_str()) {
            return Ok(requeue);
        }

        service.meta.revision = context.observed_version().resource_revision();
        service.meta.annotations.insert(
            AnnotationKey(BUILD_WATCH_REVISION_ANNOTATION.to_string()),
            remote,
        );
        match self
            .writer
            .replace(context.store(), context.observed_version(), &service)
            .await
        {
            Ok(true) => Ok(requeue),
            Ok(false) => Ok(Action::Requeue(CONFLICT_RETRY)),
            Err(BuildWatchWriteError::Controller(error)) => {
                Err(ReconcileError::Infrastructure(error))
            }
            Err(error) => Err(ReconcileError::Terminal {
                reason: "BuildWatchWriteFailed".to_string(),
                message: error.to_string(),
            }),
        }
    }

    async fn latest_deployment(
        &self,
        store: &FencedStore,
        service_id: &ServiceId,
    ) -> Result<Option<Deployment>, ReconcileError> {
        let listed = store.list(&self.deployment_prefix).await?;
        let mut latest: Option<Deployment> = None;
        for stored in listed.values {
            let deployment = decode::<Deployment>(stored, "Deployment")?;
            if deployment.spec.service_id != *service_id
                || deployment.meta.deletion_timestamp.is_some()
            {
                continue;
            }
            if latest
                .as_ref()
                .is_none_or(|current| deployment_order(&deployment, current).is_gt())
            {
                latest = Some(deployment);
            }
        }
        Ok(latest)
    }

    async fn load_build(
        &self,
        store: &FencedStore,
        build_id: &BuildId,
    ) -> Result<Option<Build>, ReconcileError> {
        let key = self
            .keyspace
            .resource(&self.build_kind, &build_id.clone().into());
        store
            .get(&key)
            .await?
            .map(|stored| decode::<Build>(stored, "Build"))
            .transpose()
    }
}

#[async_trait]
impl Reconciler for BuildWatchReconciler {
    type Id = ServiceId;
    type Spec = ServiceSpec;
    type Status = ServiceStatus;

    const KIND: &'static str = "Service";

    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.poll(resource, &context).await
    }
}

fn watched_source(
    service: &Service,
) -> Result<Option<(&BuildSource, Option<&kernel_api::SecretValue>)>, ReconcileError> {
    let ArtifactTemplate::Build { template } = &service.spec.artifact else {
        return Ok(None);
    };
    if !template.watch {
        return Ok(None);
    }
    match &template.source {
        source @ BuildSource::Git { .. } => Ok(Some((source, template.secrets.get("GH_TOKEN")))),
        BuildSource::Tarball { .. } => Err(ReconcileError::Terminal {
            reason: "UnsupportedBuildWatchSource".to_string(),
            message: "build watch requires a Git source".to_string(),
        }),
    }
}

fn desired_revision(service: &Service) -> Option<&str> {
    service
        .meta
        .annotations
        .get(&AnnotationKey(BUILD_WATCH_REVISION_ANNOTATION.to_string()))
        .map(String::as_str)
}

fn captured_revision(deployment: &Deployment) -> Option<&str> {
    let ArtifactTemplate::Build { template } = &deployment.spec.service.artifact else {
        return None;
    };
    let BuildSource::Git { revision, .. } = &template.source else {
        return None;
    };
    Some(revision)
}

fn deployment_order(left: &Deployment, right: &Deployment) -> Ordering {
    left.spec
        .service_generation
        .cmp(&right.spec.service_generation)
        .then_with(|| left.status.created_at.cmp(&right.status.created_at))
        .then_with(|| left.meta.id.cmp(&right.meta.id))
}

fn decode<Resource: serde::de::DeserializeOwned>(
    stored: StoredValue,
    kind: &'static str,
) -> Result<Resource, ReconcileError> {
    serde_json::from_slice(&stored.value).map_err(|error| {
        ReconcileError::Infrastructure(ControllerError::MalformedResource {
            kind,
            message: error.to_string(),
        })
    })
}

fn retry_source(error: BuildSourceError) -> ReconcileError {
    ReconcileError::Retryable {
        message: error.to_string(),
    }
}
