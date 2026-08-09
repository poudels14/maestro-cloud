use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{NodeId, NodeSpec, NodeStatus, Object, ResourceKind, Service};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{Clock, Keyspace, MonotonicTime, StorePrefix};
use tokio::sync::Mutex;

use crate::repository::{GithubRepository, service_repository};
use crate::source_plan::{
    PreviewFeedback, PreviewFeedbackKind, PreviewSourcePlanError, RepositoryPullRequests,
    plan_preview_sources,
};
use crate::source_snapshot::PreviewSourceSnapshot;
use crate::source_writer::{PreviewSourceWriteOutcome, PreviewSourceWriter};
use crate::{
    PreviewError, PreviewSettings, PullRequestApi, PullRequestApiError, PullRequestDeployment,
    PullRequestDeploymentState,
};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);

/// Polling, quota, and repository retry policy for preview discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreviewSourceSettings {
    /// Normal cadence for refreshing open pull requests.
    pub poll_interval: Duration,
    /// Cluster-wide Preview limit; closing previews continue to consume slots.
    pub max_concurrent_previews: usize,
    /// First delay after a temporary repository failure.
    pub initial_backoff: Duration,
    /// Maximum delay after repeated repository failures.
    pub max_backoff: Duration,
}

/// Construction failure for the cluster-wide preview source reconciler.
#[derive(Debug, thiserror::Error)]
pub enum PreviewSourceError {
    /// Polling at zero duration would create a hot loop.
    #[error("preview source poll interval must be greater than zero")]
    ZeroPollInterval,
    /// A zero global quota can never make progress.
    #[error("maximum concurrent previews must be greater than zero")]
    EmptyQuota,
    /// Repository retry delays must be positive and ordered.
    #[error("preview source backoff must be nonzero and initial must not exceed maximum")]
    InvalidBackoff,
    /// A built-in resource identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
}

#[derive(Debug, Clone, Copy)]
struct RepositoryRetry {
    retry_at: MonotonicTime,
    next_delay: Duration,
}

#[derive(Clone)]
struct CachedRepository {
    snapshot: RepositoryPullRequests,
    refresh_at: MonotonicTime,
}

/// Discovers GitHub pull requests and persists the global Preview resource set.
pub struct PreviewSourceReconciler {
    api: Arc<dyn PullRequestApi>,
    settings: PreviewSourceSettings,
    preview_settings: PreviewSettings,
    keyspace: Keyspace,
    node_prefix: StorePrefix,
    timestamp_clock: Arc<dyn TimestampClock>,
    monotonic_clock: Arc<dyn Clock>,
    repositories: Mutex<BTreeMap<String, CachedRepository>>,
    retries: Mutex<BTreeMap<String, RepositoryRetry>>,
    writer: PreviewSourceWriter,
}

impl PreviewSourceReconciler {
    /// Constructs a source reconciler without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        api: Arc<dyn PullRequestApi>,
        settings: PreviewSourceSettings,
        preview_settings: PreviewSettings,
        timestamp_clock: Arc<dyn TimestampClock>,
        monotonic_clock: Arc<dyn Clock>,
    ) -> Result<Self, PreviewSourceError> {
        if settings.poll_interval.is_zero() {
            return Err(PreviewSourceError::ZeroPollInterval);
        }
        if settings.max_concurrent_previews == 0 {
            return Err(PreviewSourceError::EmptyQuota);
        }
        if settings.initial_backoff.is_zero()
            || settings.max_backoff.is_zero()
            || settings.initial_backoff > settings.max_backoff
        {
            return Err(PreviewSourceError::InvalidBackoff);
        }
        let keyspace = Keyspace::new(&cluster_id);
        let node_kind = ResourceKind::new("Node")?;
        Ok(Self {
            api,
            settings,
            preview_settings,
            node_prefix: keyspace.resource_kind(&node_kind),
            writer: PreviewSourceWriter::new(&cluster_id)?,
            keyspace,
            timestamp_clock,
            monotonic_clock,
            repositories: Mutex::new(BTreeMap::new()),
            retries: Mutex::new(BTreeMap::new()),
        })
    }

    /// Wraps source discovery in the shared watch, resync, and fencing runtime.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.node_prefix.clone(),
            self.keyspace.cluster(),
            store,
            self.monotonic_clock.clone(),
            config,
        )
    }

    async fn poll(
        &self,
        node_id: &NodeId,
        context: &ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        let snapshot = PreviewSourceSnapshot::load(context.store(), &self.keyspace)
            .await
            .map_err(classify_preview)?;
        if snapshot.coordinator() != Some(node_id) {
            return Ok(Action::Done);
        }
        let now = self.monotonic_clock.now();
        let repositories = self.fetch_repositories(&snapshot, now).await;
        let services = snapshot
            .services
            .values()
            .map(|service| service.resource.clone())
            .collect::<Vec<_>>();
        let previews = snapshot
            .previews
            .values()
            .map(|preview| preview.resource.clone())
            .collect::<Vec<_>>();
        let plan = plan_preview_sources(
            &services,
            &previews,
            &repositories,
            self.timestamp_clock.now(),
            self.settings.max_concurrent_previews,
        )
        .map_err(classify_plan)?;
        match self
            .writer
            .apply(context.store(), &snapshot, &plan)
            .await
            .map_err(classify_preview)?
        {
            PreviewSourceWriteOutcome::Conflict => {
                return Ok(Action::Requeue(CONFLICT_RETRY));
            }
            PreviewSourceWriteOutcome::Applied | PreviewSourceWriteOutcome::Noop => {}
        }
        self.publish_feedback(context.store(), &plan.feedback)
            .await?;
        Ok(Action::Requeue(self.settings.poll_interval))
    }

    async fn fetch_repositories(
        &self,
        snapshot: &PreviewSourceSnapshot,
        now: MonotonicTime,
    ) -> Vec<RepositoryPullRequests> {
        let coordinates =
            repository_coordinates(snapshot.services.values().map(|service| &service.resource));
        let mut repositories = Vec::new();
        for repository in coordinates {
            let cached = self
                .repositories
                .lock()
                .await
                .get(&repository.full_name)
                .cloned();
            if let Some(cached) = &cached
                && cached.refresh_at > now
            {
                repositories.push(cached.snapshot.clone());
                continue;
            }
            if !self.repository_available(&repository.full_name, now).await {
                if let Some(cached) = cached {
                    repositories.push(cached.snapshot);
                }
                continue;
            }
            match self
                .api
                .list_open(&repository.owner, &repository.name)
                .await
            {
                Ok(pull_requests) => {
                    self.retries.lock().await.remove(&repository.full_name);
                    let snapshot = RepositoryPullRequests {
                        repository: repository.full_name,
                        pull_requests,
                    };
                    self.repositories.lock().await.insert(
                        snapshot.repository.clone(),
                        CachedRepository {
                            snapshot: snapshot.clone(),
                            refresh_at: now.saturating_add(self.settings.poll_interval),
                        },
                    );
                    repositories.push(snapshot);
                }
                Err(error) => {
                    self.record_failure(&repository.full_name, "pull-request refresh", now, &error)
                        .await;
                    if let Some(cached) = cached {
                        repositories.push(cached.snapshot);
                    }
                }
            }
        }
        repositories
    }

    async fn publish_feedback(
        &self,
        store: &FencedStore,
        feedback: &[PreviewFeedback],
    ) -> Result<(), ReconcileError> {
        for feedback in feedback {
            let now = self.monotonic_clock.now();
            if !self.repository_available(&feedback.repository, now).await {
                continue;
            }
            let Some((owner, repository)) = feedback.repository.split_once('/') else {
                continue;
            };
            store.verify_leadership().await?;
            let deployment = github_deployment(feedback, &self.preview_settings);
            if let Err(error) = self
                .api
                .publish_deployment(owner, repository, &deployment)
                .await
            {
                self.record_failure(
                    &feedback.repository,
                    "GitHub deployment publication",
                    now,
                    &error,
                )
                .await;
            }
        }
        Ok(())
    }

    async fn repository_available(&self, repository: &str, now: MonotonicTime) -> bool {
        self.retries
            .lock()
            .await
            .get(repository)
            .is_none_or(|retry| retry.retry_at <= now)
    }

    async fn record_failure(
        &self,
        repository: &str,
        operation: &'static str,
        now: MonotonicTime,
        error: &PullRequestApiError,
    ) {
        let mut retries = self.retries.lock().await;
        let previous = retries.get(repository).copied();
        let delay = match error {
            PullRequestApiError::RateLimited { retry_after } => {
                (*retry_after).max(Duration::from_secs(1))
            }
            PullRequestApiError::Unavailable { .. } => {
                previous.map_or(self.settings.initial_backoff, |retry| retry.next_delay)
            }
            PullRequestApiError::Rejected { .. } => self.settings.max_backoff,
        };
        let next_delay = delay
            .saturating_mul(2)
            .min(self.settings.max_backoff)
            .max(self.settings.initial_backoff);
        let retry_at = now.saturating_add(delay);
        retries.insert(
            repository.to_string(),
            RepositoryRetry {
                retry_at,
                next_delay,
            },
        );
        tracing::warn!(
            target: "maestro::controller",
            kind = "PreviewSource",
            repository,
            operation,
            reason = "PreviewRepositoryRequestFailed",
            error = %error,
            retry_in_ms = delay.as_millis(),
            "preview repository request failed; retry scheduled"
        );
    }
}

#[async_trait]
impl Reconciler for PreviewSourceReconciler {
    type Id = NodeId;
    type Spec = NodeSpec;
    type Status = NodeStatus;

    const KIND: &'static str = "Node";

    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.poll(&resource.meta.id, &context).await
    }
}

fn repository_coordinates<'a>(
    services: impl Iterator<Item = &'a Service>,
) -> BTreeSet<GithubRepository> {
    services
        .filter(|service| {
            service.spec.preview.is_some() && service.meta.deletion_timestamp.is_none()
        })
        .filter_map(|service| service_repository(service).ok())
        .collect()
}

pub(crate) fn github_deployment(
    feedback: &PreviewFeedback,
    settings: &PreviewSettings,
) -> PullRequestDeployment {
    let (state, description) = match feedback.kind {
        PreviewFeedbackKind::Creating => (
            PullRequestDeploymentState::InProgress,
            "Maestro is building this preview.",
        ),
        PreviewFeedbackKind::Updating => (
            PullRequestDeploymentState::InProgress,
            "Maestro is deploying the latest commit.",
        ),
        PreviewFeedbackKind::Ready => (
            PullRequestDeploymentState::Success,
            "Maestro preview is ready.",
        ),
        PreviewFeedbackKind::Failed => (
            PullRequestDeploymentState::Failure,
            "Maestro preview deployment failed.",
        ),
        PreviewFeedbackKind::Reopened => (
            PullRequestDeploymentState::InProgress,
            "Maestro is restoring this preview.",
        ),
        PreviewFeedbackKind::Closing | PreviewFeedbackKind::Ineligible => (
            PullRequestDeploymentState::Inactive,
            "Maestro preview was removed.",
        ),
        PreviewFeedbackKind::QuotaExceeded => (
            PullRequestDeploymentState::Queued,
            "Waiting for Maestro preview capacity.",
        ),
    };
    PullRequestDeployment {
        head_revision: feedback.head_revision.clone(),
        environment: format!(
            "maestro-preview/{}/pr-{}",
            feedback.base_service_id, feedback.pull_request_number
        ),
        state,
        description: description.to_string(),
        environment_url: (feedback.kind == PreviewFeedbackKind::Ready).then(|| {
            format!(
                "https://{}.{}",
                feedback.service_id.as_str(),
                settings.preview_domain()
            )
        }),
        log_url: dashboard_url(settings, feedback),
    }
}

fn dashboard_url(settings: &PreviewSettings, feedback: &PreviewFeedback) -> Option<String> {
    let pull_request_number = feedback.pull_request_number.to_string();
    let mut url = settings.dashboard_origin()?.clone();
    url.path_segments_mut()
        .ok()?
        .pop_if_empty()
        .push("services")
        .push(feedback.base_service_id.as_str())
        .push("prs")
        .push(&pull_request_number)
        .push("deployments");
    Some(url.into())
}

fn classify_preview(error: PreviewError) -> ReconcileError {
    match error {
        PreviewError::Controller(error) => ReconcileError::Infrastructure(error),
        error => ReconcileError::Terminal {
            reason: "PreviewSourceFailed".to_string(),
            message: error.to_string(),
        },
    }
}

fn classify_plan(error: PreviewSourcePlanError) -> ReconcileError {
    ReconcileError::Terminal {
        reason: "PreviewSourcePlanFailed".to_string(),
        message: error.to_string(),
    }
}
