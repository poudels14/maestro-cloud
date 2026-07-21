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
use crate::{PreviewError, PullRequestApi, PullRequestApiError};

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

/// Discovers GitHub pull requests and persists the global Preview resource set.
pub struct PreviewSourceReconciler {
    api: Arc<dyn PullRequestApi>,
    settings: PreviewSourceSettings,
    keyspace: Keyspace,
    node_prefix: StorePrefix,
    timestamp_clock: Arc<dyn TimestampClock>,
    monotonic_clock: Arc<dyn Clock>,
    retries: Mutex<BTreeMap<String, RepositoryRetry>>,
    writer: PreviewSourceWriter,
}

impl PreviewSourceReconciler {
    /// Constructs a source reconciler without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        api: Arc<dyn PullRequestApi>,
        settings: PreviewSourceSettings,
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
            node_prefix: keyspace.resource_kind(&node_kind),
            writer: PreviewSourceWriter::new(&cluster_id)?,
            keyspace,
            timestamp_clock,
            monotonic_clock,
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
            if !self.repository_available(&repository.full_name, now).await {
                continue;
            }
            match self
                .api
                .list_open(&repository.owner, &repository.name)
                .await
            {
                Ok(pull_requests) => {
                    self.retries.lock().await.remove(&repository.full_name);
                    repositories.push(RepositoryPullRequests {
                        repository: repository.full_name,
                        pull_requests,
                    });
                }
                Err(error) => {
                    self.record_failure(&repository.full_name, now, &error)
                        .await;
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
            let body = feedback_body(feedback.kind, self.settings.max_concurrent_previews);
            if let Err(error) = self
                .api
                .upsert_comment(
                    owner,
                    repository,
                    feedback.pull_request_number,
                    &feedback.comment_key,
                    &body,
                )
                .await
            {
                self.record_failure(&feedback.repository, now, &error).await;
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
        retries.insert(
            repository.to_string(),
            RepositoryRetry {
                retry_at: now.saturating_add(delay),
                next_delay,
            },
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

fn feedback_body(kind: PreviewFeedbackKind, max_concurrent_previews: usize) -> String {
    match kind {
        PreviewFeedbackKind::Creating => {
            "### Maestro preview: building\n\nThe preview has been admitted and is being built."
                .to_string()
        }
        PreviewFeedbackKind::Updating => {
            "### Maestro preview: updating\n\nA new commit is being deployed to the existing preview URL."
                .to_string()
        }
        PreviewFeedbackKind::Ready => {
            "### Maestro preview: ready\n\nThe current pull-request commit is deployed."
                .to_string()
        }
        PreviewFeedbackKind::Failed => {
            "### Maestro preview: failed\n\nThe current pull-request commit did not deploy successfully."
                .to_string()
        }
        PreviewFeedbackKind::Reopened => {
            "### Maestro preview: restored\n\nScheduled removal was canceled after the pull request reopened."
                .to_string()
        }
        PreviewFeedbackKind::Closing => {
            "### Maestro preview: removal scheduled\n\nThe pull request closed; removal will begin after the configured grace period."
                .to_string()
        }
        PreviewFeedbackKind::Ineligible => {
            "### Maestro preview: skipped\n\nDraft, forked, invalid, and expired pull requests do not receive previews."
                .to_string()
        }
        PreviewFeedbackKind::QuotaExceeded => format!(
            "### Maestro preview: skipped (quota)\n\nThe cluster is already running its limit of {max_concurrent_previews} concurrent previews. This pull request will be reconsidered automatically."
        ),
    }
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
