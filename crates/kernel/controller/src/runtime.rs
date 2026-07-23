use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{FinalizerName, Object};
use kernel_store::{
    Clock, Compare, ExpectedVersion, MonotonicTime, Mutation, StoreKey, StorePrefix, StoredValue,
    Transaction, TransactionOutcome, Version, WatchCursor, WatchEventKind, WatchStart,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tracing::Instrument;

use crate::queue::WorkQueue;
use crate::{
    Action, Backoff, ControllerError, FencedStore, ReconcileContext, ReconcileError, Reconciler,
};
#[cfg(feature = "test-util")]
use crate::{JournalEntry, ReconcileJournal};

/// Validated scheduling policy for one controller runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    resync_interval: Duration,
    retry_backoff: Backoff,
}

impl RuntimeConfig {
    /// Creates a level-triggered scheduling policy.
    pub fn new(
        resync_interval: Duration,
        retry_backoff: Backoff,
    ) -> Result<Self, RuntimeConfigError> {
        if resync_interval.is_zero() {
            Err(RuntimeConfigError::ZeroResyncInterval)
        } else {
            Ok(Self {
                resync_interval,
                retry_backoff,
            })
        }
    }
}

/// Invalid controller runtime scheduling configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeConfigError {
    /// A zero resync interval would cause an unbounded hot loop.
    #[error("controller resync interval must be greater than zero")]
    ZeroResyncInterval,
}

/// Watch-driven, level-triggered executor for one typed reconciler.
pub struct ControllerRuntime<R> {
    reconciler: Arc<R>,
    resource_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
    fenced_store: Arc<FencedStore>,
    clock: Arc<dyn Clock>,
    config: RuntimeConfig,
    #[cfg(feature = "test-util")]
    journal: ReconcileJournal,
}

impl<R> ControllerRuntime<R>
where
    R: Reconciler + 'static,
    R::Id: DeserializeOwned + Serialize + Display,
    R::Spec: DeserializeOwned + Serialize,
    R::Status: DeserializeOwned + Serialize,
{
    /// Binds a typed reconciler to its resource prefix and active leader fence.
    pub fn new(
        reconciler: Arc<R>,
        prefix: StorePrefix,
        fenced_store: Arc<FencedStore>,
        clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> Self {
        Self::new_with_trigger_prefix(
            reconciler,
            prefix.clone(),
            prefix,
            fenced_store,
            clock,
            config,
        )
    }

    /// Binds a typed reconciler to a broader prefix containing dependency events.
    ///
    /// Values under `resource_prefix` remain the only objects passed to the
    /// reconciler. A change elsewhere under `trigger_prefix` immediately
    /// reschedules every known primary object, coalescing repeated events by key.
    pub fn new_with_trigger_prefix(
        reconciler: Arc<R>,
        resource_prefix: StorePrefix,
        trigger_prefix: StorePrefix,
        fenced_store: Arc<FencedStore>,
        clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> Self {
        Self {
            reconciler,
            resource_prefix,
            trigger_prefix,
            fenced_store,
            clock,
            config,
            #[cfg(feature = "test-util")]
            journal: ReconcileJournal::default(),
        }
    }

    /// Returns the invocation journal owned by this runtime.
    #[cfg(feature = "test-util")]
    pub fn journal(&self) -> ReconcileJournal {
        self.journal.clone()
    }

    /// Reconciles one linearizable prefix snapshot in lexicographic key order.
    ///
    /// This bounded entry point is useful for deterministic tests and startup
    /// validation. Production controllers normally use [`Self::run`].
    pub async fn reconcile_snapshot(&self) -> Result<usize, ControllerError> {
        self.fenced_store.verify_leadership().await?;
        let snapshot = self
            .fenced_store
            .raw_store()
            .list(&self.resource_prefix)
            .await?;
        let mut invoked = 0_usize;
        for stored in snapshot.values {
            if self.process(stored, 0).await?.invoked {
                invoked = invoked.saturating_add(1);
            }
        }
        Ok(invoked)
    }

    /// Runs watch, retry, and resync scheduling until shutdown or fence loss.
    ///
    /// A watch cursor loss triggers a full linearizable relist. All resource
    /// processing is serialized by key; this implementation deliberately uses
    /// one executor, which also bounds total concurrency for a controller.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), ControllerError> {
        let (trigger_cursor, resources) = self.snapshot_inputs().await?;
        let mut queue = WorkQueue::default();
        let now = self.clock.now();
        for stored in &resources {
            queue.schedule(stored.key.clone(), now, 0);
        }
        let mut stream = self.fenced_store.raw_store().watch(
            self.trigger_prefix.clone(),
            WatchStart::After(trigger_cursor),
        )?;
        let mut resync_at = now.saturating_add(self.config.resync_interval);

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let wake_at = queue.next_deadline().min(resync_at);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                event = stream.next() => {
                    match event {
                        Ok(event) => self.schedule_event(&mut queue, event.kind),
                        Err(kernel_store::StoreError::CursorExpired { .. }) => {
                            let (trigger_cursor, resources) = self.snapshot_inputs().await?;
                            queue.replace_with(&resources, self.clock.now());
                            stream = self.fenced_store.raw_store().watch(
                                self.trigger_prefix.clone(),
                                WatchStart::After(trigger_cursor),
                            )?;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                () = self.clock.sleep_until(wake_at) => {
                    let now = self.clock.now();
                    if now >= resync_at {
                        self.fenced_store.verify_leadership().await?;
                        let (trigger_cursor, resources) = self.snapshot_inputs().await?;
                        queue.replace_with(&resources, now);
                        stream = self.fenced_store.raw_store().watch(
                            self.trigger_prefix.clone(),
                            WatchStart::After(trigger_cursor),
                        )?;
                        resync_at = now.saturating_add(self.config.resync_interval);
                    }
                    if let Some((key, attempt)) = queue.take_due(now) {
                        self.run_scheduled(&mut queue, key, attempt, now).await?;
                    }
                }
            }
        }
    }

    async fn snapshot_inputs(&self) -> Result<(WatchCursor, Vec<StoredValue>), ControllerError> {
        let trigger_snapshot = self
            .fenced_store
            .raw_store()
            .list(&self.trigger_prefix)
            .await?;
        let resources = if self.trigger_prefix == self.resource_prefix {
            trigger_snapshot.values
        } else {
            self.fenced_store
                .raw_store()
                .list(&self.resource_prefix)
                .await?
                .values
        };
        Ok((trigger_snapshot.cursor, resources))
    }

    fn schedule_event(&self, queue: &mut WorkQueue, event: WatchEventKind) {
        let now = self.clock.now();
        match event {
            WatchEventKind::Put(value)
                if value
                    .key
                    .as_str()
                    .starts_with(self.resource_prefix.as_str()) =>
            {
                queue.schedule(value.key, now, 0);
            }
            WatchEventKind::Delete { key, .. }
                if key.as_str().starts_with(self.resource_prefix.as_str()) =>
            {
                queue.remove(&key);
                if self.trigger_prefix != self.resource_prefix {
                    queue.schedule_all(now);
                }
            }
            WatchEventKind::Put(_) | WatchEventKind::Delete { .. } => queue.schedule_all(now),
        }
    }

    async fn run_scheduled(
        &self,
        queue: &mut WorkQueue,
        key: StoreKey,
        attempt: u32,
        now: MonotonicTime,
    ) -> Result<(), ControllerError> {
        let Some(stored) = self.fenced_store.raw_store().get(&key).await? else {
            return Ok(());
        };
        match self.process(stored, attempt).await? {
            ProcessResult {
                action: Some(Action::Done),
                ..
            }
            | ProcessResult { action: None, .. } => {}
            ProcessResult {
                action: Some(Action::Requeue(delay)),
                next_attempt,
                ..
            } => queue.schedule(key, now.saturating_add(delay), next_attempt),
            ProcessResult {
                action: Some(Action::RequeueAt(deadline)),
                next_attempt,
                ..
            } => queue.schedule(key, deadline, next_attempt),
        }
        Ok(())
    }

    async fn process(
        &self,
        stored: StoredValue,
        attempt: u32,
    ) -> Result<ProcessResult, ControllerError> {
        self.fenced_store.verify_leadership().await?;
        let mut resource: Object<R::Id, R::Spec, R::Status> = serde_json::from_slice(&stored.value)
            .map_err(|error| ControllerError::MalformedResource {
                kind: R::KIND,
                message: error.to_string(),
            })?;
        resource.meta.revision = stored.version.resource_revision();

        let finalizer = R::FINALIZER.map(|name| FinalizerName(name.to_string()));
        if resource.meta.deletion_timestamp.is_none()
            && let Some(finalizer) = finalizer.as_ref()
            && !resource.meta.finalizers.contains(finalizer)
        {
            resource.meta.finalizers.insert(finalizer.clone());
            self.persist_resource(
                &stored.key,
                stored.version,
                &resource,
                PersistenceAction::Store,
            )
            .await?;
            return Ok(ProcessResult::changed());
        }

        if resource.meta.deletion_timestamp.is_some() {
            return self
                .process_deletion(stored.key, stored.version, resource, finalizer, attempt)
                .await;
        }

        self.invoke(resource, stored.version, attempt, ReconcilePhase::Active)
            .await
    }

    async fn process_deletion(
        &self,
        key: StoreKey,
        version: Version,
        mut resource: Object<R::Id, R::Spec, R::Status>,
        finalizer: Option<FinalizerName>,
        attempt: u32,
    ) -> Result<ProcessResult, ControllerError> {
        let Some(finalizer) = finalizer else {
            if resource.meta.finalizers.is_empty() {
                self.persist_resource(&key, version, &resource, PersistenceAction::Delete)
                    .await?;
            }
            return Ok(ProcessResult::changed());
        };
        if !resource.meta.finalizers.contains(&finalizer) {
            if resource.meta.finalizers.is_empty() {
                self.persist_resource(&key, version, &resource, PersistenceAction::Delete)
                    .await?;
            }
            return Ok(ProcessResult::changed());
        }

        let outcome = self
            .invoke(
                resource.clone(),
                version,
                attempt,
                ReconcilePhase::Finalizing,
            )
            .await?;
        if outcome.action == Some(Action::Done) && outcome.successful {
            resource.meta.finalizers.remove(&finalizer);
            let persistence = if resource.meta.finalizers.is_empty() {
                PersistenceAction::Delete
            } else {
                PersistenceAction::Store
            };
            self.persist_resource(&key, version, &resource, persistence)
                .await?;
            Ok(ProcessResult {
                action: None,
                invoked: true,
                next_attempt: 0,
                successful: true,
            })
        } else {
            Ok(outcome)
        }
    }

    async fn invoke(
        &self,
        resource: Object<R::Id, R::Spec, R::Status>,
        version: Version,
        attempt: u32,
        phase: ReconcilePhase,
    ) -> Result<ProcessResult, ControllerError> {
        #[cfg(feature = "test-util")]
        let started_at = self.clock.now();
        #[cfg(feature = "test-util")]
        let resource_id = resource.meta.id.to_string();
        #[cfg(feature = "test-util")]
        let observed_revision = version.resource_revision();
        let deleting = phase.is_deleting();
        let span = tracing::info_span!(
            "reconcile",
            kind = R::KIND,
            resource_id = %resource.meta.id,
            deleting,
            attempt,
        );
        let context = ReconcileContext::new(
            self.fenced_store.clone(),
            self.clock.clone(),
            version,
            attempt,
        );
        let result = match phase {
            ReconcilePhase::Active => {
                self.reconciler
                    .reconcile(resource, context)
                    .instrument(span.clone())
                    .await
            }
            ReconcilePhase::Finalizing => {
                self.reconciler
                    .finalize(resource, context)
                    .instrument(span.clone())
                    .await
            }
        };
        let outcome = match result {
            Ok(action) => ProcessResult {
                action: Some(action),
                invoked: true,
                next_attempt: 0,
                successful: true,
            },
            Err(ReconcileError::Retryable { message }) => {
                span.in_scope(|| {
                    tracing::warn!(kind = R::KIND, %message, "reconcile will be retried");
                });
                ProcessResult {
                    action: Some(Action::Requeue(self.config.retry_backoff.delay(attempt))),
                    invoked: true,
                    next_attempt: attempt.saturating_add(1),
                    successful: false,
                }
            }
            Err(ReconcileError::Terminal { reason, message }) => {
                span.in_scope(|| {
                    tracing::error!(
                        kind = R::KIND,
                        %reason,
                        %message,
                        "reconcile requires an external change"
                    );
                });
                ProcessResult {
                    action: Some(Action::Done),
                    invoked: true,
                    next_attempt: 0,
                    successful: false,
                }
            }
            Err(ReconcileError::Infrastructure(error)) => return Err(error),
        };
        #[cfg(feature = "test-util")]
        if let Some(action) = outcome.action {
            self.journal.record(JournalEntry {
                sequence: 0,
                kind: R::KIND,
                resource_id,
                observed_revision,
                action: action.into(),
                duration: self
                    .clock
                    .now()
                    .as_duration()
                    .saturating_sub(started_at.as_duration()),
                deleting,
            });
        }
        Ok(outcome)
    }

    async fn persist_resource(
        &self,
        key: &StoreKey,
        version: Version,
        resource: &Object<R::Id, R::Spec, R::Status>,
        action: PersistenceAction,
    ) -> Result<(), ControllerError> {
        let mutation = match action {
            PersistenceAction::Store => Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(resource).map_err(|error| {
                    ControllerError::SerializeResource {
                        kind: R::KIND,
                        message: error.to_string(),
                    }
                })?,
                session: None,
            },
            PersistenceAction::Delete => Mutation::Delete { key: key.clone() },
        };
        let outcome = self
            .fenced_store
            .txn(Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Exact(version),
                }],
                mutations: vec![mutation],
            })
            .await?;
        match outcome {
            TransactionOutcome::Applied { .. } | TransactionOutcome::Conflict => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcilePhase {
    Active,
    Finalizing,
}

impl ReconcilePhase {
    const fn is_deleting(self) -> bool {
        matches!(self, Self::Finalizing)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistenceAction {
    Store,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessResult {
    action: Option<Action>,
    invoked: bool,
    next_attempt: u32,
    successful: bool,
}

impl ProcessResult {
    fn changed() -> Self {
        Self {
            action: None,
            invoked: false,
            next_attempt: 0,
            successful: false,
        }
    }
}
