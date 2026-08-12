use std::collections::BTreeSet;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream::{FuturesUnordered, StreamExt};
use kernel_store::{
    Clock, MonotonicTime, StoreError, StoreKey, StorePrefix, StoredValue, WatchCursor,
    WatchEventKind, WatchStart,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

#[cfg(feature = "test-util")]
use crate::ReconcileJournal;
use crate::queue::WorkQueue;
use crate::{Action, Backoff, ControllerError, FencedStore, Reconciler};

mod resource_processing;

use resource_processing::ProcessResult;

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
    /// A controller must always retain at least one reconciliation worker.
    #[error("controller maximum concurrency must be greater than zero")]
    ZeroMaxConcurrency,
}

/// Watch-driven, level-triggered executor for one typed reconciler.
pub struct ControllerRuntime<R> {
    reconciler: Arc<R>,
    resource_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
    ignored_trigger_prefixes: Vec<StorePrefix>,
    fenced_store: Arc<FencedStore>,
    clock: Arc<dyn Clock>,
    config: RuntimeConfig,
    max_concurrency: usize,
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

    /// Allows distinct resource keys to reconcile concurrently while preserving
    /// strict single-flight execution for each individual key.
    pub fn with_max_concurrency(
        mut self,
        max_concurrency: usize,
    ) -> Result<Self, RuntimeConfigError> {
        if max_concurrency == 0 {
            return Err(RuntimeConfigError::ZeroMaxConcurrency);
        }
        self.max_concurrency = max_concurrency;
        Ok(self)
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
            ignored_trigger_prefixes: Vec::new(),
            fenced_store,
            clock,
            config,
            max_concurrency: 1,
            #[cfg(feature = "test-util")]
            journal: ReconcileJournal::default(),
        }
    }

    /// Ignores dependency events below a backend-owned subtree.
    ///
    /// This is useful when a reconciler's broad dependency watch contains the
    /// integration state that the reconciler publishes itself. Primary resource
    /// events must never be ignored.
    pub fn with_ignored_trigger_prefix(mut self, prefix: StorePrefix) -> Self {
        self.ignored_trigger_prefixes.push(prefix);
        self
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
    /// A watch cursor loss triggers a full linearizable relist. Resource
    /// processing is single-flight per key and bounded by the configured
    /// controller concurrency.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), ControllerError> {
        let mut failure_attempt = 0_u32;
        loop {
            match self.run_until_interrupted(&mut shutdown).await {
                Ok(()) => return Ok(()),
                Err(error) if retryable_runtime_error(&error) => {
                    let delay = self.config.retry_backoff.delay(failure_attempt);
                    failure_attempt = failure_attempt.saturating_add(1);
                    tracing::warn!(
                        kind = R::KIND,
                        error = %error,
                        retry_delay_ms = delay.as_millis(),
                        "transient controller store failure; rebuilding snapshot"
                    );
                    let retry_at = self.clock.now().saturating_add(delay);
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        () = self.clock.sleep_until(retry_at) => {}
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn run_until_interrupted(
        &self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), ControllerError> {
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
        let mut active = BTreeSet::new();
        let mut in_flight: FuturesUnordered<BoxFuture<'_, ScheduledResult>> =
            FuturesUnordered::new();

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let now = self.clock.now();
            while in_flight.len() < self.max_concurrency {
                let Some((key, attempt)) = queue.take_due_excluding(now, &active) else {
                    break;
                };
                active.insert(key.clone());
                in_flight.push(self.process_scheduled(key, attempt, now).boxed());
            }
            let work_at = if in_flight.len() < self.max_concurrency {
                queue.next_deadline_excluding(&active)
            } else {
                MonotonicTime::from_duration(Duration::MAX)
            };
            let wake_at = work_at.min(resync_at);
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
                completed = in_flight.next(), if !in_flight.is_empty() => {
                    let Some(completed) = completed else {
                        continue;
                    };
                    active.remove(&completed.key);
                    let result = completed.result?;
                    if let Some(result) = result {
                        self.apply_result(&mut queue, completed.key, completed.started_at, result);
                    }
                }
                () = self.clock.sleep_until(wake_at) => {
                    let now = self.clock.now();
                    if now >= resync_at {
                        let (trigger_cursor, resources) = self.snapshot_inputs().await?;
                        queue.replace_with(&resources, now);
                        stream = self.fenced_store.raw_store().watch(
                            self.trigger_prefix.clone(),
                            WatchStart::After(trigger_cursor),
                        )?;
                        resync_at = now.saturating_add(self.config.resync_interval);
                    }
                }
            }
        }
    }

    async fn snapshot_inputs(&self) -> Result<(WatchCursor, Vec<StoredValue>), ControllerError> {
        self.fenced_store.verify_leadership().await?;
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
        let event_key = match &event {
            WatchEventKind::Put(value) => &value.key,
            WatchEventKind::Delete { key, .. } => key,
        };
        if self
            .ignored_trigger_prefixes
            .iter()
            .any(|prefix| event_key.as_str().starts_with(prefix.as_str()))
        {
            return;
        }
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

    async fn process_scheduled(
        &self,
        key: StoreKey,
        attempt: u32,
        started_at: MonotonicTime,
    ) -> ScheduledResult {
        let result = self.process_key(&key, attempt).await;
        ScheduledResult {
            key,
            started_at,
            result,
        }
    }

    async fn process_key(
        &self,
        key: &StoreKey,
        attempt: u32,
    ) -> Result<Option<ProcessResult>, ControllerError> {
        let Some(stored) = self.fenced_store.raw_store().get(key).await? else {
            return Ok(None);
        };
        self.process(stored, attempt).await.map(Some)
    }

    fn apply_result(
        &self,
        queue: &mut WorkQueue,
        key: StoreKey,
        started_at: MonotonicTime,
        result: ProcessResult,
    ) {
        if !queue.is_known(&key) || queue.is_scheduled(&key) {
            return;
        }
        match result {
            ProcessResult {
                action: Some(Action::Done),
                ..
            }
            | ProcessResult { action: None, .. } => {}
            ProcessResult {
                action: Some(Action::Requeue(delay)),
                next_attempt,
                ..
            } => queue.schedule(key, started_at.saturating_add(delay), next_attempt),
            ProcessResult {
                action: Some(Action::RequeueAt(deadline)),
                next_attempt,
                ..
            } => queue.schedule(key, deadline, next_attempt),
        }
    }
}

struct ScheduledResult {
    key: StoreKey,
    started_at: MonotonicTime,
    result: Result<Option<ProcessResult>, ControllerError>,
}

fn retryable_runtime_error(error: &ControllerError) -> bool {
    matches!(
        error,
        ControllerError::Store(StoreError::CursorExpired { .. } | StoreError::Unavailable { .. })
    )
}
