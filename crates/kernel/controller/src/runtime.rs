use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::Duration;

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
}

fn retryable_runtime_error(error: &ControllerError) -> bool {
    matches!(
        error,
        ControllerError::Store(StoreError::CursorExpired { .. } | StoreError::Unavailable { .. })
    )
}
