use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_store::{Clock, MonotonicTime};
use tokio::sync::watch;

pub(super) struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        pending::<()>().await;
    }
}

#[derive(Clone)]
pub(super) struct ManualClock {
    inner: Arc<ManualClockInner>,
}

struct ManualClockInner {
    now: Mutex<MonotonicTime>,
    sleeps: AtomicUsize,
    changes: watch::Sender<u64>,
}

impl ManualClock {
    pub(super) fn new() -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            inner: Arc::new(ManualClockInner {
                now: Mutex::new(MonotonicTime::default()),
                sleeps: AtomicUsize::new(0),
                changes,
            }),
        }
    }

    pub(super) fn advance(&self, duration: Duration) {
        let mut now = self
            .inner
            .now
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *now = now.saturating_add(duration);
        self.inner
            .changes
            .send_replace(now.as_duration().as_nanos() as u64);
    }

    pub(super) fn sleep_count(&self) -> usize {
        self.inner.sleeps.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        *self
            .inner
            .now
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        self.inner.sleeps.fetch_add(1, Ordering::SeqCst);
        let mut changes = self.inner.changes.subscribe();
        loop {
            if self.now() >= deadline {
                return;
            }
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
}
