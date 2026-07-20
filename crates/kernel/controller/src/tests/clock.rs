use std::future::pending;
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
    changes: watch::Sender<u64>,
}

impl ManualClock {
    pub(super) fn new() -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            inner: Arc::new(ManualClockInner {
                now: Mutex::new(MonotonicTime::default()),
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
