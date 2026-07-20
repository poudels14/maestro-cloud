use std::time::Duration;

use async_trait::async_trait;

/// Monotonic implementation-defined time used for runtime deadlines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonotonicTime(Duration);

impl MonotonicTime {
    /// Creates a monotonic instant from an implementation-defined epoch.
    pub const fn from_duration(duration: Duration) -> Self {
        Self(duration)
    }

    /// Returns elapsed time from the implementation-defined epoch.
    pub const fn as_duration(self) -> Duration {
        self.0
    }

    /// Adds a duration without wrapping on overflow.
    pub fn saturating_add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration))
    }
}

/// Injected monotonic clock for deterministic runtime deadlines and stream polling.
#[async_trait]
pub trait RuntimeClock: Send + Sync {
    /// Returns the current monotonic instant.
    fn now(&self) -> MonotonicTime;

    /// Waits until the requested instant; canceling affects only this caller's wait.
    async fn sleep_until(&self, deadline: MonotonicTime);
}

/// Production runtime clock backed by Tokio's monotonic timer.
#[derive(Debug, Clone)]
pub struct TokioRuntimeClock {
    origin: tokio::time::Instant,
}

impl TokioRuntimeClock {
    /// Starts a new clock epoch at the current Tokio instant.
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl Default for TokioRuntimeClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RuntimeClock for TokioRuntimeClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime(self.origin.elapsed())
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        if let Some(deadline) = self.origin.checked_add(deadline.as_duration()) {
            tokio::time::sleep_until(deadline).await;
        } else {
            std::future::pending::<()>().await;
        }
    }
}
