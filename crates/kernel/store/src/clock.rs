use std::time::Duration;

use async_trait::async_trait;

/// Monotonic implementation-defined time used for deadlines and TTLs.
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

/// Injected monotonic time source for deterministic kernel behavior.
#[async_trait]
pub trait Clock: Send + Sync {
    /// Returns the current monotonic instant.
    fn now(&self) -> MonotonicTime;

    /// Waits until the requested instant.
    ///
    /// Canceling this future cancels only the caller's wait and does not alter
    /// the clock or any other sleeper.
    async fn sleep_until(&self, deadline: MonotonicTime);
}

/// Production monotonic clock backed by Tokio's runtime timer.
#[derive(Debug, Clone)]
pub struct TokioClock {
    origin: tokio::time::Instant,
}

impl TokioClock {
    /// Starts a clock epoch at the current Tokio monotonic instant.
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Clock for TokioClock {
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
