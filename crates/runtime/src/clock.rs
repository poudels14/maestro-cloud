use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use kernel_api::Timestamp;

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

/// Injected clock for deterministic runtime deadlines, stream polling, and persisted timestamps.
#[async_trait]
pub trait RuntimeClock: Send + Sync {
    /// Returns the current monotonic instant.
    fn now(&self) -> MonotonicTime;

    /// Returns the current Unix timestamp in milliseconds.
    fn timestamp(&self) -> Timestamp;

    /// Waits until the requested instant; canceling affects only this caller's wait.
    async fn sleep_until(&self, deadline: MonotonicTime);
}

/// Production runtime clock backed by Tokio's monotonic timer and the host wall clock.
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

    fn timestamp(&self) -> Timestamp {
        let milliseconds = SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
            |error| milliseconds(error.duration()).saturating_neg(),
            milliseconds,
        );
        Timestamp(milliseconds)
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        if let Some(deadline) = self.origin.checked_add(deadline.as_duration()) {
            tokio::time::sleep_until(deadline).await;
        } else {
            std::future::pending::<()>().await;
        }
    }
}

fn milliseconds(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}
