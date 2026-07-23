use std::time::{Duration, Instant};

/// Monotonic process lifetime boundary for deterministic health snapshots.
pub trait UptimeClock: Send + Sync {
    /// Returns elapsed process time.
    fn elapsed(&self) -> Duration;
}

/// Production uptime clock backed by a monotonic system instant.
#[derive(Debug, Clone)]
pub struct SystemUptimeClock {
    started_at: Instant,
}

impl SystemUptimeClock {
    /// Starts a process lifetime at the current monotonic instant.
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Default for SystemUptimeClock {
    fn default() -> Self {
        Self::new()
    }
}

impl UptimeClock for SystemUptimeClock {
    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}
