//! Injectable clock so time-driven controller behavior (build timeouts,
//! drain grace periods) is testable with deterministic advancement.

use std::sync::Arc;

pub trait Clock: Send + Sync {
    /// Milliseconds since the unix epoch.
    fn now_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        super::time::current_time_millis().unwrap_or(0)
    }
}

pub fn system() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

#[cfg(test)]
pub use fake::FakeClock;

#[cfg(test)]
mod fake {
    use super::Clock;
    use std::sync::atomic::{AtomicU64, Ordering};

    pub struct FakeClock {
        now: AtomicU64,
    }

    impl FakeClock {
        pub fn at(start_ms: u64) -> Self {
            Self {
                now: AtomicU64::new(start_ms),
            }
        }

        pub fn advance(&self, by_ms: u64) {
            self.now.fetch_add(by_ms, Ordering::SeqCst);
        }

        #[allow(dead_code)]
        pub fn set(&self, ms: u64) {
            self.now.store(ms, Ordering::SeqCst);
        }
    }

    impl Clock for FakeClock {
        fn now_ms(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }
    }
}
