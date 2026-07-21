use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kernel_api::Timestamp;

/// Injected UTC time source for controller decisions persisted in resources.
pub trait TimestampClock: Send + Sync {
    /// Returns the current Unix timestamp in milliseconds.
    fn now(&self) -> Timestamp;
}

/// Production UTC clock backed by the host system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemTimestampClock;

impl TimestampClock for SystemTimestampClock {
    fn now(&self) -> Timestamp {
        timestamp(SystemTime::now())
    }
}

pub(crate) fn timestamp(time: SystemTime) -> Timestamp {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => Timestamp(milliseconds(duration)),
        Err(error) => Timestamp(milliseconds(error.duration()).saturating_neg()),
    }
}

pub(crate) fn milliseconds(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}
