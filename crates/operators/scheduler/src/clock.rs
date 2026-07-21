use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kernel_api::Timestamp;

/// Injected UTC time source used for persisted liveness grace calculations.
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

fn timestamp(time: SystemTime) -> Timestamp {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => Timestamp(milliseconds(duration)),
        Err(error) => Timestamp(milliseconds(error.duration()).saturating_neg()),
    }
}

fn milliseconds(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_timestamp_conversion_is_millisecond_precise_and_saturating() {
        assert_eq!(
            timestamp(UNIX_EPOCH + Duration::from_micros(1_234_999)),
            Timestamp(1_234)
        );
        assert_eq!(
            timestamp(UNIX_EPOCH - Duration::from_millis(42)),
            Timestamp(-42)
        );
        assert_eq!(milliseconds(Duration::MAX), i64::MAX);
    }
}
