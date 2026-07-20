use std::time::Duration;

/// Validated exponential retry policy with a fixed upper bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    initial: Duration,
    maximum: Duration,
}

impl Backoff {
    /// Creates a retry policy whose delay doubles after each failed attempt.
    pub fn new(initial: Duration, maximum: Duration) -> Result<Self, BackoffError> {
        if initial.is_zero() {
            Err(BackoffError::ZeroInitialDelay)
        } else if maximum < initial {
            Err(BackoffError::MaximumBeforeInitial)
        } else {
            Ok(Self { initial, maximum })
        }
    }

    /// Returns the bounded delay for a zero-based failure attempt.
    pub fn delay(self, attempt: u32) -> Duration {
        let multiplier = 1_u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX);
        self.initial.saturating_mul(multiplier).min(self.maximum)
    }
}

/// Invalid retry policy configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BackoffError {
    /// A zero initial delay would produce an unbounded hot retry loop.
    #[error("backoff initial delay must be greater than zero")]
    ZeroInitialDelay,
    /// The maximum delay cannot be below the first delay.
    #[error("backoff maximum delay must be at least the initial delay")]
    MaximumBeforeInitial,
}
