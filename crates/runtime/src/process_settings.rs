use std::time::Duration;

use crate::RuntimeError;

/// Timing policy for process exit observation and stream polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessRuntimeSettings {
    /// Interval between adopted-process status checks and stream filesystem reads.
    pub poll_interval: Duration,
    /// Maximum wait after SIGKILL before surfacing a retryable timeout.
    pub kill_timeout: Duration,
}

impl ProcessRuntimeSettings {
    /// Validates that polling and forced-stop deadlines cannot create a hot loop.
    pub fn validate(self) -> Result<Self, RuntimeError> {
        if self.poll_interval.is_zero() || self.kill_timeout.is_zero() {
            Err(RuntimeError::InvalidSpec {
                message: "process runtime intervals must be greater than zero".to_owned(),
            })
        } else {
            Ok(self)
        }
    }
}

impl Default for ProcessRuntimeSettings {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            kill_timeout: Duration::from_secs(5),
        }
    }
}
