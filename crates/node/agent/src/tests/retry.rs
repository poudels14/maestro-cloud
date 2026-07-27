use std::time::Duration;

use async_trait::async_trait;
use kernel_store::{Clock, MonotonicTime, StoreError, WatchCursor};
use tokio::sync::watch;

use crate::retry::{retryable_store_error, wait_for_store_retry_or_shutdown};

#[test]
fn retry_policy_distinguishes_availability_from_integrity_failures() {
    assert!(retryable_store_error(&StoreError::CursorExpired {
        cursor: WatchCursor::default(),
    }));
    assert!(retryable_store_error(&StoreError::Unavailable {
        message: "temporary outage".to_owned(),
    }));
    assert!(!retryable_store_error(&StoreError::Protection {
        message: "authentication failed".to_owned(),
    }));
    assert!(!retryable_store_error(&StoreError::Contract {
        message: "invalid backend response".to_owned(),
    }));
}

#[tokio::test]
async fn retry_wait_stops_immediately_on_shutdown() {
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let waiting = wait_for_store_retry_or_shutdown(&PendingClock, &mut shutdown_rx);
    assert!(shutdown.send(true).is_ok());
    assert!(waiting.await);
}

struct PendingClock;

#[async_trait]
impl Clock for PendingClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::ZERO)
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}
