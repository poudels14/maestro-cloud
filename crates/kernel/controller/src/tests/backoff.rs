use std::time::Duration;

use crate::{Backoff, BackoffError};

#[test]
fn backoff_doubles_and_saturates_at_the_configured_maximum() {
    let backoff =
        Backoff::new(Duration::from_millis(100), Duration::from_secs(1)).expect("valid backoff");

    assert_eq!(backoff.delay(0), Duration::from_millis(100));
    assert_eq!(backoff.delay(1), Duration::from_millis(200));
    assert_eq!(backoff.delay(3), Duration::from_millis(800));
    assert_eq!(backoff.delay(4), Duration::from_secs(1));
    assert_eq!(backoff.delay(u32::MAX), Duration::from_secs(1));
}

#[test]
fn backoff_rejects_hot_loops_and_inverted_bounds() {
    assert_eq!(
        Backoff::new(Duration::ZERO, Duration::from_secs(1)),
        Err(BackoffError::ZeroInitialDelay)
    );
    assert_eq!(
        Backoff::new(Duration::from_secs(2), Duration::from_secs(1)),
        Err(BackoffError::MaximumBeforeInitial)
    );
}
