use std::time::Duration;

use kernel_api::{AssignmentId, Timestamp};

use crate::assignment_restart::{RetrySchedule, RetryTracker};

#[test]
fn retry_tracker_uses_backon_scaling_caps_attempts_and_resets() {
    let assignment = AssignmentId::new("assignment-1").expect("assignment id");
    let now = Timestamp(1_750_000_000_000);
    let mut retries = RetryTracker::new(Duration::from_secs(1), Duration::from_secs(5 * 60));

    assert_eq!(
        retries.schedule(assignment.clone(), Some(2), now),
        RetrySchedule::Scheduled {
            attempt: 1,
            retry_at: Timestamp(now.0 + 1_000),
        }
    );
    assert_eq!(
        retries.schedule(assignment.clone(), Some(2), now),
        RetrySchedule::Scheduled {
            attempt: 2,
            retry_at: Timestamp(now.0 + 1_500),
        }
    );
    retries.mark_started(&assignment);
    assert_eq!(retries.retry_at(&assignment), None);
    assert_eq!(
        retries.schedule(assignment.clone(), Some(2), now),
        RetrySchedule::Exhausted { maximum: 2 }
    );
    assert_eq!(retries.retry_at(&assignment), None);
    assert_eq!(
        retries.schedule(assignment.clone(), Some(2), now),
        RetrySchedule::Exhausted { maximum: 2 }
    );

    retries.clear(&assignment);
    assert_eq!(
        retries.schedule(assignment.clone(), None, now),
        RetrySchedule::Scheduled {
            attempt: 1,
            retry_at: Timestamp(now.0 + 1_000),
        }
    );

    let mut tenth = now;
    for _ in 2..=10 {
        let retry_at = match retries.schedule(assignment.clone(), None, now) {
            RetrySchedule::Scheduled { retry_at, .. } => retry_at,
            RetrySchedule::Exhausted { maximum } => {
                assert_eq!(maximum, u32::MAX, "unlimited system retry exhausted");
                now
            }
        };
        tenth = retry_at;
    }
    assert!(tenth < Timestamp(now.0 + 60_000));

    let mut latest = tenth;
    for _ in 11..=30 {
        let retry_at = match retries.schedule(assignment.clone(), None, now) {
            RetrySchedule::Scheduled { retry_at, .. } => retry_at,
            RetrySchedule::Exhausted { maximum } => {
                assert_eq!(maximum, u32::MAX, "unlimited system retry exhausted");
                now
            }
        };
        latest = retry_at;
    }
    assert_eq!(latest, Timestamp(now.0 + 5 * 60_000));
}
