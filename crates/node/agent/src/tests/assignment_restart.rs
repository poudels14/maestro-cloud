use std::time::Duration;

use kernel_api::Timestamp;

use crate::assignment_restart::restart_deadline;

#[test]
fn restart_deadline_doubles_and_saturates_at_the_configured_maximum() {
    let now = Timestamp(1_750_000_000_000);
    let base = Duration::from_secs(5);
    let maximum = Duration::from_secs(60);

    assert_eq!(
        restart_deadline(now, base, maximum, 1),
        Timestamp(now.0 + 5_000)
    );
    assert_eq!(
        restart_deadline(now, base, maximum, 2),
        Timestamp(now.0 + 10_000)
    );
    assert_eq!(
        restart_deadline(now, base, maximum, 5),
        Timestamp(now.0 + 60_000)
    );
    assert_eq!(
        restart_deadline(now, base, maximum, u32::MAX),
        Timestamp(now.0 + 60_000)
    );
}
