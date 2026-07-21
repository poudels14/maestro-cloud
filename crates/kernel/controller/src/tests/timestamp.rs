use std::time::{Duration, UNIX_EPOCH};

use kernel_api::Timestamp;

use crate::timestamp::{milliseconds, timestamp};

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
