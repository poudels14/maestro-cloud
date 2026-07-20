use docker::container::LogOutput;

use crate::docker_stream::{decode_log, log_since};
use crate::{LogCursor, LogSource};

#[test]
fn docker_log_frames_strip_transport_timestamps_and_resume_by_second() {
    let frame = decode_log(LogOutput::StdErr {
        message: b"2026-07-20T19:42:18.123456789Z useful payload\n"
            .to_vec()
            .into(),
    })
    .unwrap();
    assert_eq!(frame.source, LogSource::Stderr);
    assert_eq!(frame.payload, b"useful payload\n");
    assert_eq!(frame.cursor.as_str(), "2026-07-20T19:42:18.123456789Z");
    assert_eq!(log_since(Some(&frame.cursor)).unwrap(), 1_784_576_538);
}

#[test]
fn docker_log_stream_rejects_malformed_cursors_and_frames() {
    assert!(log_since(Some(&LogCursor::new("not-a-time"))).is_err());
    assert!(
        decode_log(LogOutput::StdOut {
            message: b"missing-separator".to_vec().into(),
        })
        .is_err()
    );
}
