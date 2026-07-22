use crate::{ExecStreamFrame, ExecStreamProtocolError};

#[test]
fn exec_stream_frames_round_trip_without_reinterpreting_bytes() {
    let frames = [
        ExecStreamFrame::Stdin(vec![0, 1, 255]),
        ExecStreamFrame::Resize {
            columns: 120,
            rows: 40,
        },
        ExecStreamFrame::CloseStdin,
        ExecStreamFrame::Kill,
        ExecStreamFrame::Stdout(b"output".to_vec()),
        ExecStreamFrame::Stderr(b"warning".to_vec()),
        ExecStreamFrame::Exited { code: Some(-1) },
        ExecStreamFrame::Exited { code: None },
        ExecStreamFrame::Error("not running".to_owned()),
    ];
    for frame in frames {
        assert_eq!(
            ExecStreamFrame::decode(&frame.encode().unwrap()).unwrap(),
            frame
        );
    }
}

#[test]
fn exec_stream_rejects_invalid_shapes_and_directions_are_explicit() {
    assert_eq!(
        ExecStreamFrame::decode(&[]),
        Err(ExecStreamProtocolError::EmptyFrame)
    );
    assert_eq!(
        ExecStreamFrame::decode(&[1, 0, 80, 0]),
        Err(ExecStreamProtocolError::InvalidResize)
    );
    assert_eq!(
        ExecStreamFrame::decode(&[1, 0, 0, 0, 24]),
        Err(ExecStreamProtocolError::ZeroTerminalDimension)
    );
    assert_eq!(
        ExecStreamFrame::decode(&[2, 1]),
        Err(ExecStreamProtocolError::UnexpectedPayload { kind: 2 })
    );
    assert_eq!(
        ExecStreamFrame::decode(&[18, 1, 0]),
        Err(ExecStreamProtocolError::InvalidExit)
    );
    assert_eq!(
        ExecStreamFrame::decode(&[19, 255]),
        Err(ExecStreamProtocolError::InvalidUtf8)
    );
    assert!(ExecStreamFrame::Kill.is_client_input());
    assert!(!ExecStreamFrame::Stdout(Vec::new()).is_client_input());
    assert!(ExecStreamFrame::Exited { code: None }.is_terminal());
}

#[test]
fn exec_stream_applies_the_same_size_bound_when_encoding_and_decoding() {
    let oversized = vec![0; 16 * 1_024 * 1_024];
    assert_eq!(
        ExecStreamFrame::Stdin(oversized.clone()).encode(),
        Err(ExecStreamProtocolError::FrameTooLarge)
    );
    assert_eq!(
        ExecStreamFrame::decode(&[oversized, vec![0]].concat()),
        Err(ExecStreamProtocolError::FrameTooLarge)
    );
}
