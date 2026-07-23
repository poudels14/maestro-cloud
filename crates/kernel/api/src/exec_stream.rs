use std::str;

const MAXIMUM_FRAME_BYTES: usize = 16 * 1_024 * 1_024;

const STDIN: u8 = 0;
const RESIZE: u8 = 1;
const CLOSE_STDIN: u8 = 2;
const KILL: u8 = 3;
const STDOUT: u8 = 16;
const STDERR: u8 = 17;
const EXITED: u8 = 18;
const ERROR: u8 = 19;

/// One binary message in the hand-written interactive exec WebSocket protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecStreamFrame {
    /// Raw bytes sent to the command's standard input.
    Stdin(Vec<u8>),
    /// Updated dimensions for an allocated terminal.
    Resize {
        /// Terminal width in character cells.
        columns: u16,
        /// Terminal height in character cells.
        rows: u16,
    },
    /// Close command standard input while continuing to read output.
    CloseStdin,
    /// Terminate the command without terminating its workload.
    Kill,
    /// Raw standard-output or merged terminal bytes.
    Stdout(Vec<u8>),
    /// Raw standard-error bytes from a pipe-mode session.
    Stderr(Vec<u8>),
    /// Final command status reported by the workload runtime.
    Exited {
        /// Exit code, or `None` when the backend did not expose one.
        code: Option<i32>,
    },
    /// Terminal transport or runtime failure safe to display to the operator.
    Error(String),
}

impl ExecStreamFrame {
    /// Encodes one bounded WebSocket binary message.
    pub fn encode(&self) -> Result<Vec<u8>, ExecStreamProtocolError> {
        let mut encoded = Vec::new();
        match self {
            Self::Stdin(payload) => extend(&mut encoded, STDIN, payload),
            Self::Resize { columns, rows } => {
                if *columns == 0 || *rows == 0 {
                    return Err(ExecStreamProtocolError::ZeroTerminalDimension);
                }
                encoded.push(RESIZE);
                encoded.extend_from_slice(&columns.to_be_bytes());
                encoded.extend_from_slice(&rows.to_be_bytes());
            }
            Self::CloseStdin => encoded.push(CLOSE_STDIN),
            Self::Kill => encoded.push(KILL),
            Self::Stdout(payload) => extend(&mut encoded, STDOUT, payload),
            Self::Stderr(payload) => extend(&mut encoded, STDERR, payload),
            Self::Exited { code } => {
                encoded.push(EXITED);
                if let Some(code) = code {
                    encoded.push(1);
                    encoded.extend_from_slice(&code.to_be_bytes());
                } else {
                    encoded.push(0);
                }
            }
            Self::Error(message) => extend(&mut encoded, ERROR, message.as_bytes()),
        }
        if encoded.len() > MAXIMUM_FRAME_BYTES {
            return Err(ExecStreamProtocolError::FrameTooLarge);
        }
        Ok(encoded)
    }

    /// Decodes one complete WebSocket binary message.
    pub fn decode(encoded: &[u8]) -> Result<Self, ExecStreamProtocolError> {
        if encoded.is_empty() {
            return Err(ExecStreamProtocolError::EmptyFrame);
        }
        if encoded.len() > MAXIMUM_FRAME_BYTES {
            return Err(ExecStreamProtocolError::FrameTooLarge);
        }
        let (&kind, payload) = encoded
            .split_first()
            .ok_or(ExecStreamProtocolError::EmptyFrame)?;
        match kind {
            STDIN => Ok(Self::Stdin(payload.to_vec())),
            RESIZE => decode_resize(payload),
            CLOSE_STDIN if payload.is_empty() => Ok(Self::CloseStdin),
            KILL if payload.is_empty() => Ok(Self::Kill),
            STDOUT => Ok(Self::Stdout(payload.to_vec())),
            STDERR => Ok(Self::Stderr(payload.to_vec())),
            EXITED => decode_exit(payload),
            ERROR => str::from_utf8(payload)
                .map(|message| Self::Error(message.to_owned()))
                .map_err(|_| ExecStreamProtocolError::InvalidUtf8),
            CLOSE_STDIN | KILL => Err(ExecStreamProtocolError::UnexpectedPayload { kind }),
            kind => Err(ExecStreamProtocolError::UnknownFrame { kind }),
        }
    }

    /// Returns whether this message is valid input from an exec client.
    pub fn is_client_input(&self) -> bool {
        matches!(
            self,
            Self::Stdin(_) | Self::Resize { .. } | Self::CloseStdin | Self::Kill
        )
    }

    /// Returns whether this message ends the exec stream.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Exited { .. } | Self::Error(_))
    }
}

fn extend(encoded: &mut Vec<u8>, kind: u8, payload: &[u8]) {
    encoded.reserve(payload.len().saturating_add(1));
    encoded.push(kind);
    encoded.extend_from_slice(payload);
}

fn decode_resize(payload: &[u8]) -> Result<ExecStreamFrame, ExecStreamProtocolError> {
    let [column_high, column_low, row_high, row_low] = payload else {
        return Err(ExecStreamProtocolError::InvalidResize);
    };
    let columns = u16::from_be_bytes([*column_high, *column_low]);
    let rows = u16::from_be_bytes([*row_high, *row_low]);
    if columns == 0 || rows == 0 {
        Err(ExecStreamProtocolError::ZeroTerminalDimension)
    } else {
        Ok(ExecStreamFrame::Resize { columns, rows })
    }
}

fn decode_exit(payload: &[u8]) -> Result<ExecStreamFrame, ExecStreamProtocolError> {
    match payload {
        [0] => Ok(ExecStreamFrame::Exited { code: None }),
        [1, byte_1, byte_2, byte_3, byte_4] => Ok(ExecStreamFrame::Exited {
            code: Some(i32::from_be_bytes([*byte_1, *byte_2, *byte_3, *byte_4])),
        }),
        _ => Err(ExecStreamProtocolError::InvalidExit),
    }
}

/// Malformed or oversized interactive exec protocol message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExecStreamProtocolError {
    /// A WebSocket binary message contained no frame discriminator.
    #[error("exec frame is empty")]
    EmptyFrame,
    /// A frame exceeded the protocol's 16 MiB message bound.
    #[error("exec frame exceeds 16 MiB")]
    FrameTooLarge,
    /// A terminal resize did not contain two non-zero big-endian dimensions.
    #[error("exec resize frame must contain two non-zero u16 dimensions")]
    InvalidResize,
    /// A terminal dimension was zero.
    #[error("exec terminal dimensions must be non-zero")]
    ZeroTerminalDimension,
    /// An exit frame did not contain the optional-code marker and value.
    #[error("exec exit frame has an invalid status payload")]
    InvalidExit,
    /// An error frame was not valid UTF-8.
    #[error("exec error frame is not UTF-8")]
    InvalidUtf8,
    /// A payload was attached to a marker-only frame.
    #[error("exec frame type {kind} must not have a payload")]
    UnexpectedPayload {
        /// Raw wire discriminator for the marker-only frame.
        kind: u8,
    },
    /// The frame discriminator is not assigned by this protocol version.
    #[error("unknown exec frame type {kind}")]
    UnknownFrame {
        /// Raw wire discriminator rejected by this protocol version.
        kind: u8,
    },
}
