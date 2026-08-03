use async_trait::async_trait;
use kernel_api::SecretValue;
use tokio::io::{AsyncRead, AsyncReadExt};
use zeroize::Zeroizing;

use crate::ArtifactStoreError;

const READ_BYTES: usize = 8 * 1_024;
const MAX_FRAME_BYTES: usize = 16 * 1_024;
const REDACTION: &[u8] = b"[REDACTED]";

/// Native output stream produced while an artifact backend is building an image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactBuildOutputStream {
    /// Ordinary build progress and command output.
    Stdout,
    /// Diagnostic build progress and command output.
    Stderr,
}

/// Receives bounded, redacted output frames while an artifact build is running.
#[async_trait]
pub trait ArtifactBuildOutputSink: Send + Sync {
    /// Records one frame from the backend's native output stream.
    async fn write(&self, stream: ArtifactBuildOutputStream, output: Vec<u8>);
}

/// Output sink used when a caller does not consume build progress.
pub struct DiscardArtifactBuildOutput;

#[async_trait]
impl ArtifactBuildOutputSink for DiscardArtifactBuildOutput {
    async fn write(&self, _stream: ArtifactBuildOutputStream, _output: Vec<u8>) {}
}

/// Drains one process pipe into a build-output sink without retaining unbounded lines.
///
/// Exact protected values are removed before output crosses the backend boundary. The
/// redactor retains enough input between reads to catch a value split across pipe chunks.
pub async fn forward_artifact_build_output(
    mut reader: impl AsyncRead + Unpin,
    stream: ArtifactBuildOutputStream,
    sink: &dyn ArtifactBuildOutputSink,
    protected: &[SecretValue],
    description: &str,
) -> Result<(), ArtifactStoreError> {
    let mut read_buffer = Zeroizing::new(vec![0_u8; READ_BYTES]);
    let mut pending = Zeroizing::new(Vec::new());
    loop {
        let count = reader
            .read(read_buffer.as_mut_slice())
            .await
            .map_err(|error| ArtifactStoreError::Unavailable {
                message: format!("read {description}: {error}"),
            })?;
        if count == 0 {
            for frame in ready_frames(&mut pending, protected, true) {
                sink.write(stream, frame).await;
            }
            return Ok(());
        }
        let chunk = read_buffer
            .get(..count)
            .ok_or_else(|| ArtifactStoreError::Unavailable {
                message: format!("read {description}: reader returned an invalid byte count"),
            })?;
        pending.extend_from_slice(chunk);
        for frame in ready_frames(&mut pending, protected, false) {
            sink.write(stream, frame).await;
        }
    }
}

fn ready_frames(
    pending: &mut Vec<u8>,
    protected: &[SecretValue],
    end_of_stream: bool,
) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let retained = protected
        .iter()
        .map(|value| value.expose().len())
        .max()
        .unwrap_or_default()
        .saturating_sub(1);
    loop {
        let safe = if end_of_stream {
            pending.len()
        } else {
            pending.len().saturating_sub(retained)
        };
        if safe == 0 {
            break;
        }
        let newline = pending
            .get(..safe)
            .and_then(|bytes| bytes.iter().position(|byte| *byte == b'\n'))
            .map(|position| position.saturating_add(1));
        let candidate = match newline {
            Some(boundary) => boundary,
            None if end_of_stream => safe.min(MAX_FRAME_BYTES),
            None if safe >= MAX_FRAME_BYTES => MAX_FRAME_BYTES,
            None => break,
        };
        let boundary = unsplit_boundary(pending, candidate, safe, protected);
        if boundary == 0 {
            break;
        }
        let raw = Zeroizing::new(pending.drain(..boundary).collect::<Vec<u8>>());
        let mut frame = redact(&raw, protected);
        if frame.last() == Some(&b'\n') {
            frame.pop();
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
        }
        frames.push(frame);
    }
    frames
}

fn unsplit_boundary(
    input: &[u8],
    mut boundary: usize,
    safe: usize,
    protected: &[SecretValue],
) -> usize {
    loop {
        let crossing = protected
            .iter()
            .map(SecretValue::expose)
            .filter(|value| !value.is_empty())
            .flat_map(|value| occurrences(input, value.as_bytes()))
            .find(|(start, end)| *start < boundary && *end > boundary);
        let Some((start, end)) = crossing else {
            return boundary;
        };
        boundary = if end <= safe { end } else { start };
    }
}

fn occurrences<'a>(input: &'a [u8], needle: &'a [u8]) -> impl Iterator<Item = (usize, usize)> + 'a {
    input
        .windows(needle.len())
        .enumerate()
        .filter(move |(_, window)| *window == needle)
        .map(move |(start, _)| (start, start.saturating_add(needle.len())))
}

fn redact(input: &[u8], protected: &[SecretValue]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    let mut offset = 0;
    while offset < input.len() {
        let next = protected
            .iter()
            .map(SecretValue::expose)
            .filter(|value| !value.is_empty())
            .filter_map(|value| {
                find_subslice(input.get(offset..)?, value.as_bytes())
                    .map(|relative| (offset.saturating_add(relative), value.len()))
            })
            .min_by(|(left_start, left_len), (right_start, right_len)| {
                left_start
                    .cmp(right_start)
                    .then_with(|| right_len.cmp(left_len))
            });
        let Some((start, length)) = next else {
            if let Some(remaining) = input.get(offset..) {
                output.extend_from_slice(remaining);
            }
            break;
        };
        let Some(unredacted) = input.get(offset..start) else {
            break;
        };
        output.extend_from_slice(unredacted);
        output.extend_from_slice(REDACTION);
        offset = start.saturating_add(length);
    }
    output
}

fn find_subslice(input: &[u8], needle: &[u8]) -> Option<usize> {
    input
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use super::*;

    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<(ArtifactBuildOutputStream, Vec<u8>)>>);

    #[async_trait]
    impl ArtifactBuildOutputSink for RecordingSink {
        async fn write(&self, stream: ArtifactBuildOutputStream, output: Vec<u8>) {
            lock(&self.0).push((stream, output));
        }
    }

    #[tokio::test]
    async fn forwarding_preserves_lines_and_redacts_secrets_split_across_reads()
    -> Result<(), ArtifactStoreError> {
        let secret = format!("{}protected-token", "x".repeat(READ_BYTES - 2));
        let input = format!("prefix {secret} suffix\nsecond line\n");
        let sink = RecordingSink::default();

        forward_artifact_build_output(
            std::io::Cursor::new(input.as_bytes()),
            ArtifactBuildOutputStream::Stdout,
            &sink,
            &[SecretValue::new(secret.clone())],
            "test output",
        )
        .await?;

        let frames = lock(&sink.0);
        let text = frames
            .iter()
            .map(|(_, frame)| String::from_utf8_lossy(frame))
            .collect::<Vec<_>>();
        assert_eq!(text, ["prefix [REDACTED] suffix", "second line"]);
        assert!(!text.join("\n").contains(&secret));
        Ok(())
    }

    #[tokio::test]
    async fn forwarding_bounds_output_without_newlines() -> Result<(), ArtifactStoreError> {
        let sink = RecordingSink::default();
        let input = vec![b'a'; MAX_FRAME_BYTES * 3 + 7];

        forward_artifact_build_output(
            std::io::Cursor::new(input),
            ArtifactBuildOutputStream::Stderr,
            &sink,
            &[],
            "test output",
        )
        .await?;

        let frames = lock(&sink.0);
        assert_eq!(
            frames.iter().map(|(_, frame)| frame.len()).sum::<usize>(),
            MAX_FRAME_BYTES * 3 + 7
        );
        assert!(
            frames
                .iter()
                .all(|(_, frame)| frame.len() <= MAX_FRAME_BYTES)
        );
        Ok(())
    }

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
