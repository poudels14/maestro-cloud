use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalSize {
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecFrame {
    Stdin(Vec<u8>),
    Output(Vec<u8>),
    Resize(TerminalSize),
    Exit(i32),
    Error(String),
    Ping,
}

impl ExecFrame {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        match self {
            Self::Stdin(payload) => {
                encoded.push(0);
                encoded.extend_from_slice(payload);
            }
            Self::Output(payload) => {
                encoded.push(1);
                encoded.extend_from_slice(payload);
            }
            Self::Resize(size) => {
                encoded.push(2);
                encoded.extend_from_slice(&serde_json::to_vec(size)?);
            }
            Self::Exit(code) => {
                encoded.push(3);
                encoded.extend_from_slice(&serde_json::to_vec(&serde_json::json!({
                    "code": code,
                }))?);
            }
            Self::Error(message) => {
                encoded.push(4);
                encoded.extend_from_slice(message.as_bytes());
            }
            Self::Ping => encoded.push(5),
        }
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let Some((&frame_type, payload)) = encoded.split_first() else {
            bail!("exec frame is empty");
        };
        match frame_type {
            0 => Ok(Self::Stdin(payload.to_vec())),
            1 => Ok(Self::Output(payload.to_vec())),
            2 => Ok(Self::Resize(serde_json::from_slice(payload)?)),
            3 => {
                #[derive(Deserialize)]
                struct ExitPayload {
                    code: i32,
                }

                Ok(Self::Exit(
                    serde_json::from_slice::<ExitPayload>(payload)?.code,
                ))
            }
            4 => Ok(Self::Error(String::from_utf8(payload.to_vec()).map_err(
                |error| anyhow!("exec error frame is not UTF-8: {error}"),
            )?)),
            5 if payload.is_empty() => Ok(Self::Ping),
            5 => bail!("exec ping frame must not have a payload"),
            other => bail!("unknown exec frame type {other}"),
        }
    }

    pub fn terminal(&self) -> bool {
        matches!(self, Self::Exit(_) | Self::Error(_))
    }
}

pub async fn read_length_prefixed<R>(reader: &mut R) -> Result<Option<ExecFrame>>
where
    R: AsyncRead + Unpin,
{
    let mut length = [0_u8; 4];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAX_FRAME_SIZE {
        bail!("invalid exec frame length {length}");
    }
    let mut encoded = vec![0_u8; length];
    reader.read_exact(&mut encoded).await?;
    ExecFrame::decode(&encoded).map(Some)
}

pub async fn write_length_prefixed<W>(writer: &mut W, frame: &ExecFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let encoded = frame.encode()?;
    let length = u32::try_from(encoded.len()).map_err(|_| anyhow!("exec frame is too large"))?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let frames = [
            ExecFrame::Stdin(vec![0, 1, 255]),
            ExecFrame::Output(b"hello".to_vec()),
            ExecFrame::Resize(TerminalSize {
                cols: 120,
                rows: 40,
            }),
            ExecFrame::Exit(17),
            ExecFrame::Error("no shell".to_string()),
            ExecFrame::Ping,
        ];
        for frame in frames {
            assert_eq!(ExecFrame::decode(&frame.encode().unwrap()).unwrap(), frame);
        }
    }

    #[tokio::test]
    async fn length_prefixed_frames_round_trip() {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let frame = ExecFrame::Output(b"streamed".to_vec());
        write_length_prefixed(&mut writer, &frame).await.unwrap();
        assert_eq!(
            read_length_prefixed(&mut reader).await.unwrap(),
            Some(frame)
        );
    }
}
