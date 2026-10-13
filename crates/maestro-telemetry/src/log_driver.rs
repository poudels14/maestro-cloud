use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

const CURRENT_FILE: &str = "current.log";
const MAX_LINE_BYTES: usize = 256 * 1024;
const LINE_BUFFER: usize = 1024;

#[derive(Clone, Debug)]
pub struct LogDriverSettings {
    pub log_dir: PathBuf,
    pub max_file_bytes: u64,
    pub max_files: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    pub ts: u64,
    pub stream: Stream,
    pub line: String,
}

struct RotatingFile {
    directory: PathBuf,
    file: File,
    size: u64,
    settings: LogDriverSettings,
}

pub fn container_log_dir(log_dir: &Path, container_id: &str) -> PathBuf {
    log_dir.join(container_id)
}

pub async fn pump(
    container_id: &str,
    stdout: impl AsyncRead + Unpin + Send + 'static,
    stderr: impl AsyncRead + Unpin + Send + 'static,
    settings: LogDriverSettings,
) -> std::io::Result<()> {
    let (sender, mut receiver) = mpsc::channel::<LogRecord>(LINE_BUFFER);
    let stdout_task = tokio::spawn(read_lines(stdout, Stream::Stdout, sender.clone()));
    let stderr_task = tokio::spawn(read_lines(stderr, Stream::Stderr, sender));
    let mut output =
        RotatingFile::open(container_log_dir(&settings.log_dir, container_id), settings).await?;
    while let Some(record) = receiver.recv().await {
        output.append(&record).await?;
    }
    output.file.flush().await?;
    let _ = stdout_task.await;
    let _ = stderr_task.await;
    Ok(())
}

async fn read_lines(
    reader: impl AsyncRead + Unpin,
    stream: Stream,
    sender: mpsc::Sender<LogRecord>,
) {
    let mut reader = BufReader::new(reader);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let read = (&mut reader)
            .take(MAX_LINE_BYTES as u64)
            .read_until(b'\n', &mut buffer)
            .await;
        match read {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let line = String::from_utf8_lossy(&buffer)
                    .trim_end_matches(['\n', '\r'])
                    .to_string();
                let record = LogRecord {
                    ts: unix_millis(),
                    stream,
                    line,
                };
                if sender.send(record).await.is_err() {
                    break;
                }
            }
        }
    }
}

impl RotatingFile {
    async fn open(directory: PathBuf, settings: LogDriverSettings) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(&directory).await?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join(CURRENT_FILE))
            .await?;
        let size = file.metadata().await?.len();
        Ok(Self {
            directory,
            file,
            size,
            settings,
        })
    }

    async fn append(&mut self, record: &LogRecord) -> std::io::Result<()> {
        let mut encoded = serde_json::to_vec(record)?;
        encoded.push(b'\n');
        if self.size > 0 && self.size + encoded.len() as u64 > self.settings.max_file_bytes {
            self.rotate().await?;
        }
        self.file.write_all(&encoded).await?;
        self.size += encoded.len() as u64;
        Ok(())
    }

    async fn rotate(&mut self) -> std::io::Result<()> {
        self.file.flush().await?;
        let rotated = |index: usize| self.directory.join(format!("{CURRENT_FILE}.{index}"));
        let oldest = rotated(self.settings.max_files);
        match tokio::fs::remove_file(&oldest).await {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error)?,
            _ => {}
        }
        for index in (1..self.settings.max_files).rev() {
            match tokio::fs::rename(rotated(index), rotated(index + 1)).await {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error)?,
                _ => {}
            }
        }
        tokio::fs::rename(self.directory.join(CURRENT_FILE), rotated(1)).await?;
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join(CURRENT_FILE))
            .await?;
        self.size = 0;
        Ok(())
    }
}

impl Default for LogDriverSettings {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("/var/lib/maestro/logs"),
            max_file_bytes: 32 * 1024 * 1024,
            max_files: 4,
        }
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_records(path: &Path) -> Vec<LogRecord> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn writes_both_streams_as_json_lines() {
        let directory = tempfile::tempdir().unwrap();
        let settings = LogDriverSettings {
            log_dir: directory.path().to_path_buf(),
            ..LogDriverSettings::default()
        };
        let stdout: &[u8] = b"hello\nworld\r\npartial";
        let stderr: &[u8] = b"oops\n";
        pump("c1", stdout, stderr, settings).await.unwrap();

        let records = read_records(&directory.path().join("c1").join(CURRENT_FILE));
        let mut stdout_lines = records
            .iter()
            .filter(|record| record.stream == Stream::Stdout)
            .map(|record| record.line.as_str())
            .collect::<Vec<_>>();
        stdout_lines.sort();
        assert_eq!(stdout_lines, ["hello", "partial", "world"]);
        assert!(
            records
                .iter()
                .any(|record| record.stream == Stream::Stderr && record.line == "oops")
        );
    }

    #[tokio::test]
    async fn rotates_and_keeps_limited_files() {
        let directory = tempfile::tempdir().unwrap();
        let settings = LogDriverSettings {
            log_dir: directory.path().to_path_buf(),
            max_file_bytes: 200,
            max_files: 2,
        };
        let input = (0..40)
            .map(|index| format!("line number {index:02}\n"))
            .collect::<String>();
        pump(
            "c1",
            std::io::Cursor::new(input.into_bytes()),
            tokio::io::empty(),
            settings,
        )
        .await
        .unwrap();

        let container_dir = directory.path().join("c1");
        let mut names = std::fs::read_dir(&container_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, ["current.log", "current.log.1", "current.log.2"]);
        let newest = read_records(&container_dir.join(CURRENT_FILE));
        assert_eq!(newest.last().unwrap().line, "line number 39");
        let mut sizes = Vec::new();
        for name in &names {
            let mut file = File::open(container_dir.join(name)).await.unwrap();
            let mut contents = Vec::new();
            file.read_to_end(&mut contents).await.unwrap();
            sizes.push(contents.len());
        }
        assert!(sizes.iter().all(|size| *size <= 200));
    }

    #[tokio::test]
    async fn splits_oversized_lines() {
        let directory = tempfile::tempdir().unwrap();
        let settings = LogDriverSettings {
            log_dir: directory.path().to_path_buf(),
            ..LogDriverSettings::default()
        };
        let huge = "x".repeat(MAX_LINE_BYTES + 10);
        pump(
            "c1",
            std::io::Cursor::new(huge.into_bytes()),
            tokio::io::empty(),
            settings,
        )
        .await
        .unwrap();
        let records = read_records(&directory.path().join("c1").join(CURRENT_FILE));
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].line.len(), MAX_LINE_BYTES);
    }
}
