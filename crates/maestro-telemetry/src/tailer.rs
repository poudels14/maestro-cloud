use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::Result;
use crate::log_driver::{CURRENT_FILE, LogRecord};
use crate::resolver::ReplicaResolver;
use crate::store::{LogStore, TailCheckpoint};

const MAX_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

pub struct Tailer {
    pub(crate) log_dir: PathBuf,
    pub(crate) store: Arc<LogStore>,
    pub(crate) checkpoints: HashMap<String, TailCheckpoint>,
    pub(crate) resolver: Arc<dyn ReplicaResolver>,
}

#[derive(Debug)]
pub(crate) struct LogFile {
    pub path: PathBuf,
    pub inode: u64,
    pub size: u64,
}

struct Chunk {
    records: Vec<LogRecord>,
    consumed: u64,
}

enum Step {
    Ingested(usize),
    Advanced,
    Idle,
}

impl Tailer {
    pub fn new(
        log_dir: PathBuf,
        store: Arc<LogStore>,
        resolver: Arc<dyn ReplicaResolver>,
    ) -> Result<Self> {
        let checkpoints = store.checkpoints()?;
        Ok(Self {
            log_dir,
            store,
            checkpoints,
            resolver,
        })
    }

    pub fn poll(&mut self) -> Result<usize> {
        let replicas = replica_dirs(&self.log_dir)?;
        let stale = self
            .checkpoints
            .keys()
            .filter(|replica_id| !replicas.contains_key(*replica_id))
            .cloned()
            .collect::<Vec<_>>();
        stale.iter().try_for_each(|replica_id| {
            self.store.forget_checkpoint(replica_id)?;
            self.checkpoints.remove(replica_id);
            Ok::<_, crate::error::TelemetryError>(())
        })?;
        Ok(replicas
            .iter()
            .map(|(replica_id, directory)| {
                self.poll_replica(replica_id, directory)
                    .unwrap_or_else(|error| {
                        tracing::warn!(replica_id, %error, "failed to tail container logs");
                        0
                    })
            })
            .sum())
    }

    pub fn checkpoint(&self, replica_id: &str) -> Option<TailCheckpoint> {
        self.checkpoints.get(replica_id).copied()
    }

    fn poll_replica(&mut self, replica_id: &str, directory: &Path) -> Result<usize> {
        let mut ingested = 0;
        loop {
            match self.step(replica_id, directory)? {
                Step::Ingested(count) => ingested += count,
                Step::Advanced => {}
                Step::Idle => break,
            }
        }
        Ok(ingested)
    }

    fn step(&mut self, replica_id: &str, directory: &Path) -> Result<Step> {
        let files = log_files(directory)?;
        match self.position(replica_id, &files) {
            Some((index, offset)) => {
                let file = &files[index];
                match read_chunk(file, offset)? {
                    Some(chunk) if chunk.consumed > 0 => {
                        let count = chunk.records.len();
                        self.commit(
                            replica_id,
                            &chunk.records,
                            TailCheckpoint {
                                inode: file.inode,
                                offset: offset + chunk.consumed,
                            },
                        )?;
                        Ok(Step::Ingested(count))
                    }
                    Some(_) if index + 1 < files.len() => {
                        let next = &files[index + 1];
                        self.commit(
                            replica_id,
                            &[],
                            TailCheckpoint {
                                inode: next.inode,
                                offset: 0,
                            },
                        )?;
                        Ok(Step::Advanced)
                    }
                    _ => Ok(Step::Idle),
                }
            }
            None => Ok(Step::Idle),
        }
    }

    fn position(&self, replica_id: &str, files: &[LogFile]) -> Option<(usize, u64)> {
        let checkpoint = self.checkpoints.get(replica_id);
        let found = checkpoint.and_then(|checkpoint| {
            files
                .iter()
                .position(|file| file.inode == checkpoint.inode)
                .map(|index| (index, checkpoint.offset))
        });
        match (found, checkpoint) {
            (Some((index, offset)), _) if files[index].size < offset => {
                tracing::warn!(replica_id, "log file was truncated; rereading it");
                Some((index, 0))
            }
            (Some(position), _) => Some(position),
            (None, Some(_)) if !files.is_empty() => {
                tracing::warn!(
                    replica_id,
                    "checkpointed log file rotated away before it was read; resuming at the oldest file"
                );
                Some((0, 0))
            }
            (None, _) => (!files.is_empty()).then_some((0, 0)),
        }
    }

    fn commit(
        &mut self,
        replica_id: &str,
        records: &[LogRecord],
        checkpoint: TailCheckpoint,
    ) -> Result<()> {
        let identity = self.resolver.resolve(replica_id);
        self.store
            .ingest(replica_id, &identity, records, checkpoint)?;
        self.checkpoints.insert(replica_id.to_string(), checkpoint);
        Ok(())
    }
}

pub(crate) fn replica_dirs(log_dir: &Path) -> Result<HashMap<String, PathBuf>> {
    match std::fs::read_dir(log_dir) {
        Ok(entries) => Ok(entries
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .map(|entry| {
                (
                    entry.file_name().to_string_lossy().to_string(),
                    entry.path(),
                )
            })
            .collect()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn log_files(directory: &Path) -> Result<Vec<LogFile>> {
    let mut ranked = std::fs::read_dir(directory)?
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| {
            let rank = rotation_rank(&entry.file_name().to_string_lossy())?;
            let metadata = entry.metadata().ok()?;
            Some((
                rank,
                LogFile {
                    path: entry.path(),
                    inode: metadata.ino(),
                    size: metadata.len(),
                },
            ))
        })
        .collect::<Vec<_>>();
    ranked.sort_by_key(|(rank, _)| std::cmp::Reverse(*rank));
    Ok(ranked.into_iter().map(|(_, file)| file).collect())
}

fn rotation_rank(name: &str) -> Option<usize> {
    match name.strip_prefix(CURRENT_FILE) {
        Some("") => Some(0),
        Some(suffix) => suffix
            .strip_prefix('.')
            .and_then(|index| index.parse::<usize>().ok())
            .filter(|index| *index > 0),
        None => None,
    }
}

fn read_chunk(file: &LogFile, offset: u64) -> Result<Option<Chunk>> {
    let mut handle = std::fs::File::open(&file.path)?;
    match handle.metadata()?.ino() == file.inode {
        true => {
            handle.seek(SeekFrom::Start(offset))?;
            let mut buffer = Vec::new();
            handle.take(MAX_CHUNK_BYTES).read_to_end(&mut buffer)?;
            Ok(Some(parse_chunk(&file.path, &buffer)))
        }
        false => Ok(None),
    }
}

fn parse_chunk(path: &Path, buffer: &[u8]) -> Chunk {
    match buffer.iter().rposition(|byte| *byte == b'\n') {
        Some(last_newline) => Chunk {
            records: buffer[..last_newline]
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .filter_map(|line| match serde_json::from_slice::<LogRecord>(line) {
                    Ok(record) => Some(record),
                    Err(error) => {
                        tracing::warn!(path = %path.display(), %error, "skipping malformed log record");
                        None
                    }
                })
                .collect(),
            consumed: last_newline as u64 + 1,
        },
        None if buffer.len() as u64 >= MAX_CHUNK_BYTES => {
            tracing::warn!(path = %path.display(), "skipping oversized log record");
            Chunk {
                records: Vec::new(),
                consumed: buffer.len() as u64,
            }
        }
        None => Chunk {
            records: Vec::new(),
            consumed: 0,
        },
    }
}
