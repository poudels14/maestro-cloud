use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::error::Result;
use crate::store::TailCheckpoint;
use crate::tailer::{LogFile, Tailer, log_files, replica_dirs};

impl Tailer {
    pub fn sweep_idle(&mut self, idle_after: Duration) -> Result<Vec<String>> {
        let now = SystemTime::now();
        let expired = replica_dirs(&self.log_dir)?
            .into_iter()
            .filter(|(replica_id, directory)| {
                is_expired(directory, self.checkpoints.get(replica_id), idle_after, now)
                    .unwrap_or_else(|error| {
                        tracing::warn!(replica_id, %error, "failed to inspect container log dir");
                        false
                    })
            })
            .collect::<Vec<_>>();
        expired
            .into_iter()
            .map(|(replica_id, directory)| {
                std::fs::remove_dir_all(&directory)?;
                self.store.forget_checkpoint(&replica_id)?;
                self.checkpoints.remove(&replica_id);
                tracing::info!(replica_id, "removed idle container log dir");
                Ok(replica_id)
            })
            .collect()
    }
}

fn is_expired(
    directory: &Path,
    checkpoint: Option<&TailCheckpoint>,
    idle_after: Duration,
    now: SystemTime,
) -> Result<bool> {
    let files = log_files(directory)?;
    let last_write = files
        .iter()
        .filter_map(|file| {
            file.path
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
        })
        .chain(directory.metadata()?.modified().ok())
        .max();
    let idle = last_write
        .and_then(|modified| now.duration_since(modified).ok())
        .is_some_and(|elapsed| elapsed > idle_after);
    Ok(idle && fully_ingested(&files, checkpoint)?)
}

fn fully_ingested(files: &[LogFile], checkpoint: Option<&TailCheckpoint>) -> Result<bool> {
    match (files.last(), checkpoint) {
        (None, _) => Ok(true),
        (Some(newest), Some(checkpoint)) if newest.inode == checkpoint.inode => {
            match checkpoint.offset >= newest.size {
                true => Ok(true),
                false => Ok(!unread_contains_line(newest, checkpoint.offset)?),
            }
        }
        _ => Ok(false),
    }
}

fn unread_contains_line(file: &LogFile, offset: u64) -> Result<bool> {
    let mut handle = std::fs::File::open(&file.path)?;
    handle.seek(SeekFrom::Start(offset))?;
    let mut unread = Vec::new();
    handle.read_to_end(&mut unread)?;
    Ok(unread.contains(&b'\n'))
}
