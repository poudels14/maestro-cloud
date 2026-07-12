use std::path::Path;

use crate::error::{Error, Result};
use crate::logs::LogStore;

pub async fn list(db_path: &Path, sink_id: &str, limit: usize) -> Result<()> {
    let store = open_existing_store(db_path)?;
    let stats = store
        .sink_dead_letter_stats(sink_id)
        .await
        .map_err(|err| Error::internal(format!("failed to read dead-letter stats: {err}")))?;
    let entries = store
        .list_sink_dead_letters(sink_id, limit)
        .await
        .map_err(|err| Error::internal(format!("failed to list dead letters: {err}")))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "sinkId": sink_id,
            "count": stats.count,
            "payloadBytes": stats.payload_bytes,
            "entries": entries,
        }))?
    );
    Ok(())
}

pub async fn export(db_path: &Path, sink_id: &str, output: &Path) -> Result<()> {
    let store = open_existing_store(db_path)?;
    let count = store
        .export_sink_dead_letters(sink_id, output)
        .await
        .map_err(|err| Error::internal(format!("failed to export dead letters: {err}")))?;
    eprintln!(
        "[maestro]: exported {count} {sink_id} dead letters to {}",
        output.display()
    );
    Ok(())
}

pub async fn purge(db_path: &Path, sink_id: &str, through_seq: Option<i64>) -> Result<()> {
    let store = open_existing_store(db_path)?;
    let count = store
        .purge_sink_dead_letters(sink_id, through_seq)
        .await
        .map_err(|err| Error::internal(format!("failed to purge dead letters: {err}")))?;
    eprintln!("[maestro]: purged {count} {sink_id} dead letters");
    Ok(())
}

fn open_existing_store(db_path: &Path) -> Result<LogStore> {
    if !db_path.is_file() {
        return Err(Error::not_found(format!(
            "controller log spool does not exist at {}",
            db_path.display()
        )));
    }
    LogStore::open(db_path)
        .map_err(|err| Error::internal(format!("failed to open controller log spool: {err}")))
}
