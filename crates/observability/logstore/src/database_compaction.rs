use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};

use duckdb::Connection;

const COMPACT_ALIAS: &str = "__maestro_compacted";
const MINIMUM_FREE_BYTES: u64 = 1024 * 1024;
const FREE_RATIO_DENOMINATOR: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DatabaseAllocation {
    block_size: u64,
    total_blocks: u64,
    free_blocks: u64,
}

impl DatabaseAllocation {
    fn should_compact(self) -> bool {
        self.free_blocks.saturating_mul(self.block_size) >= MINIMUM_FREE_BYTES
            && self.free_blocks.saturating_mul(FREE_RATIO_DENOMINATOR) >= self.total_blocks
    }
}

/// Opens the authoritative database after repairing an interrupted compaction.
pub(crate) fn open(path: &Path) -> Result<Connection, String> {
    recover_missing_primary(path)?;
    let mut connection = match crate::schema::open(path) {
        Ok(connection) => connection,
        Err(primary_error) if restore_backup(path)? => {
            crate::schema::open(path).map_err(|error| {
                format!(
                    "open database after restoring its compaction backup: {error}; \
                 replacement open failed first: {primary_error}"
                )
            })?
        }
        Err(error) => return Err(error),
    };
    cleanup_sidecars(path)?;
    compact_if_needed(&mut connection, path)?;
    Ok(connection)
}

/// Rewrites a checkpointed database when free blocks exceed the bounded slack policy.
pub(crate) fn compact_if_needed(connection: &mut Connection, path: &Path) -> Result<u64, String> {
    connection
        .execute_batch("CHECKPOINT")
        .map_err(|error| format!("checkpoint database before compaction: {error}"))?;
    let allocation = allocation(connection)?;
    if !allocation.should_compact() {
        return Ok(0);
    }

    let compact = sidecar(path, "compact")?;
    let backup = sidecar(path, "precompact")?;
    remove_file_if_exists(&compact, "stale compaction output")?;
    remove_file_if_exists(&backup, "stale compaction backup")?;

    copy_database(connection, &compact)?;
    sync_file(&compact, "compacted database")?;
    let old_size = file_size(path, "source database")?;
    let new_size = file_size(&compact, "compacted database")?;
    if new_size >= old_size {
        remove_file_if_exists(&compact, "non-shrinking compaction output")?;
        sync_parent(path)?;
        return Ok(0);
    }

    swap_database(connection, path, &compact, &backup)?;
    Ok(old_size.saturating_sub(new_size))
}

fn allocation(connection: &Connection) -> Result<DatabaseAllocation, String> {
    let raw = connection
        .query_row(
            "SELECT block_size, total_blocks, free_blocks
             FROM pragma_database_size()",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .map_err(|error| format!("inspect database allocation before compaction: {error}"))?;
    Ok(DatabaseAllocation {
        block_size: nonnegative(raw.0, "block size")?,
        total_blocks: nonnegative(raw.1, "total block count")?,
        free_blocks: nonnegative(raw.2, "free block count")?,
    })
}

fn copy_database(connection: &Connection, destination: &Path) -> Result<(), String> {
    let destination_sql = destination
        .to_str()
        .ok_or_else(|| "compaction database path is not UTF-8".to_owned())?;
    let source = connection
        .query_row("SELECT current_database()", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|error| format!("identify source database for compaction: {error}"))?;
    connection
        .execute_batch(&format!(
            "ATTACH {} AS {}",
            sql_string(destination_sql),
            sql_identifier(COMPACT_ALIAS),
        ))
        .map_err(|error| format!("attach compacted database: {error}"))?;
    let copied = connection.execute_batch(&format!(
        "COPY FROM DATABASE {} TO {}",
        sql_identifier(&source),
        sql_identifier(COMPACT_ALIAS),
    ));
    let detached = connection
        .execute_batch(&format!("DETACH {}", sql_identifier(COMPACT_ALIAS)))
        .map_err(|error| format!("detach compacted database: {error}"));
    if let Err(error) = copied {
        if let Err(detach_error) = detached {
            return Err(format!(
                "copy live database into compacted database: {error}; {detach_error}"
            ));
        }
        remove_file_if_exists(destination, "failed compaction output")?;
        return Err(format!(
            "copy live database into compacted database: {error}"
        ));
    }
    detached?;

    let validation = crate::schema::open(destination)
        .map_err(|error| format!("validate compacted database: {error}"))?;
    validation
        .execute_batch("CHECKPOINT")
        .map_err(|error| format!("checkpoint compacted database: {error}"))?;
    drop(validation);
    Ok(())
}

fn swap_database(
    connection: &mut Connection,
    path: &Path,
    compact: &Path,
    backup: &Path,
) -> Result<(), String> {
    sync_file(path, "source database")?;
    std::fs::rename(path, backup)
        .map_err(|error| format!("stage source database compaction backup: {error}"))?;
    sync_parent(path)?;
    if let Err(error) = std::fs::rename(compact, path) {
        restore_renamed_backup(path, backup)?;
        return Err(format!("publish compacted database: {error}"));
    }
    sync_parent(path)?;

    let replacement = match crate::schema::open(path) {
        Ok(replacement) => replacement,
        Err(error) => {
            remove_file_if_exists(compact, "failed replacement holding path")?;
            std::fs::rename(path, compact)
                .map_err(|rename| format!("preserve failed compacted database: {rename}"))?;
            restore_renamed_backup(path, backup)?;
            return Err(format!("open published compacted database: {error}"));
        }
    };
    let previous = std::mem::replace(connection, replacement);
    drop(previous);
    remove_file_if_exists(backup, "completed compaction backup")?;
    sync_parent(path)
}

fn recover_missing_primary(path: &Path) -> Result<(), String> {
    if path.exists() {
        return Ok(());
    }
    let backup = sidecar(path, "precompact")?;
    let compact = sidecar(path, "compact")?;
    let recovery = if backup.exists() {
        Some(backup)
    } else if compact.exists() {
        Some(compact)
    } else {
        None
    };
    if let Some(recovery) = recovery {
        std::fs::rename(&recovery, path).map_err(|error| {
            format!(
                "restore database from interrupted compaction `{}`: {error}",
                recovery.display()
            )
        })?;
        sync_parent(path)?;
    }
    Ok(())
}

fn restore_backup(path: &Path) -> Result<bool, String> {
    let backup = sidecar(path, "precompact")?;
    if !backup.exists() {
        return Ok(false);
    }
    let compact = sidecar(path, "compact")?;
    remove_file_if_exists(&compact, "failed compaction output")?;
    if path.exists() {
        std::fs::rename(path, &compact)
            .map_err(|error| format!("preserve failed compacted database: {error}"))?;
    }
    restore_renamed_backup(path, &backup)?;
    Ok(true)
}

fn restore_renamed_backup(path: &Path, backup: &Path) -> Result<(), String> {
    std::fs::rename(backup, path)
        .map_err(|error| format!("restore source database compaction backup: {error}"))?;
    sync_parent(path)
}

fn cleanup_sidecars(path: &Path) -> Result<(), String> {
    remove_file_if_exists(&sidecar(path, "compact")?, "stale compaction output")?;
    remove_file_if_exists(&sidecar(path, "precompact")?, "completed compaction backup")?;
    sync_parent(path)
}

pub(crate) fn sidecar(path: &Path, suffix: &str) -> Result<PathBuf, String> {
    let file_name = path
        .file_name()
        .ok_or_else(|| "database path has no file name".to_owned())?;
    let mut sidecar = OsString::from(file_name);
    sidecar.push(".");
    sidecar.push(suffix);
    Ok(path.with_file_name(sidecar))
}

fn remove_file_if_exists(path: &Path, purpose: &str) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() || metadata.file_type().is_symlink() => {
            std::fs::remove_file(path)
                .map_err(|error| format!("remove {purpose} `{}`: {error}", path.display()))
        }
        Ok(_) => Err(format!("{purpose} `{}` is not a file", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("inspect {purpose} `{}`: {error}", path.display())),
    }
}

fn file_size(path: &Path, purpose: &str) -> Result<u64, String> {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| format!("inspect {purpose} `{}`: {error}", path.display()))
}

fn sync_file(path: &Path, purpose: &str) -> Result<(), String> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| format!("sync {purpose} `{}`: {error}", path.display()))
}

fn sync_parent(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "database path has no parent directory".to_owned())?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "sync database parent directory `{}`: {error}",
                parent.display()
            )
        })
}

fn nonnegative(value: i64, purpose: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("database {purpose} is negative"))
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
