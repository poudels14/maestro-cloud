use std::io::Read;
use std::path::{Path, PathBuf};

use chrono::Utc;
use duckdb::{Connection, params};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{Db, now_ms, partition_directory, sql_literal};
use crate::error::{Result, TelemetryError};

#[derive(Serialize)]
struct PartitionManifest {
    version: u8,
    partition_key: String,
    updated_at_ms: i64,
    parts: Vec<PartitionManifestPart>,
}

#[derive(Serialize)]
struct PartitionManifestPart {
    file: String,
    row_count: i64,
    seq_lo: i64,
    seq_hi: i64,
    sha256: String,
    size_bytes: u64,
}

struct RolloverGroup {
    date: String,
    row_count: i64,
    seq_lo: i64,
    seq_hi: i64,
}

struct ExportPaths {
    directory: PathBuf,
    final_path: PathBuf,
    temp_path: PathBuf,
}

impl RolloverGroup {
    fn export_paths(&self, parts_root: &Path) -> ExportPaths {
        let directory = partition_directory(parts_root, &self.date);
        let name = format!("part-{}-{}.parquet", self.seq_lo, self.seq_hi);
        ExportPaths {
            final_path: directory.join(&name),
            temp_path: directory.join(format!("{name}.tmp-{}", std::process::id())),
            directory,
        }
    }

    fn copy_sql(&self, destination: &Path) -> String {
        format!(
            r#"
                COPY (
                    SELECT
                        seq,
                        ts,
                        replica_id,
                        service_id,
                        deployment_id,
                        stream,
                        level,
                        line,
                        message,
                        attributes
                    FROM logs
                    WHERE date = CAST({} AS DATE)
                      AND seq BETWEEN {} AND {}
                    ORDER BY seq
                ) TO {} (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD
                )
            "#,
            sql_literal(&self.date),
            self.seq_lo,
            self.seq_hi,
            sql_literal(&destination.to_string_lossy())
        )
    }
}

pub(super) fn rollover_partitions(db: &Db, parts_root: &Path) -> Result<usize> {
    let started = std::time::Instant::now();
    let groups = load_groups(db)?;
    let group_count = groups.len();
    let total = groups.into_iter().try_fold(0usize, |total, group| {
        export_group(db, parts_root, &group)?;
        Ok::<_, TelemetryError>(total.saturating_add(usize::try_from(group.row_count).unwrap_or(0)))
    })?;
    if total > 0 {
        db.maintenance()?.execute_batch("CHECKPOINT")?;
        tracing::info!(
            groups = group_count,
            rows = total,
            duration_ms = started.elapsed().as_millis() as u64,
            "rolled logs over to parquet"
        );
    }
    Ok(total)
}

pub(super) fn write_partition_manifest(
    connection: &Connection,
    directory: &Path,
    partition_key: &str,
) -> Result<()> {
    let mut paths = std::fs::read_dir(directory)?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .collect::<Vec<_>>();
    paths.sort();
    let parts = paths
        .iter()
        .map(|path| {
            let (row_count, seq_lo, seq_hi) = parquet_summary(connection, path)?;
            Ok(PartitionManifestPart {
                file: path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
                row_count,
                seq_lo,
                seq_hi,
                sha256: sha256_file(path)?,
                size_bytes: path.metadata()?.len(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = PartitionManifest {
        version: 1,
        partition_key: partition_key.to_string(),
        updated_at_ms: now_ms(),
        parts,
    };
    let temp_path = directory.join(format!("manifest.json.tmp-{}", std::process::id()));
    std::fs::write(&temp_path, serde_json::to_vec_pretty(&manifest)?)?;
    sync_path(&temp_path)?;
    std::fs::rename(&temp_path, directory.join("manifest.json"))?;
    sync_path(directory)?;
    Ok(())
}

pub(super) fn remove_stale_exports(root: &Path) -> Result<()> {
    std::fs::read_dir(root)?.try_for_each(|entry| {
        let path = entry?.path();
        if path.is_dir() {
            remove_stale_exports(&path)
        } else if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(".tmp-"))
        {
            Ok(std::fs::remove_file(path)?)
        } else {
            Ok(())
        }
    })
}

fn load_groups(db: &Db) -> Result<Vec<RolloverGroup>> {
    let today = Utc::now().date_naive().to_string();
    let connection = db.reader()?;
    let mut statement = connection.prepare(
        r#"
            SELECT date::VARCHAR, count(*), min(seq), max(seq)
            FROM logs
            WHERE date < CAST(? AS DATE)
            GROUP BY ALL
            ORDER BY 1
        "#,
    )?;
    let rows = statement.query_map(params![today], |row| {
        Ok(RolloverGroup {
            date: row.get(0)?,
            row_count: row.get(1)?,
            seq_lo: row.get(2)?,
            seq_hi: row.get(3)?,
        })
    })?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

fn export_group(db: &Db, parts_root: &Path, group: &RolloverGroup) -> Result<()> {
    let paths = group.export_paths(parts_root);
    std::fs::create_dir_all(&paths.directory)?;
    let created = stage_export(db, group, &paths)?;
    let checksum = sha256_file(match created {
        true => &paths.temp_path,
        false => &paths.final_path,
    })?;
    let _visibility = db.write_parquet()?;
    let finalized = finalize_export(db, group, &paths, checksum, created);
    match finalized {
        Err(error) if created => {
            tracing::error!(
                partition = group.date,
                seq_lo = group.seq_lo,
                seq_hi = group.seq_hi,
                %error,
                "parquet rollover commit failed"
            );
            hide_failed_export(db, &paths, &group.date).map_err(|cleanup_error| {
                TelemetryError::Storage(format!(
                    "{error}; also failed to hide uncommitted export: {cleanup_error}"
                ))
            })?;
            Err(error)
        }
        other => other,
    }
}

fn stage_export(db: &Db, group: &RolloverGroup, paths: &ExportPaths) -> Result<bool> {
    let connection = db.maintenance()?;
    remove_uncommitted_parts(&connection, &paths.directory, &group.date)?;
    let created = !paths.final_path.exists();
    if created {
        if paths.temp_path.exists() {
            std::fs::remove_file(&paths.temp_path)?;
        }
        connection.execute_batch(&group.copy_sql(&paths.temp_path))?;
        verify_parquet(&connection, &paths.temp_path, group)?;
        sync_path(&paths.temp_path)?;
    }
    Ok(created)
}

fn finalize_export(
    db: &Db,
    group: &RolloverGroup,
    paths: &ExportPaths,
    checksum: String,
    created: bool,
) -> Result<()> {
    if created {
        std::fs::rename(&paths.temp_path, &paths.final_path)?;
        sync_path(&paths.directory)?;
    }
    {
        let connection = db.maintenance()?;
        verify_parquet(&connection, &paths.final_path, group)?;
        write_partition_manifest(&connection, &paths.directory, &group.date)?;
    }
    let mut connection = db.writer()?;
    let transaction = connection.transaction()?;
    transaction.execute(
        r#"
            INSERT INTO partition_state
            VALUES (?, 'exported', ?, ?, ?, ?, ?)
            ON CONFLICT DO UPDATE SET
                state = 'exported',
                row_count = excluded.row_count,
                seq_hi = excluded.seq_hi,
                sha256 = excluded.sha256,
                updated_at_ms = excluded.updated_at_ms
        "#,
        params![
            group.date,
            group.row_count,
            group.seq_lo,
            group.seq_hi,
            checksum,
            now_ms()
        ],
    )?;
    transaction.execute(
        "DELETE FROM logs WHERE date = CAST(? AS DATE) AND seq BETWEEN ? AND ?",
        params![group.date, group.seq_lo, group.seq_hi],
    )?;
    transaction.commit()?;
    Ok(())
}

fn hide_failed_export(db: &Db, paths: &ExportPaths, partition_key: &str) -> Result<()> {
    if paths.final_path.exists() {
        std::fs::rename(&paths.final_path, &paths.temp_path)?;
        sync_path(&paths.directory)?;
    }
    write_partition_manifest(&*db.maintenance()?, &paths.directory, partition_key)
}

fn verify_parquet(connection: &Connection, path: &Path, group: &RolloverGroup) -> Result<()> {
    let summary = parquet_summary(connection, path)?;
    let expected = (group.row_count, group.seq_lo, group.seq_hi);
    match summary == expected {
        true => Ok(()),
        false => Err(TelemetryError::Storage(format!(
            "parquet verification failed for {}: expected {expected:?}, got {summary:?}",
            path.display()
        ))),
    }
}

fn parquet_summary(connection: &Connection, path: &Path) -> Result<(i64, i64, i64)> {
    let sql = format!(
        "SELECT count(*), min(seq), max(seq) FROM read_parquet({})",
        sql_literal(&path.to_string_lossy())
    );
    Ok(connection.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?)
}

fn remove_uncommitted_parts(
    connection: &Connection,
    directory: &Path,
    partition_key: &str,
) -> Result<()> {
    std::fs::read_dir(directory)?.try_for_each(|entry| {
        let path = entry?.path();
        match parquet_part_seq_lo(&path) {
            Some(seq_lo) => {
                let committed: bool = connection.query_row(
                    r#"
                        SELECT EXISTS(
                            SELECT 1 FROM partition_state
                            WHERE partition_key = ? AND seq_lo = ?
                        )
                    "#,
                    params![partition_key, seq_lo],
                    |row| row.get(0),
                )?;
                if !committed {
                    std::fs::remove_file(&path)?;
                }
                Ok(())
            }
            None => Ok(()),
        }
    })
}

fn parquet_part_seq_lo(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    let range = name.strip_prefix("part-")?.strip_suffix(".parquet")?;
    range.split_once('-')?.0.parse().ok()
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        match file.read(&mut buffer)? {
            0 => break,
            read => digest.update(&buffer[..read]),
        }
    }
    Ok(hex::encode(digest.finalize()))
}

fn sync_path(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}
