use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use duckdb::{Connection, params};
use sha2::{Digest, Sha256};

use super::{Db, hive_component, now_ms, sql_lit};

#[derive(serde::Serialize)]
struct PartitionManifest {
    version: u8,
    tier: String,
    partition_key: String,
    updated_at_ms: i64,
    parts: Vec<PartitionManifestPart>,
}

#[derive(serde::Serialize)]
struct PartitionManifestPart {
    file: String,
    row_count: i64,
    seq_lo: i64,
    seq_hi: i64,
    sha256: String,
    size_bytes: u64,
}

enum PartitionIdentity {
    Service {
        service_id: String,
        deployment_id: String,
        date: String,
    },
    System {
        date: String,
    },
}

struct RolloverGroup {
    identity: PartitionIdentity,
    row_count: i64,
    seq_lo: i64,
    seq_hi: i64,
}

impl RolloverGroup {
    fn tier(&self) -> &'static str {
        match self.identity {
            PartitionIdentity::Service { .. } => "service",
            PartitionIdentity::System { .. } => "system",
        }
    }

    fn partition_key(&self) -> String {
        match &self.identity {
            PartitionIdentity::Service {
                service_id,
                deployment_id,
                date,
            } => format!("{service_id}/{deployment_id}/{date}"),
            PartitionIdentity::System { date } => date.clone(),
        }
    }

    fn directory(&self, root: &Path) -> std::path::PathBuf {
        match &self.identity {
            PartitionIdentity::Service {
                service_id,
                deployment_id,
                date,
            } => root
                .join(format!("service_id={}", hive_component(service_id)))
                .join(format!("deployment_id={}", hive_component(deployment_id)))
                .join(format!("date={date}")),
            PartitionIdentity::System { date } => root.join(format!("date={date}")),
        }
    }

    fn copy_sql(&self, destination: &Path) -> String {
        match &self.identity {
            PartitionIdentity::Service {
                service_id,
                deployment_id,
                date,
            } => format!(
                r#"
                    COPY (
                        SELECT
                            seq,
                            ts,
                            unit,
                            origin,
                            level,
                            stream,
                            text,
                            tags,
                            attributes
                        FROM logs
                        WHERE service_id = {}
                          AND deployment_id = {}
                          AND date = CAST({} AS DATE)
                          AND seq BETWEEN {} AND {}
                        ORDER BY seq
                    ) TO {} (
                        FORMAT PARQUET,
                        COMPRESSION ZSTD
                    )
                "#,
                sql_lit(service_id),
                sql_lit(deployment_id),
                sql_lit(date),
                self.seq_lo,
                self.seq_hi,
                sql_lit(&destination.to_string_lossy())
            ),
            PartitionIdentity::System { date } => format!(
                r#"
                    COPY (
                        SELECT
                            seq,
                            ts,
                            source,
                            origin,
                            level,
                            stream,
                            text,
                            tags,
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
                sql_lit(date),
                self.seq_lo,
                self.seq_hi,
                sql_lit(&destination.to_string_lossy())
            ),
        }
    }

    fn delete_hot_rows(&self, connection: &Connection) -> Result<()> {
        match &self.identity {
            PartitionIdentity::Service {
                service_id,
                deployment_id,
                date,
            } => {
                connection.execute(
                    r#"
                        DELETE FROM logs
                        WHERE service_id = ?
                          AND deployment_id = ?
                          AND date = CAST(? AS DATE)
                          AND seq BETWEEN ? AND ?
                    "#,
                    params![service_id, deployment_id, date, self.seq_lo, self.seq_hi],
                )?;
            }
            PartitionIdentity::System { date } => {
                connection.execute(
                    r#"
                        DELETE FROM logs
                        WHERE date = CAST(? AS DATE)
                          AND seq BETWEEN ? AND ?
                    "#,
                    params![date, self.seq_lo, self.seq_hi],
                )?;
            }
        }
        Ok(())
    }
}

pub(super) fn rollover_service(db: &Db, root: &Path) -> Result<usize> {
    rollover_groups(db, root, load_service_groups(db)?)
}

pub(super) fn rollover_system(db: &Db, root: &Path) -> Result<usize> {
    rollover_groups(db, root, load_system_groups(db)?)
}

fn load_service_groups(db: &Db) -> Result<Vec<RolloverGroup>> {
    let today = Utc::now().date_naive().to_string();
    let connection = db.reader()?;
    let mut statement = connection.prepare(
        r#"
            SELECT
                service_id,
                deployment_id,
                date::VARCHAR,
                count(*),
                min(seq),
                max(seq)
            FROM logs
            WHERE date < CAST(? AS DATE)
            GROUP BY ALL
            ORDER BY 1, 2, 3
        "#,
    )?;
    let rows = statement.query_map(params![today], |row| {
        Ok(RolloverGroup {
            identity: PartitionIdentity::Service {
                service_id: row.get(0)?,
                deployment_id: row.get(1)?,
                date: row.get(2)?,
            },
            row_count: row.get(3)?,
            seq_lo: row.get(4)?,
            seq_hi: row.get(5)?,
        })
    })?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

fn load_system_groups(db: &Db) -> Result<Vec<RolloverGroup>> {
    let today = Utc::now().date_naive().to_string();
    let connection = db.reader()?;
    let mut statement = connection.prepare(
        r#"
            SELECT
                date::VARCHAR,
                count(*),
                min(seq),
                max(seq)
            FROM logs
            WHERE date < CAST(? AS DATE)
            GROUP BY ALL
            ORDER BY 1
        "#,
    )?;
    let rows = statement.query_map(params![today], |row| {
        Ok(RolloverGroup {
            identity: PartitionIdentity::System { date: row.get(0)? },
            row_count: row.get(1)?,
            seq_lo: row.get(2)?,
            seq_hi: row.get(3)?,
        })
    })?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

fn rollover_groups(db: &Db, root: &Path, groups: Vec<RolloverGroup>) -> Result<usize> {
    let started = std::time::Instant::now();
    let group_count = groups.len();
    let tier = groups.first().map(RolloverGroup::tier).unwrap_or("none");
    let mut total = 0usize;
    for group in groups {
        let directory = group.directory(root);
        std::fs::create_dir_all(&directory)?;
        let final_path = directory.join(format!("part-{}-{}.parquet", group.seq_lo, group.seq_hi));
        let temp_path = directory.join(format!(
            "part-{}-{}.parquet.tmp-{}",
            group.seq_lo,
            group.seq_hi,
            std::process::id()
        ));
        let partition_key = group.partition_key();
        let created = stage_export(
            db,
            &group,
            &directory,
            &partition_key,
            &final_path,
            &temp_path,
        )?;
        let checksum = sha256_file(if created { &temp_path } else { &final_path })?;
        let _visibility = db.write_parquet()?;
        let finalized = finalize_export(
            db,
            &group,
            &directory,
            &partition_key,
            &final_path,
            &temp_path,
            checksum,
            created,
        );
        if let Err(err) = finalized {
            eprintln!(
                "[maestro]: duckdb_rollover_commit_failed tier={} partition={} seq_lo={} seq_hi={} error={err:#}",
                group.tier(),
                partition_key,
                group.seq_lo,
                group.seq_hi
            );
            if created
                && let Err(cleanup_err) = hide_failed_export(
                    db,
                    &final_path,
                    &temp_path,
                    &directory,
                    group.tier(),
                    &partition_key,
                )
            {
                return Err(err.context(format!(
                    "also failed to hide uncommitted export: {cleanup_err:#}"
                )));
            }
            return Err(err);
        }
        total = total.saturating_add(usize::try_from(group.row_count)?);
    }
    if total > 0 {
        db.maintenance()?.execute_batch("CHECKPOINT")?;
        eprintln!(
            "[maestro]: duckdb_rollover tier={tier} groups={group_count} rows={total} duration_ms={}",
            started.elapsed().as_millis()
        );
    }
    Ok(total)
}

fn stage_export(
    db: &Db,
    group: &RolloverGroup,
    directory: &Path,
    partition_key: &str,
    final_path: &Path,
    temp_path: &Path,
) -> Result<bool> {
    let connection = db.maintenance()?;
    remove_uncommitted_parts(&connection, directory, group.tier(), partition_key)?;
    let created = !final_path.exists();
    if created {
        if temp_path.exists() {
            std::fs::remove_file(temp_path)?;
        }
        connection.execute_batch(&group.copy_sql(temp_path))?;
        verify_parquet(
            &connection,
            temp_path,
            group.row_count,
            group.seq_lo,
            group.seq_hi,
        )?;
        sync_file(temp_path)?;
    }
    Ok(created)
}

fn finalize_export(
    db: &Db,
    group: &RolloverGroup,
    directory: &Path,
    partition_key: &str,
    final_path: &Path,
    temp_path: &Path,
    checksum: String,
    created: bool,
) -> Result<()> {
    if created {
        std::fs::rename(temp_path, final_path)?;
        sync_dir(directory)?;
    }
    {
        let connection = db.maintenance()?;
        verify_parquet(
            &connection,
            final_path,
            group.row_count,
            group.seq_lo,
            group.seq_hi,
        )?;
        write_partition_manifest(&connection, directory, group.tier(), partition_key)?;
    }
    let mut connection = db.writer()?;
    let transaction = connection.transaction()?;
    transaction.execute(
        r#"
            INSERT INTO partition_state
            VALUES (?, ?, 'exported', ?, ?, ?, ?, ?)
            ON CONFLICT DO UPDATE SET
                state = 'exported',
                row_count = excluded.row_count,
                seq_hi = excluded.seq_hi,
                sha256 = excluded.sha256,
                updated_at_ms = excluded.updated_at_ms
        "#,
        params![
            group.tier(),
            partition_key,
            group.row_count,
            group.seq_lo,
            group.seq_hi,
            checksum,
            now_ms()
        ],
    )?;
    group.delete_hot_rows(&transaction)?;
    transaction.commit()?;
    Ok(())
}
fn hide_failed_export(
    db: &Db,
    final_path: &Path,
    temp_path: &Path,
    directory: &Path,
    tier: &str,
    partition_key: &str,
) -> Result<()> {
    if final_path.exists() {
        std::fs::rename(final_path, temp_path)?;
        sync_dir(directory)?;
    }
    let conn = db.maintenance()?;
    write_partition_manifest(&conn, directory, tier, partition_key)
}
fn verify_parquet(conn: &Connection, path: &Path, count: i64, lo: i64, hi: i64) -> Result<()> {
    let q = format!(
        "SELECT count(*),min(seq),max(seq) FROM read_parquet({})",
        sql_lit(&path.to_string_lossy())
    );
    let got: (i64, i64, i64) = conn.query_row(&q, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    if got != (count, lo, hi) {
        bail!(
            "Parquet verification failed for {}: expected ({count},{lo},{hi}), got {got:?}",
            path.display()
        )
    }
    Ok(())
}
fn remove_uncommitted_parts(
    conn: &Connection,
    directory: &Path,
    tier: &str,
    partition_key: &str,
) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        let Some(seq_lo) = parquet_part_seq_lo(&path) else {
            continue;
        };
        let committed: bool = conn.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM partition_state
                    WHERE tier = ?
                      AND partition_key = ?
                      AND seq_lo = ?
                )
            "#,
            params![tier, partition_key, seq_lo],
            |row| row.get(0),
        )?;
        if !committed {
            std::fs::remove_file(&path).with_context(|| {
                format!("remove uncommitted rollover export {}", path.display())
            })?;
        }
    }
    Ok(())
}
fn parquet_part_seq_lo(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    let range = name.strip_prefix("part-")?.strip_suffix(".parquet")?;
    range.split_once('-')?.0.parse().ok()
}
fn sha256_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
pub(super) fn write_partition_manifest(
    conn: &Connection,
    dir: &Path,
    tier: &str,
    partition_key: &str,
) -> Result<()> {
    let mut paths = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut parts = Vec::with_capacity(paths.len());
    for path in paths {
        let query = format!(
            "SELECT count(*), min(seq), max(seq) FROM read_parquet({})",
            sql_lit(&path.to_string_lossy())
        );
        let (row_count, seq_lo, seq_hi) = conn.query_row(&query, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
        parts.push(PartitionManifestPart {
            file: path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            row_count,
            seq_lo,
            seq_hi,
            sha256: sha256_file(&path)?,
            size_bytes: path.metadata()?.len(),
        });
    }
    let manifest = PartitionManifest {
        version: 1,
        tier: tier.to_string(),
        partition_key: partition_key.to_string(),
        updated_at_ms: now_ms(),
        parts,
    };
    let temp_path = dir.join(format!("manifest.json.tmp-{}", std::process::id()));
    let final_path = dir.join("manifest.json");
    std::fs::write(&temp_path, serde_json::to_vec_pretty(&manifest)?)?;
    sync_file(&temp_path)?;
    std::fs::rename(&temp_path, &final_path)?;
    sync_dir(dir)?;
    Ok(())
}
fn sync_file(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}
fn sync_dir(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}
pub(super) fn remove_stale_exports(root: &Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            remove_stale_exports(&path)?
        } else if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().contains(".tmp-"))
        {
            std::fs::remove_file(path)?
        }
    }
    Ok(())
}
