use std::collections::BTreeSet;
use std::path::Path;

use duckdb::{Connection, OptionalExt, params};
use logs::{
    DeadLetterStoreError, IngestLogEntry, LogDeliveryStoreError, LogSequence, LogSinkCursorStats,
    LogSinkId, LogSpoolStats, LogStatsStoreError, MAX_RETAINED_DEAD_LETTERS, SequencedLogEntry,
    SinkDeadLetter, SinkDeadLetterSnapshot, SinkDeadLetterStats,
};

const MAX_DEAD_LETTER_REASON_BYTES: usize = 4_096;
const MAX_DEAD_LETTER_PAYLOAD_BYTES: usize = 5_000_000;

pub(crate) fn read_after(
    connection: &Connection,
    cursor: Option<LogSequence>,
    limit: usize,
) -> Result<Vec<SequencedLogEntry>, LogDeliveryStoreError> {
    if limit == 0 {
        return Err(delivery_rejected("delivery read limit must be non-zero"));
    }
    let cursor = cursor.map(sequence_to_i64).transpose()?;
    let limit =
        i64::try_from(limit).map_err(|_| delivery_rejected("delivery read limit is too large"))?;
    let mut statement = connection
        .prepare(
            "SELECT sequence, entry_json FROM normalized_logs
             WHERE (?1 IS NULL OR sequence > ?1)
             ORDER BY sequence ASC LIMIT ?2",
        )
        .map_err(delivery_unavailable("prepare delivery read"))?;
    let rows = statement
        .query_map(params![cursor, limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(delivery_unavailable("read delivery rows"))?;
    rows.map(|row| {
        let (sequence, encoded) = row.map_err(delivery_unavailable("decode delivery row"))?;
        let sequence = i64_to_sequence(sequence)?;
        let entry = serde_json::from_str::<IngestLogEntry>(&encoded).map_err(|error| {
            LogDeliveryStoreError::Unavailable {
                message: format!("stored normalized log could not be decoded: {error}"),
            }
        })?;
        Ok(SequencedLogEntry { sequence, entry })
    })
    .collect()
}

pub(crate) fn load_cursor(
    connection: &Connection,
    sink_id: &LogSinkId,
) -> Result<Option<LogSequence>, LogDeliveryStoreError> {
    connection
        .query_row(
            "SELECT last_sequence FROM sink_cursors WHERE sink_id = ?1",
            params![sink_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(delivery_unavailable("load sink cursor"))?
        .map(i64_to_sequence)
        .transpose()
}

pub(crate) fn commit_cursor(
    connection: &mut Connection,
    sink_id: &LogSinkId,
    sequence: LogSequence,
) -> Result<(), LogDeliveryStoreError> {
    let sequence = sequence_to_i64(sequence)?;
    let transaction = connection
        .transaction()
        .map_err(delivery_unavailable("begin cursor transaction"))?;
    let exists = transaction
        .query_row(
            "SELECT 1 FROM normalized_logs WHERE sequence = ?1",
            params![sequence],
            |_| Ok(()),
        )
        .optional()
        .map_err(delivery_unavailable("validate sink cursor"))?
        .is_some();
    if !exists {
        return Err(delivery_rejected(
            "sink cursor does not identify a stored log",
        ));
    }
    let current = transaction
        .query_row(
            "SELECT last_sequence FROM sink_cursors WHERE sink_id = ?1",
            params![sink_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(delivery_unavailable("read current sink cursor"))?;
    if current.is_some_and(|current| sequence < current) {
        return Err(delivery_rejected("sink cursor cannot regress"));
    }
    transaction
        .execute(
            "INSERT INTO sink_cursors (sink_id, last_sequence) VALUES (?1, ?2)
             ON CONFLICT (sink_id) DO UPDATE SET last_sequence = excluded.last_sequence",
            params![sink_id.as_str(), sequence],
        )
        .map_err(delivery_unavailable("write sink cursor"))?;
    transaction
        .commit()
        .map_err(delivery_unavailable("commit sink cursor transaction"))
}

pub(crate) fn record_dead_letter(
    connection: &mut Connection,
    dead_letter: &SinkDeadLetter,
) -> Result<(), DeadLetterStoreError> {
    validate_dead_letter(dead_letter)?;
    let sequence = dead_sequence_to_i64(dead_letter.source_sequence)?;
    let status_code = dead_letter.status_code.map(i64::from);
    let transaction = connection
        .transaction()
        .map_err(dead_unavailable("begin dead-letter transaction"))?;
    let existing = transaction
        .query_row(
            "SELECT payload
             FROM sink_dead_letters WHERE sink_id = ?1 AND source_sequence = ?2",
            params![dead_letter.sink_id.as_str(), sequence],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(dead_unavailable("read dead-letter identity"))?;
    match existing {
        Some(payload) if payload == dead_letter.payload => {
            return Ok(());
        }
        Some(_) => {
            return Err(dead_rejected(
                "dead-letter identity was reused with different content",
            ));
        }
        None => {}
    }
    let count = transaction
        .query_row("SELECT COUNT(*) FROM sink_dead_letters", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(dead_unavailable("count retained dead letters"))?;
    if u64::try_from(count).unwrap_or(u64::MAX) >= MAX_RETAINED_DEAD_LETTERS {
        return Err(dead_rejected("dead-letter row limit has been reached"));
    }
    transaction
        .execute(
            "INSERT INTO sink_dead_letters
             (sink_id, source_sequence, status_code, reason, payload, recorded_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                dead_letter.sink_id.as_str(),
                sequence,
                status_code,
                dead_letter.reason,
                dead_letter.payload,
                dead_letter.recorded_at.0
            ],
        )
        .map_err(dead_unavailable("insert dead letter"))?;
    transaction
        .commit()
        .map_err(dead_unavailable("commit dead-letter transaction"))
}

pub(crate) fn list_dead_letters(
    connection: &Connection,
    sink_id: &LogSinkId,
    after: Option<LogSequence>,
    limit: usize,
) -> Result<Vec<SinkDeadLetter>, DeadLetterStoreError> {
    if limit == 0 {
        return Err(dead_rejected("dead-letter list limit must be non-zero"));
    }
    let limit =
        i64::try_from(limit).map_err(|_| dead_rejected("dead-letter list limit is too large"))?;
    let after = after.map(dead_sequence_to_i64).transpose()?;
    let mut statement = connection
        .prepare(
            "SELECT source_sequence, status_code, reason, payload, recorded_at_ms
             FROM sink_dead_letters
             WHERE sink_id = ?1 AND (?2 IS NULL OR source_sequence > ?2)
             ORDER BY source_sequence ASC LIMIT ?3",
        )
        .map_err(dead_unavailable("prepare dead-letter list"))?;
    let rows = statement
        .query_map(params![sink_id.as_str(), after, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(dead_unavailable("list dead letters"))?;
    rows.map(|row| {
        let (sequence, status, reason, payload, recorded_at) =
            row.map_err(dead_unavailable("decode dead-letter row"))?;
        Ok(SinkDeadLetter {
            sink_id: sink_id.clone(),
            source_sequence: dead_i64_to_sequence(sequence)?,
            status_code: status
                .map(|status| {
                    u16::try_from(status)
                        .map_err(|_| dead_unavailable_message("stored status code is invalid"))
                })
                .transpose()?,
            reason,
            payload,
            recorded_at: kernel_api::Timestamp(recorded_at),
        })
    })
    .collect()
}

pub(crate) fn dead_letter_stats(
    connection: &Connection,
    sink_id: &LogSinkId,
) -> Result<SinkDeadLetterStats, DeadLetterStoreError> {
    let (count, bytes) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(octet_length(payload)), 0)
             FROM sink_dead_letters WHERE sink_id = ?1",
            params![sink_id.as_str()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(dead_unavailable("read dead-letter stats"))?;
    Ok(SinkDeadLetterStats {
        count: u64::try_from(count)
            .map_err(|_| dead_unavailable_message("stored dead-letter count is invalid"))?,
        payload_bytes: u64::try_from(bytes)
            .map_err(|_| dead_unavailable_message("stored payload byte count is invalid"))?,
    })
}

pub(crate) fn purge_dead_letters(
    connection: &Connection,
    sink_id: &LogSinkId,
    through: Option<LogSequence>,
) -> Result<u64, DeadLetterStoreError> {
    let deleted = match through {
        Some(through) => connection.execute(
            "DELETE FROM sink_dead_letters WHERE sink_id = ?1 AND source_sequence <= ?2",
            params![sink_id.as_str(), dead_sequence_to_i64(through)?],
        ),
        None => connection.execute(
            "DELETE FROM sink_dead_letters WHERE sink_id = ?1",
            params![sink_id.as_str()],
        ),
    }
    .map_err(dead_unavailable("purge dead letters"))?;
    Ok(u64::try_from(deleted).unwrap_or(u64::MAX))
}

pub(crate) fn stats_snapshot(
    connection: &Connection,
    path: &Path,
    sink_ids: &[LogSinkId],
) -> Result<LogSpoolStats, LogStatsStoreError> {
    let (row_count, high_watermark) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(MAX(sequence), 0) FROM normalized_logs",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(stats_unavailable("read spool totals"))?;
    let oldest_entry_at_ms = connection
        .query_row(
            "SELECT event_at_ms FROM normalized_logs ORDER BY sequence ASC LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(stats_unavailable("read oldest retained log"))?;
    let high_watermark = i64_to_stats_sequence(high_watermark)?;
    let mut sinks = Vec::new();
    for sink_id in sink_ids.iter().cloned().collect::<BTreeSet<_>>() {
        let cursor = load_stats_cursor(connection, &sink_id)?;
        let cursor_value = cursor.map_or(0, |sequence| sequence.0);
        let pending_entries = high_watermark.0.saturating_sub(cursor_value);
        let cursor_value = i64::try_from(cursor_value)
            .map_err(|_| stats_rejected("sink cursor exceeds durable range"))?;
        let oldest_pending_at_ms = connection
            .query_row(
                "SELECT event_at_ms FROM normalized_logs
                 WHERE sequence > ?1 ORDER BY sequence ASC LIMIT 1",
                params![cursor_value],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(stats_unavailable("read oldest pending log"))?;
        sinks.push(LogSinkCursorStats {
            sink_id,
            cursor,
            pending_entries,
            oldest_pending_at_ms,
        });
    }
    let (dead_count, dead_bytes) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(octet_length(payload)), 0)
             FROM sink_dead_letters",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(stats_unavailable("read dead-letter totals"))?;
    let latest = connection
        .query_row(
            "SELECT status_code, reason, recorded_at_ms
             FROM sink_dead_letters
             ORDER BY recorded_at_ms DESC, sink_id DESC, source_sequence DESC LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(stats_unavailable("read latest dead letter"))?;
    let (latest_status, latest_error, latest_at_ms) = match latest {
        Some((status, error, recorded_at)) => (
            status
                .map(|status| {
                    u16::try_from(status)
                        .map_err(|_| stats_unavailable_message("stored status code is invalid"))
                })
                .transpose()?,
            Some(error),
            Some(recorded_at),
        ),
        None => (None, None, None),
    };
    Ok(LogSpoolStats {
        row_count: u64::try_from(row_count)
            .map_err(|_| stats_unavailable_message("stored log row count is invalid"))?,
        high_watermark,
        oldest_entry_at_ms,
        database_bytes: database_file_set_bytes(path),
        sinks,
        dead_letters: SinkDeadLetterSnapshot {
            count: u64::try_from(dead_count)
                .map_err(|_| stats_unavailable_message("stored dead-letter count is invalid"))?,
            payload_bytes: u64::try_from(dead_bytes).map_err(|_| {
                stats_unavailable_message("stored dead-letter byte count is invalid")
            })?,
            latest_at_ms,
            latest_status,
            latest_error,
        },
    })
}

fn load_stats_cursor(
    connection: &Connection,
    sink_id: &LogSinkId,
) -> Result<Option<LogSequence>, LogStatsStoreError> {
    connection
        .query_row(
            "SELECT last_sequence FROM sink_cursors WHERE sink_id = ?1",
            params![sink_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(stats_unavailable("read stats sink cursor"))?
        .map(i64_to_stats_sequence)
        .transpose()
}

fn database_file_set_bytes(path: &Path) -> u64 {
    let mut wal = path.as_os_str().to_owned();
    wal.push(".wal");
    [path.to_path_buf(), wal.into()]
        .into_iter()
        .filter_map(|candidate| std::fs::metadata(candidate).ok())
        .fold(0_u64, |total, metadata| {
            total.saturating_add(metadata.len())
        })
}

fn validate_dead_letter(dead_letter: &SinkDeadLetter) -> Result<(), DeadLetterStoreError> {
    if dead_letter.reason.len() > MAX_DEAD_LETTER_REASON_BYTES {
        return Err(dead_rejected("dead-letter reason exceeds 4096 bytes"));
    }
    if dead_letter.payload.len() > MAX_DEAD_LETTER_PAYLOAD_BYTES {
        return Err(dead_rejected("dead-letter payload exceeds 5000000 bytes"));
    }
    Ok(())
}

fn sequence_to_i64(sequence: LogSequence) -> Result<i64, LogDeliveryStoreError> {
    i64::try_from(sequence.0).map_err(|_| delivery_rejected("log sequence exceeds durable range"))
}

fn i64_to_sequence(sequence: i64) -> Result<LogSequence, LogDeliveryStoreError> {
    u64::try_from(sequence)
        .map(LogSequence)
        .map_err(|_| delivery_unavailable_message("stored log sequence is negative"))
}

fn dead_sequence_to_i64(sequence: LogSequence) -> Result<i64, DeadLetterStoreError> {
    i64::try_from(sequence.0).map_err(|_| dead_rejected("log sequence exceeds durable range"))
}

fn dead_i64_to_sequence(sequence: i64) -> Result<LogSequence, DeadLetterStoreError> {
    u64::try_from(sequence)
        .map(LogSequence)
        .map_err(|_| dead_unavailable_message("stored dead-letter sequence is negative"))
}

fn i64_to_stats_sequence(sequence: i64) -> Result<LogSequence, LogStatsStoreError> {
    u64::try_from(sequence)
        .map(LogSequence)
        .map_err(|_| stats_unavailable_message("stored log sequence is negative"))
}

fn delivery_rejected(message: &str) -> LogDeliveryStoreError {
    LogDeliveryStoreError::Rejected {
        message: message.to_owned(),
    }
}

fn delivery_unavailable(
    action: &'static str,
) -> impl FnOnce(duckdb::Error) -> LogDeliveryStoreError {
    move |error| LogDeliveryStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}

fn delivery_unavailable_message(message: &str) -> LogDeliveryStoreError {
    LogDeliveryStoreError::Unavailable {
        message: message.to_owned(),
    }
}

fn stats_rejected(message: &str) -> LogStatsStoreError {
    LogStatsStoreError::Rejected {
        message: message.to_owned(),
    }
}

fn stats_unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> LogStatsStoreError {
    move |error| LogStatsStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}

fn stats_unavailable_message(message: &str) -> LogStatsStoreError {
    LogStatsStoreError::Unavailable {
        message: message.to_owned(),
    }
}

fn dead_rejected(message: &str) -> DeadLetterStoreError {
    DeadLetterStoreError::Rejected {
        message: message.to_owned(),
    }
}

fn dead_unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> DeadLetterStoreError {
    move |error| DeadLetterStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}

fn dead_unavailable_message(message: &str) -> DeadLetterStoreError {
    DeadLetterStoreError::Unavailable {
        message: message.to_owned(),
    }
}
