use std::path::Path;

use logs::{DeadLetterStoreError, LogDeliveryStoreError, LogStatsStoreError};
use tokio::sync::{mpsc, oneshot};

use crate::duck::Command;
use crate::{LogArchiveError, delivery_schema, log_archive, schema};

pub(crate) fn run_worker(
    path: &Path,
    cold_root: &Path,
    mut commands: mpsc::Receiver<Command>,
    initialized: oneshot::Sender<Result<(), String>>,
) {
    if let Err(error) = log_archive::prepare(cold_root) {
        let _ignored = initialized.send(Err(error.to_string()));
        return;
    }
    let mut connection = match schema::open(path) {
        Ok(connection) => {
            if initialized.send(Ok(())).is_err() {
                return;
            }
            connection
        }
        Err(error) => {
            let _ignored = initialized.send(Err(error));
            return;
        }
    };
    while let Some(command) = commands.blocking_recv() {
        match command {
            Command::Append { entries, response } => {
                let _ignored = response.send(schema::append(&mut connection, &entries));
            }
            Command::ReadAfter {
                cursor,
                limit,
                response,
            } => {
                let _ignored =
                    response.send(delivery_schema::read_after(&connection, cursor, limit));
            }
            Command::LoadCursor { sink_id, response } => {
                let _ignored = response.send(delivery_schema::load_cursor(&connection, &sink_id));
            }
            Command::CommitCursor {
                sink_id,
                sequence,
                response,
            } => {
                let _ignored = response.send(delivery_schema::commit_cursor(
                    &mut connection,
                    &sink_id,
                    sequence,
                ));
            }
            Command::RecordDeadLetter {
                dead_letter,
                response,
            } => {
                let _ignored = response.send(delivery_schema::record_dead_letter(
                    &mut connection,
                    &dead_letter,
                ));
            }
            Command::ListDeadLetters {
                sink_id,
                after,
                limit,
                response,
            } => {
                let _ignored = response.send(delivery_schema::list_dead_letters(
                    &connection,
                    &sink_id,
                    after,
                    limit,
                ));
            }
            Command::DeadLetterStats { sink_id, response } => {
                let _ignored =
                    response.send(delivery_schema::dead_letter_stats(&connection, &sink_id));
            }
            Command::PurgeDeadLetters {
                sink_id,
                through,
                response,
            } => {
                let _ignored = response.send(delivery_schema::purge_dead_letters(
                    &connection,
                    &sink_id,
                    through,
                ));
            }
            Command::StatsSnapshot { sink_ids, response } => {
                let _ignored = response.send(delivery_schema::stats_snapshot(
                    &connection,
                    path,
                    &sink_ids,
                ));
            }
            Command::Rollover { before, response } => {
                let _ignored = response.send(log_archive::rollover_before(
                    &mut connection,
                    cold_root,
                    before,
                ));
            }
            Command::Shutdown { response } => {
                drop(connection);
                let _ignored = response.send(());
                return;
            }
        }
    }
}

pub(crate) fn archive_worker_stopped(action: &'static str) -> LogArchiveError {
    LogArchiveError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}

pub(crate) fn delivery_worker_stopped(action: &'static str) -> LogDeliveryStoreError {
    LogDeliveryStoreError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}

pub(crate) fn dead_worker_stopped(action: &'static str) -> DeadLetterStoreError {
    DeadLetterStoreError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}

pub(crate) fn stats_worker_stopped(action: &'static str) -> LogStatsStoreError {
    LogStatsStoreError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}
