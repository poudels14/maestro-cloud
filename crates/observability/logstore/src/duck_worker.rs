use std::path::Path;

use logs::{
    DeadLetterStoreError, LogDeliveryStoreError, LogStatsStoreError, OtlpEnvelopeStoreError,
};
use tokio::sync::{mpsc, oneshot};

use crate::duck::Command;
use crate::{
    LogArchiveError, LogBackupError, LogRetentionError, delivery_schema, log_archive,
    log_backup_schema, log_backup_stats_schema, log_retention, schema,
};

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
            Command::AppendMigration { entries, response } => {
                let _ignored = response.send(schema::append_migration(&mut connection, &entries));
            }
            Command::AppendOtlpEnvelopes {
                envelopes,
                response,
            } => {
                let _ignored = response.send(crate::otlp_envelope_schema::append(
                    &mut connection,
                    &envelopes,
                ));
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
            Command::Rollover {
                before,
                sink_ids,
                response,
            } => {
                let _ignored = response.send(log_archive::rollover_before(
                    &mut connection,
                    cold_root,
                    before,
                    &sink_ids,
                ));
            }
            Command::PendingBackups { response } => {
                let _ignored = response.send(log_backup_schema::pending_partitions(
                    &connection,
                    cold_root,
                ));
            }
            Command::MarkBackedUp {
                partition,
                updated_at,
                response,
            } => {
                let _ignored = response.send(log_backup_schema::mark_backed_up(
                    &mut connection,
                    &partition,
                    updated_at.0,
                ));
            }
            Command::LoadBackupStats { response } => {
                let _ignored = response.send(log_backup_stats_schema::load(&connection));
            }
            Command::SaveBackupStats {
                stats,
                updated_at,
                response,
            } => {
                let _ignored = response.send(log_backup_stats_schema::save(
                    &connection,
                    &stats,
                    updated_at.0,
                ));
            }
            Command::PruneBackedUp { cutoff, response } => {
                let _ignored = response.send(log_retention::prune_backed_up_before(
                    &mut connection,
                    cold_root,
                    cutoff,
                ));
            }
            Command::QueryLogs { query, response } => {
                let _ignored = response.send(crate::log_query_schema::query_logs(
                    &connection,
                    cold_root,
                    &query,
                ));
            }
            Command::QueryHistogram { query, response } => {
                let _ignored = response.send(crate::log_query_schema::query_histogram(
                    &connection,
                    cold_root,
                    &query,
                ));
            }
            Command::AppendStatsMetrics { points, response } => {
                let _ignored =
                    response.send(crate::stats_metric_schema::append(&mut connection, &points));
            }
            Command::QueryStatsMetrics { query, response } => {
                let _ignored =
                    response.send(crate::stats_metric_schema::query(&connection, &query));
            }
            Command::QueryIngressTraffic { query, response } => {
                let _ignored = response.send(crate::traffic_query_schema::query_ingress_traffic(
                    &connection,
                    cold_root,
                    &query,
                ));
            }
            Command::QueryServiceTraffic { query, response } => {
                let _ignored = response.send(crate::traffic_query_schema::query_service_traffic(
                    &connection,
                    cold_root,
                    &query,
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

pub(crate) fn backup_worker_stopped(action: &'static str) -> LogBackupError {
    LogBackupError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}

pub(crate) fn retention_worker_stopped(action: &'static str) -> LogRetentionError {
    LogRetentionError::Unavailable {
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

pub(crate) fn otlp_worker_stopped(action: &'static str) -> OtlpEnvelopeStoreError {
    OtlpEnvelopeStoreError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}

pub(crate) fn query_worker_stopped(action: &'static str) -> logs::LogQueryStoreError {
    logs::LogQueryStoreError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}
