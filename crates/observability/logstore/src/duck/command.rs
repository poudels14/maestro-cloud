use kernel_api::Timestamp;
use logs::{
    DeadLetterStoreError, IngestLogEntry, IngressTrafficBreakdown, IngressTrafficQuery,
    LogAppendReport, LogDeliveryStoreError, LogHistogramBucket, LogHistogramQuery,
    LogQueryStoreError, LogReadQuery, LogSequence, LogSinkId, LogSpoolStats, LogStatsStoreError,
    LogStoreError, OtlpEnvelope, OtlpEnvelopeAppendReport, OtlpEnvelopeStoreError,
    SequencedLogEntry, ServiceTrafficQuery, SinkDeadLetter, SinkDeadLetterStats,
    StatsMetricAppendReport, StatsMetricPoint, StatsMetricQuery, StatsMetricStoreError,
    TrafficMetricPoint, TrafficQueryError,
};
use tokio::sync::oneshot;

use crate::log_backup_schema::PendingLogBackupPartition;
use crate::{
    LogArchiveError, LogBackupError, LogRetentionError, LogRetentionReport, LogRolloverReport,
};

pub(crate) enum Command {
    Append {
        entries: Vec<IngestLogEntry>,
        response: oneshot::Sender<Result<LogAppendReport, LogStoreError>>,
    },
    AppendOtlpEnvelopes {
        envelopes: Vec<OtlpEnvelope>,
        response: oneshot::Sender<Result<OtlpEnvelopeAppendReport, OtlpEnvelopeStoreError>>,
    },
    ReadAfter {
        cursor: Option<LogSequence>,
        limit: usize,
        response: oneshot::Sender<Result<Vec<SequencedLogEntry>, LogDeliveryStoreError>>,
    },
    LoadCursor {
        sink_id: LogSinkId,
        response: oneshot::Sender<Result<Option<LogSequence>, LogDeliveryStoreError>>,
    },
    CommitCursor {
        sink_id: LogSinkId,
        sequence: LogSequence,
        response: oneshot::Sender<Result<(), LogDeliveryStoreError>>,
    },
    RecordDeadLetter {
        dead_letter: SinkDeadLetter,
        response: oneshot::Sender<Result<(), DeadLetterStoreError>>,
    },
    ListDeadLetters {
        sink_id: LogSinkId,
        after: Option<LogSequence>,
        limit: usize,
        response: oneshot::Sender<Result<Vec<SinkDeadLetter>, DeadLetterStoreError>>,
    },
    DeadLetterStats {
        sink_id: LogSinkId,
        response: oneshot::Sender<Result<SinkDeadLetterStats, DeadLetterStoreError>>,
    },
    PurgeDeadLetters {
        sink_id: LogSinkId,
        through: Option<LogSequence>,
        response: oneshot::Sender<Result<u64, DeadLetterStoreError>>,
    },
    StatsSnapshot {
        sink_ids: Vec<LogSinkId>,
        response: oneshot::Sender<Result<LogSpoolStats, LogStatsStoreError>>,
    },
    Rollover {
        before: Timestamp,
        response: oneshot::Sender<Result<LogRolloverReport, LogArchiveError>>,
    },
    PendingBackups {
        response: oneshot::Sender<Result<Vec<PendingLogBackupPartition>, LogBackupError>>,
    },
    MarkBackedUp {
        partition: PendingLogBackupPartition,
        updated_at: Timestamp,
        response: oneshot::Sender<Result<(), LogBackupError>>,
    },
    LoadBackupStats {
        response: oneshot::Sender<Result<Option<logs::BackupStatsSnapshot>, LogBackupError>>,
    },
    SaveBackupStats {
        stats: logs::BackupStatsSnapshot,
        updated_at: Timestamp,
        response: oneshot::Sender<Result<(), LogBackupError>>,
    },
    PruneBackedUp {
        cutoff: chrono::NaiveDate,
        response: oneshot::Sender<Result<LogRetentionReport, LogRetentionError>>,
    },
    QueryLogs {
        query: LogReadQuery,
        response: oneshot::Sender<Result<Vec<SequencedLogEntry>, LogQueryStoreError>>,
    },
    QueryHistogram {
        query: LogHistogramQuery,
        response: oneshot::Sender<Result<Vec<LogHistogramBucket>, LogQueryStoreError>>,
    },
    AppendStatsMetrics {
        points: Vec<StatsMetricPoint>,
        response: oneshot::Sender<Result<StatsMetricAppendReport, StatsMetricStoreError>>,
    },
    QueryStatsMetrics {
        query: StatsMetricQuery,
        response: oneshot::Sender<Result<Vec<StatsMetricPoint>, StatsMetricStoreError>>,
    },
    QueryIngressTraffic {
        query: IngressTrafficQuery,
        response: oneshot::Sender<Result<IngressTrafficBreakdown, TrafficQueryError>>,
    },
    QueryServiceTraffic {
        query: ServiceTrafficQuery,
        response: oneshot::Sender<Result<Vec<TrafficMetricPoint>, TrafficQueryError>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}
