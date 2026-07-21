use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp};

use crate::{
    DeadLetterStore, InMemoryDeadLetterStore, InMemoryLogDeliveryStore, IngestLogEntry, LogBody,
    LogDeliveryStore, LogOrigin, LogProducer, LogRecordId, LogSequence, LogSink, LogSinkError,
    LogSinkId, LogSinkOutcome, LogStream, OriginCursor, RecordingLogSink, SequencedLogEntry,
    SinkDeadLetter, SinkSleeper, SinkWorker, SinkWorkerError, SinkWorkerSettings,
};

#[test]
fn sink_ids_and_worker_bounds_fail_closed() {
    assert!(LogSinkId::new("").is_err());
    assert!(LogSinkId::new("unsafe/sink").is_err());
    assert!(LogSinkId::new("a".repeat(129)).is_err());

    let settings = SinkWorkerSettings {
        batch_size: 0,
        max_batches_per_run: 1,
        max_attempts: 1,
        initial_retry_delay: Duration::from_millis(1),
        max_retry_delay: Duration::from_millis(1),
    };
    assert!(settings.validate().is_err());
}

#[tokio::test]
async fn worker_drains_bounded_batches_and_advances_only_complete_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogDeliveryStore::new(entries(5)?)?);
    let sink = Arc::new(RecordingLogSink::new(sink_id()?, []));
    let worker = worker(
        store.clone(),
        sink.clone(),
        Arc::new(RecordingSleeper::default()),
    )?;

    let first = worker.drain_once().await?;
    assert_eq!(first.batches, 2);
    assert_eq!(first.handled_entries, 4);
    assert_eq!(first.cursor, Some(LogSequence(4)));
    assert_eq!(store.cursor(sink.id())?, Some(LogSequence(4)));

    let second = worker.drain_once().await?;
    assert_eq!(second.handled_entries, 1);
    assert_eq!(second.cursor, Some(LogSequence(5)));
    assert_eq!(sink.attempts()?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn worker_retries_with_injected_exponential_delays() -> Result<(), Box<dyn std::error::Error>>
{
    let store = Arc::new(InMemoryLogDeliveryStore::new(entries(1)?)?);
    let sink = Arc::new(RecordingLogSink::new(
        sink_id()?,
        [
            Err(unavailable_sink()),
            Err(unavailable_sink()),
            Ok(LogSinkOutcome::default()),
        ],
    ));
    let sleeper = Arc::new(RecordingSleeper::default());
    let worker = worker(store, sink.clone(), sleeper.clone())?;

    let report = worker.drain_once().await?;
    assert_eq!(report.retries, 2);
    assert_eq!(sink.attempts()?.len(), 3);
    assert_eq!(
        sleeper.delays()?,
        vec![Duration::from_millis(5), Duration::from_millis(10)]
    );
    Ok(())
}

#[tokio::test]
async fn exhausted_delivery_never_advances_the_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogDeliveryStore::new(entries(1)?)?);
    let sink = Arc::new(RecordingLogSink::new(
        sink_id()?,
        [
            Err(unavailable_sink()),
            Err(unavailable_sink()),
            Err(unavailable_sink()),
        ],
    ));
    let worker = worker(
        store.clone(),
        sink.clone(),
        Arc::new(RecordingSleeper::default()),
    )?;

    assert!(matches!(
        worker.drain_once().await,
        Err(SinkWorkerError::Sink { attempts: 3, .. })
    ));
    assert_eq!(store.cursor(sink.id())?, None);
    Ok(())
}

#[tokio::test]
async fn cursor_commit_failure_deliberately_replays_the_accepted_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogDeliveryStore::new(entries(1)?)?);
    store.fail_next_commit();
    let sink = Arc::new(RecordingLogSink::new(sink_id()?, []));
    let worker = worker(
        store.clone(),
        sink.clone(),
        Arc::new(RecordingSleeper::default()),
    )?;

    assert!(matches!(
        worker.drain_once().await,
        Err(SinkWorkerError::Store {
            action: "commit sink cursor",
            ..
        })
    ));
    assert_eq!(store.cursor(sink.id())?, None);
    worker.drain_once().await?;
    assert_eq!(sink.attempts()?, vec![vec![LogSequence(1)]; 2]);
    Ok(())
}

#[tokio::test]
async fn inconsistent_sink_outcome_never_advances_the_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogDeliveryStore::new(entries(1)?)?);
    let sink = Arc::new(RecordingLogSink::new(
        sink_id()?,
        [Ok(LogSinkOutcome {
            filtered_entries: 1,
            quarantined_entries: 1,
        })],
    ));
    let worker = worker(
        store.clone(),
        sink.clone(),
        Arc::new(RecordingSleeper::default()),
    )?;

    assert!(matches!(
        worker.drain_once().await,
        Err(SinkWorkerError::InvalidBatch { .. })
    ));
    assert_eq!(store.cursor(sink.id())?, None);
    Ok(())
}

#[tokio::test]
async fn dead_letters_are_idempotent_collision_safe_and_explicitly_purgeable()
-> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryDeadLetterStore::default();
    let first = dead_letter(1, b"first");
    let second = dead_letter(2, b"second");
    store.record(&first).await?;
    store.record(&first).await?;
    store.record(&second).await?;

    assert_eq!(store.stats(&sink_id()?).await?.count, 2);
    assert_eq!(store.stats(&sink_id()?).await?.payload_bytes, 11);
    assert_eq!(store.list(&sink_id()?, 1).await?, vec![first.clone()]);

    let mut collision = first;
    collision.reason = "different".to_owned();
    assert!(store.record(&collision).await.is_err());
    assert_eq!(store.purge(&sink_id()?, Some(LogSequence(1))).await?, 1);
    assert_eq!(store.purge(&sink_id()?, None).await?, 1);
    Ok(())
}

#[derive(Default)]
struct RecordingSleeper {
    delays: Mutex<Vec<Duration>>,
}

impl RecordingSleeper {
    fn delays(&self) -> Result<Vec<Duration>, LogSinkError> {
        self.delays
            .lock()
            .map(|delays| delays.clone())
            .map_err(|_| unavailable_sink())
    }
}

#[async_trait]
impl SinkSleeper for RecordingSleeper {
    async fn sleep(&self, duration: Duration) {
        if let Ok(mut delays) = self.delays.lock() {
            delays.push(duration);
        }
    }
}

fn worker(
    store: Arc<dyn LogDeliveryStore>,
    sink: Arc<RecordingLogSink>,
    sleeper: Arc<dyn SinkSleeper>,
) -> Result<SinkWorker, crate::SinkWorkerSettingsError> {
    SinkWorker::new(
        store,
        sink,
        sleeper,
        SinkWorkerSettings {
            batch_size: 2,
            max_batches_per_run: 2,
            max_attempts: 3,
            initial_retry_delay: Duration::from_millis(5),
            max_retry_delay: Duration::from_millis(20),
        },
    )
}

fn entries(count: u64) -> Result<Vec<SequencedLogEntry>, kernel_api::InvalidIdentifier> {
    (1..=count).map(entry).collect()
}

fn entry(sequence: u64) -> Result<SequencedLogEntry, kernel_api::InvalidIdentifier> {
    let cluster_id = ClusterId::new("cluster-1")?;
    let node_id = NodeId::new("node-1")?;
    Ok(SequencedLogEntry {
        sequence: LogSequence(sequence),
        entry: IngestLogEntry {
            id: LogRecordId {
                node_id: node_id.clone(),
                producer: LogProducer::System("daemon".to_owned()),
                cursor: OriginCursor::new(format!("cursor-{sequence}")),
            },
            observed_at: Timestamp(i64::try_from(sequence).unwrap_or(i64::MAX)),
            event_at: Timestamp(i64::try_from(sequence).unwrap_or(i64::MAX)),
            severity: "info".to_owned(),
            stream: LogStream::System,
            origin: LogOrigin::System {
                cluster_id,
                node_id: Some(node_id),
                component: "daemon".to_owned(),
            },
            body: LogBody::Text(format!("record-{sequence}")),
            attributes: BTreeMap::new(),
        },
    })
}

fn dead_letter(sequence: u64, payload: &[u8]) -> SinkDeadLetter {
    SinkDeadLetter {
        sink_id: sink_id().unwrap(),
        source_sequence: LogSequence(sequence),
        status_code: Some(400),
        reason: "rejected".to_owned(),
        payload: payload.to_vec(),
        recorded_at: Timestamp(1),
    }
}

fn sink_id() -> Result<LogSinkId, crate::LogSinkIdError> {
    LogSinkId::new("datadog")
}

fn unavailable_sink() -> LogSinkError {
    LogSinkError::Unavailable {
        message: "injected destination failure".to_owned(),
    }
}
