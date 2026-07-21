use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp};
use tokio::sync::Semaphore;

use crate::{
    DeadLetterStore, InMemoryDeadLetterStore, InMemoryLogDeliveryStore, IngestLogEntry, LogBody,
    LogDeliveryStore, LogOrigin, LogProducer, LogRecordId, LogSequence, LogSink, LogSinkError,
    LogSinkId, LogSinkOutcome, LogStream, OriginCursor, RecordingLogSink, SequencedLogEntry,
    SinkDeadLetter, SinkRuntimeRegistry, SinkSleeper, SinkWorker, SinkWorkerError,
    SinkWorkerSettings,
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
        poll_interval: Duration::from_millis(1),
    };
    assert!(settings.validate().is_err());
}

#[tokio::test]
async fn in_memory_delivery_and_dead_letter_stores_pass_shared_conformance()
-> Result<(), Box<dyn std::error::Error>> {
    let entries = entries(3)?;
    let delivery = InMemoryLogDeliveryStore::new(entries.clone())?;
    crate::conformance::check_log_delivery_store(&delivery, &entries).await?;
    crate::conformance::check_dead_letter_store(&InMemoryDeadLetterStore::default()).await?;
    Ok(())
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
async fn continuous_worker_recovers_on_a_later_poll_and_stops_cleanly()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogDeliveryStore::new(entries(1)?)?);
    let sink = Arc::new(RecordingLogSink::new(
        sink_id()?,
        [Err(unavailable_sink()), Ok(LogSinkOutcome::default())],
    ));
    let sleeper = Arc::new(ControlledSleeper::default());
    let worker = SinkWorker::new(
        store.clone(),
        sink.clone(),
        sleeper.clone(),
        SinkWorkerSettings {
            batch_size: 1,
            max_batches_per_run: 1,
            max_attempts: 1,
            initial_retry_delay: Duration::from_millis(1),
            max_retry_delay: Duration::from_millis(1),
            poll_interval: Duration::from_millis(5),
        },
    )?;
    let (shutdown, shutdown_receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move { worker.run(shutdown_receiver).await });

    sleeper.wait_for_sleeps(1).await?;
    assert_eq!(store.cursor(sink.id())?, None);
    sleeper.release();
    wait_for_attempts(&sink, 2).await?;
    assert_eq!(store.cursor(sink.id())?, Some(LogSequence(1)));

    shutdown.send(true)?;
    task.await?;
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
async fn runtime_health_claims_progress_only_after_cursor_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogDeliveryStore::new(entries(1)?)?);
    store.fail_next_commit();
    let outcome = LogSinkOutcome {
        filtered_entries: 1,
        quarantined_entries: 0,
    };
    let sink = Arc::new(RecordingLogSink::new(
        sink_id()?,
        [Ok(outcome), Ok(outcome)],
    ));
    let runtime = SinkRuntimeRegistry::default();
    let worker = worker(store, sink.clone(), Arc::new(RecordingSleeper::default()))?
        .with_runtime_registry(runtime.clone());

    assert!(worker.drain_once().await.is_err());
    let failed = runtime.snapshot(sink.id());
    assert_eq!(failed.consecutive_failures, 1);
    assert_eq!(failed.last_success_at_ms, None);
    assert_eq!(failed.last_cursor_advance_at_ms, None);
    assert_eq!(failed.filtered_entries, 0);

    worker.drain_once().await?;
    let recovered = runtime.snapshot(sink.id());
    assert_eq!(recovered.consecutive_failures, 0);
    assert!(recovered.last_success_at_ms.is_some());
    assert!(recovered.last_cursor_advance_at_ms.is_some());
    assert_eq!(recovered.filtered_entries, 1);
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

    let metadata = first.metadata();
    assert_eq!(metadata.payload_bytes, 5);
    assert_eq!(
        metadata.payload_sha256,
        "a7937b64b8caa58f03721bb6bacf5c78cb235febe0e70b1b84cd99541461a08e"
    );

    assert_eq!(store.stats(&sink_id()?).await?.count, 2);
    assert_eq!(store.stats(&sink_id()?).await?.payload_bytes, 11);
    assert_eq!(store.list(&sink_id()?, None, 1).await?, vec![first.clone()]);

    let mut collision = first;
    collision.payload = b"different".to_vec();
    assert!(store.record(&collision).await.is_err());
    assert_eq!(store.purge(&sink_id()?, Some(LogSequence(1))).await?, 1);
    assert_eq!(store.purge(&sink_id()?, None).await?, 1);
    Ok(())
}

#[derive(Default)]
struct RecordingSleeper {
    delays: Mutex<Vec<Duration>>,
}

struct ControlledSleeper {
    sleeps: std::sync::atomic::AtomicUsize,
    permits: Semaphore,
}

impl Default for ControlledSleeper {
    fn default() -> Self {
        Self {
            sleeps: std::sync::atomic::AtomicUsize::new(0),
            permits: Semaphore::new(0),
        }
    }
}

impl ControlledSleeper {
    async fn wait_for_sleeps(&self, expected: usize) -> Result<(), &'static str> {
        for _ in 0..10_000 {
            if self.sleeps.load(std::sync::atomic::Ordering::SeqCst) >= expected {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err("sink worker did not enter its poll wait")
    }

    fn release(&self) {
        self.permits.add_permits(1);
    }
}

#[async_trait]
impl SinkSleeper for ControlledSleeper {
    async fn sleep(&self, _duration: Duration) {
        self.sleeps
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Ok(permit) = self.permits.acquire().await {
            permit.forget();
        }
    }
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
            poll_interval: Duration::from_millis(25),
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

async fn wait_for_attempts(sink: &RecordingLogSink, expected: usize) -> Result<(), LogSinkError> {
    for _ in 0..10_000 {
        if sink.attempts()?.len() >= expected {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(LogSinkError::Unavailable {
        message: "sink worker did not make bounded retry progress".to_owned(),
    })
}
