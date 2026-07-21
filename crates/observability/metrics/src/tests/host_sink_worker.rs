use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::{
    HostMetricDeliveryStore, HostMetricSequence, HostMetricSink, HostMetricSinkWorker,
    HostMetricSinkWorkerError, HostMetricStore, InMemoryHostMetricStore, MetricSinkError,
    MetricSinkId, MetricSinkSleeper, MetricSinkWorkerSettings, RecordingHostMetricSink,
};

#[tokio::test]
async fn host_worker_retries_bounded_batches_and_commits_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryHostMetricStore::new());
    store
        .append_host_metrics(&[
            crate::conformance::host_metric_point("node-1", 1, 10)?,
            crate::conformance::host_metric_point("node-1", 2, 20)?,
            crate::conformance::host_metric_point("node-1", 3, 30)?,
        ])
        .await?;
    let sink = Arc::new(RecordingHostMetricSink::new(
        MetricSinkId::new("datadog")?,
        [Err(unavailable()), Ok(())],
    ));
    let sleeper = Arc::new(RecordingSleeper::default());
    let worker = worker(store.clone(), sink.clone(), sleeper.clone())?;

    let report = worker.drain_once().await?;
    assert_eq!(report.batches, 2);
    assert_eq!(report.handled_points, 3);
    assert_eq!(report.retries, 1);
    assert_eq!(report.cursor, Some(HostMetricSequence(3)));
    assert_eq!(
        sink.attempts()?,
        vec![
            vec![HostMetricSequence(1), HostMetricSequence(2)],
            vec![HostMetricSequence(1), HostMetricSequence(2)],
            vec![HostMetricSequence(3)],
        ]
    );
    assert_eq!(sleeper.delays()?, vec![Duration::from_millis(5)]);
    Ok(())
}

#[tokio::test]
async fn host_worker_replays_remote_acceptance_after_cursor_commit_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryHostMetricStore::new());
    store
        .append_host_metrics(&[crate::conformance::host_metric_point("node-1", 1, 10)?])
        .await?;
    store.fail_next_cursor_commit();
    let sink = Arc::new(RecordingHostMetricSink::new(
        MetricSinkId::new("datadog")?,
        [],
    ));
    let worker = worker(
        store.clone(),
        sink.clone(),
        Arc::new(RecordingSleeper::default()),
    )?;

    assert!(matches!(
        worker.drain_once().await,
        Err(HostMetricSinkWorkerError::Store {
            action: "commit host metric sink cursor",
            ..
        })
    ));
    assert_eq!(store.load_host_metric_sink_cursor(sink.id()).await?, None);
    worker.drain_once().await?;
    assert_eq!(
        sink.attempts()?,
        vec![vec![HostMetricSequence(1)], vec![HostMetricSequence(1)]]
    );
    Ok(())
}

#[tokio::test]
async fn host_worker_does_not_retry_permanently_rejected_batches()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryHostMetricStore::new());
    store
        .append_host_metrics(&[crate::conformance::host_metric_point("node-1", 1, 10)?])
        .await?;
    let sink = Arc::new(RecordingHostMetricSink::new(
        MetricSinkId::new("datadog")?,
        [Err(MetricSinkError::Rejected {
            message: "invalid".to_owned(),
        })],
    ));
    let sleeper = Arc::new(RecordingSleeper::default());
    let worker = worker(store, sink.clone(), sleeper.clone())?;

    assert!(matches!(
        worker.drain_once().await,
        Err(HostMetricSinkWorkerError::Sink { attempts: 1, .. })
    ));
    assert_eq!(sink.attempts()?.len(), 1);
    assert!(sleeper.delays()?.is_empty());
    Ok(())
}

#[derive(Default)]
struct RecordingSleeper {
    delays: Mutex<Vec<Duration>>,
}

impl RecordingSleeper {
    fn delays(&self) -> Result<Vec<Duration>, MetricSinkError> {
        self.delays
            .lock()
            .map(|delays| delays.clone())
            .map_err(|_| unavailable())
    }
}

#[async_trait]
impl MetricSinkSleeper for RecordingSleeper {
    async fn sleep(&self, duration: Duration) {
        if let Ok(mut delays) = self.delays.lock() {
            delays.push(duration);
        }
    }
}

fn worker(
    store: Arc<InMemoryHostMetricStore>,
    sink: Arc<RecordingHostMetricSink>,
    sleeper: Arc<dyn MetricSinkSleeper>,
) -> Result<HostMetricSinkWorker, crate::MetricSinkWorkerSettingsError> {
    HostMetricSinkWorker::new(
        store,
        sink,
        sleeper,
        MetricSinkWorkerSettings {
            batch_size: 2,
            max_batches_per_run: 2,
            max_attempts: 3,
            initial_retry_delay: Duration::from_millis(5),
            max_retry_delay: Duration::from_millis(20),
            poll_interval: Duration::from_millis(25),
        },
    )
}

fn unavailable() -> MetricSinkError {
    MetricSinkError::Unavailable {
        message: "offline".to_owned(),
    }
}
