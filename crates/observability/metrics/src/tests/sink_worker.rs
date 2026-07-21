use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::{
    InMemoryMetricStore, MetricSequence, MetricSinkError, MetricSinkId, MetricSinkSleeper,
    MetricSinkWorker, MetricSinkWorkerError, MetricSinkWorkerSettings, MetricStore,
    RecordingMetricSink,
};

#[tokio::test]
async fn metric_worker_retries_drains_bounded_batches_and_commits_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryMetricStore::new());
    store.append(&points()?).await?;
    let sink = Arc::new(RecordingMetricSink::new(
        MetricSinkId::new("datadog")?,
        [Err(MetricSinkError::Unavailable {
            message: "injected outage".to_owned(),
        })],
    ));
    let sleeper = Arc::new(RecordingSleeper::default());
    let worker = MetricSinkWorker::new(store, sink.clone(), sleeper.clone(), settings())?;

    let report = worker.drain_once().await?;

    assert_eq!(report.batches, 2);
    assert_eq!(report.handled_points, 3);
    assert_eq!(report.retries, 1);
    assert_eq!(report.cursor, Some(MetricSequence(3)));
    assert_eq!(
        sink.attempts()?,
        vec![
            vec![MetricSequence(1), MetricSequence(2)],
            vec![MetricSequence(1), MetricSequence(2)],
            vec![MetricSequence(3)],
        ]
    );
    assert_eq!(sleeper.delays()?, vec![Duration::from_millis(10)]);
    Ok(())
}

#[tokio::test]
async fn metric_worker_replays_an_accepted_batch_after_cursor_commit_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryMetricStore::new());
    store.append(&points()?).await?;
    store.fail_next_cursor_commit();
    let sink = Arc::new(RecordingMetricSink::new(MetricSinkId::new("datadog")?, []));
    let worker = MetricSinkWorker::new(
        store,
        sink.clone(),
        Arc::new(RecordingSleeper::default()),
        settings(),
    )?;

    assert!(matches!(
        worker.drain_once().await,
        Err(MetricSinkWorkerError::Store { .. })
    ));
    let recovered = worker.drain_once().await?;

    assert_eq!(recovered.cursor, Some(MetricSequence(3)));
    assert_eq!(
        sink.attempts()?,
        vec![
            vec![MetricSequence(1), MetricSequence(2)],
            vec![MetricSequence(1), MetricSequence(2)],
            vec![MetricSequence(3)],
        ]
    );
    Ok(())
}

#[tokio::test]
async fn metric_worker_exhaustion_never_advances_the_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryMetricStore::new());
    store.append(&points()?).await?;
    let sink = Arc::new(RecordingMetricSink::new(
        MetricSinkId::new("datadog")?,
        (0..2).map(|_| {
            Err(MetricSinkError::Unavailable {
                message: "still unavailable".to_owned(),
            })
        }),
    ));
    let worker = MetricSinkWorker::new(
        store,
        sink,
        Arc::new(RecordingSleeper::default()),
        settings(),
    )?;

    assert!(matches!(
        worker.drain_once().await,
        Err(MetricSinkWorkerError::Sink { attempts: 2, .. })
    ));
    Ok(())
}

#[test]
fn metric_worker_rejects_zero_or_inverted_bounds() {
    let invalid = MetricSinkWorkerSettings {
        batch_size: 0,
        ..settings()
    };
    assert!(invalid.validate().is_err());
    let invalid = MetricSinkWorkerSettings {
        initial_retry_delay: Duration::from_secs(2),
        max_retry_delay: Duration::from_secs(1),
        ..settings()
    };
    assert!(invalid.validate().is_err());
}

fn points() -> Result<Vec<crate::WorkloadMetricPoint>, kernel_api::InvalidIdentifier> {
    Ok(vec![
        crate::conformance::metric_point("workload-1", 1, 10)?,
        crate::conformance::metric_point("workload-2", 2, 20)?,
        crate::conformance::metric_point("workload-1", 3, 30)?,
    ])
}

fn settings() -> MetricSinkWorkerSettings {
    MetricSinkWorkerSettings {
        batch_size: 2,
        max_batches_per_run: 2,
        max_attempts: 2,
        initial_retry_delay: Duration::from_millis(10),
        max_retry_delay: Duration::from_millis(20),
        poll_interval: Duration::from_secs(1),
    }
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
            .map_err(|_| MetricSinkError::Unavailable {
                message: "recording metric sleeper lock was poisoned".to_owned(),
            })
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
