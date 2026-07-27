use std::future::{self, Future};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use kernel_store::{Clock, MonotonicTime};
use tokio::sync::Notify;

use crate::RoleError;
use crate::role_tasks::{shutdown_role_tasks, wait_for_role_task};

#[tokio::test]
async fn role_failure_wait_collects_only_the_finished_worker() {
    let pending = tokio::spawn(async {
        future::pending::<()>().await;
        Ok(())
    });
    let failed = tokio::spawn(async { Err(RoleError::new("worker stopped")) });
    let mut tasks = vec![pending, failed];

    let failure = wait_for_role_task(&mut tasks).await;

    assert_eq!(failure.detail(), "worker stopped");
    assert_eq!(tasks.len(), 1);
    if let Some(task) = tasks.pop() {
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn canceled_role_failure_wait_keeps_the_worker_owned() {
    let task = tokio::spawn(async {
        future::pending::<()>().await;
        Ok(())
    });
    let mut tasks = vec![task];
    let mut wait = Box::pin(wait_for_role_task(&mut tasks));
    future::poll_fn(|context| {
        assert!(wait.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(wait);

    assert_eq!(tasks.len(), 1);
    if let Some(task) = tasks.pop() {
        assert!(!task.is_finished());
        task.abort();
        assert!(task.await.is_err_and(|error| error.is_cancelled()));
    }
}

#[tokio::test]
async fn role_task_shutdown_aborts_and_reaps_workers_at_the_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let dropped = Arc::new(AtomicBool::new(false));
    let task_dropped = dropped.clone();
    let task = tokio::spawn(async move {
        let _drop_signal = DropSignal(task_dropped);
        future::pending::<()>().await;
        Ok(())
    });
    let clock = Arc::new(ManualClock::default());
    let shutdown_clock = clock.clone();
    let shutdown = tokio::spawn(async move {
        let mut tasks = vec![task];
        let failures =
            shutdown_role_tasks(&mut tasks, shutdown_clock.as_ref(), Duration::from_secs(5)).await;
        (tasks, failures)
    });
    tokio::task::yield_now().await;

    clock.advance(Duration::from_secs(5));
    let (tasks, failures) = tokio::time::timeout(Duration::from_secs(1), shutdown).await??;

    assert!(tasks.is_empty());
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(failures.len(), 1);
    assert!(
        failures
            .first()
            .is_some_and(|failure| failure.contains("aborted 1 remaining task"))
    );
    Ok(())
}

#[tokio::test]
async fn role_task_shutdown_preserves_worker_failures() {
    let task = tokio::spawn(async { Err(RoleError::new("worker rejected shutdown")) });
    let mut tasks = vec![task];

    let failures =
        shutdown_role_tasks(&mut tasks, &ManualClock::default(), Duration::from_secs(5)).await;

    assert!(tasks.is_empty());
    assert_eq!(failures, vec!["worker rejected shutdown"]);
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct ManualClock {
    millis: AtomicU64,
    advanced: Notify,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.millis.fetch_add(millis, Ordering::SeqCst);
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(self.millis.load(Ordering::SeqCst)))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        while self.now() < deadline {
            self.advanced.notified().await;
        }
    }
}
