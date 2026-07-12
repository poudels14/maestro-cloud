use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{Notify, broadcast};
use tokio::time::sleep;

use super::store::{LogEntry, LogStore};
use crate::signal::ShutdownEvent;

const BATCH_SIZE: usize = 200;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_RETRIES: u32 = 5;
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(500);

#[async_trait]
pub trait LogSink: Send + Sync {
    fn id(&self) -> &str;
    async fn send(&self, entries: &[LogEntry]) -> Result<()>;
}

#[async_trait]
pub trait SinkStore: Send + Sync {
    fn notifier(&self) -> Arc<Notify>;
    async fn read_after(&self, after_seq: i64, limit: usize) -> Result<Vec<LogEntry>>;
    async fn get_sink_cursor(&self, sink_id: &str) -> Result<i64>;
    async fn set_sink_cursor(&self, sink_id: &str, seq: i64) -> Result<()>;
}

#[async_trait]
impl SinkStore for LogStore {
    fn notifier(&self) -> Arc<Notify> {
        self.notifier()
    }
    async fn read_after(&self, after_seq: i64, limit: usize) -> Result<Vec<LogEntry>> {
        self.read_after(after_seq, limit).await
    }
    async fn get_sink_cursor(&self, sink_id: &str) -> Result<i64> {
        self.get_sink_cursor(sink_id).await
    }
    async fn set_sink_cursor(&self, sink_id: &str, seq: i64) -> Result<()> {
        self.set_sink_cursor(sink_id, seq).await
    }
}

pub struct SinkWorker {
    store: Arc<dyn SinkStore>,
    sink: Box<dyn LogSink>,
    notify: Arc<Notify>,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
}

impl SinkWorker {
    pub fn new<T: SinkStore + 'static>(
        store: Arc<T>,
        sink: Box<dyn LogSink>,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
    ) -> Self {
        let notify = store.notifier();
        Self {
            store,
            sink,
            notify,
            signal_rx,
        }
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(mut self) {
        let sink_id = self.sink.id().to_string();
        let Some(mut cursor) = self.load_cursor(&sink_id).await else {
            return;
        };

        loop {
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = sleep(FLUSH_INTERVAL) => {}
                signal = self.signal_rx.recv() => {
                    match signal {
                        Ok(ShutdownEvent::Graceful) | Ok(ShutdownEvent::Force)
                        | Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
            }

            loop {
                let entries = match self.store.read_after(cursor, BATCH_SIZE).await {
                    Ok(entries) => entries,
                    Err(err) => {
                        eprintln!("[maestro]: log sink `{sink_id}` read error: {err}");
                        break;
                    }
                };

                if entries.is_empty() {
                    break;
                }

                let last_seq = entries.last().unwrap().seq;

                if let Err(err) = self.send_with_retry(&entries).await {
                    eprintln!(
                        "[maestro]: log sink `{sink_id}` failed after {MAX_RETRIES} retries: {err}"
                    );
                    break;
                }

                if !self.advance_cursor(&sink_id, &mut cursor, last_seq).await {
                    break;
                }

                if entries.len() < BATCH_SIZE {
                    break;
                }
            }
        }
    }

    async fn load_cursor(&mut self, sink_id: &str) -> Option<i64> {
        loop {
            match self.store.get_sink_cursor(sink_id).await {
                Ok(cursor) => return Some(cursor),
                Err(err) => {
                    eprintln!(
                        "[maestro]: log sink `{sink_id}` cursor read error (retrying): {err}"
                    );
                }
            }

            tokio::select! {
                _ = sleep(FLUSH_INTERVAL) => {}
                signal = self.signal_rx.recv() => {
                    match signal {
                        Ok(ShutdownEvent::Graceful) | Ok(ShutdownEvent::Force)
                        | Err(broadcast::error::RecvError::Closed) => return None,
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
            }
        }
    }

    async fn advance_cursor(&self, sink_id: &str, cursor: &mut i64, seq: i64) -> bool {
        match self.store.set_sink_cursor(sink_id, seq).await {
            Ok(()) => {
                *cursor = seq;
                true
            }
            Err(err) => {
                eprintln!("[maestro]: log sink `{sink_id}` cursor update error: {err}");
                false
            }
        }
    }

    async fn send_with_retry(&self, entries: &[LogEntry]) -> Result<()> {
        let mut delay = INITIAL_RETRY_DELAY;
        for attempt in 0..MAX_RETRIES {
            match self.sink.send(entries).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if attempt + 1 < MAX_RETRIES {
                        sleep(delay).await;
                        delay *= 2;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct MockStore {
        cursor_reads: AtomicUsize,
        fail_cursor_update: bool,
        notify: Arc<Notify>,
    }

    impl MockStore {
        fn new(fail_cursor_update: bool) -> Self {
            Self {
                cursor_reads: AtomicUsize::new(0),
                fail_cursor_update,
                notify: Arc::new(Notify::new()),
            }
        }
    }

    #[async_trait]
    impl SinkStore for MockStore {
        fn notifier(&self) -> Arc<Notify> {
            self.notify.clone()
        }

        async fn read_after(&self, _after_seq: i64, _limit: usize) -> Result<Vec<LogEntry>> {
            Ok(Vec::new())
        }

        async fn get_sink_cursor(&self, _sink_id: &str) -> Result<i64> {
            if self.cursor_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                anyhow::bail!("transient cursor read failure");
            }
            Ok(42)
        }

        async fn set_sink_cursor(&self, _sink_id: &str, _seq: i64) -> Result<()> {
            if self.fail_cursor_update {
                anyhow::bail!("cursor update failure");
            }
            Ok(())
        }
    }

    struct MockSink;

    #[async_trait]
    impl LogSink for MockSink {
        fn id(&self) -> &str {
            "mock"
        }

        async fn send(&self, _entries: &[LogEntry]) -> Result<()> {
            Ok(())
        }
    }

    fn worker(store: Arc<MockStore>, signal_rx: broadcast::Receiver<ShutdownEvent>) -> SinkWorker {
        SinkWorker::new(store, Box::new(MockSink), signal_rx)
    }

    #[tokio::test]
    async fn cursor_read_errors_are_retried_without_defaulting_to_zero() {
        let store = Arc::new(MockStore::new(false));
        let (signal_tx, signal_rx) = broadcast::channel(1);
        signal_tx
            .send(ShutdownEvent::Graceful)
            .expect("first queued signal");
        signal_tx
            .send(ShutdownEvent::Force)
            .expect("second queued signal");
        let mut worker = worker(store.clone(), signal_rx);

        assert_eq!(worker.load_cursor("mock").await, Some(42));
        assert_eq!(store.cursor_reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cursor_update_errors_do_not_advance_the_in_memory_cursor() {
        let store = Arc::new(MockStore::new(true));
        let (_signal_tx, signal_rx) = broadcast::channel(1);
        let worker = worker(store, signal_rx);
        let mut cursor = 7;

        assert!(!worker.advance_cursor("mock", &mut cursor, 9).await);
        assert_eq!(cursor, 7);
    }
}
