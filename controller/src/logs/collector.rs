use std::sync::Arc;
use std::time::Duration;

use super::store::{LogEntry, LogOrigin, LogStore};

const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct LogConfig {
    pub sender: flume::Sender<LogEntry>,
    pub tags: Vec<String>,
    pub origin: LogOrigin,
}

impl LogConfig {
    pub fn build_tags(&self) -> Arc<serde_json::Value> {
        let arr: Vec<serde_json::Value> = self
            .tags
            .iter()
            .map(|t| serde_json::Value::String(t.clone()))
            .collect();
        Arc::new(serde_json::Value::Array(arr))
    }
}

pub struct LogCollector {
    rx: flume::Receiver<LogEntry>,
    store: Arc<LogStore>,
}

impl LogCollector {
    pub fn new(store: Arc<LogStore>) -> (Self, flume::Sender<LogEntry>) {
        let (tx, rx) = flume::bounded(10_000);
        (Self { rx, store }, tx)
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self) {
        let mut batch: Vec<LogEntry> = Vec::with_capacity(256);
        let mut flush_interval = tokio::time::interval(FLUSH_INTERVAL);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut cleanup_interval = tokio::time::interval(CLEANUP_INTERVAL);
        cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                result = self.rx.recv_async() => {
                    match result {
                        Ok(entry) => {
                            batch.push(entry);
                            if batch.len() >= 256 {
                                let _ = self.store.append(&batch).await;
                                batch.clear();
                            }
                        }
                        Err(_) => {
                            if !batch.is_empty() {
                                let _ = self.store.append(&batch).await;
                            }
                            break;
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    if !batch.is_empty() {
                        let _ = self.store.append(&batch).await;
                        batch.clear();
                    }
                }
                _ = cleanup_interval.tick() => {
                    self.cleanup_flushed_logs().await;
                }
            }
        }
    }

    async fn cleanup_flushed_logs(&self) {
        let min_cursor = match self.store.min_sink_cursor().await {
            Ok(Some(seq)) => seq,
            _ => return,
        };
        if min_cursor > 0 {
            if let Err(err) = self.store.delete_before(min_cursor).await {
                eprintln!("[maestro]: log cleanup error: {err}");
            }
        }
    }
}
