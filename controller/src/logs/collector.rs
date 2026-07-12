use std::sync::Arc;
use std::time::Duration;

use super::store::{LogEntry, LogOrigin, LogStore};

const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const APPEND_RETRY_INTERVAL: Duration = Duration::from_secs(1);

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
                                self.flush(&mut batch).await;
                            }
                        }
                        Err(_) => {
                            if !batch.is_empty() {
                                self.flush(&mut batch).await;
                            }
                            break;
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    if !batch.is_empty() {
                        self.flush(&mut batch).await;
                    }
                }
                _ = cleanup_interval.tick() => {
                    // The controller SQLite spool fans out to every configured
                    // sink. Reclaim only the prefix acknowledged by the slowest
                    // registered sink (probe, Datadog, and future consumers).
                    match self.store.min_sink_cursor().await {
                        Ok(Some(cursor)) if cursor > 0 => {
                            if let Err(err) = self.store.delete_before(cursor).await {
                                eprintln!(
                                    "[maestro]: log spool retention delete error (retrying): {err}"
                                );
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!(
                                "[maestro]: log spool retention cursor error (retrying): {err}"
                            );
                        }
                    }
                }
            }
        }
    }

    async fn flush(&self, batch: &mut Vec<LogEntry>) {
        while !batch.is_empty() {
            match self.store.append(batch).await {
                Ok(()) => {
                    batch.clear();
                    return;
                }
                Err(err) => {
                    eprintln!("[maestro]: log append error (retrying): {err}");
                    tokio::time::sleep(APPEND_RETRY_INTERVAL).await;
                }
            }
        }
    }
}
