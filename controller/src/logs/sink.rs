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
    fn advance_cursor_on_retry_exhaustion(&self) -> bool {
        false
    }
    async fn send(&self, entries: &[LogEntry]) -> Result<()>;
}

pub struct SinkWorker {
    store: Arc<LogStore>,
    sink: Box<dyn LogSink>,
    notify: Arc<Notify>,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
}

impl SinkWorker {
    pub fn new(
        store: Arc<LogStore>,
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
        let mut cursor = self.store.get_sink_cursor(&sink_id).await.unwrap_or(0);

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
                    if !self.sink.advance_cursor_on_retry_exhaustion() {
                        break;
                    }
                    eprintln!(
                        "[maestro]: log sink `{sink_id}` dropping batch through seq {last_seq} after retry exhaustion"
                    );
                }

                self.advance_cursor(&sink_id, &mut cursor, last_seq).await;

                if entries.len() < BATCH_SIZE {
                    break;
                }
            }
        }
    }

    async fn advance_cursor(&self, sink_id: &str, cursor: &mut i64, seq: i64) {
        *cursor = seq;
        if let Err(err) = self.store.set_sink_cursor(sink_id, *cursor).await {
            eprintln!("[maestro]: log sink `{sink_id}` cursor update error: {err}");
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
