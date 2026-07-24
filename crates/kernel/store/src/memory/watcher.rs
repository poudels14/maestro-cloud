use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use super::{Inner, expire_sessions};
use crate::{StoreError, StorePrefix, StoreWatch, WatchCursor, WatchEvent, WatchEventKind};

#[derive(Clone)]
pub(super) struct RawEvent {
    pub(super) cursor: WatchCursor,
    pub(super) kind: WatchEventKind,
}

pub(super) struct InMemoryWatch {
    inner: Arc<Inner>,
    changes: watch::Receiver<u64>,
    prefix: StorePrefix,
    last_cursor: WatchCursor,
}

impl InMemoryWatch {
    pub(super) fn new(inner: Arc<Inner>, prefix: StorePrefix, last_cursor: WatchCursor) -> Self {
        let changes = inner.changes.subscribe();
        Self {
            inner,
            changes,
            prefix,
            last_cursor,
        }
    }
}

#[async_trait]
impl StoreWatch for InMemoryWatch {
    async fn next(&mut self) -> Result<WatchEvent, StoreError> {
        loop {
            expire_sessions(&self.inner).await;
            let state = self.inner.state.lock().await;
            if self.last_cursor < state.discarded_through {
                return Err(StoreError::CursorExpired {
                    cursor: self.last_cursor,
                });
            }
            if let Some(event) = state
                .history
                .iter()
                .find(|event| event.cursor > self.last_cursor && event_matches(event, &self.prefix))
            {
                self.last_cursor = event.cursor;
                return Ok(WatchEvent {
                    prefix: self.prefix.clone(),
                    cursor: event.cursor,
                    kind: event.kind.clone(),
                });
            }
            self.last_cursor = WatchCursor::snapshot(state.version);
            let next_expiry = state
                .sessions
                .values()
                .map(|session| session.expires_at)
                .min();
            self.changes.borrow_and_update();
            drop(state);

            if let Some(deadline) = next_expiry {
                tokio::select! {
                    changed = self.changes.changed() => {
                        changed.map_err(|_| StoreError::Contract {
                            message: "in-memory change channel closed while the store is alive"
                                .to_string(),
                        })?;
                    }
                    () = self.inner.clock.sleep_until(deadline) => {}
                }
            } else {
                self.changes
                    .changed()
                    .await
                    .map_err(|_| StoreError::Contract {
                        message: "in-memory change channel closed while the store is alive"
                            .to_string(),
                    })?;
            }
        }
    }
}

fn event_matches(event: &RawEvent, prefix: &StorePrefix) -> bool {
    match &event.kind {
        WatchEventKind::Put(value) => value.key.as_str().starts_with(prefix.as_str()),
        WatchEventKind::Delete { key, .. } => key.as_str().starts_with(prefix.as_str()),
    }
}
