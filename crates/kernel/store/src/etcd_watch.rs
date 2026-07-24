use std::collections::VecDeque;

use async_trait::async_trait;
use etcd_client::{Client, EventType, WatchOptions, WatchStream};

use crate::etcd_support::{revision, revision_i64, stored_value, unavailable, version};
use crate::etcd_value::ValueProtector;
use crate::{
    StoreError, StoreKey, StorePrefix, StoreWatch, WatchCursor, WatchEvent, WatchEventKind,
};

pub(crate) struct EtcdWatch {
    client: Client,
    values: ValueProtector,
    prefix: StorePrefix,
    resume_after: Option<WatchCursor>,
    stream: Option<WatchStream>,
    pending: VecDeque<WatchEvent>,
    active_revision: Option<u64>,
    next_event_index: u32,
}

impl EtcdWatch {
    pub(crate) fn new(
        client: Client,
        values: ValueProtector,
        prefix: StorePrefix,
        resume_after: Option<WatchCursor>,
    ) -> Self {
        Self {
            client,
            values,
            prefix,
            resume_after,
            stream: None,
            pending: VecDeque::new(),
            active_revision: None,
            next_event_index: 0,
        }
    }
}

#[async_trait]
impl StoreWatch for EtcdWatch {
    async fn next(&mut self) -> Result<WatchEvent, StoreError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            if self.stream.is_none() {
                let mut options = WatchOptions::new().with_prefix().with_prev_key();
                if let Some(cursor) = self.resume_after {
                    options = options.with_start_revision(revision_i64(cursor.revision)?);
                }
                let stream = self
                    .client
                    .watch(self.prefix.as_str(), Some(options))
                    .await
                    .map_err(unavailable)?;
                self.stream = Some(stream);
            }
            let response = self
                .stream
                .as_mut()
                .ok_or_else(|| StoreError::Contract {
                    message: "etcd watch stream was not initialized".to_string(),
                })?
                .message()
                .await
                .map_err(unavailable)?
                .ok_or_else(|| StoreError::Unavailable {
                    message: "etcd closed the watch stream".to_string(),
                })?;
            if response.compact_revision() > 0 {
                return Err(StoreError::CursorExpired {
                    cursor: self.resume_after.unwrap_or_default(),
                });
            }
            if response.canceled() {
                return Err(StoreError::Unavailable {
                    message: format!("etcd canceled the watch: {}", response.cancel_reason()),
                });
            }
            for event in response.events() {
                let kv = event.kv().ok_or_else(|| StoreError::Contract {
                    message: "etcd watch event omitted its key/value metadata".to_string(),
                })?;
                let revision = revision(kv.mod_revision())?;
                if self.active_revision != Some(revision) {
                    self.active_revision = Some(revision);
                    self.next_event_index = 0;
                }
                let cursor = WatchCursor::event(revision, self.next_event_index);
                self.next_event_index = self.next_event_index.saturating_add(1);
                if self.resume_after.is_some_and(|resume| cursor <= resume) {
                    continue;
                }
                let kind = match event.event_type() {
                    EventType::Put => WatchEventKind::Put(stored_value(kv, &self.values)?),
                    EventType::Delete => {
                        let previous = event.prev_kv().ok_or_else(|| StoreError::Contract {
                            message: "etcd delete watch event omitted its previous value"
                                .to_string(),
                        })?;
                        WatchEventKind::Delete {
                            key: StoreKey::from_backend(kv.key())?,
                            previous_version: version(previous.mod_revision())?,
                        }
                    }
                };
                self.pending.push_back(WatchEvent {
                    prefix: self.prefix.clone(),
                    cursor,
                    kind,
                });
            }
        }
    }
}
