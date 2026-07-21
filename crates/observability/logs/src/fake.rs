use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use crate::{
    DeadLetterStore, DeadLetterStoreError, IngestLogEntry, LogAppendReport, LogDeliveryStore,
    LogDeliveryStoreError, LogRecordId, LogSequence, LogSinkId, LogStore, LogStoreError,
    LogStoreRuntime, LogStoreRuntimeError, SequencedLogEntry, SinkDeadLetter, SinkDeadLetterStats,
};

/// Deterministic idempotent log store for pipeline and composition tests.
#[derive(Default)]
pub struct InMemoryLogStore {
    state: Mutex<InMemoryLogState>,
}

#[derive(Default)]
struct InMemoryLogState {
    entries: BTreeMap<LogRecordId, SequencedLogEntry>,
    last_sequence: u64,
    cursors: BTreeMap<LogSinkId, LogSequence>,
    dead_letters: BTreeMap<(LogSinkId, LogSequence), SinkDeadLetter>,
}

impl InMemoryLogStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all committed entries in stable record-id order.
    pub fn entries(&self) -> Result<Vec<IngestLogEntry>, LogStoreError> {
        Ok(lock_state_for_append(&self.state)?
            .entries
            .values()
            .map(|entry| entry.entry.clone())
            .collect())
    }
}

#[async_trait]
impl LogStore for InMemoryLogStore {
    async fn append(&self, entries: &[IngestLogEntry]) -> Result<LogAppendReport, LogStoreError> {
        let mut state = lock_state_for_append(&self.state)?;
        let mut pending = BTreeMap::<LogRecordId, IngestLogEntry>::new();
        let mut pending_order = Vec::new();
        let mut deduplicated = 0_usize;
        for entry in entries {
            let existing = pending
                .get(&entry.id)
                .or_else(|| state.entries.get(&entry.id).map(|stored| &stored.entry));
            match existing {
                Some(existing) if existing == entry => {
                    deduplicated = deduplicated.saturating_add(1);
                }
                Some(_) => {
                    return Err(LogStoreError::Rejected {
                        message: format!(
                            "record identity `{:?}/{}` was reused with different content",
                            entry.id.producer,
                            entry.id.cursor.as_str()
                        ),
                    });
                }
                None => {
                    pending.insert(entry.id.clone(), entry.clone());
                    pending_order.push(entry.id.clone());
                }
            }
        }
        let committed_count = pending.len();
        let additional = u64::try_from(committed_count).map_err(|_| LogStoreError::Rejected {
            message: "append contains too many normalized logs".to_owned(),
        })?;
        state
            .last_sequence
            .checked_add(additional)
            .ok_or_else(|| LogStoreError::Rejected {
                message: "normalized log sequence space is exhausted".to_owned(),
            })?;
        for id in pending_order {
            let entry = pending
                .remove(&id)
                .ok_or_else(|| LogStoreError::Unavailable {
                    message: "in-memory append lost a validated pending record".to_owned(),
                })?;
            state.last_sequence = state.last_sequence.saturating_add(1);
            let sequence = LogSequence(state.last_sequence);
            state
                .entries
                .insert(id, SequencedLogEntry { sequence, entry });
        }
        Ok(LogAppendReport {
            committed: committed_count,
            deduplicated,
        })
    }
}

#[async_trait]
impl LogDeliveryStore for InMemoryLogStore {
    async fn read_after(
        &self,
        cursor: Option<LogSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedLogEntry>, LogDeliveryStoreError> {
        if limit == 0 {
            return Err(LogDeliveryStoreError::Rejected {
                message: "delivery read limit must be non-zero".to_owned(),
            });
        }
        let mut entries = lock_state_for_delivery(&self.state)?
            .entries
            .values()
            .filter(|entry| cursor.is_none_or(|cursor| entry.sequence > cursor))
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.sequence);
        entries.truncate(limit);
        Ok(entries)
    }

    async fn load_sink_cursor(
        &self,
        sink_id: &LogSinkId,
    ) -> Result<Option<LogSequence>, LogDeliveryStoreError> {
        Ok(lock_state_for_delivery(&self.state)?
            .cursors
            .get(sink_id)
            .copied())
    }

    async fn commit_sink_cursor(
        &self,
        sink_id: &LogSinkId,
        sequence: LogSequence,
    ) -> Result<(), LogDeliveryStoreError> {
        let mut state = lock_state_for_delivery(&self.state)?;
        if !state
            .entries
            .values()
            .any(|entry| entry.sequence == sequence)
        {
            return Err(LogDeliveryStoreError::Rejected {
                message: "sink cursor does not identify a stored log".to_owned(),
            });
        }
        if state
            .cursors
            .get(sink_id)
            .is_some_and(|cursor| sequence < *cursor)
        {
            return Err(LogDeliveryStoreError::Rejected {
                message: "sink cursor cannot regress".to_owned(),
            });
        }
        state.cursors.insert(sink_id.clone(), sequence);
        Ok(())
    }
}

#[async_trait]
impl DeadLetterStore for InMemoryLogStore {
    async fn record(&self, dead_letter: &SinkDeadLetter) -> Result<(), DeadLetterStoreError> {
        let mut state = lock_state_for_dead_letters(&self.state)?;
        let key = (dead_letter.sink_id.clone(), dead_letter.source_sequence);
        match state.dead_letters.get(&key) {
            Some(existing) if existing.payload == dead_letter.payload => Ok(()),
            Some(_) => Err(DeadLetterStoreError::Rejected {
                message: "dead-letter identity was reused with different content".to_owned(),
            }),
            None => {
                state.dead_letters.insert(key, dead_letter.clone());
                Ok(())
            }
        }
    }

    async fn list(
        &self,
        sink_id: &LogSinkId,
        limit: usize,
    ) -> Result<Vec<SinkDeadLetter>, DeadLetterStoreError> {
        if limit == 0 {
            return Err(DeadLetterStoreError::Rejected {
                message: "dead-letter list limit must be non-zero".to_owned(),
            });
        }
        Ok(lock_state_for_dead_letters(&self.state)?
            .dead_letters
            .iter()
            .filter(|((candidate, _), _)| candidate == sink_id)
            .take(limit)
            .map(|(_, dead_letter)| dead_letter.clone())
            .collect())
    }

    async fn stats(
        &self,
        sink_id: &LogSinkId,
    ) -> Result<SinkDeadLetterStats, DeadLetterStoreError> {
        let state = lock_state_for_dead_letters(&self.state)?;
        let mut stats = SinkDeadLetterStats::default();
        for dead_letter in state
            .dead_letters
            .iter()
            .filter(|((candidate, _), _)| candidate == sink_id)
            .map(|(_, dead_letter)| dead_letter)
        {
            stats.count = stats.count.saturating_add(1);
            stats.payload_bytes = stats
                .payload_bytes
                .saturating_add(u64::try_from(dead_letter.payload.len()).unwrap_or(u64::MAX));
        }
        Ok(stats)
    }

    async fn purge(
        &self,
        sink_id: &LogSinkId,
        through: Option<LogSequence>,
    ) -> Result<u64, DeadLetterStoreError> {
        let mut state = lock_state_for_dead_letters(&self.state)?;
        let before = state.dead_letters.len();
        state.dead_letters.retain(|(candidate, sequence), _| {
            candidate != sink_id || through.is_some_and(|through| *sequence > through)
        });
        Ok(u64::try_from(before.saturating_sub(state.dead_letters.len())).unwrap_or(u64::MAX))
    }
}

/// No-op lifecycle owner for an in-memory log store used by composition tests.
pub struct InMemoryLogStoreRuntime {
    store: Arc<InMemoryLogStore>,
}

impl InMemoryLogStoreRuntime {
    /// Creates an empty in-memory runtime.
    pub fn new() -> Self {
        Self {
            store: Arc::new(InMemoryLogStore::new()),
        }
    }

    /// Returns a typed handle for inspecting committed entries.
    pub fn store_handle(&self) -> Arc<InMemoryLogStore> {
        self.store.clone()
    }
}

impl Default for InMemoryLogStoreRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LogStoreRuntime for InMemoryLogStoreRuntime {
    fn store(&self) -> Arc<dyn LogStore> {
        self.store.clone()
    }

    fn delivery_store(&self) -> Arc<dyn LogDeliveryStore> {
        self.store.clone()
    }

    fn dead_letter_store(&self) -> Arc<dyn DeadLetterStore> {
        self.store.clone()
    }

    async fn shutdown(self: Box<Self>) -> Result<(), LogStoreRuntimeError> {
        Ok(())
    }
}

fn lock_state_for_append(
    state: &Mutex<InMemoryLogState>,
) -> Result<MutexGuard<'_, InMemoryLogState>, LogStoreError> {
    state.lock().map_err(|_| LogStoreError::Unavailable {
        message: "in-memory log store lock was poisoned".to_owned(),
    })
}

fn lock_state_for_delivery(
    state: &Mutex<InMemoryLogState>,
) -> Result<MutexGuard<'_, InMemoryLogState>, LogDeliveryStoreError> {
    state
        .lock()
        .map_err(|_| LogDeliveryStoreError::Unavailable {
            message: "in-memory log delivery lock was poisoned".to_owned(),
        })
}

fn lock_state_for_dead_letters(
    state: &Mutex<InMemoryLogState>,
) -> Result<MutexGuard<'_, InMemoryLogState>, DeadLetterStoreError> {
    state.lock().map_err(|_| DeadLetterStoreError::Unavailable {
        message: "in-memory dead-letter lock was poisoned".to_owned(),
    })
}
