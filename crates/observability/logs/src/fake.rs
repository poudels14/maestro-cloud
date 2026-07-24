use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use self::telemetry::StatsMetricIdentity;
use crate::{
    DeadLetterStore, DeadLetterStoreError, IngestLogEntry, LogAppendReport, LogDeliveryStore,
    LogDeliveryStoreError, LogQueryStore, LogQueryStoreError, LogRecordId, LogSequence,
    LogSinkCursorStats, LogSinkId, LogSpoolStats, LogStatsStore, LogStatsStoreError, LogStore,
    LogStoreError, LogStoreRuntime, LogStoreRuntimeError, SequencedLogEntry, SinkDeadLetter,
    SinkDeadLetterSnapshot, SinkDeadLetterStats, StatsMetricPoint, StatsMetricStore,
    TrafficQueryStore,
};

mod telemetry;

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
    stats_metrics: BTreeMap<StatsMetricIdentity, StatsMetricPoint>,
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

    pub(crate) fn query_entries(&self) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
        self.state
            .lock()
            .map_err(|_| LogQueryStoreError::Unavailable {
                message: "in-memory log query lock was poisoned".to_owned(),
            })
            .map(|state| state.entries.values().cloned().collect())
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
        after: Option<LogSequence>,
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
            .filter(|((candidate, sequence), _)| {
                candidate == sink_id && after.is_none_or(|after| *sequence > after)
            })
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

#[async_trait]
impl LogStatsStore for InMemoryLogStore {
    async fn stats_snapshot(
        &self,
        sink_ids: &[LogSinkId],
    ) -> Result<LogSpoolStats, LogStatsStoreError> {
        let state = self
            .state
            .lock()
            .map_err(|_| LogStatsStoreError::Unavailable {
                message: "in-memory log stats lock was poisoned".to_owned(),
            })?;
        let mut entries = state.entries.values().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.sequence);
        let mut requested = sink_ids.to_vec();
        requested.sort();
        requested.dedup();
        let sinks = requested
            .into_iter()
            .map(|sink_id| {
                let cursor = state.cursors.get(&sink_id).copied();
                let mut pending = entries
                    .iter()
                    .filter(|entry| cursor.is_none_or(|cursor| entry.sequence > cursor));
                let oldest_pending_at_ms = pending.next().map(|entry| entry.entry.event_at.0);
                let pending_entries = u64::try_from(pending.count())
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::from(oldest_pending_at_ms.is_some()));
                LogSinkCursorStats {
                    sink_id,
                    cursor,
                    pending_entries,
                    oldest_pending_at_ms,
                }
            })
            .collect();
        let latest = state.dead_letters.values().max_by_key(|dead_letter| {
            (
                dead_letter.recorded_at.0,
                &dead_letter.sink_id,
                dead_letter.source_sequence,
            )
        });
        let dead_letters = SinkDeadLetterSnapshot {
            count: u64::try_from(state.dead_letters.len()).unwrap_or(u64::MAX),
            payload_bytes: state
                .dead_letters
                .values()
                .fold(0_u64, |total, dead_letter| {
                    total.saturating_add(
                        u64::try_from(dead_letter.payload.len()).unwrap_or(u64::MAX),
                    )
                }),
            latest_at_ms: latest.map(|dead_letter| dead_letter.recorded_at.0),
            latest_status: latest.and_then(|dead_letter| dead_letter.status_code),
            latest_error: latest.map(|dead_letter| dead_letter.reason.clone()),
        };
        Ok(LogSpoolStats {
            row_count: u64::try_from(entries.len()).unwrap_or(u64::MAX),
            high_watermark: LogSequence(state.last_sequence),
            oldest_entry_at_ms: entries.first().map(|entry| entry.entry.event_at.0),
            database_bytes: 0,
            sinks,
            dead_letters,
        })
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

    fn stats_store(&self) -> Arc<dyn LogStatsStore> {
        self.store.clone()
    }

    fn query_store(&self) -> Arc<dyn LogQueryStore> {
        self.store.clone()
    }

    fn stats_metric_store(&self) -> Arc<dyn StatsMetricStore> {
        self.store.clone()
    }

    fn traffic_query_store(&self) -> Arc<dyn TrafficQueryStore> {
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
