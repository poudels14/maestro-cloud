use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;

use crate::{
    DeadLetterStore, DeadLetterStoreError, LogDeliveryStore, LogDeliveryStoreError, LogSequence,
    LogSink, LogSinkError, LogSinkId, LogSinkOutcome, SequencedLogEntry, SinkDeadLetter,
    SinkDeadLetterStats,
};

type DeadLetterKey = (LogSinkId, LogSequence);
type DeadLetterRecords = BTreeMap<DeadLetterKey, SinkDeadLetter>;

/// Deterministic in-memory delivery cursor store for conformance and composition tests.
pub struct InMemoryLogDeliveryStore {
    state: Mutex<DeliveryState>,
    fail_next_commit: AtomicBool,
}

struct DeliveryState {
    entries: Vec<SequencedLogEntry>,
    cursors: BTreeMap<LogSinkId, LogSequence>,
}

impl InMemoryLogDeliveryStore {
    /// Creates a fake store from a strictly increasing sequence.
    pub fn new(entries: Vec<SequencedLogEntry>) -> Result<Self, LogDeliveryStoreError> {
        if entries.windows(2).any(|pair| {
            let [left, right] = pair else {
                return false;
            };
            left.sequence >= right.sequence
        }) {
            return Err(LogDeliveryStoreError::Rejected {
                message: "fake delivery entries must be strictly increasing".to_owned(),
            });
        }
        Ok(Self {
            state: Mutex::new(DeliveryState {
                entries,
                cursors: BTreeMap::new(),
            }),
            fail_next_commit: AtomicBool::new(false),
        })
    }

    /// Injects one cursor commit failure without mutating durable progress.
    pub fn fail_next_commit(&self) {
        self.fail_next_commit.store(true, Ordering::SeqCst);
    }

    /// Returns one sink's durable cursor for assertions.
    pub fn cursor(
        &self,
        sink_id: &LogSinkId,
    ) -> Result<Option<LogSequence>, LogDeliveryStoreError> {
        Ok(lock_delivery(&self.state)?.cursors.get(sink_id).copied())
    }
}

#[async_trait]
impl LogDeliveryStore for InMemoryLogDeliveryStore {
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
        let state = lock_delivery(&self.state)?;
        Ok(state
            .entries
            .iter()
            .filter(|entry| cursor.is_none_or(|cursor| entry.sequence > cursor))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn load_sink_cursor(
        &self,
        sink_id: &LogSinkId,
    ) -> Result<Option<LogSequence>, LogDeliveryStoreError> {
        self.cursor(sink_id)
    }

    async fn commit_sink_cursor(
        &self,
        sink_id: &LogSinkId,
        sequence: LogSequence,
    ) -> Result<(), LogDeliveryStoreError> {
        if self.fail_next_commit.swap(false, Ordering::SeqCst) {
            return Err(LogDeliveryStoreError::Unavailable {
                message: "injected cursor commit failure".to_owned(),
            });
        }
        let mut state = lock_delivery(&self.state)?;
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

/// Scriptable sink that records every attempted sequence batch.
pub struct RecordingLogSink {
    id: LogSinkId,
    responses: Mutex<VecDeque<Result<LogSinkOutcome, LogSinkError>>>,
    attempts: Mutex<Vec<Vec<LogSequence>>>,
}

impl RecordingLogSink {
    /// Creates a sink that consumes scripted responses then succeeds by default.
    pub fn new(
        id: LogSinkId,
        responses: impl IntoIterator<Item = Result<LogSinkOutcome, LogSinkError>>,
    ) -> Self {
        Self {
            id,
            responses: Mutex::new(responses.into_iter().collect()),
            attempts: Mutex::new(Vec::new()),
        }
    }

    /// Returns every attempted sequence batch, including retries.
    pub fn attempts(&self) -> Result<Vec<Vec<LogSequence>>, LogSinkError> {
        self.attempts
            .lock()
            .map(|attempts| attempts.clone())
            .map_err(|_| LogSinkError::Unavailable {
                message: "recording sink attempt lock was poisoned".to_owned(),
            })
    }
}

#[async_trait]
impl LogSink for RecordingLogSink {
    fn id(&self) -> &LogSinkId {
        &self.id
    }

    async fn send(&self, entries: &[SequencedLogEntry]) -> Result<LogSinkOutcome, LogSinkError> {
        self.attempts
            .lock()
            .map_err(|_| LogSinkError::Unavailable {
                message: "recording sink attempt lock was poisoned".to_owned(),
            })?
            .push(entries.iter().map(|entry| entry.sequence).collect());
        self.responses
            .lock()
            .map_err(|_| LogSinkError::Unavailable {
                message: "recording sink response lock was poisoned".to_owned(),
            })?
            .pop_front()
            .unwrap_or(Ok(LogSinkOutcome::default()))
    }
}

/// Deterministic in-memory dead-letter administration store.
#[derive(Default)]
pub struct InMemoryDeadLetterStore {
    records: Mutex<DeadLetterRecords>,
}

#[async_trait]
impl DeadLetterStore for InMemoryDeadLetterStore {
    async fn record(&self, dead_letter: &SinkDeadLetter) -> Result<(), DeadLetterStoreError> {
        let mut records = lock_dead_letters(&self.records)?;
        let key = (dead_letter.sink_id.clone(), dead_letter.source_sequence);
        match records.get(&key) {
            Some(existing) if existing.payload == dead_letter.payload => Ok(()),
            Some(_) => Err(DeadLetterStoreError::Rejected {
                message: "dead-letter identity was reused with different content".to_owned(),
            }),
            None => {
                records.insert(key, dead_letter.clone());
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
        Ok(lock_dead_letters(&self.records)?
            .iter()
            .filter(|((candidate, _), _)| candidate == sink_id)
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn stats(
        &self,
        sink_id: &LogSinkId,
    ) -> Result<SinkDeadLetterStats, DeadLetterStoreError> {
        let records = lock_dead_letters(&self.records)?;
        let mut stats = SinkDeadLetterStats::default();
        for record in records
            .iter()
            .filter(|((candidate, _), _)| candidate == sink_id)
            .map(|(_, record)| record)
        {
            stats.count = stats.count.saturating_add(1);
            stats.payload_bytes = stats
                .payload_bytes
                .saturating_add(u64::try_from(record.payload.len()).unwrap_or(u64::MAX));
        }
        Ok(stats)
    }

    async fn purge(
        &self,
        sink_id: &LogSinkId,
        through: Option<LogSequence>,
    ) -> Result<u64, DeadLetterStoreError> {
        let mut records = lock_dead_letters(&self.records)?;
        let before = records.len();
        records.retain(|(candidate, sequence), _| {
            candidate != sink_id || through.is_some_and(|through| *sequence > through)
        });
        Ok(u64::try_from(before.saturating_sub(records.len())).unwrap_or(u64::MAX))
    }
}

fn lock_delivery(
    state: &Mutex<DeliveryState>,
) -> Result<MutexGuard<'_, DeliveryState>, LogDeliveryStoreError> {
    state
        .lock()
        .map_err(|_| LogDeliveryStoreError::Unavailable {
            message: "in-memory delivery store lock was poisoned".to_owned(),
        })
}

fn lock_dead_letters(
    records: &Mutex<DeadLetterRecords>,
) -> Result<MutexGuard<'_, DeadLetterRecords>, DeadLetterStoreError> {
    records
        .lock()
        .map_err(|_| DeadLetterStoreError::Unavailable {
            message: "in-memory dead-letter store lock was poisoned".to_owned(),
        })
}
