use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::{IngestLogEntry, LogAppendReport, LogRecordId, LogStore, LogStoreError};

/// Deterministic idempotent log store for pipeline and composition tests.
#[derive(Default)]
pub struct InMemoryLogStore {
    entries: Mutex<BTreeMap<LogRecordId, IngestLogEntry>>,
}

impl InMemoryLogStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all committed entries in stable record-id order.
    pub fn entries(&self) -> Result<Vec<IngestLogEntry>, LogStoreError> {
        self.entries
            .lock()
            .map(|entries| entries.values().cloned().collect())
            .map_err(|_| LogStoreError::Unavailable {
                message: "in-memory log store lock was poisoned".to_owned(),
            })
    }
}

#[async_trait]
impl LogStore for InMemoryLogStore {
    async fn append(&self, entries: &[IngestLogEntry]) -> Result<LogAppendReport, LogStoreError> {
        let mut committed = self
            .entries
            .lock()
            .map_err(|_| LogStoreError::Unavailable {
                message: "in-memory log store lock was poisoned".to_owned(),
            })?;
        let mut pending = BTreeMap::<LogRecordId, IngestLogEntry>::new();
        let mut deduplicated = 0_usize;
        for entry in entries {
            let existing = pending.get(&entry.id).or_else(|| committed.get(&entry.id));
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
                }
            }
        }
        let committed_count = pending.len();
        committed.extend(pending);
        Ok(LogAppendReport {
            committed: committed_count,
            deduplicated,
        })
    }
}
