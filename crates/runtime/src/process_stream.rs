use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId};

use crate::{
    EventCursor, EventRequest, RuntimeClock, RuntimeError, RuntimeEvent, RuntimeEventKind,
    RuntimeEventStream, WorkloadMetadata,
};

const EVENT_CAPACITY: usize = 4_096;

static NEXT_JOURNAL: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub(crate) struct ProcessEventJournal {
    inner: Arc<Mutex<EventJournalState>>,
}

impl ProcessEventJournal {
    pub(crate) fn new() -> Self {
        let generation = NEXT_JOURNAL.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::new(Mutex::new(EventJournalState {
                epoch: format!("{}-{generation}", std::process::id()),
                sequence: 0,
                records: VecDeque::new(),
            })),
        }
    }

    pub(crate) fn emit(
        &self,
        metadata: &WorkloadMetadata,
        kind: RuntimeEventKind,
        exit_code: Option<i32>,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        state.sequence = state.sequence.saturating_add(1);
        let cursor = EventCursor::new(format!("{}:{}", state.epoch, state.sequence));
        if state.records.len() == EVENT_CAPACITY {
            state.records.pop_front();
        }
        state.records.push_back(EventRecord {
            cluster_id: metadata.cluster_id.clone(),
            node_id: metadata.node_id.clone(),
            event: RuntimeEvent {
                cursor,
                workload_id: metadata.workload_id.clone(),
                kind,
                exit_code,
            },
        });
        Ok(())
    }

    pub(crate) fn stream(
        &self,
        request: EventRequest,
        clock: Arc<dyn RuntimeClock>,
        poll_interval: Duration,
    ) -> Result<Box<dyn RuntimeEventStream>, RuntimeError> {
        let state = self.lock()?;
        let after = parse_event_cursor(request.after.as_ref(), &state.epoch)?;
        drop(state);
        Ok(Box::new(ProcessEventStream {
            journal: self.clone(),
            cluster_id: request.cluster_id,
            node_id: request.node_id,
            after,
            clock,
            poll_interval,
        }))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, EventJournalState>, RuntimeError> {
        self.inner.lock().map_err(|_| RuntimeError::Unavailable {
            message: "process event journal lock was poisoned".to_owned(),
        })
    }
}

struct EventJournalState {
    epoch: String,
    sequence: u64,
    records: VecDeque<EventRecord>,
}

struct EventRecord {
    cluster_id: ClusterId,
    node_id: NodeId,
    event: RuntimeEvent,
}

struct ProcessEventStream {
    journal: ProcessEventJournal,
    cluster_id: ClusterId,
    node_id: NodeId,
    after: u64,
    clock: Arc<dyn RuntimeClock>,
    poll_interval: Duration,
}

#[async_trait]
impl RuntimeEventStream for ProcessEventStream {
    async fn next(&mut self) -> Result<Option<RuntimeEvent>, RuntimeError> {
        loop {
            let event = {
                let state = self.journal.lock()?;
                state
                    .records
                    .iter()
                    .find(|record| {
                        record.cluster_id == self.cluster_id
                            && record.node_id == self.node_id
                            && event_sequence(&record.event.cursor, &state.epoch)
                                .is_ok_and(|sequence| sequence > self.after)
                    })
                    .map(|record| record.event.clone())
            };
            if let Some(event) = event {
                self.after = event_sequence(&event.cursor, &self.journal.lock()?.epoch)?;
                return Ok(Some(event));
            }
            let deadline = self.clock.now().saturating_add(self.poll_interval);
            self.clock.sleep_until(deadline).await;
        }
    }
}

fn parse_event_cursor(cursor: Option<&EventCursor>, epoch: &str) -> Result<u64, RuntimeError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let Some((observed_epoch, sequence)) = cursor.as_str().split_once(':') else {
        return Err(RuntimeError::Stream {
            message: format!("process event cursor `{}` is invalid", cursor.as_str()),
        });
    };
    if observed_epoch == epoch {
        parse_sequence(sequence, "event", cursor.as_str())
    } else {
        Ok(0)
    }
}

fn event_sequence(cursor: &EventCursor, epoch: &str) -> Result<u64, RuntimeError> {
    parse_event_cursor(Some(cursor), epoch)
}

fn parse_sequence(value: &str, kind: &str, cursor: &str) -> Result<u64, RuntimeError> {
    value.parse().map_err(|_| RuntimeError::Stream {
        message: format!("process {kind} cursor `{cursor}` is invalid"),
    })
}
