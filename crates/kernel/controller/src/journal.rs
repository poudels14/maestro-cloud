use std::sync::{Arc, Mutex};
use std::time::Duration;

use kernel_api::ResourceRevision;
use kernel_store::MonotonicTime;

use crate::Action;

/// Stable journal representation of a controller scheduling decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalAction {
    /// Desired and observed state agreed.
    Done,
    /// The resource requested a relative retry delay.
    Requeue(Duration),
    /// The resource requested an absolute injected-clock deadline.
    RequeueAt(MonotonicTime),
}

impl From<Action> for JournalAction {
    fn from(action: Action) -> Self {
        match action {
            Action::Done => Self::Done,
            Action::Requeue(delay) => Self::Requeue(delay),
            Action::RequeueAt(deadline) => Self::RequeueAt(deadline),
        }
    }
}

/// One completed typed reconcile invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    /// Monotonic sequence within this runtime instance.
    pub sequence: u64,
    /// Stable resource kind registered by the reconciler.
    pub kind: &'static str,
    /// Display form of the typed resource identity.
    pub resource_id: String,
    /// Exact resource revision observed at invocation start.
    pub observed_revision: ResourceRevision,
    /// Scheduling action returned or derived from the reconcile result.
    pub action: JournalAction,
    /// Elapsed injected-clock duration of the invocation.
    pub duration: Duration,
    /// Whether this invocation executed deletion cleanup.
    pub deleting: bool,
}

/// In-memory invocation history used by deterministic harnesses.
#[derive(Clone, Default)]
pub struct ReconcileJournal {
    inner: Arc<Mutex<JournalState>>,
}

impl ReconcileJournal {
    /// Returns a point-in-time copy ordered by invocation sequence.
    pub fn entries(&self) -> Vec<JournalEntry> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .clone()
    }

    /// Returns the most recent entries, retaining chronological order.
    pub fn tail(&self, maximum: usize) -> Vec<JournalEntry> {
        let entries = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        entries
            .entries
            .iter()
            .skip(entries.entries.len().saturating_sub(maximum))
            .cloned()
            .collect()
    }

    pub(crate) fn record(&self, mut entry: JournalEntry) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.next_sequence = state.next_sequence.saturating_add(1);
        entry.sequence = state.next_sequence;
        state.entries.push(entry);
    }
}

#[derive(Default)]
struct JournalState {
    next_sequence: u64,
    entries: Vec<JournalEntry>,
}
