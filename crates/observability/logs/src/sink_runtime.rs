use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use kernel_api::Timestamp;
use serde::{Deserialize, Serialize};

use crate::LogSinkId;

const MAX_ERROR_CHARACTERS: usize = 500;

/// Wall-clock boundary used to make sink health transitions deterministic in tests.
pub trait SinkRuntimeClock: Send + Sync {
    /// Returns the current Unix timestamp in milliseconds.
    fn now(&self) -> Timestamp;
}

/// System wall clock used by production sink workers.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemSinkRuntimeClock;

impl SinkRuntimeClock for SystemSinkRuntimeClock {
    fn now(&self) -> Timestamp {
        let milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        Timestamp(i64::try_from(milliseconds).unwrap_or(i64::MAX))
    }
}

/// API-compatible runtime health for one independently checkpointed log sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkRuntimeSnapshot {
    /// Stable sink cursor namespace.
    pub id: String,
    /// Most recent time a delivered batch and its cursor were committed.
    pub last_success_at_ms: Option<i64>,
    /// Most recent failed drain time.
    pub last_error_at_ms: Option<i64>,
    /// Bounded safe diagnostic from the latest failed drain.
    pub last_error: Option<String>,
    /// Failures observed since the latest successful or idle drain.
    pub consecutive_failures: u64,
    /// Most recent time durable cursor progress was committed.
    pub last_cursor_advance_at_ms: Option<i64>,
    /// Entries removed by sink-local filters over this process lifetime.
    #[serde(default)]
    pub filtered_entries: u64,
}

/// Shared process-local sink health registry used by workers and stats APIs.
#[derive(Clone)]
pub struct SinkRuntimeRegistry {
    states: Arc<RwLock<BTreeMap<LogSinkId, SinkRuntimeSnapshot>>>,
    clock: Arc<dyn SinkRuntimeClock>,
}

impl SinkRuntimeRegistry {
    /// Creates an empty registry using an injected wall clock.
    pub fn new(clock: Arc<dyn SinkRuntimeClock>) -> Self {
        Self {
            states: Arc::new(RwLock::new(BTreeMap::new())),
            clock,
        }
    }

    /// Makes a configured sink visible before its first delivery outcome.
    pub fn register(&self, sink_id: &LogSinkId) {
        self.with_state(sink_id, |_| {});
    }

    /// Records progress only after both destination acceptance and durable cursor commit.
    pub fn record_success(&self, sink_id: &LogSinkId, filtered_entries: usize) {
        let now = self.clock.now().0;
        self.with_state(sink_id, |state| {
            state.last_success_at_ms = Some(now);
            state.last_cursor_advance_at_ms = Some(now);
            state.consecutive_failures = 0;
            state.filtered_entries = state
                .filtered_entries
                .saturating_add(u64::try_from(filtered_entries).unwrap_or(u64::MAX));
        });
    }

    /// Records one failed bounded drain without claiming cursor progress.
    pub fn record_failure(&self, sink_id: &LogSinkId, error: &str) {
        let now = self.clock.now().0;
        self.with_state(sink_id, |state| {
            state.last_error_at_ms = Some(now);
            state.last_error = Some(error.chars().take(MAX_ERROR_CHARACTERS).collect());
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        });
    }

    /// Clears an old failure streak after a healthy idle drain.
    pub fn record_recovered(&self, sink_id: &LogSinkId) {
        self.with_state(sink_id, |state| {
            state.consecutive_failures = 0;
        });
    }

    /// Returns one sink's current health, registering a default row when absent.
    pub fn snapshot(&self, sink_id: &LogSinkId) -> SinkRuntimeSnapshot {
        self.register(sink_id);
        self.states
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(sink_id)
            .cloned()
            .unwrap_or_else(|| snapshot_for(sink_id))
    }

    /// Returns every registered sink in stable identifier order.
    pub fn snapshots(&self) -> Vec<SinkRuntimeSnapshot> {
        self.states
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    fn with_state(&self, sink_id: &LogSinkId, update: impl FnOnce(&mut SinkRuntimeSnapshot)) {
        let mut states = self
            .states
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        update(
            states
                .entry(sink_id.clone())
                .or_insert_with(|| snapshot_for(sink_id)),
        );
    }
}

impl Default for SinkRuntimeRegistry {
    fn default() -> Self {
        Self::new(Arc::new(SystemSinkRuntimeClock))
    }
}

fn snapshot_for(sink_id: &LogSinkId) -> SinkRuntimeSnapshot {
    SinkRuntimeSnapshot {
        id: sink_id.as_str().to_owned(),
        last_success_at_ms: None,
        last_error_at_ms: None,
        last_error: None,
        consecutive_failures: 0,
        last_cursor_advance_at_ms: None,
        filtered_entries: 0,
    }
}
