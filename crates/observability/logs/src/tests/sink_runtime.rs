use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use kernel_api::Timestamp;

use crate::{LogSinkId, SinkRuntimeClock, SinkRuntimeRegistry};

#[test]
fn registry_tracks_progress_failures_recovery_and_exact_wire_shape()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(ManualClock::new(1_000));
    let registry = SinkRuntimeRegistry::new(clock.clone());
    let sink_id = LogSinkId::new("datadog")?;
    registry.register(&sink_id);

    registry.record_failure(&sink_id, &"x".repeat(600));
    clock.set(2_000);
    registry.record_failure(&sink_id, "temporarily unavailable");
    assert_eq!(registry.snapshot(&sink_id).consecutive_failures, 2);

    clock.set(3_000);
    registry.record_success(&sink_id, 4);
    let snapshot = registry.snapshot(&sink_id);
    assert_eq!(snapshot.consecutive_failures, 0);
    assert_eq!(snapshot.filtered_entries, 4);
    assert_eq!(snapshot.last_success_at_ms, Some(3_000));
    assert_eq!(snapshot.last_cursor_advance_at_ms, Some(3_000));
    assert_eq!(
        serde_json::to_value(snapshot)?,
        serde_json::json!({
            "id": "datadog",
            "lastSuccessAtMs": 3_000,
            "lastErrorAtMs": 2_000,
            "lastError": "temporarily unavailable",
            "consecutiveFailures": 0,
            "lastCursorAdvanceAtMs": 3_000,
            "filteredEntries": 4,
        })
    );
    Ok(())
}

#[test]
fn registry_bounds_diagnostics_and_orders_sinks() -> Result<(), Box<dyn std::error::Error>> {
    let registry = SinkRuntimeRegistry::new(Arc::new(ManualClock::new(1_000)));
    let second = LogSinkId::new("z-last")?;
    let first = LogSinkId::new("a-first")?;
    registry.record_failure(&second, &"é".repeat(600));
    registry.register(&first);

    let snapshots = registry.snapshots();
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| snapshot.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a-first", "z-last"]
    );
    assert_eq!(
        registry
            .snapshot(&second)
            .last_error
            .as_deref()
            .map(str::chars)
            .map(Iterator::count),
        Some(500)
    );
    registry.record_recovered(&second);
    assert_eq!(registry.snapshot(&second).consecutive_failures, 0);
    Ok(())
}

struct ManualClock(AtomicI64);

impl ManualClock {
    fn new(now: i64) -> Self {
        Self(AtomicI64::new(now))
    }

    fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl SinkRuntimeClock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}
