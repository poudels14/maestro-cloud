use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};
use kernel_api::{AssignmentId, Timestamp};

const BACKOFF_FACTOR: f32 = 1.5;

/// Volatile retry state. Desired assignments remain durable, while retry timing intentionally
/// starts over whenever the node agent restarts.
pub(crate) struct RetryTracker {
    entries: BTreeMap<AssignmentId, RetryState>,
    builder: ExponentialBuilder,
    maximum_delay: Duration,
}

struct RetryState {
    backoff: ExponentialBackoff,
    attempts: u32,
    retry_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetrySchedule {
    Scheduled { attempt: u32, retry_at: Timestamp },
    Exhausted { maximum: u32 },
}

impl RetryTracker {
    pub(crate) fn new(minimum_delay: Duration, maximum_delay: Duration) -> Self {
        Self {
            entries: BTreeMap::new(),
            builder: ExponentialBuilder::default()
                .with_factor(BACKOFF_FACTOR)
                .with_min_delay(minimum_delay)
                .with_max_delay(maximum_delay)
                .without_max_times(),
            maximum_delay,
        }
    }

    pub(crate) fn retry_at(&self, assignment_id: &AssignmentId) -> Option<Timestamp> {
        self.entries
            .get(assignment_id)
            .and_then(|state| state.retry_at)
    }

    pub(crate) fn schedule(
        &mut self,
        assignment_id: AssignmentId,
        maximum: Option<u32>,
        now: Timestamp,
    ) -> RetrySchedule {
        let state = self
            .entries
            .entry(assignment_id)
            .or_insert_with(|| RetryState {
                backoff: self.builder.build(),
                attempts: 0,
                retry_at: None,
            });
        if let Some(maximum) = maximum
            && state.attempts >= maximum
        {
            state.retry_at = None;
            return RetrySchedule::Exhausted { maximum };
        }
        state.attempts = state.attempts.saturating_add(1);
        let delay = state.backoff.next().unwrap_or(self.maximum_delay);
        let retry_at = add_duration(now, delay);
        state.retry_at = Some(retry_at);
        RetrySchedule::Scheduled {
            attempt: state.attempts,
            retry_at,
        }
    }

    pub(crate) fn mark_started(&mut self, assignment_id: &AssignmentId) {
        if let Some(state) = self.entries.get_mut(assignment_id) {
            state.retry_at = None;
        }
    }

    pub(crate) fn clear(&mut self, assignment_id: &AssignmentId) {
        self.entries.remove(assignment_id);
    }

    pub(crate) fn retain(&mut self, active: &BTreeSet<AssignmentId>) {
        self.entries
            .retain(|assignment_id, _| active.contains(assignment_id));
    }
}

fn add_duration(now: Timestamp, delay: Duration) -> Timestamp {
    let milliseconds = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
    Timestamp(now.0.saturating_add(milliseconds))
}
