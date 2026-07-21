use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kernel_store::{MonotonicTime, StoreKey, StoredValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Scheduled {
    deadline: MonotonicTime,
    attempt: u32,
}

#[derive(Default)]
pub(crate) struct WorkQueue {
    known: BTreeSet<StoreKey>,
    scheduled: BTreeMap<StoreKey, Scheduled>,
}

impl WorkQueue {
    pub(crate) fn schedule(&mut self, key: StoreKey, deadline: MonotonicTime, attempt: u32) {
        self.known.insert(key.clone());
        self.scheduled.insert(key, Scheduled { deadline, attempt });
    }

    pub(crate) fn remove(&mut self, key: &StoreKey) {
        self.known.remove(key);
        self.scheduled.remove(key);
    }

    pub(crate) fn schedule_all(&mut self, deadline: MonotonicTime) {
        for key in self.known.clone() {
            self.scheduled.insert(
                key,
                Scheduled {
                    deadline,
                    attempt: 0,
                },
            );
        }
    }

    pub(crate) fn next_deadline(&self) -> MonotonicTime {
        self.scheduled
            .values()
            .map(|scheduled| scheduled.deadline)
            .min()
            .unwrap_or_else(|| MonotonicTime::from_duration(Duration::MAX))
    }

    pub(crate) fn take_due(&mut self, now: MonotonicTime) -> Option<(StoreKey, u32)> {
        let key = self
            .scheduled
            .iter()
            .find_map(|(key, scheduled)| (scheduled.deadline <= now).then(|| key.clone()))?;
        self.scheduled
            .remove(&key)
            .map(|scheduled| (key, scheduled.attempt))
    }

    pub(crate) fn replace_with(&mut self, values: &[StoredValue], now: MonotonicTime) {
        let present: BTreeSet<_> = values.iter().map(|stored| stored.key.clone()).collect();
        self.scheduled.retain(|key, _| present.contains(key));
        self.known = present;
        self.schedule_all(now);
    }
}
