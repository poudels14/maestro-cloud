use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, watch};

use crate::{
    CasOutcome, Clock, Compare, DeleteRequest, ExpectedVersion, ListResult, MonotonicTime,
    Mutation, MutationResult, PutRequest, Session, SessionBinding, SessionId, Store, StoreError,
    StoreKey, StorePrefix, StoreWatch, StoredValue, Transaction, TransactionOutcome, Version,
    WatchCursor, WatchEvent, WatchEventKind, WatchStart,
};

const EVENT_HISTORY_CAPACITY: usize = 4_096;

/// Linearizable in-memory store used by reconciler and acceptance tests.
///
/// All state has one async owner lock. Change notification retains only the
/// latest wake sequence; durable resume comes from the bounded event history,
/// and lag beyond that history surfaces as [`StoreError::CursorExpired`].
#[derive(Clone)]
pub struct InMemoryStore {
    inner: Arc<Inner>,
}

impl InMemoryStore {
    /// Creates an empty store using an injected monotonic clock.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        // Tokio watch retains one latest sequence; event history is the durable resume source.
        let (changes, _) = watch::channel(0);
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                changes,
                current_revision: AtomicU64::new(0),
                clock,
            }),
        }
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn get(&self, key: &StoreKey) -> Result<Option<StoredValue>, StoreError> {
        expire_sessions(&self.inner).await;
        let state = self.inner.state.lock().await;
        Ok(state
            .entries
            .get(key)
            .map(|entry| entry.stored(key.clone())))
    }

    async fn list(&self, prefix: &StorePrefix) -> Result<ListResult, StoreError> {
        expire_sessions(&self.inner).await;
        let state = self.inner.state.lock().await;
        let values = state
            .entries
            .iter()
            .filter(|(key, _)| key.as_str().starts_with(prefix.as_str()))
            .map(|(key, entry)| entry.stored(key.clone()))
            .collect();
        Ok(ListResult {
            values,
            cursor: WatchCursor::snapshot(state.version),
        })
    }

    async fn put_cas(&self, request: PutRequest) -> Result<CasOutcome<StoredValue>, StoreError> {
        expire_sessions(&self.inner).await;
        let mut state = self.inner.state.lock().await;
        let actual = state.entries.get(&request.key).map(|entry| entry.version);
        if !matches_expected(request.expected, actual) {
            Ok(CasOutcome::Conflict { actual })
        } else {
            validate_session(&state, request.session)?;
            let version = state.next_version();
            let entry = Entry {
                value: request.value,
                version,
                session: request.session,
            };
            let stored = entry.stored(request.key.clone());
            state.entries.insert(request.key, entry);
            state.push_event(WatchEventKind::Put(stored.clone()), version, 0, &self.inner);
            Ok(CasOutcome::Applied(stored))
        }
    }

    async fn delete_cas(&self, request: DeleteRequest) -> Result<CasOutcome<Version>, StoreError> {
        expire_sessions(&self.inner).await;
        let mut state = self.inner.state.lock().await;
        let actual = state.entries.get(&request.key).map(|entry| entry.version);
        if actual != Some(request.expected) {
            Ok(CasOutcome::Conflict { actual })
        } else {
            let removed =
                state
                    .entries
                    .remove(&request.key)
                    .ok_or_else(|| StoreError::Contract {
                        message: "a matched delete key disappeared while holding the store lock"
                            .to_string(),
                    })?;
            let deletion_version = state.next_version();
            state.push_event(
                WatchEventKind::Delete {
                    key: request.key,
                    previous_version: removed.version,
                },
                deletion_version,
                0,
                &self.inner,
            );
            Ok(CasOutcome::Applied(removed.version))
        }
    }

    async fn txn(&self, transaction: Transaction) -> Result<TransactionOutcome, StoreError> {
        expire_sessions(&self.inner).await;
        let mut state = self.inner.state.lock().await;
        validate_transaction(&state, &transaction)?;
        if transaction
            .compares
            .iter()
            .all(|compare| compare_matches(&state, compare))
        {
            if transaction.mutations.is_empty() {
                return Ok(TransactionOutcome::Applied {
                    results: Vec::new(),
                    cursor: WatchCursor::snapshot(state.version),
                });
            }
            let version = state.next_version();
            let mut results = Vec::with_capacity(transaction.mutations.len());
            let mut last_cursor = WatchCursor::snapshot(state.version);
            for (event_index, mutation) in transaction.mutations.into_iter().enumerate() {
                let event_index = u32::try_from(event_index).unwrap_or(u32::MAX);
                match mutation {
                    Mutation::Put {
                        key,
                        value,
                        session,
                    } => {
                        let entry = Entry {
                            value,
                            version,
                            session,
                        };
                        let stored = entry.stored(key.clone());
                        state.entries.insert(key, entry);
                        last_cursor = state.push_event(
                            WatchEventKind::Put(stored.clone()),
                            version,
                            event_index,
                            &self.inner,
                        );
                        results.push(MutationResult::Put(stored));
                    }
                    Mutation::Delete { key } => {
                        if let Some(removed) = state.entries.remove(&key) {
                            state.push_event(
                                WatchEventKind::Delete {
                                    key: key.clone(),
                                    previous_version: removed.version,
                                },
                                version,
                                event_index,
                                &self.inner,
                            );
                            last_cursor = WatchCursor::event(version.0, event_index);
                            results.push(MutationResult::Deleted {
                                key,
                                previous_version: removed.version,
                            });
                        } else {
                            results.push(MutationResult::DeleteMissing { key });
                        }
                    }
                }
            }
            Ok(TransactionOutcome::Applied {
                results,
                cursor: last_cursor,
            })
        } else {
            Ok(TransactionOutcome::Conflict)
        }
    }

    fn watch(
        &self,
        prefix: StorePrefix,
        start: WatchStart,
    ) -> Result<Box<dyn StoreWatch>, StoreError> {
        let last_cursor = match start {
            WatchStart::Current => {
                WatchCursor::snapshot(self.inner.current_revision.load(Ordering::Acquire))
            }
            WatchStart::After(cursor) => cursor,
        };
        Ok(Box::new(InMemoryWatch {
            inner: self.inner.clone(),
            changes: self.inner.changes.subscribe(),
            prefix,
            last_cursor,
        }))
    }

    async fn session(&self, ttl: Duration) -> Result<Box<dyn Session>, StoreError> {
        expire_sessions(&self.inner).await;
        let mut state = self.inner.state.lock().await;
        state.next_session_id = state.next_session_id.saturating_add(1);
        let session_id = SessionId(state.next_session_id);
        state.sessions.insert(
            session_id,
            SessionRecord {
                ttl,
                expires_at: self.inner.clock.now().saturating_add(ttl),
            },
        );
        state.signal_change(&self.inner);
        Ok(Box::new(InMemorySession {
            inner: self.inner.clone(),
            session_id,
            ttl,
        }))
    }
}

struct Inner {
    state: Mutex<State>,
    changes: watch::Sender<u64>,
    current_revision: AtomicU64,
    clock: Arc<dyn Clock>,
}

#[derive(Default)]
struct State {
    entries: BTreeMap<StoreKey, Entry>,
    sessions: BTreeMap<SessionId, SessionRecord>,
    history: VecDeque<RawEvent>,
    version: u64,
    discarded_through: WatchCursor,
    next_session_id: u64,
    change_sequence: u64,
}

impl State {
    fn next_version(&mut self) -> Version {
        self.version = self.version.saturating_add(1);
        Version(self.version)
    }

    fn push_event(
        &mut self,
        kind: WatchEventKind,
        version: Version,
        event_index: u32,
        inner: &Inner,
    ) -> WatchCursor {
        let cursor = WatchCursor::event(version.0, event_index);
        self.history.push_back(RawEvent { cursor, kind });
        if self.history.len() > EVENT_HISTORY_CAPACITY
            && let Some(discarded) = self.history.pop_front()
        {
            self.discarded_through = discarded.cursor;
        }
        inner.current_revision.store(version.0, Ordering::Release);
        self.signal_change(inner);
        cursor
    }

    fn signal_change(&mut self, inner: &Inner) {
        self.change_sequence = self.change_sequence.saturating_add(1);
        inner.changes.send_replace(self.change_sequence);
    }
}

struct Entry {
    value: Vec<u8>,
    version: Version,
    session: Option<SessionBinding>,
}

impl Entry {
    fn stored(&self, key: StoreKey) -> StoredValue {
        StoredValue {
            key,
            value: self.value.clone(),
            version: self.version,
        }
    }
}

struct SessionRecord {
    ttl: Duration,
    expires_at: MonotonicTime,
}

#[derive(Clone)]
struct RawEvent {
    cursor: WatchCursor,
    kind: WatchEventKind,
}

struct InMemoryWatch {
    inner: Arc<Inner>,
    changes: watch::Receiver<u64>,
    prefix: StorePrefix,
    last_cursor: WatchCursor,
}

#[async_trait]
impl StoreWatch for InMemoryWatch {
    async fn next(&mut self) -> Result<WatchEvent, StoreError> {
        loop {
            expire_sessions(&self.inner).await;
            let state = self.inner.state.lock().await;
            if self.last_cursor < state.discarded_through {
                return Err(StoreError::CursorExpired {
                    cursor: self.last_cursor,
                });
            }
            if let Some(event) = state
                .history
                .iter()
                .find(|event| event.cursor > self.last_cursor && event_matches(event, &self.prefix))
            {
                self.last_cursor = event.cursor;
                return Ok(WatchEvent {
                    prefix: self.prefix.clone(),
                    cursor: event.cursor,
                    kind: event.kind.clone(),
                });
            }
            self.last_cursor = WatchCursor::snapshot(state.version);
            let next_expiry = state
                .sessions
                .values()
                .map(|session| session.expires_at)
                .min();
            self.changes.borrow_and_update();
            drop(state);

            if let Some(deadline) = next_expiry {
                tokio::select! {
                    changed = self.changes.changed() => {
                        changed.map_err(|_| StoreError::Contract {
                            message: "in-memory change channel closed while the store is alive"
                                .to_string(),
                        })?;
                    }
                    () = self.inner.clock.sleep_until(deadline) => {}
                }
            } else {
                self.changes
                    .changed()
                    .await
                    .map_err(|_| StoreError::Contract {
                        message: "in-memory change channel closed while the store is alive"
                            .to_string(),
                    })?;
            }
        }
    }
}

struct InMemorySession {
    inner: Arc<Inner>,
    session_id: SessionId,
    ttl: Duration,
}

#[async_trait]
impl Session for InMemorySession {
    fn id(&self) -> SessionId {
        self.session_id
    }

    fn ttl(&self) -> Duration {
        self.ttl
    }

    async fn keep_alive(&self) -> Result<(), StoreError> {
        expire_sessions(&self.inner).await;
        let mut state = self.inner.state.lock().await;
        if let Some(session) = state.sessions.get_mut(&self.session_id) {
            session.expires_at = self.inner.clock.now().saturating_add(session.ttl);
            state.signal_change(&self.inner);
            Ok(())
        } else {
            Err(StoreError::SessionExpired {
                session_id: self.session_id,
            })
        }
    }

    async fn close(&self) -> Result<(), StoreError> {
        let mut state = self.inner.state.lock().await;
        if state.sessions.remove(&self.session_id).is_some() {
            remove_session_entries(&mut state, self.session_id, &self.inner);
            state.signal_change(&self.inner);
            Ok(())
        } else {
            Err(StoreError::SessionExpired {
                session_id: self.session_id,
            })
        }
    }
}

async fn expire_sessions(inner: &Arc<Inner>) {
    let now = inner.clock.now();
    let mut state = inner.state.lock().await;
    let expired = state
        .sessions
        .iter()
        .filter_map(|(session_id, session)| (session.expires_at <= now).then_some(*session_id))
        .collect::<Vec<_>>();
    for session_id in expired {
        state.sessions.remove(&session_id);
        remove_session_entries(&mut state, session_id, inner);
    }
}

fn remove_session_entries(state: &mut State, session_id: SessionId, inner: &Inner) {
    let keys = state
        .entries
        .iter()
        .filter_map(|(key, entry)| {
            (entry.session == Some(SessionBinding { session_id })).then_some(key.clone())
        })
        .collect::<Vec<_>>();
    let deletion_version = (!keys.is_empty()).then(|| state.next_version());
    for (event_index, key) in keys.into_iter().enumerate() {
        if let Some(removed) = state.entries.remove(&key) {
            state.push_event(
                WatchEventKind::Delete {
                    key,
                    previous_version: removed.version,
                },
                deletion_version.unwrap_or(removed.version),
                u32::try_from(event_index).unwrap_or(u32::MAX),
                inner,
            );
        }
    }
}

fn matches_expected(expected: ExpectedVersion, actual: Option<Version>) -> bool {
    match expected {
        ExpectedVersion::Missing => actual.is_none(),
        ExpectedVersion::Exact(version) => actual == Some(version),
    }
}

fn validate_session(state: &State, binding: Option<SessionBinding>) -> Result<(), StoreError> {
    if let Some(binding) = binding
        && !state.sessions.contains_key(&binding.session_id)
    {
        Err(StoreError::SessionExpired {
            session_id: binding.session_id,
        })
    } else {
        Ok(())
    }
}

fn validate_transaction(state: &State, transaction: &Transaction) -> Result<(), StoreError> {
    let unique_keys = transaction
        .mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        })
        .collect::<BTreeSet<_>>();
    if unique_keys.len() != transaction.mutations.len() {
        Err(StoreError::Contract {
            message: "a transaction cannot mutate the same key more than once".to_string(),
        })
    } else {
        for mutation in &transaction.mutations {
            if let Mutation::Put { session, .. } = mutation {
                validate_session(state, *session)?;
            }
        }
        Ok(())
    }
}

fn compare_matches(state: &State, compare: &Compare) -> bool {
    let actual = state.entries.get(&compare.key).map(|entry| entry.version);
    matches_expected(compare.expected, actual)
}

fn event_matches(event: &RawEvent, prefix: &StorePrefix) -> bool {
    match &event.kind {
        WatchEventKind::Put(value) => value.key.as_str().starts_with(prefix.as_str()),
        WatchEventKind::Delete { key, .. } => key.as_str().starts_with(prefix.as_str()),
    }
}
