use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, ResourceKind, ResourceName};
use tokio::sync::watch;

use crate::{
    CasOutcome, Clock, Compare, DeleteRequest, ExpectedVersion, InMemoryStore, Keyspace,
    MonotonicTime, Mutation, PutRequest, SessionBinding, Store, StoreError, Transaction,
    TransactionOutcome, WatchEventKind, WatchStart,
};

#[tokio::test]
async fn cas_writes_and_deletes_reject_stale_versions() -> Result<(), String> {
    let (store, _) = a_store();
    let key = a_key(&Keyspace::new(&a_cluster()), "api");

    let created = store
        .put_cas(PutRequest {
            key: key.clone(),
            value: b"first".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await
        .expect("create value");
    let CasOutcome::Applied(created) = created else {
        return Err("initial put should apply".to_string());
    };
    assert!(matches!(
        store
            .put_cas(PutRequest {
                key: key.clone(),
                value: b"duplicate".to_vec(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await
            .expect("conflicting create"),
        CasOutcome::Conflict { actual: Some(version) } if version == created.version
    ));

    let updated = store
        .put_cas(PutRequest {
            key: key.clone(),
            value: b"second".to_vec(),
            expected: ExpectedVersion::Exact(created.version),
            session: None,
        })
        .await
        .expect("update value");
    let CasOutcome::Applied(updated) = updated else {
        return Err("versioned update should apply".to_string());
    };
    assert_ne!(updated.version, created.version);
    assert!(matches!(
        store
            .delete_cas(DeleteRequest {
                key: key.clone(),
                expected: created.version,
            })
            .await
            .expect("stale delete"),
        CasOutcome::Conflict { actual: Some(version) } if version == updated.version
    ));
    assert!(matches!(
        store
            .delete_cas(DeleteRequest {
                key: key.clone(),
                expected: updated.version,
            })
            .await
            .expect("current delete"),
        CasOutcome::Applied(version) if version == updated.version
    ));
    assert!(store.get(&key).await.expect("read deleted key").is_none());
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_and_watch_resumes_after_its_cursor() -> Result<(), String> {
    let (store, _) = a_store();
    let keys = Keyspace::new(&a_cluster());
    let prefix = keys.resource_kind(&a_kind());
    let first_key = a_key(&keys, "api");
    let second_key = a_key(&keys, "worker");
    let mut watcher = store
        .watch(prefix.clone(), WatchStart::Current)
        .expect("create watcher");

    let outcome = store
        .txn(Transaction {
            compares: vec![
                Compare {
                    key: first_key.clone(),
                    expected: ExpectedVersion::Missing,
                },
                Compare {
                    key: second_key.clone(),
                    expected: ExpectedVersion::Missing,
                },
            ],
            mutations: vec![
                Mutation::Put {
                    key: first_key.clone(),
                    value: b"api".to_vec(),
                    session: None,
                },
                Mutation::Put {
                    key: second_key.clone(),
                    value: b"worker".to_vec(),
                    session: None,
                },
            ],
        })
        .await
        .expect("atomic create");
    let TransactionOutcome::Applied { results, cursor } = outcome else {
        return Err("transaction should apply".to_string());
    };
    assert_eq!(results.len(), 2);

    let first_event = watcher.next().await.expect("first transaction event");
    let second_event = watcher.next().await.expect("second transaction event");
    assert!(first_event.cursor < second_event.cursor);
    assert_eq!(second_event.cursor, cursor);

    let mut resumed = store
        .watch(prefix, WatchStart::After(first_event.cursor))
        .expect("resume watcher");
    assert_eq!(
        resumed.next().await.expect("resumed event").cursor,
        second_event.cursor
    );
    let listed = store.list(&keys.resources()).await.expect("list resources");
    assert_eq!(listed.values.len(), 2);
    assert_eq!(
        listed.values.first().expect("first listed value").version,
        listed.values.get(1).expect("second listed value").version
    );

    assert_eq!(
        store
            .txn(Transaction {
                compares: vec![Compare {
                    key: first_key,
                    expected: ExpectedVersion::Missing,
                }],
                mutations: vec![Mutation::Delete { key: second_key }],
            })
            .await
            .expect("conflicting transaction"),
        TransactionOutcome::Conflict
    );
    assert_eq!(store.list(&keys.resources()).await.unwrap().values.len(), 2);
    Ok(())
}

#[tokio::test]
async fn transaction_rejects_operations_beyond_the_backend_limit() {
    let (store, _) = a_store();
    let keys = Keyspace::new(&a_cluster());
    let mutations = (0..=crate::TRANSACTION_OPERATION_LIMIT)
        .map(|index| Mutation::Put {
            key: a_key(&keys, &format!("oversized-{index}")),
            value: Vec::new(),
            session: None,
        })
        .collect();

    let error = store
        .txn(Transaction {
            compares: Vec::new(),
            mutations,
        })
        .await
        .expect_err("oversized transaction should be rejected");
    assert!(matches!(error, StoreError::Unavailable { .. }));
    assert!(
        store
            .list(&keys.resource_kind(&a_kind()))
            .await
            .expect("list resources")
            .values
            .is_empty()
    );
}

#[tokio::test]
async fn session_expiry_deletes_bound_keys_and_wakes_watchers() {
    let (store, clock) = a_store();
    let keys = Keyspace::new(&a_cluster());
    let key = a_key(&keys, "api");
    let session = store
        .session(Duration::from_secs(10))
        .await
        .expect("create session");
    store
        .put_cas(PutRequest {
            key: key.clone(),
            value: b"live".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await
        .expect("write session key");
    let mut watcher = store
        .watch(keys.resource_kind(&a_kind()), WatchStart::Current)
        .expect("watch session key");

    clock.advance(Duration::from_secs(11));

    let event = watcher.next().await.expect("session expiry event");
    assert!(matches!(
        event.kind,
        WatchEventKind::Delete { key: deleted, .. } if deleted == key
    ));
    assert!(store.get(&key).await.expect("read expired key").is_none());
    assert!(matches!(
        session.keep_alive().await,
        Err(StoreError::SessionExpired { .. })
    ));
}

fn a_store() -> (InMemoryStore, ManualClock) {
    test_store()
}

pub(super) fn test_store() -> (InMemoryStore, ManualClock) {
    let clock = ManualClock::new();
    (InMemoryStore::new(Arc::new(clock.clone())), clock)
}

fn a_cluster() -> ClusterId {
    ClusterId::new("acceptance").expect("cluster id")
}

fn a_kind() -> ResourceKind {
    ResourceKind::new("Service").expect("resource kind")
}

fn a_key(keys: &Keyspace, id: &str) -> crate::StoreKey {
    keys.resource(&a_kind(), &ResourceName::new(id).expect("resource name"))
}

#[derive(Clone)]
pub(super) struct ManualClock {
    now_millis: Arc<AtomicU64>,
    changes: watch::Sender<u64>,
}

impl ManualClock {
    fn new() -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            now_millis: Arc::new(AtomicU64::new(0)),
            changes,
        }
    }

    fn advance(&self, duration: Duration) {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let now = self
            .now_millis
            .fetch_add(millis, Ordering::AcqRel)
            .saturating_add(millis);
        self.changes.send_replace(now);
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(
            self.now_millis.load(Ordering::Acquire),
        ))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        let mut changes = self.changes.subscribe();
        while self.now() < deadline {
            if changes.changed().await.is_err() {
                break;
            }
        }
    }
}
