//! Reusable behavioral battery for every [`crate::Store`] implementation.

use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use kernel_api::{ClusterId, InvalidIdentifier, ResourceKind, ResourceName};

use crate::{
    CasOutcome, Compare, DeleteRequest, ExpectedVersion, Keyspace, Mutation, PutRequest,
    SessionBinding, Store, StoreError, StoreKey, Transaction, TransactionOutcome, WatchStart,
};

macro_rules! require {
    ($condition:expr, $message:expr $(,)?) => {
        if $condition {
            Ok(())
        } else {
            violation($message)
        }
    };
}

/// Evidence returned after a backend passes the shared store contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConformanceReport {
    /// Ordered watch events observed from one atomic batch.
    pub watch_events: usize,
    /// Conditional conflicts deliberately verified by the battery.
    pub conflicts: usize,
    /// Session-bound keys proven to disappear on close.
    pub expired_session_keys: usize,
}

/// Matchable failure from the reusable backend conformance battery.
#[derive(Debug, thiserror::Error)]
pub enum ConformanceError {
    /// The backend returned an operational or contract error.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The isolated test namespace could not be represented safely.
    #[error(transparent)]
    Identifier(#[from] InvalidIdentifier),
    /// The backend returned a successful result that violated the trait contract.
    #[error("store conformance violation: {message}")]
    Violation {
        /// Exact failed invariant.
        message: String,
    },
}

/// Runs CAS, transaction, watch-resume, list, and session-lifetime checks.
///
/// The caller must supply an isolated cluster identity. The battery removes
/// its fixed resource keys before and after the run, but does not delete any
/// other data under that cluster namespace.
pub async fn run(
    store: Arc<dyn Store>,
    cluster_id: ClusterId,
) -> Result<ConformanceReport, ConformanceError> {
    let keys = Keyspace::new(&cluster_id);
    let kind = ResourceKind::new("Conformance")?;
    let prefix = keys.resource_kind(&kind);
    let first_key = keys.resource(&kind, &ResourceName::new("first")?);
    let second_key = keys.resource(&kind, &ResourceName::new("second")?);
    let leased_key = keys.resource(&kind, &ResourceName::new("leased")?);
    for key in [&first_key, &second_key, &leased_key] {
        delete_if_present(store.as_ref(), key).await?;
    }

    let snapshot = store.list(&prefix).await?;
    require!(snapshot.values.is_empty(), "isolated prefix was not empty")?;
    let mut watch = store.watch(prefix.clone(), WatchStart::After(snapshot.cursor))?;
    let mut pending_next = test_util::task::spawn(watch.next());
    require!(
        matches!(pending_next.poll(), Poll::Pending),
        "an idle watch did not wait for a future mutation",
    )?;
    drop(pending_next);
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
                    value: b"first".to_vec(),
                    session: None,
                },
                Mutation::Put {
                    key: second_key.clone(),
                    value: b"second".to_vec(),
                    session: None,
                },
            ],
        })
        .await?;
    let TransactionOutcome::Applied { results, cursor } = outcome else {
        return violation("atomic create unexpectedly conflicted");
    };
    require!(results.len() == 2, "atomic create omitted mutation results")?;
    let first_event = watch.next().await?;
    let second_event = watch.next().await?;
    require!(
        first_event.cursor < second_event.cursor,
        "transaction watch events were not strictly ordered",
    )?;
    require!(
        second_event.cursor == cursor,
        "transaction cursor did not follow its final watch event",
    )?;

    let mut resumed = store.watch(prefix.clone(), WatchStart::After(first_event.cursor))?;
    require!(
        resumed.next().await?.cursor == second_event.cursor,
        "watch did not resume within an atomic transaction",
    )?;
    let first = store
        .get(&first_key)
        .await?
        .ok_or_else(|| ConformanceError::Violation {
            message: "created key was absent from a linearizable read".to_string(),
        })?;
    let conflict = store
        .put_cas(PutRequest {
            key: first_key.clone(),
            value: b"stale".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    require!(
        matches!(conflict, CasOutcome::Conflict { actual: Some(actual) } if actual == first.version),
        "stale create did not report the current version",
    )?;
    let listed = store.list(&prefix).await?;
    require!(
        listed.values.len() == 2,
        "linearizable prefix list returned the wrong key count",
    )?;

    let session = store.session(Duration::from_secs(5)).await?;
    let leased = store
        .put_cas(PutRequest {
            key: leased_key.clone(),
            value: b"leased".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await?;
    require!(
        matches!(leased, CasOutcome::Applied(_)),
        "session-bound create unexpectedly conflicted",
    )?;
    session.keep_alive().await?;
    session.close().await?;
    require!(
        store.get(&leased_key).await?.is_none(),
        "closing a session did not remove its attached key",
    )?;

    delete_if_present(store.as_ref(), &first_key).await?;
    delete_if_present(store.as_ref(), &second_key).await?;
    Ok(ConformanceReport {
        watch_events: 2,
        conflicts: 1,
        expired_session_keys: 1,
    })
}

async fn delete_if_present(store: &dyn Store, key: &StoreKey) -> Result<(), ConformanceError> {
    if let Some(value) = store.get(key).await? {
        let outcome = store
            .delete_cas(DeleteRequest {
                key: key.clone(),
                expected: value.version,
            })
            .await?;
        require!(
            matches!(outcome, CasOutcome::Applied(_)),
            "cleanup delete conflicted in an isolated namespace",
        )?;
    }
    Ok(())
}

fn violation<T>(message: &str) -> Result<T, ConformanceError> {
    Err(ConformanceError::Violation {
        message: message.to_string(),
    })
}
