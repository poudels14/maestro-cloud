use std::time::Duration;

use kernel_api::{ClusterId, ResourceKind, ResourceName};

use crate::{
    CasOutcome, Compare, DeleteRequest, EtcdStore, ExpectedVersion, Keyspace, Mutation, PutRequest,
    SessionBinding, Store, Transaction, TransactionOutcome, WatchStart,
};

#[tokio::test]
#[ignore = "requires MAESTRO_ETCD_ENDPOINTS or a local etcd on 127.0.0.1:2379"]
async fn etcd_backend_preserves_cas_watch_transaction_and_session_contracts()
-> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("MAESTRO_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_string());
    let store = EtcdStore::connect(endpoint.split(',').map(str::to_string)).await?;
    let keys = Keyspace::new(&ClusterId::new("etcd-conformance")?);
    let kind = ResourceKind::new("Service")?;
    let prefix = keys.resource_kind(&kind);
    let first_key = keys.resource(&kind, &ResourceName::new("first")?);
    let second_key = keys.resource(&kind, &ResourceName::new("second")?);
    delete_if_present(&store, &first_key).await?;
    delete_if_present(&store, &second_key).await?;

    let listed = store.list(&prefix).await?;
    assert!(listed.values.is_empty());
    let mut watch = store.watch(prefix.clone(), WatchStart::After(listed.cursor))?;
    let transaction = store
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
    let TransactionOutcome::Applied { results, cursor } = transaction else {
        return Err("transaction should create both keys".into());
    };
    assert_eq!(results.len(), 2);
    let first_event = watch.next().await?;
    let second_event = watch.next().await?;
    assert!(first_event.cursor < second_event.cursor);
    assert_eq!(second_event.cursor, cursor);

    let mut resumed = store.watch(prefix, WatchStart::After(first_event.cursor))?;
    assert_eq!(resumed.next().await?.cursor, second_event.cursor);
    let first = store
        .get(&first_key)
        .await?
        .ok_or("first key should exist")?;
    assert!(matches!(
        store
            .put_cas(PutRequest {
                key: first_key.clone(),
                value: b"stale".to_vec(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?,
        CasOutcome::Conflict { actual: Some(actual) } if actual == first.version
    ));

    let session = store.session(Duration::from_secs(5)).await?;
    let leased_key = keys.resource(&kind, &ResourceName::new("leased")?);
    delete_if_present(&store, &leased_key).await?;
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
    assert!(matches!(leased, CasOutcome::Applied(_)));
    session.keep_alive().await?;
    session.close().await?;
    assert!(store.get(&leased_key).await?.is_none());

    delete_if_present(&store, &first_key).await?;
    delete_if_present(&store, &second_key).await?;
    Ok(())
}

async fn delete_if_present(
    store: &EtcdStore,
    key: &crate::StoreKey,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(value) = store.get(key).await? {
        let outcome = store
            .delete_cas(DeleteRequest {
                key: key.clone(),
                expected: value.version,
            })
            .await?;
        assert!(matches!(outcome, CasOutcome::Applied(_)));
    }
    Ok(())
}
