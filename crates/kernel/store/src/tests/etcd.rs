use std::time::Duration;

#[cfg(feature = "test-util")]
use std::sync::Arc;

use kernel_api::{ClusterId, ResourceKind, ResourceName};

use crate::{
    CasOutcome, Compare, DeleteRequest, EtcdStore, ExpectedVersion, Keyspace, Mutation, PutRequest,
    SessionBinding, Store, StoreError, Transaction, TransactionOutcome, WatchStart, derive_key,
};

#[tokio::test]
#[ignore = "requires MAESTRO_ETCD_ENDPOINTS or a local etcd on 127.0.0.1:2379"]
async fn encrypted_etcd_never_persists_plaintext_and_binds_the_key()
-> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("MAESTRO_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_string());
    let endpoints = endpoint.split(',').map(str::to_string).collect::<Vec<_>>();
    let store = EtcdStore::connect_encrypted(
        endpoints.clone(),
        derive_key("etcd-test-encryption-secret-with-32-characters")?,
    )
    .await?;
    let keys = Keyspace::new(&ClusterId::new("etcd-encrypted")?);
    let kind = ResourceKind::new("Service")?;
    let key = keys.resource(&kind, &ResourceName::new("secret")?);
    let mut raw = etcd_client::Client::connect(endpoints.clone(), None).await?;
    raw.delete(key.as_str(), None).await?;

    let outcome = store
        .put_cas(PutRequest {
            key: key.clone(),
            value: b"database-password".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(outcome, CasOutcome::Applied(_)));
    let response = raw.get(key.as_str(), None).await?;
    let persisted = response.kvs().first().ok_or("encrypted key should exist")?;
    assert_ne!(persisted.value(), b"database-password");
    assert!(persisted.value().starts_with(b"MAE1"));
    assert_eq!(
        store
            .get(&key)
            .await?
            .ok_or("decrypted key should exist")?
            .value,
        b"database-password"
    );

    let wrong_key_store = EtcdStore::connect_encrypted(
        endpoints,
        derive_key("different-etcd-test-secret-with-32-characters")?,
    )
    .await?;
    assert!(matches!(
        wrong_key_store.get(&key).await,
        Err(StoreError::Protection { .. })
    ));

    let traefik_key = keys.traefik_entry("http/routers/api/rule")?;
    raw.delete(traefik_key.as_str(), None).await?;
    let outcome = store
        .put_cas(PutRequest {
            key: traefik_key.clone(),
            value: b"Host(`api.example.test`)".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(outcome, CasOutcome::Applied(_)));
    let response = raw.get(traefik_key.as_str(), None).await?;
    assert_eq!(
        response
            .kvs()
            .first()
            .ok_or("Traefik key should exist")?
            .value(),
        b"Host(`api.example.test`)"
    );

    let traefik_provider_key =
        keys.traefik_provider_entry("http/routers/api/rule")?;
    raw.delete(traefik_provider_key.as_str(), None).await?;
    let outcome = store
        .put_cas(PutRequest {
            key: traefik_provider_key.clone(),
            value: b"Host(`api.example.test`)".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(outcome, CasOutcome::Applied(_)));
    let response = raw.get(traefik_provider_key.as_str(), None).await?;
    assert_eq!(
        response
            .kvs()
            .first()
            .ok_or("Traefik provider key should exist")?
            .value(),
        b"Host(`api.example.test`)"
    );
    assert_eq!(
        store
            .get(&traefik_provider_key)
            .await?
            .ok_or("Traefik provider key should be readable")?
            .value,
        b"Host(`api.example.test`)"
    );

    raw.delete(key.as_str(), None).await?;
    raw.delete(traefik_key.as_str(), None).await?;
    raw.delete(traefik_provider_key.as_str(), None).await?;
    Ok(())
}

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
    #[cfg(feature = "test-util")]
    {
        let report = crate::conformance::run(
            Arc::new(store.clone()),
            ClusterId::new("etcd-shared-conformance")?,
        )
        .await?;
        assert_eq!(report.watch_events, 2);
        assert_eq!(report.conflicts, 1);
        assert_eq!(report.expired_session_keys, 1);
        assert_eq!(report.snapshot_resources, 3);
    }
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
