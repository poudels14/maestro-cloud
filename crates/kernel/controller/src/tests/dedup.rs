use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, NodeInstanceId, RequestId, ResourceKind, ResourceName};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, Mutation, PutRequest, SessionBinding,
    Store, Transaction,
};

use super::clock::NoopClock;
use crate::{
    ControllerError, DedupOutcome, FencedStore, LeaderIdentity, LeadershipToken,
    RequestDeduplicator, RequestFingerprint,
};

#[tokio::test]
async fn request_claim_commits_with_mutation_and_replays_only_matching_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
    let keys = Keyspace::new(&ClusterId::new("acceptance")?);
    let leader_key = keys.leader();
    let session = store.session(Duration::from_secs(30)).await?;
    let leader = store
        .put_cas(PutRequest {
            key: leader_key.clone(),
            value: b"leader".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await?;
    let CasOutcome::Applied(leader) = leader else {
        return Err("leader campaign should apply".into());
    };
    let fenced = FencedStore::new(
        store.clone(),
        leader_key,
        LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: NodeId::new("node-1")?,
                instance_id: NodeInstanceId::new("instance-1")?,
            },
            session.id(),
            leader.version,
        ),
    );
    let claim_key = keys.request_claim(&RequestId::new("request-1")?);
    let resource_key = keys.resource(&ResourceKind::new("Service")?, &ResourceName::new("api")?);
    let fingerprint = RequestFingerprint::new([1; 32]);

    assert!(matches!(
        fenced
            .deduplicate(
                claim_key.clone(),
                fingerprint,
                b"created".to_vec(),
                create(resource_key.clone(), b"first"),
            )
            .await?,
        DedupOutcome::Committed { .. }
    ));
    assert_eq!(
        fenced
            .deduplicate(
                claim_key.clone(),
                fingerprint,
                b"ignored".to_vec(),
                Transaction {
                    compares: Vec::new(),
                    mutations: vec![Mutation::Put {
                        key: resource_key.clone(),
                        value: b"must-not-overwrite".to_vec(),
                        session: None,
                    }],
                },
            )
            .await?,
        DedupOutcome::Duplicate {
            response: b"created".to_vec()
        }
    );
    assert_eq!(
        store
            .get(&resource_key)
            .await?
            .ok_or_else(|| std::io::Error::other("resource should exist"))?
            .value,
        b"first"
    );
    assert_eq!(
        fenced
            .deduplicate(
                claim_key,
                RequestFingerprint::new([2; 32]),
                b"collision".to_vec(),
                Transaction {
                    compares: Vec::new(),
                    mutations: Vec::new(),
                },
            )
            .await,
        Err(ControllerError::RequestCollision)
    );
    Ok(())
}

#[tokio::test]
async fn any_node_request_claim_is_atomic_with_its_resource_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
    let keys = Keyspace::new(&ClusterId::new("any-node")?);
    let claim_key = keys.request_claim(&RequestId::new("request-2")?);
    let resource_key = keys.resource(&ResourceKind::new("Service")?, &ResourceName::new("api")?);
    let deduplicator = RequestDeduplicator::new(store.clone());
    let fingerprint = RequestFingerprint::new([3; 32]);

    store
        .put_cas(PutRequest {
            key: resource_key.clone(),
            value: b"existing".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert_eq!(
        deduplicator
            .deduplicate(
                claim_key.clone(),
                fingerprint,
                b"updated".to_vec(),
                create(resource_key.clone(), b"conflict"),
            )
            .await?,
        DedupOutcome::MutationConflict
    );
    assert!(store.get(&claim_key).await?.is_none());

    let current = store
        .get(&resource_key)
        .await?
        .ok_or_else(|| std::io::Error::other("resource should exist"))?;
    let update = Transaction {
        compares: vec![kernel_store::Compare {
            key: resource_key.clone(),
            expected: ExpectedVersion::Exact(current.version),
        }],
        mutations: vec![Mutation::Put {
            key: resource_key.clone(),
            value: b"updated".to_vec(),
            session: None,
        }],
    };
    assert!(matches!(
        deduplicator
            .deduplicate(claim_key.clone(), fingerprint, b"accepted".to_vec(), update,)
            .await?,
        DedupOutcome::Committed { .. }
    ));
    assert_eq!(
        deduplicator
            .deduplicate(
                claim_key,
                fingerprint,
                b"ignored".to_vec(),
                create(resource_key.clone(), b"must-not-apply"),
            )
            .await?,
        DedupOutcome::Duplicate {
            response: b"accepted".to_vec()
        }
    );
    assert_eq!(
        store
            .get(&resource_key)
            .await?
            .ok_or_else(|| std::io::Error::other("resource should remain"))?
            .value,
        b"updated"
    );
    Ok(())
}

fn create(key: kernel_store::StoreKey, value: &[u8]) -> Transaction {
    Transaction {
        compares: vec![kernel_store::Compare {
            key: key.clone(),
            expected: ExpectedVersion::Missing,
        }],
        mutations: vec![Mutation::Put {
            key,
            value: value.to_vec(),
            session: None,
        }],
    }
}
