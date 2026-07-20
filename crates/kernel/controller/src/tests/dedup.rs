use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, NodeInstanceId, RequestId, ResourceKind, ResourceName};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, Mutation, PutRequest, SessionBinding,
    Store, Transaction,
};

use super::clock::NoopClock;
use crate::{
    ControllerError, DedupOutcome, FencedStore, LeaderIdentity, LeadershipToken, RequestFingerprint,
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
