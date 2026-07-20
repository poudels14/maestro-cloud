use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, NodeInstanceId, ResourceKind, ResourceName};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, Mutation,
    PutRequest, SessionBinding, Store, Transaction, TransactionOutcome,
};

use crate::{ControllerError, FencedStore, LeaderIdentity, LeadershipToken};

#[tokio::test]
async fn stale_leaders_cannot_commit_after_the_leader_key_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
    let keys = Keyspace::new(&ClusterId::new("acceptance")?);
    let leader_key = keys.leader();
    let first_session = store.session(Duration::from_secs(30)).await?;
    let first_leader = store
        .put_cas(PutRequest {
            key: leader_key.clone(),
            value: b"first".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: first_session.id(),
            }),
        })
        .await?;
    let CasOutcome::Applied(first_leader) = first_leader else {
        return Err("first leader campaign should apply".into());
    };
    let token = LeadershipToken::from_campaign(
        an_identity("instance-1")?,
        first_session.id(),
        first_leader.version,
    );
    let fenced = FencedStore::new(store.clone(), leader_key.clone(), token);
    let service_kind = ResourceKind::new("Service")?;
    let first_key = keys.resource(&service_kind, &ResourceName::new("api")?);

    assert!(matches!(
        fenced
            .txn(Transaction {
                compares: Vec::new(),
                mutations: vec![Mutation::Put {
                    key: first_key,
                    value: b"desired".to_vec(),
                    session: None,
                }],
            })
            .await?,
        TransactionOutcome::Applied { .. }
    ));

    first_session.close().await?;
    let second_session = store.session(Duration::from_secs(30)).await?;
    let second_leader = store
        .put_cas(PutRequest {
            key: leader_key,
            value: b"second".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: second_session.id(),
            }),
        })
        .await?;
    assert!(matches!(second_leader, CasOutcome::Applied(_)));
    let stale_key = keys.resource(&service_kind, &ResourceName::new("stale-write")?);

    assert_eq!(
        fenced
            .txn(Transaction {
                compares: Vec::new(),
                mutations: vec![Mutation::Put {
                    key: stale_key.clone(),
                    value: b"must-not-commit".to_vec(),
                    session: None,
                }],
            })
            .await,
        Err(ControllerError::LeadershipLost)
    );
    assert!(store.get(&stale_key).await?.is_none());
    Ok(())
}

fn an_identity(instance_id: &str) -> Result<LeaderIdentity, kernel_api::InvalidIdentifier> {
    Ok(LeaderIdentity {
        node_id: NodeId::new("node-1")?,
        instance_id: NodeInstanceId::new(instance_id)?,
    })
}

struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        pending::<()>().await;
    }
}
