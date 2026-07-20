use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, NodeInstanceId};
use kernel_store::{InMemoryStore, Keyspace};

use super::clock::NoopClock;
use crate::{
    ControllerError, LeaderElector, LeaderIdentity, LeadershipObservation, StoreLeaderElector,
};

#[tokio::test]
async fn store_elector_campaigns_observes_and_hands_off_without_deleting_a_successor()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
    let leader_key = Keyspace::new(&ClusterId::new("acceptance")?).leader();
    let elector = StoreLeaderElector::new(store, leader_key);
    let first_identity = an_identity("node-1", "instance-1")?;
    let second_identity = an_identity("node-2", "instance-2")?;

    assert_eq!(elector.observe().await?, LeadershipObservation::Vacant);
    let first = elector
        .campaign(first_identity.clone(), Duration::from_secs(15))
        .await?
        .ok_or_else(|| std::io::Error::other("first candidate should acquire leadership"))?;
    assert_eq!(
        elector.observe().await?,
        LeadershipObservation::Leader(first_identity)
    );
    assert!(
        elector
            .campaign(second_identity.clone(), Duration::from_secs(15))
            .await?
            .is_none()
    );

    first.keep_alive().await?;
    first.resign().await?;
    let second = elector
        .campaign(second_identity.clone(), Duration::from_secs(15))
        .await?
        .ok_or_else(|| std::io::Error::other("second candidate should acquire leadership"))?;
    assert_eq!(
        elector.observe().await?,
        LeadershipObservation::Leader(second_identity)
    );
    assert_eq!(first.resign().await, Err(ControllerError::LeadershipLost));
    assert!(matches!(
        elector.observe().await?,
        LeadershipObservation::Leader(_)
    ));
    second.resign().await?;
    assert_eq!(elector.observe().await?, LeadershipObservation::Vacant);
    Ok(())
}

fn an_identity(
    node_id: &str,
    instance_id: &str,
) -> Result<LeaderIdentity, kernel_api::InvalidIdentifier> {
    Ok(LeaderIdentity {
        node_id: NodeId::new(node_id)?,
        instance_id: NodeInstanceId::new(instance_id)?,
    })
}
