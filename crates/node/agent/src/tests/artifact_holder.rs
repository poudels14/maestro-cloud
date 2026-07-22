use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId};
use kernel_store::{InMemoryStore, Store, TokioClock};
use runtime::ArtifactDigest;

use crate::ArtifactHolderRegistry;

#[tokio::test]
async fn holder_registry_publishes_lists_and_removes_both_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let session = store.session(Duration::from_secs(30)).await?;
    let registry = registry(store.clone(), session.id())?;
    let digest = ArtifactDigest::new("registry.test/api@sha256:abc")?;

    registry.publish(&digest).await?;
    assert_eq!(registry.holders(&digest).await?.len(), 1);
    assert_eq!(registry.local_holders().await?.len(), 1);

    registry.remove(&digest).await?;
    assert!(registry.holders(&digest).await?.is_empty());
    assert!(registry.local_holders().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn holder_indexes_expire_with_the_shared_node_session()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let session = store.session(Duration::from_secs(30)).await?;
    let registry = registry(store.clone(), session.id())?;
    let digest = ArtifactDigest::new("sha256:def")?;

    registry.publish(&digest).await?;
    session.close().await?;
    assert!(registry.holders(&digest).await?.is_empty());
    assert!(registry.local_holders().await?.is_empty());
    Ok(())
}

fn registry(
    store: Arc<InMemoryStore>,
    session_id: kernel_store::SessionId,
) -> Result<ArtifactHolderRegistry, Box<dyn std::error::Error>> {
    Ok(ArtifactHolderRegistry::new(
        store,
        &ClusterId::new("artifact-holder-test")?,
        NodeId::new("node-1")?,
        session_id,
    ))
}
