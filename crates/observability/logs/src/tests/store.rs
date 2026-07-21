use std::collections::BTreeMap;

use kernel_api::{ClusterId, NodeId, Timestamp};

use crate::{
    InMemoryLogStore, IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStore,
    LogStoreError, LogStream, OriginCursor,
};

#[tokio::test]
async fn in_memory_store_passes_the_shared_conformance_battery()
-> Result<(), Box<dyn std::error::Error>> {
    crate::conformance::check_log_store(&InMemoryLogStore::new()).await?;
    Ok(())
}

#[tokio::test]
async fn in_memory_store_is_atomic_idempotent_and_collision_safe()
-> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryLogStore::new();
    let entry = system_entry("first");

    assert_eq!(
        store.append(std::slice::from_ref(&entry)).await?.committed,
        1
    );
    assert_eq!(
        store
            .append(std::slice::from_ref(&entry))
            .await?
            .deduplicated,
        1
    );
    let mut collision = entry;
    collision.body = LogBody::Text("different".to_owned());
    assert!(matches!(
        store.append(&[collision]).await,
        Err(LogStoreError::Rejected { .. })
    ));
    assert_eq!(store.entries()?.len(), 1);
    Ok(())
}

fn system_entry(body: &str) -> IngestLogEntry {
    let cluster_id = ClusterId::new("cluster-1").unwrap();
    let node_id = NodeId::new("node-1").unwrap();
    IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("daemon".to_owned()),
            cursor: OriginCursor::new("1"),
        },
        observed_at: Timestamp(1),
        event_at: Timestamp(1),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id,
            node_id: Some(node_id),
            component: "daemon".to_owned(),
        },
        body: LogBody::Text(body.to_owned()),
        attributes: BTreeMap::new(),
    }
}
