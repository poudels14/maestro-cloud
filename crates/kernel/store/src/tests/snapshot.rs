use kernel_api::{ClusterId, ResourceKind, ResourceName, ResourceRevision};
use serde_json::{Value, json};

use super::memory::test_store;
use crate::{
    CasOutcome, ExpectedVersion, Keyspace, PutRequest, SnapshotError, Store, StoreKey,
    StoreSnapshotExt,
};

#[tokio::test]
async fn dump_decodes_builtins_and_preserves_open_resource_kinds()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _) = test_store();
    let cluster_id = ClusterId::new("snapshot-decode")?;
    let keys = Keyspace::new(&cluster_id);
    let tombstone_kind = ResourceKind::new("NodeTombstone")?;
    let tombstone_id = ResourceName::new("retired-node")?;
    let custom_kind = ResourceKind::new("Widget")?;
    let custom_id = ResourceName::new("custom-one")?;
    let tombstone = tombstone("retired-node");
    let custom = json!({"token": "preserved-value", "enabled": true});

    let tombstone_revision = put_json(
        &store,
        keys.resource(&tombstone_kind, &tombstone_id),
        &tombstone,
    )
    .await?;
    put_json(&store, keys.resource(&custom_kind, &custom_id), &custom).await?;

    let snapshot = store.dump(&cluster_id).await?;
    assert_eq!(snapshot.resource_count(), 2);
    assert_eq!(snapshot.node_tombstones.len(), 1);
    let decoded = snapshot
        .node_tombstones
        .first()
        .ok_or("decoded tombstone missing")?;
    assert_eq!(decoded.meta.id.as_str(), "retired-node");
    assert_eq!(decoded.meta.revision, tombstone_revision);
    assert_eq!(snapshot.unregistered_resources.len(), 1);
    let unregistered = snapshot
        .unregistered_resources
        .first()
        .ok_or("unregistered resource missing")?;
    assert_eq!(unregistered.kind, custom_kind);
    assert_eq!(unregistered.id, custom_id);
    assert_eq!(unregistered.value(), &custom);
    Ok(())
}

#[tokio::test]
async fn dump_rejects_key_payload_identity_disagreement() -> Result<(), Box<dyn std::error::Error>>
{
    let (store, _) = test_store();
    let cluster_id = ClusterId::new("snapshot-identity")?;
    let key = Keyspace::new(&cluster_id).resource(
        &ResourceKind::new("NodeTombstone")?,
        &ResourceName::new("key-node")?,
    );
    put_json(&store, key, &tombstone("payload-node")).await?;

    assert!(matches!(
        store.dump(&cluster_id).await,
        Err(SnapshotError::IdentityMismatch {
            key_id,
            payload_id,
            ..
        }) if key_id == "key-node" && payload_id == "payload-node"
    ));
    Ok(())
}

#[tokio::test]
async fn dump_rejects_noncanonical_resource_keys() -> Result<(), Box<dyn std::error::Error>> {
    let (store, _) = test_store();
    let cluster_id = ClusterId::new("snapshot-key")?;
    let malformed = StoreKey::from_backend(
        b"/maestro/clusters/snapshot-key/resources/NodeTombstone/node/extra",
    )?;
    put_json(&store, malformed, &tombstone("node")).await?;

    assert!(matches!(
        store.dump(&cluster_id).await,
        Err(SnapshotError::MalformedKey { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn normalized_snapshot_redacts_known_and_unregistered_secrets()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _) = test_store();
    let cluster_id = ClusterId::new("snapshot-redaction")?;
    let keys = Keyspace::new(&cluster_id);
    let webhook_kind = ResourceKind::new("Webhook")?;
    let custom_kind = ResourceKind::new("FutureSecret")?;
    put_json(
        &store,
        keys.resource(&webhook_kind, &ResourceName::new("alerts")?),
        &json!({
            "meta": {
                "id": "alerts",
                "revision": 91,
                "generation": 1,
                "deletionTimestamp": 1_721_500_000_000_i64
            },
            "spec": {
                "name": "alerts",
                "endpoint": "https://user:webhook-plaintext@example.invalid/hook",
                "events": ["deploymentTransition"],
                "categories": ["error"],
                "enabled": true,
                "format": "maestro",
                "signingSecret": "signing-plaintext"
            },
            "status": {
                "consecutiveFailures": 1,
                "retryAt": 1_721_500_100_000_i64
            }
        }),
    )
    .await?;
    put_json(
        &store,
        keys.resource(&custom_kind, &ResourceName::new("opaque")?),
        &json!({"credential": "custom-plaintext"}),
    )
    .await?;

    let snapshot = store.dump(&cluster_id).await?;
    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("custom-plaintext"));

    let normalized = snapshot.normalized()?;
    let encoded = serde_json::to_string(&normalized)?;
    assert!(!encoded.contains("webhook-plaintext"));
    assert!(!encoded.contains("signing-plaintext"));
    assert!(!encoded.contains("custom-plaintext"));
    let webhook = normalized
        .webhooks
        .first()
        .ok_or("normalized webhook missing")?;
    assert_eq!(webhook.pointer("/meta/revision"), Some(&Value::from(0)));
    assert_eq!(
        webhook.pointer("/meta/deletionTimestamp"),
        Some(&Value::String("<timestamp>".to_string()))
    );
    assert_eq!(
        webhook.pointer("/spec/signingSecret"),
        Some(&Value::String("[REDACTED]".to_string()))
    );
    let unregistered = normalized
        .unregistered_resources
        .first()
        .ok_or("normalized unregistered resource missing")?;
    assert_eq!(unregistered.payload, "[REDACTED]");
    Ok(())
}

fn tombstone(id: &str) -> Value {
    json!({
        "meta": {
            "id": id,
            "revision": 1,
            "generation": 1
        },
        "spec": {
            "hostAddress": "10.1.0.10",
            "role": "worker",
            "requestedAt": 1_721_500_000_000_i64
        },
        "status": {
            "removedAt": 1_721_500_001_000_i64
        }
    })
}

async fn put_json(
    store: &impl Store,
    key: StoreKey,
    value: &Value,
) -> Result<ResourceRevision, Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(value)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    match outcome {
        CasOutcome::Applied(stored) => Ok(stored.version.resource_revision()),
        CasOutcome::Conflict { .. } => {
            Err("isolated snapshot fixture key unexpectedly existed".into())
        }
    }
}
