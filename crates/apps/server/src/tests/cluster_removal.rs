use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use cluster::{
    MemberActivation, StoreJoinTicket, StoreMember, StoreProvider, StoreProviderError,
    StoreRecovery, StoreRecoveryPermit, StoreRuntime, StoreStartMode,
};
use kernel_api::{
    ConditionReason, ConditionState, Node, NodeId, NodeRemovalResponse, NodeRemovalState,
    NodeTombstone, ResourceKind, ResourceName, ResourceRevision,
};
use kernel_store::{
    CasOutcome, DeleteRequest, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store,
};
use serde_json::json;
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, put, request, seeded_store};

struct RecordingProvider {
    removed: Mutex<Vec<NodeId>>,
}

#[async_trait]
impl StoreProvider for RecordingProvider {
    async fn start(
        &self,
        _mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        Err(unused())
    }

    async fn stage_member(
        &self,
        _member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
        Err(unused())
    }

    async fn activate_member(
        &self,
        _ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError> {
        Err(unused())
    }

    async fn remove_member(&self, node_id: &NodeId) -> Result<(), StoreProviderError> {
        self.removed
            .lock()
            .map_err(|_| unused())?
            .push(node_id.clone());
        Ok(())
    }

    async fn recover(
        &self,
        _permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError> {
        Err(unused())
    }
}

#[tokio::test]
async fn removal_drains_cleans_membership_tombstones_and_replays()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let provider = Arc::new(RecordingProvider {
        removed: Mutex::new(Vec::new()),
    });
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_store_provider(provider.clone());
    let initial = get_node(&server).await?;

    let first = remove(&server, "remove-start").await?;
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<NodeRemovalResponse>(first).await?.state,
        NodeRemovalState::Draining
    );
    let started = get_node(&server).await?;
    assert!(started.meta.revision > initial.meta.revision);
    assert_eq!(
        decode::<NodeRemovalResponse>(remove(&server, "remove-start").await?)
            .await?
            .state,
        NodeRemovalState::Draining
    );

    let ready = mark_drain_ready(&store, &cluster_id).await?;
    seed_assignment_and_generation(&store, &cluster_id).await?;
    assert_eq!(
        decode::<NodeRemovalResponse>(remove(&server, "remove-wait").await?)
            .await?
            .state,
        NodeRemovalState::Draining
    );
    assert!(
        provider
            .removed
            .lock()
            .map_err(|_| "lock poisoned")?
            .is_empty()
    );
    assert_eq!(
        restore(&server, "restore-during-removal", ready)
            .await?
            .status(),
        StatusCode::CONFLICT
    );

    remove_assignment_and_advance_generation(&store, &cluster_id).await?;
    seed_cleanup_targets(&store, &cluster_id).await?;
    let final_response = remove(&server, "remove-final").await?;
    assert_eq!(final_response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<NodeRemovalResponse>(final_response).await?,
        NodeRemovalResponse {
            node_id: NodeId::new("node-1")?,
            state: NodeRemovalState::Removed,
        }
    );
    assert_eq!(
        provider
            .removed
            .lock()
            .map_err(|_| "lock poisoned")?
            .as_slice(),
        [NodeId::new("node-1")?]
    );

    let keys = Keyspace::new(&cluster_id);
    let tombstone = store
        .get(&keys.node_tombstone(&NodeId::new("node-1")?))
        .await?
        .ok_or("node tombstone missing")?;
    let tombstone: NodeTombstone = serde_json::from_slice(&tombstone.value)?;
    assert_eq!(tombstone.spec.host_address.to_string(), "10.20.0.1");
    assert!(tombstone.status.removed_at >= tombstone.spec.requested_at);
    for key in cleanup_keys(&keys)? {
        assert!(store.get(&key).await?.is_none(), "cleanup left {key}");
    }
    assert_eq!(
        decode::<NodeRemovalResponse>(remove(&server, "remove-final").await?)
            .await?
            .state,
        NodeRemovalState::Removed
    );
    assert_eq!(
        decode::<NodeRemovalResponse>(remove(&server, "remove-after-tombstone").await?)
            .await?
            .state,
        NodeRemovalState::Removed
    );
    assert_eq!(
        provider.removed.lock().map_err(|_| "lock poisoned")?.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn removal_requires_matching_confirmation_and_a_store_owning_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let mismatched = server
        .router()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/api/cluster/nodes/node-1")
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "mismatched-removal")
                .body(Body::from(serde_json::to_vec(
                    &json!({"nodeId": "another-node"}),
                )?))?,
        )
        .await?;
    assert_eq!(mismatched.status(), StatusCode::BAD_REQUEST);

    assert_eq!(
        decode::<NodeRemovalResponse>(remove(&server, "unavailable-start").await?)
            .await?
            .state,
        NodeRemovalState::Draining
    );
    mark_drain_ready(&store, &cluster_id).await?;
    assert_eq!(
        remove(&server, "unavailable-finish").await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        store
            .get(&Keyspace::new(&cluster_id).node_tombstone(&NodeId::new("node-1")?))
            .await?
            .is_none()
    );
    Ok(())
}

async fn get_node(server: &ApiServer) -> Result<Node, Box<dyn std::error::Error>> {
    decode(request(server, "/api/cluster/nodes/node-1", None).await?).await
}

async fn remove(
    server: &ApiServer,
    idempotency_key: &str,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/api/cluster/nodes/node-1")
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(
                    &json!({"nodeId": "node-1"}),
                )?))?,
        )
        .await?)
}

async fn restore(
    server: &ApiServer,
    idempotency_key: &str,
    expected_revision: ResourceRevision,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    mutate(
        server,
        Method::POST,
        "/api/cluster/nodes/node-1/restore",
        idempotency_key,
        expected_revision,
    )
    .await
}

async fn mutate(
    server: &ApiServer,
    method: Method,
    uri: &str,
    idempotency_key: &str,
    expected_revision: ResourceRevision,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(
                    &json!({"expectedRevision": expected_revision}),
                )?))?,
        )
        .await?)
}

async fn mark_drain_ready(
    store: &InMemoryStore,
    cluster_id: &kernel_api::ClusterId,
) -> Result<ResourceRevision, Box<dyn std::error::Error>> {
    let key = Keyspace::new(cluster_id)
        .resource(&ResourceKind::new("Node")?, &ResourceName::new("node-1")?);
    let stored = store.get(&key).await?.ok_or("Node missing")?;
    let mut node: Node = serde_json::from_slice(&stored.value)?;
    let condition = node
        .status
        .conditions
        .iter_mut()
        .find(|condition| condition.condition_type == kernel_api::ConditionType::Draining)
        .ok_or("drain condition missing")?;
    condition.state = ConditionState::True;
    condition.reason = ConditionReason("PeerCopiesReady".to_string());
    let applied = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&node)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    match applied {
        CasOutcome::Applied(stored) => Ok(stored.version.resource_revision()),
        CasOutcome::Conflict { .. } => Err("drain readiness update conflicted".into()),
    }
}

async fn seed_assignment_and_generation(
    store: &InMemoryStore,
    cluster_id: &kernel_api::ClusterId,
) -> Result<(), Box<dyn std::error::Error>> {
    put(
        store,
        cluster_id,
        "Assignment",
        "assignment-node-1",
        &json!({
            "meta": {"id": "assignment-node-1", "revision": 0, "generation": 1},
            "spec": {
                "serviceId": "api",
                "deploymentId": "api-v1",
                "replicaIndex": 0,
                "nodeId": "node-1",
                "placementEpoch": 1,
                "workloadAddress": "172.22.1.10"
            },
            "status": {"phase": "running"}
        }),
    )
    .await?;
    store
        .put_cas(PutRequest {
            key: Keyspace::new(cluster_id).scheduler_generation(),
            value: b"generation-1".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    Ok(())
}

async fn remove_assignment_and_advance_generation(
    store: &InMemoryStore,
    cluster_id: &kernel_api::ClusterId,
) -> Result<(), Box<dyn std::error::Error>> {
    let keys = Keyspace::new(cluster_id);
    let assignment = keys.resource(
        &ResourceKind::new("Assignment")?,
        &ResourceName::new("assignment-node-1")?,
    );
    let stored = store.get(&assignment).await?.ok_or("assignment missing")?;
    store
        .delete_cas(DeleteRequest {
            key: assignment,
            expected: stored.version,
        })
        .await?;
    let generation_key = keys.scheduler_generation();
    let generation = store
        .get(&generation_key)
        .await?
        .ok_or("scheduler generation missing")?;
    store
        .put_cas(PutRequest {
            key: generation_key,
            value: b"generation-2".to_vec(),
            expected: ExpectedVersion::Exact(generation.version),
            session: None,
        })
        .await?;
    Ok(())
}

async fn seed_cleanup_targets(
    store: &InMemoryStore,
    cluster_id: &kernel_api::ClusterId,
) -> Result<(), Box<dyn std::error::Error>> {
    put(store, cluster_id, "NodeNetwork", "node-1", &json!({})).await?;
    put(store, cluster_id, "NodeFirewall", "node-1", &json!({})).await?;
    let keys = Keyspace::new(cluster_id);
    for key in [
        keys.node_liveness(&NodeId::new("node-1")?),
        keys.node_upgrade_command(&NodeId::new("node-1")?),
        keys.join_record(&NodeId::new("node-1")?),
    ] {
        store
            .put_cas(PutRequest {
                key,
                value: b"cleanup".to_vec(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
    }
    Ok(())
}

fn cleanup_keys(
    keys: &Keyspace,
) -> Result<Vec<kernel_store::StoreKey>, Box<dyn std::error::Error>> {
    let node_id = NodeId::new("node-1")?;
    let resource_name = ResourceName::new("node-1")?;
    Ok(vec![
        keys.resource(&ResourceKind::new("Node")?, &resource_name),
        keys.resource(&ResourceKind::new("NodeNetwork")?, &resource_name),
        keys.resource(&ResourceKind::new("NodeFirewall")?, &resource_name),
        keys.node_liveness(&node_id),
        keys.node_upgrade_command(&node_id),
        keys.join_record(&node_id),
        keys.node_removal(&node_id),
    ])
}

fn unused() -> StoreProviderError {
    StoreProviderError::InvalidConfiguration {
        reason: "unused test operation".to_string(),
    }
}
