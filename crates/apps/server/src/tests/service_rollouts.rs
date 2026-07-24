use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use kernel_api::{
    ClusterId, CommandRequest, FirewallDirection, FirewallPolicy, FirewallPolicySpec, FirewallRule,
    FirewallSubject, FirewallVerdict, Generation, IngressRoute, IngressRouteSpec,
    IngressRouteStatus, Object, ObjectMeta, PortRange, ResourceRevision, RolloutState, Service,
    ServiceDiffStatus, ServiceId, ServiceRolloutDiffRequest, ServiceRolloutDiffResponse,
    ServiceRolloutRequest, ServiceRolloutResponse, ServiceRolloutSpec, TransportProtocol,
};
use kernel_store::{ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::service;

#[tokio::test]
async fn declarative_rollout_atomically_creates_updates_and_removes_managed_resources()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("atomic-rollout")?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let desired = desired()?;
    let preview = diff(&server, desired.clone()).await?;
    assert_eq!(preview.status, ServiceDiffStatus::New);
    assert_eq!(preview.expected_revisions, Default::default());

    let request = ServiceRolloutRequest {
        expected_revisions: preview.expected_revisions,
        force: false,
        desired: desired.clone(),
    };
    let applied = apply(&server, "atomic-create", &request).await?;
    assert_eq!(applied.status(), StatusCode::ACCEPTED);
    let accepted: ServiceRolloutResponse = decode(applied).await?;
    assert_eq!(accepted.service_generation.0, 1);
    assert_eq!(accepted.ingress_generation.map(|value| value.0), Some(1));
    assert_eq!(accepted.egress_generation.map(|value| value.0), Some(1));

    let keys = Keyspace::new(&cluster_id);
    let service: Service = stored(&store, &keys, "Service", "api").await?;
    let route: IngressRoute = stored(&store, &keys, "IngressRoute", "api-ingress").await?;
    let policy: FirewallPolicy = stored(&store, &keys, "FirewallPolicy", "api-egress").await?;
    assert_eq!(route.spec.service_id, service.meta.id);
    assert_eq!(
        policy.spec.subject,
        FirewallSubject::Service(service.meta.id)
    );
    assert_eq!(route.meta.owner_refs.len(), 1);
    assert_eq!(policy.meta.owner_refs.len(), 1);

    let replay = apply(&server, "atomic-create", &request).await?;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(decode::<ServiceRolloutResponse>(replay).await?, accepted);
    assert_eq!(
        apply(&server, "stale-create", &request).await?.status(),
        StatusCode::CONFLICT
    );

    let unchanged = diff(&server, desired.clone()).await?;
    assert_eq!(unchanged.status, ServiceDiffStatus::Unchanged);
    let unchanged_response = apply(
        &server,
        "atomic-unchanged",
        &ServiceRolloutRequest {
            expected_revisions: unchanged.expected_revisions,
            force: false,
            desired: desired.clone(),
        },
    )
    .await?;
    assert_eq!(unchanged_response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<ServiceRolloutResponse>(unchanged_response).await?,
        accepted
    );
    assert_eq!(
        stored::<Service>(&store, &keys, "Service", "api")
            .await?
            .meta
            .revision,
        service.meta.revision
    );
    assert_eq!(
        stored::<IngressRoute>(&store, &keys, "IngressRoute", "api-ingress")
            .await?
            .meta
            .revision,
        route.meta.revision
    );
    assert_eq!(
        stored::<FirewallPolicy>(&store, &keys, "FirewallPolicy", "api-egress")
            .await?
            .meta
            .revision,
        policy.meta.revision
    );

    let mut updated = desired;
    updated.service.replicas += 1;
    updated
        .ingress
        .as_mut()
        .ok_or("missing desired ingress")?
        .hosts = vec!["next.api.example.test".to_string()];
    updated
        .egress
        .as_mut()
        .and_then(|policy| policy.rules.first_mut())
        .ok_or("missing desired egress rule")?
        .cidr = "192.0.2.0/24".to_string();
    let update = diff(&server, updated.clone()).await?;
    assert_eq!(update.status, ServiceDiffStatus::Changed);
    let update_response = apply(
        &server,
        "atomic-update",
        &ServiceRolloutRequest {
            expected_revisions: update.expected_revisions,
            force: false,
            desired: updated.clone(),
        },
    )
    .await?;
    assert_eq!(update_response.status(), StatusCode::ACCEPTED);
    let updated_response: ServiceRolloutResponse = decode(update_response).await?;
    assert_eq!(updated_response.service_generation.0, 2);
    assert_eq!(
        updated_response.ingress_generation.map(|value| value.0),
        Some(2)
    );
    assert_eq!(
        updated_response.egress_generation.map(|value| value.0),
        Some(2)
    );
    assert_eq!(
        stored::<Service>(&store, &keys, "Service", "api")
            .await?
            .spec,
        updated.service
    );
    assert_eq!(
        stored::<IngressRoute>(&store, &keys, "IngressRoute", "api-ingress")
            .await?
            .spec,
        updated.ingress.clone().ok_or("missing updated ingress")?
    );
    assert_eq!(
        stored::<FirewallPolicy>(&store, &keys, "FirewallPolicy", "api-egress")
            .await?
            .spec,
        updated.egress.clone().ok_or("missing updated egress")?
    );
    let without_auxiliary = ServiceRolloutSpec {
        ingress: None,
        egress: None,
        ..updated
    };
    let removal = diff(&server, without_auxiliary.clone()).await?;
    assert_eq!(removal.status, ServiceDiffStatus::Changed);
    assert!(
        removal
            .changes
            .iter()
            .any(|change| change.field == "ingress")
    );
    assert!(
        removal
            .changes
            .iter()
            .any(|change| change.field == "egress")
    );
    let removed = apply(
        &server,
        "remove-auxiliary",
        &ServiceRolloutRequest {
            expected_revisions: removal.expected_revisions,
            force: false,
            desired: without_auxiliary,
        },
    )
    .await?;
    assert_eq!(removed.status(), StatusCode::ACCEPTED);
    assert!(missing(&store, &keys, "IngressRoute", "api-ingress").await?);
    assert!(missing(&store, &keys, "FirewallPolicy", "api-egress").await?);
    assert!(!missing(&store, &keys, "Service", "api").await?);
    Ok(())
}

#[tokio::test]
async fn rollout_policy_honors_freeze_and_bypasses_exactly_one_forced_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("forced-rollout")?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let initial = desired()?;
    let create = diff(&server, initial.clone()).await?;
    assert_eq!(
        apply(
            &server,
            "forced-create",
            &ServiceRolloutRequest {
                expected_revisions: create.expected_revisions,
                force: false,
                desired: initial.clone(),
            },
        )
        .await?
        .status(),
        StatusCode::ACCEPTED
    );

    let current = diff(&server, initial.clone()).await?;
    assert_eq!(
        freeze(
            &server,
            current
                .expected_revisions
                .service
                .ok_or("service revision missing")?,
        )
        .await?
        .status(),
        StatusCode::ACCEPTED
    );

    let mut updated = initial;
    updated.service.version = "2.0.0".to_string();
    let preview = diff(&server, updated.clone()).await?;
    assert_eq!(
        apply(
            &server,
            "frozen-update",
            &ServiceRolloutRequest {
                expected_revisions: preview.expected_revisions,
                force: false,
                desired: updated.clone(),
            },
        )
        .await?
        .status(),
        StatusCode::ACCEPTED
    );

    let service: Service = stored(&store, &Keyspace::new(&cluster_id), "Service", "api").await?;
    assert_eq!(service.status.rollout, RolloutState::Frozen);
    assert_eq!(service.status.rollout_bypass_generation, None);
    assert_eq!(service.meta.generation, Generation(2));

    updated.service.version = "3.0.0".to_string();
    let preview = diff(&server, updated.clone()).await?;
    assert_eq!(
        apply(
            &server,
            "forced-update",
            &ServiceRolloutRequest {
                expected_revisions: preview.expected_revisions,
                force: true,
                desired: updated,
            },
        )
        .await?
        .status(),
        StatusCode::ACCEPTED
    );

    let service: Service = stored(&store, &Keyspace::new(&cluster_id), "Service", "api").await?;
    assert_eq!(
        service.status.rollout_bypass_generation,
        Some(service.meta.generation)
    );
    assert_eq!(service.meta.generation, Generation(3));
    Ok(())
}

#[tokio::test]
async fn declarative_rollout_rejects_reserved_resource_identity_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("rollout-collision")?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let desired = desired()?;
    let collision = Object {
        meta: ObjectMeta {
            id: kernel_api::IngressRouteId::new("api-ingress")?,
            labels: Default::default(),
            annotations: Default::default(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: Default::default(),
            deletion_timestamp: None,
        },
        spec: desired.ingress.clone().ok_or("missing desired ingress")?,
        status: IngressRouteStatus {
            applied_generation: Generation::default(),
            conditions: Vec::new(),
        },
    };
    let keys = Keyspace::new(&cluster_id);
    let key = keys.resource(
        &kernel_api::ResourceKind::new("IngressRoute")?,
        &kernel_api::ResourceName::new("api-ingress")?,
    );
    store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&collision)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;

    assert_eq!(
        diff_response(&server, desired).await?.status(),
        StatusCode::CONFLICT
    );
    Ok(())
}

fn desired() -> Result<ServiceRolloutSpec, Box<dyn std::error::Error>> {
    let service_id = ServiceId::new("api")?;
    Ok(ServiceRolloutSpec {
        service: service()?.spec,
        ingress: Some(IngressRouteSpec {
            service_id: service_id.clone(),
            hosts: vec!["api.example.test".to_string()],
            path_prefix: None,
            target_port: 8080,
            session_affinity: None,
        }),
        egress: Some(FirewallPolicySpec {
            direction: FirewallDirection::Egress,
            subject: FirewallSubject::Service(service_id),
            rules: vec![FirewallRule {
                cidr: "10.0.0.0/8".to_string(),
                protocol: TransportProtocol::Tcp,
                ports: vec![PortRange {
                    start: 443,
                    end: 443,
                }],
                verdict: FirewallVerdict::Allow,
            }],
            default_verdict: FirewallVerdict::Deny,
        }),
    })
}

async fn diff(
    server: &ApiServer,
    desired: ServiceRolloutSpec,
) -> Result<ServiceRolloutDiffResponse, Box<dyn std::error::Error>> {
    let response = diff_response(server, desired).await?;
    assert_eq!(response.status(), StatusCode::OK);
    decode(response).await
}

async fn diff_response(
    server: &ApiServer,
    desired: ServiceRolloutSpec,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/services/api/rollout/diff")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(
                    &ServiceRolloutDiffRequest { desired },
                )?))?,
        )
        .await?)
}

async fn apply(
    server: &ApiServer,
    request_id: &str,
    payload: &ServiceRolloutRequest,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/services/api/rollout")
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", request_id)
                .body(Body::from(serde_json::to_vec(payload)?))?,
        )
        .await?)
}

async fn freeze(
    server: &ApiServer,
    expected_revision: ResourceRevision,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/services/api/freeze")
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "forced-freeze")
                .body(Body::from(serde_json::to_vec(&CommandRequest {
                    expected_revision,
                })?))?,
        )
        .await?)
}

async fn decode<Response: serde::de::DeserializeOwned>(
    response: axum::response::Response,
) -> Result<Response, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(
        &response.into_body().collect().await?.to_bytes(),
    )?)
}

async fn stored<Resource: serde::de::DeserializeOwned>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
    id: &str,
) -> Result<Resource, Box<dyn std::error::Error>> {
    let key = keys.resource(
        &kernel_api::ResourceKind::new(kind)?,
        &kernel_api::ResourceName::new(id)?,
    );
    let value = store.get(&key).await?.ok_or("resource is missing")?;
    Ok(serde_json::from_slice(&value.value)?)
}

async fn missing(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
    id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let key = keys.resource(
        &kernel_api::ResourceKind::new(kind)?,
        &kernel_api::ResourceName::new(id)?,
    );
    Ok(store.get(&key).await?.is_none())
}
