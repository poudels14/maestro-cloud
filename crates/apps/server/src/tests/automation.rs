use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use kernel_api::{
    Preview, PreviewId, PreviewPhase, PreviewSpec, PreviewStatus, ResourceKind, ResourceName,
    ResourceRevision, ServiceId, Timestamp, Webhook, WebhookFormat,
};
use kernel_store::{Keyspace, Store};
use serde_json::{Value, json};
use tower::ServiceExt;
use webhook::{WebhookDelivery, WebhookDeliveryBackend, WebhookDeliveryError};

use crate::{ApiServer, ServerSettings};

use super::{decode, metadata, put, request, seeded_store};

const SIGNING_SECRET: &str = "0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn preview_routes_expose_revisioned_source_status() -> Result<(), Box<dyn std::error::Error>>
{
    let (store, cluster_id) = seeded_store().await?;
    let preview = Preview {
        meta: metadata(PreviewId::new("owner-repo-42")?),
        spec: PreviewSpec {
            base_service_id: ServiceId::new("api")?,
            repository: "owner/repo".to_string(),
            pull_request_number: 42,
            title: "Add pagination".to_string(),
            head_revision: "abc123".to_string(),
            service_id: ServiceId::new("preview-api-42")?,
            close_grace_period_secs: 300,
            expires_at: Timestamp(10_000),
        },
        status: PreviewStatus {
            pull_request_state: kernel_api::PullRequestState::Open,
            phase: PreviewPhase::Active,
            teardown_at: None,
            conditions: Vec::new(),
        },
    };
    put(
        &store,
        &cluster_id,
        "Preview",
        preview.meta.id.as_str(),
        &preview,
    )
    .await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let listed: Vec<Preview> = decode(request(&server, "/api/previews", None).await?).await?;
    assert_eq!(listed.len(), 1);
    let listed_preview = listed.first().ok_or("preview is missing")?;
    assert_eq!(listed_preview.meta.id, preview.meta.id);
    assert!(listed_preview.meta.revision.0 > 0);
    let fetched: Preview =
        decode(request(&server, "/api/previews/owner-repo-42", None).await?).await?;
    assert_eq!(fetched.status.phase, PreviewPhase::Active);
    assert_eq!(fetched.meta.revision, listed_preview.meta.revision);
    Ok(())
}

#[tokio::test]
async fn webhook_writes_validate_mask_preserve_and_delete() -> Result<(), Box<dyn std::error::Error>>
{
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let invalid_endpoint = webhook_request(None, "http://hooks.example.test", Some(SIGNING_SECRET));
    assert_eq!(
        mutate(&server, Method::PUT, "invalid-endpoint", invalid_endpoint)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    let mut duplicate_events =
        webhook_request(None, "https://hooks.example.test", Some(SIGNING_SECRET));
    duplicate_events
        .as_object_mut()
        .ok_or("webhook request is not an object")?
        .insert(
            "events".to_string(),
            json!(["deploymentTransition", "deploymentTransition"]),
        );
    assert_eq!(
        mutate(&server, Method::PUT, "duplicate-events", duplicate_events)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    let weak_secret = webhook_request(None, "https://hooks.example.test", Some("short"));
    assert_eq!(
        mutate(&server, Method::PUT, "weak-secret", weak_secret)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );

    let create = webhook_request(
        None,
        "https://hooks.example.test/events",
        Some(SIGNING_SECRET),
    );
    let created = mutate(&server, Method::PUT, "create-webhook", create.clone()).await?;
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<Value>(created).await?,
        json!({"webhookId": "deployments", "generation": 1})
    );
    assert_eq!(
        mutate(&server, Method::PUT, "create-webhook", create)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        command(
            &server,
            Method::POST,
            "/api/webhooks/deployments/test",
            "unconfigured-webhook-test",
            json!({}),
        )
        .await?
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let response = request(&server, "/api/webhooks/deployments", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let fetched: Webhook = decode(response).await?;
    assert_eq!(fetched.spec.endpoint.expose(), "••••ents");
    assert_eq!(
        fetched
            .spec
            .signing_secret
            .as_ref()
            .map(kernel_api::SecretValue::expose),
        Some("••••cdef")
    );
    let listed: Vec<Webhook> = decode(request(&server, "/api/webhooks", None).await?).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed
            .first()
            .ok_or("webhook is missing")?
            .spec
            .signing_secret
            .as_ref()
            .map(kernel_api::SecretValue::expose),
        Some("••••cdef")
    );

    let update = webhook_request(
        Some(fetched.meta.revision),
        "https://hooks.example.test/v2/events",
        None,
    );
    let updated = mutate(&server, Method::PUT, "update-webhook", update).await?;
    assert_eq!(updated.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<Value>(updated)
            .await?
            .get("generation")
            .ok_or("generation is missing")?,
        &json!(2)
    );
    let current: Webhook =
        decode(request(&server, "/api/webhooks/deployments", None).await?).await?;
    assert_eq!(
        current
            .spec
            .signing_secret
            .as_ref()
            .map(kernel_api::SecretValue::expose),
        Some("••••cdef")
    );
    let stored = store
        .get(&Keyspace::new(&cluster_id).resource(
            &ResourceKind::new("Webhook")?,
            &ResourceName::new("deployments")?,
        ))
        .await?
        .ok_or("stored Webhook is missing")?;
    let stored: Webhook = serde_json::from_slice(&stored.value)?;
    assert_eq!(
        stored
            .spec
            .signing_secret
            .as_ref()
            .map(kernel_api::SecretValue::expose),
        Some(SIGNING_SECRET)
    );

    let mut no_op = webhook_request(
        Some(current.meta.revision),
        "https://hooks.example.test/v2/events",
        None,
    );
    no_op
        .as_object_mut()
        .ok_or("webhook request is not an object")?
        .remove("endpoint");
    assert_eq!(
        mutate(&server, Method::PUT, "no-op-webhook", no_op)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    let unchanged: Webhook =
        decode(request(&server, "/api/webhooks/deployments", None).await?).await?;
    assert_eq!(unchanged.meta.revision, current.meta.revision);

    let stale = ResourceRevision(unchanged.meta.revision.0.saturating_sub(1));
    assert_eq!(
        delete(&server, "stale-delete", stale).await?.status(),
        StatusCode::CONFLICT
    );
    let deleted = delete(&server, "delete-webhook", unchanged.meta.revision).await?;
    assert_eq!(deleted.status(), StatusCode::ACCEPTED);
    assert_eq!(
        request(&server, "/api/webhooks/deployments", None)
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        delete(&server, "delete-webhook", unchanged.meta.revision)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    Ok(())
}

#[tokio::test]
async fn slack_webhooks_preserve_legacy_controls_without_a_signing_secret()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let payload = json!({
        "name": "  Operations  ",
        "endpoint": "https://hooks.slack.test/services/example",
        "events": ["deploymentTransition", "nodeAvailability"],
        "categories": ["error"],
        "enabled": false,
        "format": "slack"
    });
    let mut unnamed = payload.clone();
    unnamed
        .as_object_mut()
        .ok_or("Slack webhook request is not an object")?
        .remove("name");
    assert_eq!(
        mutate(&server, Method::PUT, "unnamed-slack-webhook", unnamed)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );

    assert_eq!(
        mutate(&server, Method::PUT, "create-slack-webhook", payload)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    let webhook: Webhook =
        decode(request(&server, "/api/webhooks/deployments", None).await?).await?;
    assert_eq!(webhook.spec.name, "Operations");
    assert_eq!(webhook.spec.endpoint.expose(), "••••mple");
    assert_eq!(webhook.spec.format, WebhookFormat::Slack);
    assert_eq!(
        webhook.spec.categories,
        vec![kernel_api::WebhookCategory::Error]
    );
    assert!(!webhook.spec.enabled);
    assert!(webhook.spec.signing_secret.is_none());
    let stored = store
        .get(&Keyspace::new(&cluster_id).resource(
            &ResourceKind::new("Webhook")?,
            &ResourceName::new("deployments")?,
        ))
        .await?
        .ok_or("stored Slack webhook is missing")?;
    let stored: Webhook = serde_json::from_slice(&stored.value)?;
    assert_eq!(
        stored.spec.endpoint.expose(),
        "https://hooks.slack.test/services/example"
    );
    Ok(())
}

#[tokio::test]
async fn webhook_test_retries_failures_and_replays_success()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let backend = Arc::new(RecordingWebhookBackend::default());
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_webhook_backend(backend.clone());
    let create = webhook_request(
        None,
        "https://hooks.example.test/events",
        Some(SIGNING_SECRET),
    );
    assert_eq!(
        mutate(&server, Method::PUT, "create-test-webhook", create)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );

    backend.reject.store(true, Ordering::SeqCst);
    assert_eq!(
        command(
            &server,
            Method::POST,
            "/api/webhooks/deployments/test",
            "test-webhook",
            json!({}),
        )
        .await?
        .status(),
        StatusCode::BAD_GATEWAY
    );
    backend.reject.store(false, Ordering::SeqCst);
    let successful = command(
        &server,
        Method::POST,
        "/api/webhooks/deployments/test",
        "test-webhook",
        json!({}),
    )
    .await?;
    assert_eq!(successful.status(), StatusCode::OK);
    let successful: Value = decode(successful).await?;
    assert_eq!(successful.get("webhookId"), Some(&json!("deployments")));
    assert!(
        successful
            .get("deliveryId")
            .and_then(Value::as_str)
            .is_some()
    );

    let replayed = command(
        &server,
        Method::POST,
        "/api/webhooks/deployments/test",
        "test-webhook",
        json!({}),
    )
    .await?;
    assert_eq!(replayed.status(), StatusCode::OK);
    assert_eq!(decode::<Value>(replayed).await?, successful);
    let attempts = backend
        .attempts
        .lock()
        .map_err(|_| "webhook attempts lock was poisoned")?;
    assert_eq!(attempts.len(), 2);
    let first = attempts.first().ok_or("first webhook attempt is missing")?;
    let second = attempts.get(1).ok_or("second webhook attempt is missing")?;
    assert_eq!(first.delivery.delivery_id, second.delivery.delivery_id);
    assert!(second.delivery.test);
    assert_eq!(second.endpoint, "https://hooks.example.test/events");
    assert_eq!(second.format, WebhookFormat::Maestro);
    assert_eq!(second.signing_secret, SIGNING_SECRET);
    Ok(())
}

#[derive(Default)]
struct RecordingWebhookBackend {
    attempts: Mutex<Vec<RecordedWebhook>>,
    reject: AtomicBool,
}

struct RecordedWebhook {
    endpoint: String,
    format: WebhookFormat,
    signing_secret: String,
    delivery: WebhookDelivery,
}

#[async_trait]
impl WebhookDeliveryBackend for RecordingWebhookBackend {
    async fn deliver(
        &self,
        endpoint: &str,
        format: WebhookFormat,
        signing_secret: Option<&kernel_api::SecretValue>,
        delivery: &WebhookDelivery,
    ) -> Result<(), WebhookDeliveryError> {
        let signing_secret = signing_secret.ok_or_else(|| WebhookDeliveryError::Rejected {
            message: "test backend expected a signing secret".to_string(),
        })?;
        self.attempts
            .lock()
            .map_err(|_| WebhookDeliveryError::Unavailable {
                message: "webhook attempts lock was poisoned".to_string(),
            })?
            .push(RecordedWebhook {
                endpoint: endpoint.to_string(),
                format,
                signing_secret: signing_secret.expose().to_string(),
                delivery: delivery.clone(),
            });
        if self.reject.load(Ordering::SeqCst) {
            return Err(WebhookDeliveryError::Rejected {
                message: "receiver returned an unsuccessful status".to_string(),
            });
        }
        Ok(())
    }
}

fn webhook_request(
    expected_revision: Option<ResourceRevision>,
    endpoint: &str,
    signing_secret: Option<&str>,
) -> Value {
    let mut payload = serde_json::Map::from_iter([
        ("endpoint".to_string(), json!(endpoint)),
        (
            "events".to_string(),
            json!(["deploymentTransition", "nodeAvailability"]),
        ),
    ]);
    if let Some(revision) = expected_revision {
        payload.insert("expectedRevision".to_string(), json!(revision));
    }
    if let Some(signing_secret) = signing_secret {
        payload.insert("signingSecret".to_string(), json!(signing_secret));
    }
    Value::Object(payload)
}

async fn delete(
    server: &ApiServer,
    idempotency_key: &str,
    expected_revision: ResourceRevision,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    mutate(
        server,
        Method::DELETE,
        idempotency_key,
        json!({"expectedRevision": expected_revision}),
    )
    .await
}

async fn mutate(
    server: &ApiServer,
    method: Method,
    idempotency_key: &str,
    payload: Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    command(
        server,
        method,
        "/api/webhooks/deployments",
        idempotency_key,
        payload,
    )
    .await
}

async fn command(
    server: &ApiServer,
    method: Method,
    uri: &str,
    idempotency_key: &str,
    payload: Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(&payload)?))?,
        )
        .await?)
}
