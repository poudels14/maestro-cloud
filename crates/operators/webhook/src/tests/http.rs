use std::time::Duration;

use kernel_api::{
    ClusterId, DeploymentPhase, ResourceName, ResourceRevision, SecretValue, Timestamp,
    WebhookFormat, WebhookId, WebhookObservedState,
};

use crate::{HttpWebhookBackend, HttpWebhookBackendError, WebhookDelivery};

#[test]
fn http_backend_builds_bounded_signed_secret_safe_requests() {
    assert!(matches!(
        HttpWebhookBackend::new(Duration::ZERO),
        Err(HttpWebhookBackendError::ZeroTimeout)
    ));
    let backend = HttpWebhookBackend::new(Duration::from_secs(10)).unwrap();
    let delivery = WebhookDelivery::new(
        ClusterId::new("test-cluster").unwrap(),
        WebhookId::new("deployments").unwrap(),
        ResourceName::new("deployment-1").unwrap(),
        Some(WebhookObservedState::DeploymentTransition(
            DeploymentPhase::Queued,
        )),
        WebhookObservedState::DeploymentTransition(DeploymentPhase::Ready),
        ResourceRevision(42),
        Timestamp(10_000),
    )
    .unwrap();
    let secret = SecretValue::new("0123456789abcdef0123456789abcdef");
    let request = backend
        .request(
            "https://hooks.example.test/events",
            WebhookFormat::Maestro,
            Some(&secret),
            &delivery,
        )
        .unwrap()
        .build()
        .unwrap();

    assert_eq!(request.url().scheme(), "https");
    assert_eq!(request.headers()["x-maestro-event"], "deploymentTransition");
    assert_eq!(
        request.headers()["x-maestro-delivery"],
        delivery.delivery_id
    );
    let signature = request.headers()["x-maestro-signature-256"]
        .to_str()
        .unwrap();
    assert!(signature.starts_with("sha256="));
    assert_eq!(signature.len(), 71);
    let body = request.body().and_then(reqwest::Body::as_bytes).unwrap();
    let encoded: WebhookDelivery = serde_json::from_slice(body).unwrap();
    assert_eq!(
        encoded.text,
        "Maestro `test-cluster`: `deployment-1` transitioned from `queued` to `ready`."
    );
    assert!(
        !body
            .windows(secret.expose().len())
            .any(|part| part == secret.expose().as_bytes())
    );
}

#[test]
fn slack_requests_contain_only_text_and_no_maestro_signature() {
    let backend = HttpWebhookBackend::new(Duration::from_secs(10)).unwrap();
    let delivery = WebhookDelivery::new(
        ClusterId::new("test-cluster").unwrap(),
        WebhookId::new("slack-operations").unwrap(),
        ResourceName::new("node-1").unwrap(),
        None,
        WebhookObservedState::NodeAvailability(kernel_api::WebhookNodeAvailability::Unavailable),
        ResourceRevision(7),
        Timestamp(20_000),
    )
    .unwrap();
    let request = backend
        .request(
            "https://hooks.slack.test/services/example",
            WebhookFormat::Slack,
            None,
            &delivery,
        )
        .unwrap()
        .build()
        .unwrap();

    assert!(request.headers().get("x-maestro-event").is_none());
    assert!(request.headers().get("x-maestro-delivery").is_none());
    assert!(request.headers().get("x-maestro-signature-256").is_none());
    let body = request.body().and_then(reqwest::Body::as_bytes).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(body).unwrap(),
        serde_json::json!({"text": delivery.text})
    );
}
