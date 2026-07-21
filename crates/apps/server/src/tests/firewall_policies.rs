use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use kernel_api::{FirewallPolicy, ResourceRevision};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

#[tokio::test]
async fn firewall_policy_writes_validate_scope_and_use_optimistic_idempotent_lifecycle()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    assert_eq!(
        put_policy(
            &server,
            "invalid-subject",
            "bad-subject",
            policy_request(None, "hostInput", "service", "api", "10.0.0.0/8", 80, 80),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        put_policy(
            &server,
            "invalid-cidr",
            "bad-cidr",
            policy_request(None, "egress", "service", "api", "10.0.0.1/8", 80, 80),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        put_policy(
            &server,
            "invalid-port",
            "bad-port",
            policy_request(None, "egress", "service", "api", "10.0.0.0/8", 443, 80),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );

    let create = policy_request(None, "egress", "service", "api", "10.0.0.0/8", 443, 443);
    let created = put_policy(&server, "create-policy", "api-egress", create.clone()).await?;
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<Value>(created).await?,
        json!({"policyId": "api-egress", "generation": 1})
    );
    assert_eq!(
        put_policy(&server, "create-policy", "api-egress", create)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        put_policy(
            &server,
            "duplicate-scope",
            "other-egress",
            policy_request(None, "egress", "service", "api", "192.0.2.0/24", 80, 80),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );

    let current = get_policy(&server, "api-egress").await?;
    assert_eq!(
        put_policy(
            &server,
            "policy-no-op",
            "api-egress",
            policy_request(
                Some(current.meta.revision),
                "egress",
                "service",
                "api",
                "10.0.0.0/8",
                443,
                443,
            ),
        )
        .await?
        .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        get_policy(&server, "api-egress").await?.meta.revision,
        current.meta.revision
    );
    assert_eq!(
        put_policy(
            &server,
            "stale-update",
            "api-egress",
            policy_request(
                Some(ResourceRevision(current.meta.revision.0.saturating_sub(1))),
                "egress",
                "service",
                "api",
                "10.0.0.0/8",
                443,
                443,
            ),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );
    let mut update = policy_request(
        Some(current.meta.revision),
        "egress",
        "service",
        "api",
        "10.0.0.0/8",
        443,
        443,
    );
    update
        .get_mut("spec")
        .and_then(Value::as_object_mut)
        .ok_or("missing policy spec")?
        .insert("defaultVerdict".to_string(), json!("deny"));
    assert_eq!(
        put_policy(&server, "update-policy", "api-egress", update)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    let updated = get_policy(&server, "api-egress").await?;
    assert_eq!(updated.meta.generation.0, 2);

    assert_eq!(
        delete_policy(&server, "delete-policy", updated.meta.revision)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    let deleting = get_policy(&server, "api-egress").await?;
    assert!(deleting.meta.deletion_timestamp.is_some());
    let revision = deleting.meta.revision;
    assert_eq!(
        delete_policy(&server, "delete-policy-no-op", revision)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        get_policy(&server, "api-egress").await?.meta.revision,
        revision
    );
    Ok(())
}

fn policy_request(
    expected_revision: Option<ResourceRevision>,
    direction: &str,
    subject_type: &str,
    subject_id: &str,
    cidr: &str,
    port_start: u16,
    port_end: u16,
) -> Value {
    let mut request = json!({
        "spec": {
            "direction": direction,
            "subject": {"type": subject_type, "id": subject_id},
            "rules": [{
                "cidr": cidr,
                "protocol": "tcp",
                "ports": [{"start": port_start, "end": port_end}],
                "verdict": "allow"
            }],
            "defaultVerdict": "allow"
        }
    });
    if let Some(revision) = expected_revision
        && let Some(object) = request.as_object_mut()
    {
        object.insert("expectedRevision".to_string(), json!(revision));
    }
    request
}

async fn get_policy(
    server: &ApiServer,
    policy_id: &str,
) -> Result<FirewallPolicy, Box<dyn std::error::Error>> {
    decode(request(server, &format!("/api/firewall/policies/{policy_id}"), None).await?).await
}

async fn put_policy(
    server: &ApiServer,
    idempotency_key: &str,
    policy_id: &str,
    payload: Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    mutate(server, Method::PUT, policy_id, idempotency_key, payload).await
}

async fn delete_policy(
    server: &ApiServer,
    idempotency_key: &str,
    expected_revision: ResourceRevision,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    mutate(
        server,
        Method::DELETE,
        "api-egress",
        idempotency_key,
        json!({"expectedRevision": expected_revision}),
    )
    .await
}

async fn mutate(
    server: &ApiServer,
    method: Method,
    policy_id: &str,
    idempotency_key: &str,
    payload: Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(format!("/api/firewall/policies/{policy_id}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(&payload)?))?,
        )
        .await?)
}
