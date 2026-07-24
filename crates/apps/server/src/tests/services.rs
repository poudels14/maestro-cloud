use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use kernel_api::ClusterId;
use kernel_store::{InMemoryStore, Keyspace, Store, TokioClock};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{request, service};

#[tokio::test]
async fn service_put_creates_updates_replays_and_rejects_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let cluster_id = ClusterId::new("service-writes")?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let spec = serde_json::to_value(service()?.spec)?;
    let create = json!({"spec": spec});

    let response = put(&server, "request-create", &create).await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(required(&decode(response).await?, "generation")?, &json!(1));
    let service_key = Keyspace::new(&cluster_id).resource(
        &kernel_api::ResourceKind::new("Service")?,
        &kernel_api::ResourceName::new("new-api")?,
    );
    let first = store
        .get(&service_key)
        .await?
        .ok_or("created Service is missing")?;
    assert!(String::from_utf8(first.value.clone())?.contains("database-password"));

    let replay = put(&server, "request-create", &create).await?;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(required(&decode(replay).await?, "generation")?, &json!(1));
    assert_eq!(
        store
            .get(&service_key)
            .await?
            .ok_or("replayed Service is missing")?
            .version,
        first.version
    );

    let mut collision = create.clone();
    collision
        .get_mut("spec")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| std::io::Error::other("Service spec is missing"))?
        .insert("version".to_string(), json!("2.0.0"));
    assert_eq!(
        put(&server, "request-create", &collision).await?.status(),
        StatusCode::CONFLICT
    );

    let fetched = request(&server, "/api/services/new-api", None).await?;
    assert_eq!(fetched.status(), StatusCode::OK);
    let fetched = decode(fetched).await?;
    let revision = required(required(&fetched, "meta")?, "revision")?.clone();
    assert!(!fetched.to_string().contains("database-password"));
    assert!(fetched.to_string().contains("••••word"));
    let unchanged = json!({
        "expectedRevision": revision.clone(),
        "spec": required(&create, "spec")?.clone()
    });
    let response = put(&server, "request-unchanged", &unchanged).await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(required(&decode(response).await?, "generation")?, &json!(1));
    assert_eq!(
        store
            .get(&service_key)
            .await?
            .ok_or("unchanged Service is missing")?
            .version,
        first.version
    );

    let update = json!({
        "expectedRevision": revision,
        "spec": required(&collision, "spec")?.clone()
    });
    let response = put(&server, "request-update", &update).await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(required(&decode(response).await?, "generation")?, &json!(2));

    assert_eq!(
        put(&server, "request-stale", &update).await?.status(),
        StatusCode::CONFLICT
    );
    Ok(())
}

#[tokio::test]
async fn service_put_requires_idempotency_and_semantically_valid_specs()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let server = ApiServer::new(
        store,
        ClusterId::new("service-validation")?,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let mut spec = serde_json::to_value(service()?.spec)?;
    let missing_key = put_optional(&server, None, &json!({"spec": spec.clone()})).await?;
    assert_eq!(missing_key.status(), StatusCode::BAD_REQUEST);

    spec.as_object_mut()
        .ok_or_else(|| std::io::Error::other("Service spec is not an object"))?
        .insert("exposedPorts".to_string(), json!([8080, 8080]));
    let invalid = put(&server, "request-invalid", &json!({"spec": spec})).await?;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn service_diff_returns_exact_revision_and_masks_changed_values()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let server = ApiServer::new(
        store,
        ClusterId::new("service-diff")?,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let mut spec = serde_json::to_value(service()?.spec)?;
    assert_eq!(
        put(&server, "diff-create", &json!({"spec": spec.clone()}))
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    let unchanged = diff(&server, &json!({"spec": spec.clone()})).await?;
    assert_eq!(required(&unchanged, "status")?, "unchanged");
    assert!(required(&unchanged, "expectedRevision")?.is_number());
    assert!(unchanged.get("changes").is_none());

    let spec_object = spec
        .as_object_mut()
        .ok_or_else(|| std::io::Error::other("Service spec is not an object"))?;
    spec_object.insert(
        "environment".to_string(),
        json!({"DATABASE_URL": "postgres://replacement"}),
    );
    spec_object.insert(
        "secrets".to_string(),
        json!({
            "format": "dotenv",
            "mountPath": "/run/secrets/service.env",
            "items": {"DATABASE_PASSWORD": "replacement-password"}
        }),
    );
    let changed = diff(&server, &json!({"spec": spec})).await?;
    assert_eq!(required(&changed, "status")?, "changed");
    let encoded = changed.to_string();
    assert!(encoded.contains("environment.DATABASE_URL"));
    assert!(encoded.contains("secrets.items.DATABASE_PASSWORD"));
    assert!(encoded.contains("••••ment"));
    assert!(encoded.contains("••••word"));
    assert!(!encoded.contains("postgres://replacement"));
    assert!(!encoded.contains("replacement-password"));
    Ok(())
}

async fn put(
    server: &ApiServer,
    idempotency_key: &str,
    payload: &Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    put_optional(server, Some(idempotency_key), payload).await
}

async fn put_optional(
    server: &ApiServer,
    idempotency_key: Option<&str>,
    payload: &Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    let mut request = Request::builder()
        .method("PUT")
        .uri("/api/services/new-api")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(idempotency_key) = idempotency_key {
        request = request.header("Idempotency-Key", idempotency_key);
    }
    Ok(server
        .router()
        .oneshot(request.body(Body::from(serde_json::to_vec(payload)?))?)
        .await?)
}

async fn diff(server: &ApiServer, payload: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = server
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/services/new-api/diff")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(payload)?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    decode(response).await
}

async fn decode(response: axum::response::Response) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(
        &response.into_body().collect().await?.to_bytes(),
    )?)
}

fn required<'a>(value: &'a Value, field: &str) -> Result<&'a Value, std::io::Error> {
    value
        .get(field)
        .ok_or_else(|| std::io::Error::other(format!("response field `{field}` is missing")))
}
