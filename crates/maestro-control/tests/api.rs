mod support;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use maestro_control::{ApiState, StoreWriter, router};
use maestro_core::{DeploymentStatus, ServiceId};
use serde_json::{Value, json};
use support::{FakeObserved, LocalWriter};
use tower::ServiceExt;

fn app() -> (Arc<LocalWriter>, axum::Router) {
    let writer = Arc::new(LocalWriter::new());
    let state = Arc::new(ApiState {
        writer: writer.clone(),
        observed: Arc::new(FakeObserved::default()),
        key: Arc::new(maestro_store::SecretKey::from_bytes(&[3u8; 32])),
    });
    (writer, router(state))
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn services_file(image: &str) -> Value {
    json!({
        "services": {
            "api": { "name": "API", "image": image, "deploy": { "replicas": 2 } },
            "web": { "name": "Web", "image": "nginx:1.27" }
        }
    })
}

#[tokio::test]
async fn rollout_previews_then_applies_changes() {
    let (writer, app) = app();

    let (status, preview) = call(&app, "POST", "/api/rollout", Some(services_file("api:1"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(preview["created"], json!(["api", "web"]));
    assert_eq!(preview["applied"], false);
    assert!(writer.state().services().unwrap().is_empty());

    let (_, applied) = call(
        &app,
        "POST",
        "/api/rollout?apply=true",
        Some(services_file("api:1")),
    )
    .await;
    assert_eq!(applied["deployments"].as_array().unwrap().len(), 2);
    assert_eq!(
        writer
            .state()
            .deployments_with_status(DeploymentStatus::Queued)
            .unwrap()
            .len(),
        2
    );

    let (_, second) = call(
        &app,
        "POST",
        "/api/rollout?apply=true",
        Some(services_file("api:2")),
    )
    .await;
    assert_eq!(second["updated"], json!(["api"]));
    assert_eq!(second["unchanged"], json!(["web"]));
    assert_eq!(
        writer
            .state()
            .deployments(&ServiceId::from("api"))
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn rejects_invalid_specs() {
    let (_, app) = app();
    let (status, body) = call(
        &app,
        "PUT",
        "/api/services/api",
        Some(json!({ "name": "API", "image": "nginx", "build": { "dockerfile": "Dockerfile" } })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("either image or build")
    );
}

#[tokio::test]
async fn queues_and_lists_deployments() {
    let (_, app) = app();
    let (status, _) = call(&app, "POST", "/api/services/api/deployments", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    call(
        &app,
        "PUT",
        "/api/services/api",
        Some(json!({ "name": "API", "image": "nginx" })),
    )
    .await;
    let (status, queued) = call(&app, "POST", "/api/services/api/deployments", None).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let deployment_id = queued["deploymentId"].as_str().unwrap().to_string();
    assert!(deployment_id.starts_with('d'));

    let (_, deployments) = call(&app, "GET", "/api/services/api/deployments", None).await;
    assert_eq!(deployments[0]["id"], deployment_id);
    assert_eq!(deployments[0]["status"], "queued");
}

#[tokio::test]
async fn deleting_service_cancels_its_deployments() {
    let (writer, app) = app();
    call(
        &app,
        "POST",
        "/api/rollout?apply=true",
        Some(services_file("api:1")),
    )
    .await;

    let (status, _) = call(&app, "DELETE", "/api/services/api", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        writer
            .state()
            .service(&ServiceId::from("api"))
            .unwrap()
            .is_none()
    );
    let (status, _) = call(&app, "DELETE", "/api/services/api", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn seals_env_values_and_masks_responses() {
    let (writer, app) = app();
    let file = json!({
        "services": {
            "api": {
                "name": "API",
                "image": "api:1",
                "deploy": { "env": { "items": { "DB_PASSWORD": "hunter2" } } }
            }
        }
    });
    call(&app, "POST", "/api/rollout?apply=true", Some(file.clone())).await;

    let stored = writer
        .state()
        .service(&ServiceId::from("api"))
        .unwrap()
        .unwrap();
    let sealed = &stored.deploy.env.items["DB_PASSWORD"];
    assert!(sealed.starts_with("enc:v1:"));

    let (_, services) = call(&app, "GET", "/api/services", None).await;
    assert_eq!(
        services[0]["spec"]["deploy"]["env"]["items"]["DB_PASSWORD"],
        "********"
    );
    assert_eq!(
        services[0]["latestDeployment"]["spec"]["deploy"]["env"]["items"]["DB_PASSWORD"],
        "********"
    );

    let (_, again) = call(&app, "POST", "/api/rollout", Some(file)).await;
    assert_eq!(again["unchanged"], json!(["api"]));
}

#[tokio::test]
async fn token_middleware_guards_everything_but_health() {
    use std::time::Duration;

    use maestro_control::{ApiAuth, require_token};

    let (_, app) = app();
    let auth = Arc::new(ApiAuth::new(b"cluster-secret-for-tests-0000000"));
    let guarded = app.layer(axum::middleware::from_fn_with_state(
        auth.clone(),
        require_token,
    ));

    let (status, _) = call(&guarded, "GET", "/api/health", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&guarded, "GET", "/api/services", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let token = auth.issue("cli", Duration::from_secs(60));
    let request = Request::builder()
        .uri("/api/services")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = guarded.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn returns_service_detail_and_scales_with_a_new_deployment() {
    let (writer, app) = app();
    call(
        &app,
        "POST",
        "/api/rollout?apply=true",
        Some(services_file("api:1")),
    )
    .await;

    let (status, detail) = call(&app, "GET", "/api/services/api", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["spec"]["deploy"]["replicas"], 2);
    assert_eq!(detail["deployments"].as_array().unwrap().len(), 1);

    let (status, queued) = call(
        &app,
        "POST",
        "/api/services/api/scale",
        Some(json!({ "replicas": 5 })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(queued["deploymentId"].is_string());
    let stored = writer
        .state()
        .service(&ServiceId::from("api"))
        .unwrap()
        .unwrap();
    assert_eq!(stored.deploy.replicas, 5);
    assert_eq!(
        writer
            .state()
            .deployments(&ServiceId::from("api"))
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn cancels_only_in_flight_deployments() {
    let (writer, app) = app();
    call(
        &app,
        "POST",
        "/api/rollout?apply=true",
        Some(services_file("api:1")),
    )
    .await;
    let deployment = writer.state().deployments(&ServiceId::from("api")).unwrap()[0]
        .id
        .to_string();
    let path = format!("/api/services/api/deployments/{deployment}/cancel");

    let (status, _) = call(&app, "POST", &path, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body) = call(&app, "POST", &path, None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("can no longer be cancelled")
    );
    let (status, _) = call(
        &app,
        "POST",
        "/api/services/api/deployments/missing/cancel",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn lists_routes_and_node_summaries() {
    let (_, app) = app();
    call(
        &app,
        "POST",
        "/api/rollout?apply=true",
        Some(services_file("api:1")),
    )
    .await;
    let (status, routes) = call(&app, "GET", "/api/routes", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(routes["routes"].is_object());
    let (status, nodes) = call(&app, "GET", "/api/nodes", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(nodes, json!([]));
}
