use super::*;

fn sample_manifest() -> &'static str {
    r#"{
      "id": "my-service",
      "name": "My Service",
      "build": {
        "dockerfile": "Dockerfile"
      },
      "deploy": {
        "healthcheckPath": "/_healthy",
        "replicas": 1
      }
    }"#
}

#[test]
fn parse_service_manifest_accepts_jsonc() {
    let manifest = parse_service_manifest(sample_manifest()).expect("parse");
    assert_eq!(manifest.id, "my-service");
    assert_eq!(manifest.name, "My Service");
    assert!(manifest.build.is_some());
}

#[test]
fn build_upload_payload_accepts_missing_repo() {
    let manifest = parse_service_manifest(sample_manifest()).expect("parse");
    let payload = build_upload_payload(manifest).expect("payload");
    assert_eq!(payload.id, "my-service");
    assert_eq!(payload.name, "My Service");
    assert!(
        payload
            .build
            .as_ref()
            .map(|build| build.repo.is_none())
            .unwrap_or(false),
        "build.repo should remain unset for uploads",
    );
}

#[test]
fn build_upload_payload_rejects_empty_id() {
    let raw = sample_manifest().replace("\"id\": \"my-service\"", "\"id\": \"\"");
    let manifest = parse_service_manifest(&raw).expect("parse");
    let err = build_upload_payload(manifest).expect_err("should reject empty id");
    let message = format!("{err}");
    assert!(message.contains("id"), "error mentions id: {message}");
}

#[tokio::test]
async fn up_rejects_a_build_without_registry_for_a_multinode_target() {
    let app = axum::Router::new().route(
        "/api/config",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({
                "cluster": { "nodes": { "node1": {}, "node2": {} } }
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let root = std::env::temp_dir().join(format!(
        "maestro-up-registry-test-{}",
        crate::utils::nanoid::unique_id(8)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let config = root.join("service.jsonc");
    std::fs::write(&config, sample_manifest()).unwrap();

    let error = run_up(&address.to_string(), &config, &root)
        .await
        .expect_err("multi-node upload should require build.registry");
    assert!(
        error
            .to_string()
            .contains("build.registry is required for services built in multi-node mode"),
        "unexpected error: {error}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(root);
}
