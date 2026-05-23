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
