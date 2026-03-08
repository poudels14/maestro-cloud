use super::*;

fn sample_config() -> &'static str {
    r#"{
      // cluster services
      "services": {
        "service-1": {
          /* service display name */
          "name": "Service One",
          "build": {
            "repo": "https://example.com/org/repo.git",
            "dockerfilePath": "Dockerfile"
          },
          "deploy": {
            "ports": ["8080:80", "8443:443"],
            "flags": ["--network=host"],
            "healthcheckPath": "/_healthy"
          }
        }
      }
    }"#
}

#[test]
fn parse_config_supports_jsonc() {
    let parsed = parse_config(sample_config()).expect("should parse");
    assert_eq!(parsed.services.len(), 1);
}

#[test]
fn patch_endpoint_adds_http_when_missing() {
    let endpoint = patch_endpoint("127.0.0.1:3000").expect("should build endpoint");
    assert_eq!(endpoint, "http://127.0.0.1:3000/api/services/patch");
}

#[test]
fn service_payload_uses_map_key_as_service_id() {
    let parsed = parse_config(sample_config()).expect("should parse");
    let template = parsed
        .services
        .get("service-1")
        .expect("service should exist");
    let payload = service_payload("service-1", template).expect("payload should build");
    assert_eq!(payload.id, "service-1");
    assert_eq!(payload.name, "Service One");
    assert_eq!(
        payload.build.as_ref().map(|build| build.repo.as_str()),
        Some("https://example.com/org/repo.git"),
    );
    assert_eq!(
        payload.deploy.ports,
        vec!["8080:80".to_string(), "8443:443".to_string()]
    );
    assert_eq!(payload.deploy.flags, vec!["--network=host".to_string()]);
}
