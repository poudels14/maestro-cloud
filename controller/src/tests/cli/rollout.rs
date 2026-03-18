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
            "exposePorts": [8080, 8443],
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
fn normalize_base_url_adds_http_when_missing() {
    let base = normalize_base_url("127.0.0.1:3000").expect("should normalize");
    assert_eq!(base, "http://127.0.0.1:3000");
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
    assert_eq!(payload.deploy.expose_ports, vec![8080, 8443]);
    assert_eq!(payload.deploy.flags, vec!["--network=host".to_string()]);
}
