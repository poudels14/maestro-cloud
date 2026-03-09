use super::*;
use crate::deployment::types::{Command, ServiceBuildConfig, ServiceDeployConfig, ServiceProvider};

fn sample_patch_request(id: &str, name: &str) -> PatchServiceRequest {
    PatchServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        provider: ServiceProvider::Docker,
        build: Some(ServiceBuildConfig {
            repo: "https://example.com/repo.git".to_string(),
            dockerfile_path: "./Dockerfile".to_string(),
        }),
        image: None,
        deploy: ServiceDeployConfig {
            flags: vec![],
            ports: vec![],
            command: Some(Command {
                command: "arc-deploy".to_string(),
                args: vec!["--prod".to_string()],
            }),
            healthcheck_path: Some("/_healthy".to_string()),
        },
    }
}

fn sample_patch_request_with_image(id: &str, name: &str, image: &str) -> PatchServiceRequest {
    PatchServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        provider: ServiceProvider::Docker,
        build: None,
        image: Some(image.to_string()),
        deploy: ServiceDeployConfig {
            flags: vec![],
            ports: vec![],
            command: Some(Command {
                command: "arc-deploy".to_string(),
                args: vec!["--prod".to_string()],
            }),
            healthcheck_path: Some("/_healthy".to_string()),
        },
    }
}

#[test]
fn build_service_config_is_deterministic() {
    let first = build_service_config(sample_patch_request("svc-1", "Service 1"))
        .expect("first hash should succeed");
    let second = build_service_config(sample_patch_request("svc-1", "Service 1"))
        .expect("second hash should succeed");

    assert_eq!(first.version, second.version);
    assert!(first.version.starts_with("cfg-"));
}

#[test]
fn build_service_config_changes_when_config_changes() {
    let original = build_service_config(sample_patch_request("svc-1", "Service 1"))
        .expect("hash should succeed");
    let changed = build_service_config(sample_patch_request("svc-1", "Service 1 Updated"))
        .expect("hash should succeed");

    assert_ne!(original.version, changed.version);
}

#[test]
fn build_service_config_accepts_image_without_build() {
    let config = build_service_config(sample_patch_request_with_image(
        "svc-1",
        "Service 1",
        "ghcr.io/org/service:1.2.3",
    ))
    .expect("hash should succeed");

    assert!(config.build.is_none());
    assert_eq!(config.image.as_deref(), Some("ghcr.io/org/service:1.2.3"),);
}

#[test]
fn build_service_config_rejects_missing_build_and_image() {
    let mut request = sample_patch_request("svc-1", "Service 1");
    request.build = None;
    request.image = None;

    let err = build_service_config(request).expect_err("should reject");
    assert!(err.contains("either `build` or `image`"));
}

#[test]
fn build_service_config_rejects_build_and_image_together() {
    let mut request = sample_patch_request("svc-1", "Service 1");
    request.image = Some("ghcr.io/org/service:1.2.3".to_string());

    let err = build_service_config(request).expect_err("should reject");
    assert!(err.contains("either `build` or `image`"));
}

#[test]
fn build_service_config_rejects_shell_provider_with_image() {
    let mut request = sample_patch_request_with_image("svc-1", "Service 1", "busybox:latest");
    request.provider = ServiceProvider::Shell;

    let err = build_service_config(request).expect_err("should reject");
    assert!(err.contains("shell provider"));
}

#[test]
fn validate_service_id_accepts_url_safe_chars() {
    assert!(validate_service_id("service-1", "id").is_ok());
    assert!(validate_service_id("service_2", "id").is_ok());
    assert!(validate_service_id("serviceABC123", "id").is_ok());
}

#[test]
fn validate_service_id_rejects_non_url_safe_chars() {
    let slash = validate_service_id("service/1", "id").expect_err("slash must be rejected");
    assert!(slash.contains("URL-safe"));

    let space = validate_service_id("service 1", "id").expect_err("space must be rejected");
    assert!(space.contains("URL-safe"));
}
