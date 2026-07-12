use super::*;
use crate::deployment::types::{Command, ServiceBuildConfig, ServiceDeployConfig};
use crate::validation::validate_service_id;

fn sample_patch_request(id: &str, name: &str) -> RolloutServiceRequest {
    RolloutServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        build: Some(ServiceBuildConfig {
            repo: Some("https://example.com/repo.git".to_string()),
            branch: None,
            dockerfile: "./Dockerfile".to_string(),
            watch: false,
            registry: None,
            depot: None,
            env: Default::default(),
            secrets: Default::default(),
        }),
        image: None,
        deploy: ServiceDeployConfig {
            flags: vec![],
            expose_ports: vec![],
            command: Some(Command {
                command: "arc-deploy".to_string(),
                args: vec!["--prod".to_string()],
            }),
            healthcheck_path: Some("/_healthy".to_string()),
            replicas: 1,
            max_restarts: None,
            env: Default::default(),
            secrets: None,
            volumes: vec![],
            healthcheck_interval: 60,
        },
        ingress: None,
    }
}

fn sample_patch_request_with_image(id: &str, name: &str, image: &str) -> RolloutServiceRequest {
    RolloutServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        build: None,
        image: Some(image.to_string()),
        deploy: ServiceDeployConfig {
            flags: vec![],
            expose_ports: vec![],
            command: Some(Command {
                command: "arc-deploy".to_string(),
                args: vec!["--prod".to_string()],
            }),
            healthcheck_path: Some("/_healthy".to_string()),
            replicas: 1,
            max_restarts: None,
            env: Default::default(),
            secrets: None,
            volumes: vec![],
            healthcheck_interval: 60,
        },
        ingress: None,
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

#[test]
fn upgrade_accepts_only_a_higher_semantic_version() {
    let (current, target) = validate_upgrade_version("1.2.3", "1.3.0").expect("higher version");
    assert_eq!(current.to_string(), "1.2.3");
    assert_eq!(target.to_string(), "1.3.0");
}

#[test]
fn upgrade_rejects_an_equal_semantic_version() {
    assert!(matches!(
        validate_upgrade_version("1.2.3", "1.2.3"),
        Err(UpgradeVersionError::NotNewer { .. })
    ));
}

#[test]
fn upgrade_rejects_a_lower_semantic_version() {
    assert!(matches!(
        validate_upgrade_version("1.2.3", "1.2.2"),
        Err(UpgradeVersionError::NotNewer { .. })
    ));
}

#[test]
fn upgrade_rejects_a_malformed_semantic_version() {
    assert!(matches!(
        validate_upgrade_version("1.2.3", "release-next"),
        Err(UpgradeVersionError::InvalidTarget(_))
    ));
}
