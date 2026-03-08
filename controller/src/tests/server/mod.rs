use super::*;
use crate::service::{ArcCommand, ServiceBuildConfig, ServiceDeployConfig};
use base64::engine::general_purpose::STANDARD;

fn sample_patch_request(id: &str, name: &str) -> PatchServiceRequest {
    PatchServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        build: Some(ServiceBuildConfig {
            repo: "https://example.com/repo.git".to_string(),
            dockerfile_path: "./Dockerfile".to_string(),
        }),
        image: None,
        deploy: ServiceDeployConfig {
            command: Some(ArcCommand {
                command: "arc-deploy".to_string(),
                args: vec!["--prod".to_string()],
            }),
            healthcheck_path: "/_healthy".to_string(),
        },
    }
}

fn sample_patch_request_with_image(id: &str, name: &str, image: &str) -> PatchServiceRequest {
    PatchServiceRequest {
        id: id.to_string(),
        name: name.to_string(),
        build: None,
        image: Some(image.to_string()),
        deploy: ServiceDeployConfig {
            command: Some(ArcCommand {
                command: "arc-deploy".to_string(),
                args: vec!["--prod".to_string()],
            }),
            healthcheck_path: "/_healthy".to_string(),
        },
    }
}

fn sample_service_config(id: &str) -> ServiceConfig {
    ServiceConfig {
        id: id.to_string(),
        name: format!("{id}-name"),
        version: "v1".to_string(),
        build: Some(ServiceBuildConfig {
            repo: "https://example.com/repo.git".to_string(),
            dockerfile_path: "./Dockerfile".to_string(),
        }),
        image: None,
        deploy: ServiceDeployConfig {
            command: Some(ArcCommand {
                command: "arc-deploy".to_string(),
                args: vec!["--prod".to_string()],
            }),
            healthcheck_path: "/_healthy".to_string(),
        },
    }
}

#[test]
fn compare_counter_for_missing_uses_version_check() {
    let compare = compare_counter(
        "/maetro/services/svc-1/deployments/history-next-index",
        &CounterSnapshot {
            next_index: 0,
            mod_revision: None,
        },
    );

    assert_eq!(compare["target"], "VERSION");
    assert_eq!(compare["version"], "0");
}

#[test]
fn compare_counter_for_existing_uses_mod_revision_check() {
    let compare = compare_counter(
        "/maetro/services/svc-1/deployments/history-next-index",
        &CounterSnapshot {
            next_index: 12,
            mod_revision: Some(99),
        },
    );

    assert_eq!(compare["target"], "MOD");
    assert_eq!(compare["mod_revision"], "99");
}

#[test]
fn prefix_range_end_advances_prefix() {
    let prefix = b"/maetro/services/svc-1/deployments/history/";
    let end = prefix_range_end(prefix).expect("should compute range end");
    assert!(end.as_slice() > prefix.as_slice());
}

#[test]
fn normalize_base_url_adds_http_and_trims_slash() {
    let url = normalize_base_url("127.0.0.1:2379/").expect("should normalize");
    assert_eq!(url, "http://127.0.0.1:2379");
}

#[test]
fn extract_service_configs_filters_and_sorts_by_id() {
    let config_b = sample_service_config("svc-b");
    let config_a = sample_service_config("svc-a");

    let kvs = vec![
        RangeKv {
            key: STANDARD.encode(format!("{SERVICES_ROOT}/svc-b/config").as_bytes()),
            value: STANDARD.encode(
                serde_json::to_string(&config_b)
                    .expect("serialize config")
                    .as_bytes(),
            ),
            mod_revision: "1".to_string(),
        },
        RangeKv {
            key: STANDARD.encode(format!("{SERVICES_ROOT}/svc-b/deployments/history/1").as_bytes()),
            value: STANDARD.encode(b"{}"),
            mod_revision: "1".to_string(),
        },
        RangeKv {
            key: STANDARD.encode(format!("{SERVICES_ROOT}/svc-a/config").as_bytes()),
            value: STANDARD.encode(
                serde_json::to_string(&config_a)
                    .expect("serialize config")
                    .as_bytes(),
            ),
            mod_revision: "1".to_string(),
        },
    ];

    let services = extract_service_configs(kvs).expect("extract service configs");
    assert_eq!(services.len(), 2);
    assert_eq!(services[0].id, "svc-a");
    assert_eq!(services[1].id, "svc-b");
}

#[test]
fn build_hashed_service_config_is_deterministic() {
    let first = build_hashed_service_config(sample_patch_request("svc-1", "Service 1"))
        .expect("first hash should succeed");
    let second = build_hashed_service_config(sample_patch_request("svc-1", "Service 1"))
        .expect("second hash should succeed");

    assert_eq!(first.version, second.version);
    assert!(first.version.starts_with("cfg-"));
}

#[test]
fn build_hashed_service_config_changes_when_config_changes() {
    let original = build_hashed_service_config(sample_patch_request("svc-1", "Service 1"))
        .expect("hash should succeed");
    let changed = build_hashed_service_config(sample_patch_request("svc-1", "Service 1 Updated"))
        .expect("hash should succeed");

    assert_ne!(original.version, changed.version);
}

#[test]
fn build_hashed_service_config_accepts_image_without_build() {
    let config = build_hashed_service_config(sample_patch_request_with_image(
        "svc-1",
        "Service 1",
        "ghcr.io/org/service:1.2.3",
    ))
    .expect("hash should succeed");

    assert!(config.build.is_none());
    assert_eq!(config.image.as_deref(), Some("ghcr.io/org/service:1.2.3"),);
}

#[test]
fn build_hashed_service_config_rejects_missing_build_and_image() {
    let mut request = sample_patch_request("svc-1", "Service 1");
    request.build = None;
    request.image = None;

    let err = build_hashed_service_config(request).expect_err("should reject");
    assert!(err.contains("either `build` or `image`"));
}

#[test]
fn build_hashed_service_config_rejects_build_and_image_together() {
    let mut request = sample_patch_request("svc-1", "Service 1");
    request.image = Some("ghcr.io/org/service:1.2.3".to_string());

    let err = build_hashed_service_config(request).expect_err("should reject");
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
fn active_deployment_status_matches_runtime_states() {
    assert!(is_active_deployment_status(&DeploymentStatus::Queued));
    assert!(is_active_deployment_status(&DeploymentStatus::Building));
    assert!(is_active_deployment_status(&DeploymentStatus::Ready));
    assert!(!is_active_deployment_status(&DeploymentStatus::Crashed));
    assert!(!is_active_deployment_status(&DeploymentStatus::Stopped));
    assert!(!is_active_deployment_status(&DeploymentStatus::Terminated));
    assert!(!is_active_deployment_status(&DeploymentStatus::Canceled));
}
