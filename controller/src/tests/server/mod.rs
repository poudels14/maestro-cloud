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
            node_affinity: None,
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
            node_affinity: None,
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

#[test]
fn cluster_write_scope_excludes_node_local_and_read_routes() {
    assert!(is_cluster_write(
        &axum::http::Method::POST,
        "/api/services/rollout"
    ));
    assert!(is_cluster_write(
        &axum::http::Method::DELETE,
        "/api/services/example"
    ));
    assert!(is_cluster_write(
        &axum::http::Method::POST,
        "/api/cluster/upgrade"
    ));
    assert!(!is_cluster_write(&axum::http::Method::GET, "/api/services"));
    assert!(is_cluster_write(
        &axum::http::Method::PATCH,
        "/api/ingress/blocked-ips"
    ));
    assert!(!is_node_local_read("/api/ingress/blocked-ips"));
    assert!(is_node_local_read("/api/ingress/blocked-traffic"));
    assert!(!is_cluster_write(
        &axum::http::Method::GET,
        "/api/ingress/blocked-traffic"
    ));
    assert!(!is_cluster_write(
        &axum::http::Method::POST,
        "/api/system/restart"
    ));
    assert!(!is_cluster_write(&axum::http::Method::POST, "/api/logs"));
    assert!(is_node_local_read(
        "/api/services/example/traffic/breakdown"
    ));
}

#[test]
fn dynamic_ingress_blocklist_requires_unique_canonical_ips() {
    use crate::deployment::types::validate_ingress_blocklist;

    assert!(
        validate_ingress_blocklist(&["203.0.113.9".to_string(), "2001:db8::9".to_string()]).is_ok()
    );
    assert!(validate_ingress_blocklist(&["2001:0db8::9".to_string()]).is_err());
    assert!(
        validate_ingress_blocklist(&["203.0.113.9".to_string(), "203.0.113.9".to_string()])
            .is_err()
    );
}

#[tokio::test]
async fn ingress_denied_endpoint_returns_forbidden() {
    assert_eq!(Server::ingress_denied().await, StatusCode::FORBIDDEN);
}

#[test]
fn cluster_api_tls_accepts_optional_ca_verified_client_certificates() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let ca = crate::utils::certs::generate_cluster_ca().expect("cluster CA");
    let certs = crate::utils::certs::generate_cluster_node_certs(
        &ca,
        std::net::Ipv4Addr::new(10, 20, 0, 11),
        crate::cluster::NodeRole::Voter,
    )
    .expect("cluster node certificates");
    let directory = std::env::temp_dir().join(format!(
        "maestro-api-tls-test-{}",
        crate::utils::nanoid::unique_id(12)
    ));
    std::fs::create_dir_all(&directory).expect("test certificate directory");
    let certificate = directory.join("api.pem");
    let key = directory.join("api-key.pem");
    let client_ca = directory.join("ca.pem");
    std::fs::write(&certificate, certs.api_cert_pem).expect("API certificate");
    std::fs::write(&key, certs.api_key_pem).expect("API private key");
    std::fs::write(&client_ca, certs.ca_pem).expect("client CA");
    build_api_tls_config(
        certificate.to_str().expect("certificate path"),
        key.to_str().expect("key path"),
        Some(client_ca.to_str().expect("CA path")),
    )
    .expect("optional mutual TLS configuration");
    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn ingestion_token_comparison_matches_only_the_exact_token() {
    assert!(constant_time_token_matches("secret", "secret"));
    assert!(!constant_time_token_matches("secret", "Secret"));
    assert!(!constant_time_token_matches("secret", "secret-extra"));
}

#[test]
fn cluster_request_fingerprint_covers_route_body_and_identity() {
    let request = Request::builder()
        .method(axum::http::Method::POST)
        .uri("/api/services/rollout?dryRun=false")
        .header(axum::http::header::AUTHORIZATION, "Bearer operator-a")
        .body(())
        .expect("request");
    let (parts, _) = request.into_parts();
    let first = cluster_request_fingerprint(&parts, &Bytes::from_static(br#"{"id":"svc"}"#));
    let repeated = cluster_request_fingerprint(&parts, &Bytes::from_static(br#"{"id":"svc"}"#));
    let changed = cluster_request_fingerprint(&parts, &Bytes::from_static(br#"{"id":"other"}"#));

    assert_eq!(first, repeated);
    assert_ne!(first, changed);
}

#[tokio::test]
async fn cluster_request_spool_streams_replay_and_cleans_up() {
    let spool_dir = std::env::temp_dir().join(format!(
        "maestro-cluster-spool-test-{}",
        crate::utils::nanoid::unique_id(12)
    ));
    let payload = Bytes::from_static(br#"{"id":"svc"}"#);
    let request = Request::builder()
        .method(axum::http::Method::POST)
        .uri("/api/services/rollout")
        .body(Body::from(payload.clone()))
        .expect("request");
    let (parts, body) = request.into_parts();
    let expected_fingerprint = cluster_request_fingerprint(&parts, &payload);

    let spooled = spool_cluster_request(&spool_dir, &parts, body, 1024)
        .await
        .expect("request should spool");
    let spool_path = spooled.path.clone();
    assert_eq!(spooled.fingerprint, expected_fingerprint);
    assert_eq!(
        to_bytes(spooled.body().await.expect("spooled body"), 1024)
            .await
            .expect("replay body"),
        payload
    );

    drop(spooled);
    assert!(!spool_path.exists());
    std::fs::remove_dir_all(spool_dir).expect("remove spool directory");
}

#[tokio::test]
async fn cluster_request_spool_enforces_limit_without_leaking_a_file() {
    let spool_dir = std::env::temp_dir().join(format!(
        "maestro-cluster-spool-limit-test-{}",
        crate::utils::nanoid::unique_id(12)
    ));
    let request = Request::builder()
        .method(axum::http::Method::POST)
        .uri("/api/services/up")
        .body(Body::from(&b"four"[..]))
        .expect("request");
    let (parts, body) = request.into_parts();

    let error = match spool_cluster_request(&spool_dir, &parts, body, 3).await {
        Ok(_) => panic!("oversized request should fail"),
        Err(error) => error,
    };
    assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        std::fs::read_dir(&spool_dir)
            .expect("spool directory")
            .count(),
        0
    );
    std::fs::remove_dir_all(spool_dir).expect("remove spool directory");
}

#[test]
fn cluster_request_spool_cleanup_preserves_deployment_archives() {
    let spool_dir = std::env::temp_dir().join(format!(
        "maestro-cluster-spool-cleanup-test-{}",
        crate::utils::nanoid::unique_id(12)
    ));
    std::fs::create_dir_all(&spool_dir).expect("create spool directory");
    let stale = spool_dir.join(".maestro-cluster-request-old");
    let archive = spool_dir.join("service-deployment.tar.gz");
    std::fs::write(&stale, b"stale").expect("write stale spool");
    std::fs::write(&archive, b"archive").expect("write deployment archive");

    cleanup_stale_cluster_request_spools(&spool_dir);

    assert!(!stale.exists());
    assert!(archive.exists());
    std::fs::remove_dir_all(spool_dir).expect("remove spool directory");
}
