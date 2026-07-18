use super::*;
use crate::deployment::types::{
    Command, DeploymentBuildInfo, DeploymentStatus, DeploymentWithReplicas, EnvConfig,
    PreviewConfig, PreviewEnvConfig, SecretKeyMeta, SecretsConfig, ServiceBuildConfig,
    ServiceConfig, ServiceDeployConfig, ServiceDeployment, mask_secret_value,
};
use crate::utils::crypto::SecretString;
use crate::validation::validate_service_id;

#[tokio::test]
async fn websocket_exec_relay_bridges_length_prefixed_control_frames() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (control_server, mut control_client) = tokio::net::UnixStream::pair().unwrap();
    let pending_control = Arc::new(tokio::sync::Mutex::new(Some(control_server)));
    let app = Router::new().route(
        "/exec",
        get({
            let pending_control = pending_control.clone();
            move |upgrade: WebSocketUpgrade| {
                let pending_control = pending_control.clone();
                async move {
                    let control = pending_control
                        .lock()
                        .await
                        .take()
                        .expect("one relay connection");
                    let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
                    upgrade
                        .on_upgrade(move |websocket| relay_local_exec(websocket, control, permit))
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut websocket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/exec"))
        .await
        .unwrap();

    websocket
        .send(Message::Binary(
            crate::exec::ExecFrame::Stdin(b"hello".to_vec())
                .encode()
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        crate::exec::read_length_prefixed(&mut control_client)
            .await
            .unwrap(),
        Some(crate::exec::ExecFrame::Stdin(b"hello".to_vec()))
    );
    crate::exec::write_length_prefixed(
        &mut control_client,
        &crate::exec::ExecFrame::Output(b"world".to_vec()),
    )
    .await
    .unwrap();
    crate::exec::write_length_prefixed(&mut control_client, &crate::exec::ExecFrame::Exit(7))
        .await
        .unwrap();

    let output = websocket.next().await.unwrap().unwrap();
    let Message::Binary(output) = output else {
        panic!("expected binary output frame");
    };
    assert_eq!(
        crate::exec::ExecFrame::decode(&output).unwrap(),
        crate::exec::ExecFrame::Output(b"world".to_vec())
    );
    let exit = websocket.next().await.unwrap().unwrap();
    let Message::Binary(exit) = exit else {
        panic!("expected binary exit frame");
    };
    assert_eq!(
        crate::exec::ExecFrame::decode(&exit).unwrap(),
        crate::exec::ExecFrame::Exit(7)
    );
    server.abort();
}

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
            exec: true,
            max_restarts: None,
            env: Default::default(),
            secrets: None,
            volumes: vec![],
            node_affinity: None,
            egress: Default::default(),
            healthcheck_interval: 60,
        },
        ingress: None,
        preview: None,
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
            exec: true,
            max_restarts: None,
            env: Default::default(),
            secrets: None,
            volumes: vec![],
            node_affinity: None,
            egress: Default::default(),
            healthcheck_interval: 60,
        },
        ingress: None,
        preview: None,
    }
}

fn config_with_plaintext_sentinels() -> ServiceConfig {
    let mut request = sample_patch_request("secret-test", "Secret test");
    let build = request.build.as_mut().expect("build config");
    build.env.items.insert(
        "BUILD_ENV".to_string(),
        SecretString::new("build-env-plaintext".to_string()),
    );
    build.secrets.items.insert(
        "BUILD_SECRET".to_string(),
        SecretString::new("build-secret-plaintext".to_string()),
    );
    request.deploy.env.items.insert(
        "DEPLOY_ENV".to_string(),
        SecretString::new("deploy-env-plaintext".to_string()),
    );
    request.deploy.secrets = Some(SecretsConfig {
        mount_path: "/run/secrets/app.env".to_string(),
        source: None,
        items: std::collections::HashMap::from([(
            "DEPLOY_SECRET".to_string(),
            "deploy-secret-plaintext".to_string(),
        )]),
        keys: std::collections::HashMap::from([(
            "DEPLOY_SECRET".to_string(),
            SecretKeyMeta {
                hash: "secret-hash-plaintext".to_string(),
                changed: true,
            },
        )]),
    });
    request.preview = Some(PreviewConfig {
        enabled: false,
        close_grace_period: "1d".to_string(),
        replicas: 1,
        env: PreviewEnvConfig {
            items: std::collections::HashMap::from([(
                "PREVIEW_ENV".to_string(),
                SecretString::new("preview-env-plaintext".to_string()),
            )]),
        },
    });
    build_service_config(request).expect("service config")
}

fn assert_no_config_plaintext(json: &str) {
    for plaintext in [
        "build-env-plaintext",
        "build-secret-plaintext",
        "deploy-env-plaintext",
        "deploy-secret-plaintext",
        "secret-hash-plaintext",
        "preview-env-plaintext",
    ] {
        assert!(
            !json.contains(plaintext),
            "API response leaked `{plaintext}`"
        );
    }
}

fn restart_candidate(id: &str, status: DeploymentStatus, image: &str) -> ServiceDeployment {
    let mut deployment = ServiceDeployment::new(
        build_service_config(sample_patch_request_with_image("api", "API", "app:latest")).unwrap(),
    )
    .unwrap();
    deployment.id = id.to_string();
    deployment.status = status;
    deployment.build = Some(DeploymentBuildInfo {
        docker_image_id: image.to_string(),
    });
    deployment
}

#[test]
fn restart_prefers_the_active_ready_image_over_a_newer_failed_build() {
    let failed = restart_candidate("failed-new", DeploymentStatus::Crashed, "app:new");
    let active = restart_candidate("active-old", DeploymentStatus::Ready, "app:active");
    let deployments = [failed, active];

    let selected = restart_source_deployment(&deployments).unwrap();

    assert_eq!(selected.id, "active-old");
    assert_eq!(
        selected.build.as_ref().unwrap().docker_image_id,
        "app:active"
    );
}

#[test]
fn restart_falls_back_to_latest_built_image_when_service_is_stopped() {
    let latest = restart_candidate("latest", DeploymentStatus::Terminated, "app:latest-built");
    let older = restart_candidate("older", DeploymentStatus::Removed, "app:older");
    let deployments = [latest, older];

    assert_eq!(
        restart_source_deployment(&deployments).unwrap().id,
        "latest"
    );
}

#[test]
fn service_and_deployment_api_models_mask_config_values() {
    let config = config_with_plaintext_sentinels();
    let service = ServiceListItem::new(
        config.clone(),
        Some(DeploymentStatus::Ready),
        false,
        false,
        None,
    );
    let service_json = serde_json::to_string(&service).expect("service response JSON");
    assert_no_config_plaintext(&service_json);
    assert!(service_json.contains("bu*****text"));
    assert!(service_json.contains("de*****text"));
    assert!(service_json.matches("*****text").count() >= 2);
    assert!(!service_json.contains("\"hash\""));

    let deployment = DeploymentListItem::new(DeploymentWithReplicas {
        deployment: ServiceDeployment {
            id: "deployment-1".to_string(),
            created_at: 1,
            deployed_at: Some(2),
            drained_at: None,
            status: DeploymentStatus::Ready,
            config,
            git_commit: None,
            build: None,
            upload_archive: None,
        },
        replicas: Vec::new(),
    });
    let deployment_json = serde_json::to_string(&deployment).expect("deployment response JSON");
    assert_no_config_plaintext(&deployment_json);
    assert!(deployment_json.contains("bu*****text"));
    assert!(deployment_json.contains("de*****text"));
    assert!(deployment_json.matches("*****text").count() >= 2);
    assert!(!deployment_json.contains("\"hash\""));
}

#[test]
fn secret_api_mask_shows_only_the_last_four_characters() {
    assert_eq!(mask_secret_value("secret-1234"), "*****1234");
    assert_eq!(mask_secret_value("abcd"), "abcd");
}

#[test]
fn rollout_diff_masks_environment_values_before_serialization() {
    let old = EnvConfig {
        source: None,
        items: std::collections::HashMap::from([(
            "TOKEN".to_string(),
            SecretString::new("old-env-plaintext".to_string()),
        )]),
    };
    let new = EnvConfig {
        source: None,
        items: std::collections::HashMap::from([(
            "TOKEN".to_string(),
            SecretString::new("new-env-plaintext".to_string()),
        )]),
    };
    let mut changes = Vec::new();
    diff_env(&old, &new, &mut changes);

    let json = serde_json::to_string(&changes).expect("rollout diff JSON");
    assert!(!json.contains("old-env-plaintext"));
    assert!(!json.contains("new-env-plaintext"));
    assert!(json.contains("ol*****text"));
    assert!(json.contains("ne*****text"));
}

#[test]
fn preview_rollouts_require_cluster_github_configuration() {
    let mut request = sample_patch_request("preview-app", "Preview app");
    request.build.as_mut().unwrap().repo =
        Some("https://github.com/Baton-AI/baton.git".to_string());
    request.ingress = Some(crate::deployment::types::IngressConfig {
        host: Some("app.example.test".to_string()),
        hosts: Vec::new(),
        port: Some(3000),
        session_affinity: None,
    });
    request.preview = Some(PreviewConfig {
        enabled: true,
        close_grace_period: "1d".to_string(),
        replicas: 1,
        env: PreviewEnvConfig::default(),
    });
    let config = build_service_config(request).unwrap();
    assert!(validate_preview_integration(&config, false).is_err());
    assert!(validate_preview_integration(&config, true).is_ok());
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
fn build_service_config_changes_when_egress_changes() {
    let original = build_service_config(sample_patch_request("svc-1", "Service 1"))
        .expect("hash should succeed");
    let mut request = sample_patch_request("svc-1", "Service 1");
    request
        .deploy
        .egress
        .allow
        .push(crate::deployment::types::ServiceEgressRule {
            cidr: "10.0.10.0/24".to_string(),
            ports: vec![5432],
        });
    let changed = build_service_config(request).expect("hash should succeed");

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
fn ingress_traffic_queries_parse_millisecond_ranges() {
    for (path, expected_from, expected_to) in [
        (
            "/api/ingress/traffic?from=1784160569791&to=1784164169791&limit=200",
            1_784_160_569_791,
            1_784_164_169_791,
        ),
        (
            "/api/ingress/blocked-traffic?from=1784160569825&to=1784164169825&limit=200",
            1_784_160_569_825,
            1_784_164_169_825,
        ),
    ] {
        let uri = path.parse::<axum::http::Uri>().expect("traffic URI");
        let query = axum::extract::Query::<TrafficBreakdownQuery>::try_from_uri(&uri)
            .expect("deserialize traffic query")
            .0;

        assert_eq!(query.from, Some(expected_from));
        assert_eq!(query.to, Some(expected_to));
        assert_eq!(query.limit, Some(200));
    }
}

#[test]
fn stats_metric_queries_parse_millisecond_ranges() {
    let uri = "/api/metrics/stats?name=requests&from=1784160569791&to=1784164169791"
        .parse::<axum::http::Uri>()
        .expect("stats URI");
    let query = axum::extract::Query::<StatsMetricsQuery>::try_from_uri(&uri)
        .expect("deserialize stats query")
        .0;

    assert_eq!(query.name.as_deref(), Some("requests"));
    assert_eq!(query.from, Some(1_784_160_569_791));
    assert_eq!(query.to, Some(1_784_164_169_791));
}

#[test]
fn node_admin_homepage_uses_the_reserved_admin_address() {
    assert_eq!(
        node_admin_url(crate::cluster::NodeRole::Voter, "10.51.0.0/24").as_deref(),
        Some("http://10.51.0.250")
    );
    assert_eq!(
        node_admin_url(crate::cluster::NodeRole::Master, "10.100.0.0/16").as_deref(),
        Some("http://10.100.0.250")
    );
    assert_eq!(
        node_admin_url(crate::cluster::NodeRole::Worker, "10.52.0.0/24"),
        None
    );
}

#[test]
fn hs256_service_tokens_use_an_installed_crypto_provider() {
    let secret = "service-jwt-test-secret";
    let claims = serde_json::json!({
        "sub": "maestro-admin",
        "scope": "operator",
        "iat": 1_700_000_000_u64,
        "exp": 4_000_000_000_u64,
    });
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("HS256 encoding should have a crypto provider");

    validate_jwt(&token, secret).expect("HS256 verification should have a crypto provider");
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

#[tokio::test]
async fn cluster_websocket_client_connects_with_probe_mtls_identity() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let host_ip = std::net::Ipv4Addr::LOCALHOST;
    let ca = crate::utils::certs::generate_cluster_ca().expect("cluster CA");
    let certs = crate::utils::certs::generate_cluster_node_certs(
        &ca,
        host_ip,
        crate::cluster::NodeRole::Voter,
    )
    .expect("cluster node certificates");
    let directory = std::env::temp_dir().join(format!(
        "maestro-wss-mtls-test-{}",
        crate::utils::nanoid::unique_id(12)
    ));
    std::fs::create_dir_all(&directory).expect("test certificate directory");
    let ca_path = directory.join("ca.pem");
    let api_certificate_path = directory.join("api.pem");
    let api_key_path = directory.join("api-key.pem");
    let client_certificate_path = directory.join("probe-client.pem");
    let client_key_path = directory.join("probe-client-key.pem");
    std::fs::write(&ca_path, certs.ca_pem).expect("CA certificate");
    std::fs::write(&api_certificate_path, certs.api_cert_pem).expect("API certificate");
    std::fs::write(&api_key_path, certs.api_key_pem).expect("API key");
    std::fs::write(&client_certificate_path, certs.probe_client_cert_pem)
        .expect("probe client certificate");
    std::fs::write(&client_key_path, certs.probe_client_key_pem).expect("probe client key");

    let listener = std::net::TcpListener::bind((host_ip, 0)).expect("reserve TLS port");
    let address = listener.local_addr().expect("TLS address");
    drop(listener);
    let app = Router::new().route(
        "/exec",
        get(|upgrade: WebSocketUpgrade| async move {
            upgrade.on_upgrade(|mut websocket| async move {
                if let Some(Ok(message)) = websocket.recv().await {
                    let _ = websocket.send(message).await;
                }
            })
        }),
    );
    let server_config = build_api_tls_config(
        api_certificate_path.to_str().expect("API certificate path"),
        api_key_path.to_str().expect("API key path"),
        Some(ca_path.to_str().expect("CA path")),
    )
    .expect("TLS server config");
    let server_handle = axum_server::Handle::new();
    let shutdown_handle = server_handle.clone();
    let server = tokio::spawn(async move {
        axum_server::bind_rustls(address, server_config)
            .handle(server_handle)
            .serve(app.into_make_service())
            .await
    });
    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(
        cluster_ws_tls_config_from_paths(&ca_path, &client_certificate_path, &client_key_path)
            .expect("TLS client config"),
    ));
    let url = format!("wss://{address}/exec");
    let mut websocket = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match tokio_tungstenite::connect_async_tls_with_config(
                &url,
                None,
                false,
                Some(connector.clone()),
            )
            .await
            {
                Ok((websocket, _)) => break websocket,
                Err(tokio_tungstenite::tungstenite::Error::Io(error))
                    if error.kind() == std::io::ErrorKind::ConnectionRefused =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => panic!("mTLS WebSocket connection failed: {error}"),
            }
        }
    })
    .await
    .expect("TLS WebSocket server did not start");
    let frame = crate::exec::ExecFrame::Output(b"cross-node".to_vec())
        .encode()
        .expect("exec frame");
    websocket
        .send(Message::Binary(frame.clone().into()))
        .await
        .expect("send WebSocket frame");
    assert_eq!(
        websocket.next().await.expect("echoed frame").expect("echo"),
        Message::Binary(frame.into())
    );
    let _ = websocket.close(None).await;
    shutdown_handle.shutdown();
    server.await.expect("TLS server task").expect("TLS server");
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

#[test]
fn log_query_is_validated_before_store_access() {
    let query = LogsQuery {
        tail: Some(25),
        after: Some(10),
        before: Some(20),
        from: Some(1_700_000_000_000),
        to: Some(1_700_000_060_000),
        phase: None,
        query: Some("@http.status_code:[400 TO 499] AND -message:health".into()),
    };
    let read = build_log_read_query(
        LogReadScope::Prefix("api/".into()),
        Some(LogOrigin::Service),
        &query,
        25,
    )
    .expect("valid query");
    assert!(read.search.is_some());
    assert_eq!(read.from, Some(1_700_000_000_000));
    assert_eq!(read.to, Some(1_700_000_060_000));
    assert_eq!(read.before, Some(20));
    assert_eq!(read.after, None, "before keeps its existing precedence");

    let invalid = LogsQuery {
        query: Some("@http.status_code:[500 599]".into()),
        ..query
    };
    let error = build_log_read_query(LogReadScope::Prefix("api/".into()), None, &invalid, 25)
        .expect_err("invalid query");
    assert_eq!(error.0, StatusCode::BAD_REQUEST);

    let response = logs_response(Vec::new(), 42);
    assert_eq!(
        response
            .headers()
            .get(LOG_CURSOR_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("42")
    );
}

#[test]
fn cluster_log_cursor_tracks_independent_node_tiers() {
    let cursor = ClusterLogCursor {
        streams: BTreeMap::from([
            (cluster_log_stream_key("node-a", "service"), 41),
            (cluster_log_stream_key("node-a", "system"), 12),
            (cluster_log_stream_key("node-b", "service"), 99),
        ]),
    };
    let encoded = encode_cluster_log_cursor(&cursor).expect("encode cursor");
    assert_eq!(
        decode_cluster_log_cursor(Some(&encoded))
            .expect("decode cursor")
            .streams,
        cursor.streams
    );
    assert!(decode_cluster_log_cursor(Some("not-base64!")).is_err());

    let invalid = ClusterLogCursor {
        streams: BTreeMap::from([("node-a:service".to_string(), -1)]),
    };
    let invalid = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&invalid).expect("serialize cursor"));
    assert!(decode_cluster_log_cursor(Some(&invalid)).is_err());
}

#[test]
fn cluster_log_cursor_catches_up_without_skipping_a_full_page() {
    assert_eq!(
        next_cluster_log_stream_cursor(Some(10), 40, 0, 500, None),
        40,
        "an empty filtered page advances to the storage watermark"
    );
    assert_eq!(
        next_cluster_log_stream_cursor(Some(10), 900, 500, 500, Some(510)),
        510,
        "a full page advances only to its last returned entry"
    );
    assert_eq!(
        next_cluster_log_stream_cursor(Some(510), 900, 390, 500, Some(900)),
        900,
        "the final partial page catches up to the storage watermark"
    );
}

#[test]
fn cluster_log_service_filter_selects_the_correct_tier() {
    assert_eq!(
        cluster_log_scopes(None),
        vec![
            ("service", LogReadScope::AllServices),
            ("system", LogReadScope::AllSystem)
        ]
    );
    assert_eq!(
        cluster_log_scopes(Some("api")),
        vec![("service", LogReadScope::Prefix("api/".to_string()))]
    );
    assert_eq!(
        cluster_log_scopes(Some("maestro-probe")),
        vec![(
            "system",
            LogReadScope::Sources(vec![
                "maestro-probe".to_string(),
                "maestro-controller".to_string()
            ])
        )]
    );
}

#[test]
fn cluster_log_histograms_are_summed_by_bucket_and_level() {
    let mut target = Some(LogHistogram {
        from: 0,
        to: 120_000,
        bucket_ms: 60_000,
        buckets: vec![LogHistogramBucket {
            ts: 0,
            count: 2,
            levels: BTreeMap::from([("info".to_string(), 2)]),
        }],
    });
    merge_cluster_log_histogram(
        &mut target,
        LogHistogram {
            from: 0,
            to: 120_000,
            bucket_ms: 60_000,
            buckets: vec![LogHistogramBucket {
                ts: 0,
                count: 4,
                levels: BTreeMap::from([("error".to_string(), 1), ("info".to_string(), 3)]),
            }],
        },
    );
    let bucket = &target.expect("merged histogram").buckets[0];
    assert_eq!(bucket.count, 6);
    assert_eq!(bucket.levels.get("info"), Some(&5));
    assert_eq!(bucket.levels.get("error"), Some(&1));
}

#[test]
fn log_histogram_uses_bounded_adaptive_buckets_and_fills_gaps() {
    let to = 1_700_000_400_000;
    let one_hour = build_log_histogram_query(
        LogReadScope::Prefix("api/".into()),
        Some(LogOrigin::Service),
        &LogHistogramHttpQuery {
            from: Some(to - DEFAULT_LOG_RANGE_MS),
            to: Some(to),
            phase: None,
            query: Some("level:error".into()),
            bucket_ms: None,
            group_by: None,
        },
    )
    .expect("one hour");
    assert_eq!(one_hour.bucket_ms, ONE_MINUTE_MS);
    assert!(one_hour.search.is_some());

    let six_hours = build_log_histogram_query(
        LogReadScope::Prefix("api/".into()),
        None,
        &LogHistogramHttpQuery {
            from: Some(to - 6 * DEFAULT_LOG_RANGE_MS),
            to: Some(to),
            phase: None,
            query: None,
            bucket_ms: None,
            group_by: None,
        },
    )
    .expect("six hours");
    assert_eq!(six_hours.bucket_ms, FIVE_MINUTES_MS);

    let seven_days = build_log_histogram_query(
        LogReadScope::Prefix("api/".into()),
        None,
        &LogHistogramHttpQuery {
            from: Some(to - MAX_LOG_RANGE_MS),
            to: Some(to),
            phase: None,
            query: None,
            bucket_ms: None,
            group_by: None,
        },
    )
    .expect("seven days");
    assert_eq!(seven_days.bucket_ms, TWO_HOURS_MS);

    let one_day = build_log_histogram_query(
        LogReadScope::Prefix("api/".into()),
        None,
        &LogHistogramHttpQuery {
            from: Some(to - 24 * DEFAULT_LOG_RANGE_MS),
            to: Some(to),
            phase: None,
            query: None,
            bucket_ms: None,
            group_by: None,
        },
    )
    .expect("one day");
    assert_eq!(one_day.bucket_ms, TEN_MINUTES_MS);

    let client_bucket = build_log_histogram_query(
        LogReadScope::Prefix("api/".into()),
        None,
        &LogHistogramHttpQuery {
            from: Some(to - MAX_LOG_RANGE_MS),
            to: Some(to),
            phase: None,
            query: None,
            bucket_ms: Some(1),
            group_by: None,
        },
    )
    .expect("client bucket");
    assert_eq!(client_bucket.bucket_ms, MAX_LOG_RANGE_MS / 1_000);

    let first_bucket = one_hour.from - one_hour.from.rem_euclid(one_hour.bucket_ms);
    let complete = complete_log_histogram(
        &one_hour,
        vec![LogHistogramBucket {
            ts: first_bucket + ONE_MINUTE_MS,
            count: 7,
            levels: std::collections::BTreeMap::from([("error".to_string(), 7)]),
        }],
    );
    assert_eq!(complete.bucket_ms, ONE_MINUTE_MS);
    assert_eq!(complete.buckets[0].count, 0);
    assert!(complete.buckets[0].levels.is_empty());
    assert_eq!(complete.buckets[1].count, 7);
    assert_eq!(complete.buckets[1].levels.get("error"), Some(&7));
    assert!(complete.buckets.len() <= 61);
    let json = serde_json::to_value(&complete).expect("serialize histogram");
    assert_eq!(json["bucketMs"], ONE_MINUTE_MS);

    let too_wide = LogHistogramHttpQuery {
        from: Some(to - MAX_LOG_RANGE_MS - 1),
        to: Some(to),
        phase: None,
        query: None,
        bucket_ms: None,
        group_by: None,
    };
    assert!(
        build_log_histogram_query(LogReadScope::Prefix("api/".into()), None, &too_wide).is_err()
    );

    let incomplete = LogsQuery {
        tail: Some(25),
        after: None,
        before: None,
        from: Some(to - DEFAULT_LOG_RANGE_MS),
        to: None,
        phase: None,
        query: None,
    };
    assert!(
        build_log_read_query(LogReadScope::Prefix("api/".into()), None, &incomplete, 25).is_err()
    );
}
