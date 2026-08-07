use std::path::PathBuf;

#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
use cluster::StoreJoinTicket;
use cluster::{
    CertificateKeyPair, CertificateValidity, ClusterCertificateAuthority, NodeCertificateBundle,
};
use kernel_api::{NodeId, NodeInstanceId, NodeRole, SecretValue};

use crate::launch::{admin_api_settings, api_settings, panel_directory};
use crate::{
    DaemonLaunchConfig, DaemonLaunchDocument, DatadogLaunchConfig, DatadogLogsLaunchConfig,
    DatadogMetricsLaunchConfig, DepotLaunchConfig, LogBackupLaunchConfig, NixosUpgradeLaunchConfig,
    PreviewLaunchConfig, StoreLaunchMode, load_launch_config, load_launch_document,
};
use time::{Duration as TimeDuration, OffsetDateTime};

use super::cluster_with_nodes;

const TEST_JWT_SECRET_KEY: &str = "operator-test-secret-with-32-characters";

#[test]
#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
fn launch_validation_binds_store_mode_to_local_role_and_ticket()
-> Result<(), Box<dyn std::error::Error>> {
    let master = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    master.validate()?;

    let mut missing_issuer = master.clone();
    missing_issuer.certificate_issuer = None;
    assert!(missing_issuer.validate().is_err());

    let mut mismatched_issuer = master.clone();
    mismatched_issuer
        .certificate_issuer
        .as_mut()
        .ok_or("certificate issuer missing")?
        .certificate_pem = "other-root".to_string();
    assert!(mismatched_issuer.validate().is_err());

    let mut weak_store_secret = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    weak_store_secret.store_encryption_secret = SecretValue::new("too-short");
    assert!(weak_store_secret.validate().is_err());

    let joined_master = DaemonLaunchConfig {
        store_mode: StoreLaunchMode::Join {
            ticket: StoreJoinTicket::from_provider_data(NodeId::new("master")?, b"provider-ticket"),
        },
        ..master
    };
    assert!(joined_master.validate().is_err());

    let worker = DaemonLaunchConfig {
        cluster: cluster_with_nodes(&[("master", NodeRole::Master), ("worker", NodeRole::Worker)])?,
        node_id: NodeId::new("worker")?,
        etcd_binary: None,
        store_mode: StoreLaunchMode::Client,
        certificate_issuer: None,
        ..config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?
    };
    worker.validate()?;
    assert!(
        DaemonLaunchConfig {
            store_mode: StoreLaunchMode::Restart,
            ..worker
        }
        .validate()
        .is_err()
    );
    Ok(())
}

#[test]
fn launch_document_requires_owner_only_permissions() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("launch.json");
    let config = document("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    std::fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
        assert!(load_launch_document(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    assert_eq!(load_launch_document(&path)?, config);
    Ok(())
}

#[tokio::test]
async fn launch_resolution_refetches_config_without_persisting_its_secrets()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch_path = directory.path().join("launch.json");
    let config_path = directory.path().join("cluster.json");
    let cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    let node_id = NodeId::new("master")?;
    let node = cluster.nodes.get(&node_id).ok_or("master missing")?;
    let now = OffsetDateTime::now_utc();
    let validity =
        CertificateValidity::new(now - TimeDuration::minutes(1), now + TimeDuration::days(1))?;
    let authority = ClusterCertificateAuthority::generate(&cluster.name, validity)?;
    let security = authority.issue_node_certificate_for_definition(&node_id, node, validity)?;
    let encryption_secret = SecretValue::new("live-config-encryption-secret-with-32-characters");
    let launch = DaemonLaunchDocument::new(
        node_id,
        directory.path().join("data"),
        PathBuf::from("/run/containerd/containerd.sock"),
        Some(PathBuf::from("/usr/bin/etcd")),
        StoreLaunchMode::Bootstrap,
        security,
        Some(authority),
        &encryption_secret,
        None,
    )?;
    write_private(&launch_path, &launch)?;
    std::fs::write(
        &config_path,
        live_cluster_source("first-operator-jwt-secret-with-32-characters"),
    )?;

    let first =
        load_launch_config(&launch_path, config_path.to_str().ok_or("config path")?).await?;
    assert_eq!(
        first.jwt_secret_key.expose(),
        "first-operator-jwt-secret-with-32-characters"
    );
    std::fs::write(
        &config_path,
        live_cluster_source("second-operator-jwt-secret-with-32-characters"),
    )?;
    let second =
        load_launch_config(&launch_path, config_path.to_str().ok_or("config path")?).await?;
    assert_eq!(
        second.jwt_secret_key.expose(),
        "second-operator-jwt-secret-with-32-characters"
    );

    let persisted = std::fs::read_to_string(launch_path)?;
    for secret in [
        TEST_JWT_SECRET_KEY,
        "first-operator-jwt-secret-with-32-characters",
        "second-operator-jwt-secret-with-32-characters",
        encryption_secret.expose(),
        "PRIVATE KEY",
    ] {
        assert!(!persisted.contains(secret));
    }
    Ok(())
}

#[test]
#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
fn launch_validation_requires_absolute_host_paths() -> Result<(), Box<dyn std::error::Error>> {
    let config = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    assert!(
        DaemonLaunchConfig {
            data_directory: PathBuf::from("maestro-state"),
            ..config.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        DaemonLaunchConfig {
            etcd_binary: Some(PathBuf::from("etcd")),
            ..config.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        DaemonLaunchConfig {
            containerd_socket: PathBuf::from("containerd.sock"),
            ..config
        }
        .validate()
        .is_err()
    );
    Ok(())
}

#[test]
#[cfg(any(target_os = "macos", feature = "macos-platform"))]
fn macos_launch_profile_requires_one_node_and_omits_nixos_upgrades()
-> Result<(), Box<dyn std::error::Error>> {
    let launch = DaemonLaunchConfig {
        containerd_socket: PathBuf::from("ignored-by-docker"),
        ..config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?
    };
    launch.validate()?;

    let mut multiple_nodes = launch.clone();
    multiple_nodes.cluster =
        cluster_with_nodes(&[("master", NodeRole::Master), ("worker", NodeRole::Worker)])?;
    assert!(multiple_nodes.validate().is_err());

    let mut nixos_upgrade = launch;
    nixos_upgrade.nixos_upgrade = Some(NixosUpgradeLaunchConfig::new("/etc/maestro"));
    assert!(nixos_upgrade.validate().is_err());
    Ok(())
}

#[test]
fn launch_validation_requires_a_strong_redacted_jwt_key() -> Result<(), Box<dyn std::error::Error>>
{
    let mut launch = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    let secret = "operator-production-secret-with-32-characters";
    launch.jwt_secret_key = SecretValue::new(secret);
    launch.validate()?;
    assert!(!format!("{launch:?}").contains(secret));
    let serialized = serde_json::to_string(&launch)?;
    assert!(serialized.contains(secret));

    launch.jwt_secret_key = SecretValue::new("too-short");
    assert!(launch.validate().is_err());
    Ok(())
}

#[test]
fn packaged_panel_is_discovered_only_when_the_spa_shell_exists()
-> Result<(), Box<dyn std::error::Error>> {
    let package = tempfile::tempdir()?;
    assert_eq!(panel_directory(package.path()), None);
    let directory = package.path().join("share").join("maestro-panel");
    std::fs::create_dir_all(&directory)?;
    std::fs::write(directory.join("index.html"), "<main>Maestro</main>")?;
    assert_eq!(panel_directory(package.path()), Some(directory));
    Ok(())
}

#[test]
fn api_listener_includes_the_routed_workload_bridge() -> Result<(), Box<dyn std::error::Error>> {
    let mut launch = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    launch.cluster.tailscale = Some(cluster::TailscaleGatewayConfig {
        auth_key: SecretValue::new("tskey-auth-test"),
        advertise_routes: None,
        replicas: 1,
        tags: Vec::new(),
        cross_cluster_dns: Vec::new(),
    });
    let node = launch
        .cluster
        .nodes
        .get(&launch.node_id)
        .cloned()
        .ok_or("local node missing")?;
    let settings = api_settings(
        &launch.cluster,
        &node,
        &launch.security,
        launch.jwt_secret_key.clone(),
    );
    assert_eq!(
        settings.bind_address,
        std::net::SocketAddr::new(
            std::net::IpAddr::V4(node.endpoint.host_address),
            node.endpoint.api_port,
        )
    );
    assert_eq!(
        settings.operator_proxy_cidrs,
        launch
            .cluster
            .nodes
            .values()
            .map(|node| node.workload_subnet)
            .collect::<Vec<_>>()
    );
    #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
    {
        let admin = admin_api_settings(&launch.cluster, &node, launch.jwt_secret_key.clone())?
            .ok_or("Admin listener missing")?;
        assert_eq!(
            admin.bind_address,
            std::net::SocketAddr::from(([172, 22, 0, 250], node.endpoint.api_port))
        );
        assert!(
            admin
                .operator_proxy_cidrs
                .contains(&"100.64.0.0/10".parse()?)
        );
        assert!(admin.tls_identity.is_none());
        admin.validate()?;
    }
    launch.cluster.tailscale = None;
    assert!(
        admin_api_settings(&launch.cluster, &node, launch.jwt_secret_key.clone())?.is_none(),
        "Admin must not listen without the managed Tailscale gateway"
    );
    Ok(())
}

#[test]
fn datadog_launch_config_is_validated_and_debug_redacted() -> Result<(), Box<dyn std::error::Error>>
{
    let mut launch = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    launch.datadog = Some(DatadogLaunchConfig {
        api_key: SecretValue::new("test-super-secret"),
        site: "us3.datadoghq.com".to_owned(),
        include_ingress_logs: true,
        include_tailscale_logs: false,
        logs: DatadogLogsLaunchConfig {
            include_healthcheck: false,
        },
        metrics: DatadogMetricsLaunchConfig {
            enabled: true,
            tags: vec!["env:test".to_owned()],
        },
    });
    launch.validate()?;
    assert!(!format!("{launch:?}").contains("test-super-secret"));

    let mut invalid = launch;
    invalid
        .datadog
        .as_mut()
        .ok_or("Datadog config missing")?
        .site = "https://example.invalid".to_owned();
    assert!(invalid.validate().is_err());
    let datadog = invalid.datadog.as_mut().ok_or("Datadog config missing")?;
    datadog.site = "datadoghq.com".to_owned();
    datadog.metrics.tags = vec!["bad\ntag".to_owned()];
    assert!(invalid.validate().is_err());
    Ok(())
}

#[test]
fn depot_launch_config_is_validated_serialized_and_debug_redacted()
-> Result<(), Box<dyn std::error::Error>> {
    let mut launch = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    launch.depot = Some(DepotLaunchConfig {
        token: SecretValue::new("depot-super-secret"),
        executable: PathBuf::from("/opt/depot/bin/depot"),
        timeout_secs: 900,
        registry: true,
    });
    launch.validate()?;
    assert!(!format!("{launch:?}").contains("depot-super-secret"));
    let encoded = serde_json::to_value(&launch)?;
    assert_eq!(
        encoded
            .get("depot")
            .and_then(|depot| depot.get("executable"))
            .and_then(serde_json::Value::as_str),
        Some("/opt/depot/bin/depot")
    );
    assert_eq!(
        encoded
            .get("depot")
            .and_then(|depot| depot.get("registry"))
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );

    launch
        .depot
        .as_mut()
        .ok_or("Depot config missing")?
        .timeout_secs = 0;
    assert!(launch.validate().is_err());
    Ok(())
}

#[test]
fn preview_launch_config_validates_domain_quota_and_redacts_token()
-> Result<(), Box<dyn std::error::Error>> {
    let mut launch = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    launch.preview = Some(PreviewLaunchConfig {
        domain: "preview.example.test".to_string(),
        github_token: SecretValue::new("github-super-secret"),
        max_concurrent_previews: 5,
    });
    launch.validate()?;
    assert!(!format!("{launch:?}").contains("github-super-secret"));

    let preview = launch.preview.as_mut().ok_or("preview config missing")?;
    preview.domain = "not a domain!".to_string();
    assert!(launch.validate().is_err());
    let preview = launch.preview.as_mut().ok_or("preview config missing")?;
    preview.domain = "preview.example.test".to_string();
    preview.max_concurrent_previews = 0;
    assert!(launch.validate().is_err());
    Ok(())
}

#[test]
fn log_backup_launch_config_validates_s3_kms_prefix_and_retention()
-> Result<(), Box<dyn std::error::Error>> {
    let mut launch = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    launch.log_backup = Some(LogBackupLaunchConfig {
        bucket: "maestro-production-logs".to_owned(),
        kms_key_id: "alias/maestro-logs".to_owned(),
        region: Some("us-west-2".to_owned()),
        prefix: Some("clusters/production".to_owned()),
        retention_days: Some(30),
    });
    launch.validate()?;
    let encoded = serde_json::to_value(&launch)?;
    assert_eq!(
        encoded
            .get("logBackup")
            .and_then(|backup| backup.get("retentionDays"))
            .and_then(serde_json::Value::as_u64),
        Some(30)
    );

    let backup = launch.log_backup.as_mut().ok_or("backup config missing")?;
    backup.retention_days = Some(0);
    assert!(launch.validate().is_err());
    let backup = launch.log_backup.as_mut().ok_or("backup config missing")?;
    backup.retention_days = Some(30);
    backup.prefix = Some("../escape".to_owned());
    assert!(launch.validate().is_err());
    Ok(())
}

#[test]
#[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
fn nixos_upgrade_launch_config_requires_hermetic_binary_pairs()
-> Result<(), Box<dyn std::error::Error>> {
    let mut launch = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    launch.nixos_upgrade = Some(NixosUpgradeLaunchConfig::new("/etc/maestro"));
    launch.validate()?;
    let encoded = serde_json::to_value(&launch)?;
    let upgrade = encoded
        .get("nixosUpgrade")
        .ok_or("NixOS upgrade config missing")?;
    assert_eq!(upgrade.get("configuration"), Some(&"default".into()));
    let upgrade = launch
        .nixos_upgrade
        .as_mut()
        .ok_or("NixOS upgrade config missing")?;
    upgrade.nix_binary = Some(PathBuf::from("nix"));
    assert!(launch.validate().is_err());
    let upgrade = launch
        .nixos_upgrade
        .as_mut()
        .ok_or("NixOS upgrade config missing")?;
    upgrade.nix_binary = Some(PathBuf::from("/nix/store/test/bin/nix"));
    assert!(launch.validate().is_err());
    let upgrade = launch
        .nixos_upgrade
        .as_mut()
        .ok_or("NixOS upgrade config missing")?;
    upgrade.nixos_rebuild_binary = Some(PathBuf::from("/run/current-system/sw/bin/nixos-rebuild"));
    upgrade.systemctl_binary = Some(PathBuf::from("systemctl"));
    assert!(launch.validate().is_err());
    let upgrade = launch
        .nixos_upgrade
        .as_mut()
        .ok_or("NixOS upgrade config missing")?;
    upgrade.systemctl_binary = Some(PathBuf::from("/run/current-system/sw/bin/systemctl"));
    launch.validate()?;
    Ok(())
}

fn config(
    node_id: &str,
    role: NodeRole,
    store_mode: StoreLaunchMode,
) -> Result<DaemonLaunchConfig, Box<dyn std::error::Error>> {
    config_with_jwt_secret(
        node_id,
        role,
        store_mode,
        SecretValue::new(TEST_JWT_SECRET_KEY),
    )
}

fn document(
    node_id: &str,
    role: NodeRole,
    store_mode: StoreLaunchMode,
) -> Result<DaemonLaunchDocument, Box<dyn std::error::Error>> {
    let config = config(node_id, role, store_mode)?;
    Ok(DaemonLaunchDocument::new(
        config.node_id,
        config.data_directory,
        config.containerd_socket,
        config.etcd_binary,
        config.store_mode,
        config.security,
        config.certificate_issuer,
        &config.store_encryption_secret,
        config.instance_id,
    )?)
}

fn config_with_jwt_secret(
    node_id: &str,
    role: NodeRole,
    store_mode: StoreLaunchMode,
    jwt_secret_key: SecretValue,
) -> Result<DaemonLaunchConfig, Box<dyn std::error::Error>> {
    Ok(DaemonLaunchConfig {
        cluster: cluster_with_nodes(&[(node_id, role)])?,
        node_id: NodeId::new(node_id)?,
        data_directory: PathBuf::from("/var/lib/maestro"),
        containerd_socket: PathBuf::from("/run/containerd/containerd.sock"),
        etcd_binary: Some(PathBuf::from("/usr/bin/etcd")),
        store_mode,
        security: NodeCertificateBundle {
            trust_root_pem: "test-root".to_owned(),
            identity: CertificateKeyPair {
                certificate_pem: "test-identity".to_owned(),
                private_key_pem: SecretValue::new("test-private-key"),
            },
        },
        certificate_issuer: Some(ClusterCertificateAuthority {
            certificate_pem: "test-root".to_owned(),
            private_key_pem: SecretValue::new("test-ca-private-key"),
        }),
        jwt_secret_key,
        store_encryption_secret: SecretValue::new(
            "store-encryption-test-secret-with-32-characters",
        ),
        instance_id: Some(NodeInstanceId::new("instance-1")?),
        datadog: None,
        depot: None,
        log_backup: None,
        preview: None,
        nixos_upgrade: None,
    })
}

fn live_cluster_source(jwt_secret: &str) -> String {
    format!(
        r#"{{
            "jwt-secret-key": "{jwt_secret}",
            "encryption-key": "live-config-encryption-secret-with-32-characters",
            "cluster": {{
                "cluster-id": "daemon-test",
                "name": "daemon-test",
                "nodes": {{
                    "master": {{
                        "hostname": "master.internal",
                        "endpoint": "10.20.0.11:3011",
                        "subnet": "172.22.0.0/24",
                        "role": "master"
                    }}
                }},
                "control-allow-cidrs": ["10.20.0.0/24"],
                "ports": {{
                    "gateway": 3001,
                    "store-client": 2379,
                    "store-peer": 2380,
                    "wireguard": 51820
                }},
                "join-secret": "daemon-test-join-secret-with-32-characters"
            }}
        }}"#
    )
}

fn write_private(
    path: &std::path::Path,
    value: &impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}
