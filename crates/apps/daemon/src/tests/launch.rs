use std::path::PathBuf;

use cluster::{CertificateKeyPair, NodeCertificateBundle, StoreJoinTicket};
use kernel_api::{NodeId, NodeInstanceId, NodeRole, SecretValue};

use crate::{
    DaemonLaunchConfig, DatadogLaunchConfig, DatadogLogsLaunchConfig, DatadogMetricsLaunchConfig,
    LogBackupLaunchConfig, NixosUpgradeLaunchConfig, PreviewLaunchConfig, StoreLaunchMode,
    load_launch_config,
};

use super::cluster_with_nodes;

#[test]
fn launch_validation_binds_store_mode_to_local_role_and_ticket()
-> Result<(), Box<dyn std::error::Error>> {
    let master = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    master.validate()?;

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
    let config = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    std::fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
        assert!(load_launch_config(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    assert_eq!(load_launch_config(&path)?, config);
    Ok(())
}

#[test]
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
            ..config
        }
        .validate()
        .is_err()
    );
    Ok(())
}

#[test]
fn launch_validation_requires_a_strong_redacted_operator_key()
-> Result<(), Box<dyn std::error::Error>> {
    let mut launch = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    let secret = "operator-production-secret-with-32-characters";
    launch.operator_jwt_secret = SecretValue::new(secret);
    launch.validate()?;
    assert!(!format!("{launch:?}").contains(secret));

    launch.operator_jwt_secret = SecretValue::new("too-short");
    assert!(launch.validate().is_err());
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
    assert_eq!(
        upgrade.get("manifestRelativePath"),
        Some(&"crates/apps/daemon/Cargo.toml".into())
    );

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
    Ok(DaemonLaunchConfig {
        cluster: cluster_with_nodes(&[(node_id, role)])?,
        node_id: NodeId::new(node_id)?,
        data_directory: PathBuf::from("/var/lib/maestro"),
        etcd_binary: Some(PathBuf::from("/usr/bin/etcd")),
        store_mode,
        security: NodeCertificateBundle {
            trust_root_pem: "test-root".to_owned(),
            identity: CertificateKeyPair {
                certificate_pem: "test-identity".to_owned(),
                private_key_pem: SecretValue::new("test-private-key"),
            },
        },
        operator_jwt_secret: SecretValue::new("operator-test-secret-with-32-characters"),
        store_encryption_secret: SecretValue::new(
            "store-encryption-test-secret-with-32-characters",
        ),
        instance_id: Some(NodeInstanceId::new("instance-1")?),
        datadog: None,
        log_backup: None,
        preview: None,
        nixos_upgrade: None,
    })
}
