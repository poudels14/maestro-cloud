use std::path::PathBuf;

use cluster::{CertificateKeyPair, NodeCertificateBundle, StoreJoinTicket};
use kernel_api::{NodeId, NodeInstanceId, NodeRole, SecretValue};

use crate::{DaemonLaunchConfig, StoreLaunchMode, load_launch_config};

use super::cluster_with_nodes;

#[test]
fn launch_validation_binds_store_mode_to_local_role_and_ticket()
-> Result<(), Box<dyn std::error::Error>> {
    let master = config("master", NodeRole::Master, StoreLaunchMode::Bootstrap)?;
    master.validate()?;

    let joined_master = DaemonLaunchConfig {
        store_mode: StoreLaunchMode::Join {
            ticket: StoreJoinTicket::from_provider_data(NodeId::new("master")?, b"provider-ticket"),
        },
        ..master
    };
    assert!(joined_master.validate().is_err());
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
            etcd_binary: PathBuf::from("etcd"),
            ..config
        }
        .validate()
        .is_err()
    );
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
        etcd_binary: PathBuf::from("/usr/bin/etcd"),
        store_mode,
        security: NodeCertificateBundle {
            trust_root_pem: "test-root".to_owned(),
            identity: CertificateKeyPair {
                certificate_pem: "test-identity".to_owned(),
                private_key_pem: SecretValue::new("test-private-key"),
            },
        },
        instance_id: Some(NodeInstanceId::new("instance-1")?),
    })
}
