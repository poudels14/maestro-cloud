use cluster::{ClusterCertificateAuthority, NodeCertificateBundle, certificate_fingerprint};

use crate::CliError;
use crate::cluster_formation::{bootstrap, init_ca, issue_node, prepare_join};
use crate::config_source::ConfigSourceReader;

struct MemoryReader {
    source: String,
}

impl ConfigSourceReader for MemoryReader {
    async fn read(&self, _source: &str) -> Result<String, CliError> {
        Ok(self.source.clone())
    }
}

#[tokio::test]
async fn ca_initialization_and_node_issuance_are_private_create_only_operations()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: cluster_document(),
    };
    let mut output = Vec::new();
    init_ca("maestro.jsonc", directory.path(), &mut output, &reader).await?;
    let authority =
        ClusterCertificateAuthority::load(&directory.path().join("security").join("cluster-ca"))?;
    let first_fingerprint = certificate_fingerprint(&authority.certificate_pem)?;
    init_ca("maestro.jsonc", directory.path(), &mut output, &reader).await?;
    let reloaded =
        ClusterCertificateAuthority::load(&directory.path().join("security").join("cluster-ca"))?;
    assert_eq!(
        certificate_fingerprint(&reloaded.certificate_pem)?,
        first_fingerprint
    );

    issue_node(
        "maestro.jsonc",
        directory.path(),
        "node-2".to_string(),
        None,
        &mut output,
        &reader,
    )
    .await?;
    let bundle_path = directory
        .path()
        .join("security/provisioning/node-2.certificates.json");
    let bundle: NodeCertificateBundle = serde_json::from_slice(&std::fs::read(&bundle_path)?)?;
    assert_eq!(
        certificate_fingerprint(&bundle.trust_root_pem)?,
        first_fingerprint
    );
    let error = issue_node(
        "maestro.jsonc",
        directory.path(),
        "node-2".to_string(),
        None,
        &mut output,
        &reader,
    )
    .await
    .expect_err("private bundle overwrite must fail");
    assert!(error.to_string().contains("refusing to overwrite"));
    let output = String::from_utf8(output)?;
    assert!(output.contains("CA SHA-256:"));
    assert!(output.contains("issued worker certificate bundle for `node-2`"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(directory.path().join("security/cluster-ca/ca-key.pem"))?
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(bundle_path)?.permissions().mode() & 0o777,
            0o600
        );
    }
    Ok(())
}

#[tokio::test]
async fn ca_initialization_requires_the_selected_master() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: cluster_document().replace("node: \"node-1\"", "node: \"node-2\""),
    };
    let error = init_ca("maestro.jsonc", directory.path(), &mut Vec::new(), &reader)
        .await
        .expect_err("worker must not initialize the authority");
    assert!(
        error
            .to_string()
            .contains("must run for the declared master")
    );
    assert!(!directory.path().join("security/cluster-ca").exists());
    Ok(())
}

#[tokio::test]
async fn join_preparation_persists_one_private_key_and_prints_approval_command()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: cluster_document().replace("node: \"node-1\"", "node: \"node-2\""),
    };
    let mut first_output = Vec::new();
    prepare_join(
        "maestro.jsonc",
        directory.path(),
        &mut first_output,
        &reader,
    )
    .await?;
    let mut second_output = Vec::new();
    prepare_join(
        "maestro.jsonc",
        directory.path(),
        &mut second_output,
        &reader,
    )
    .await?;
    assert_eq!(first_output, second_output);
    let output = String::from_utf8(first_output)?;
    assert!(output.contains("Node: node-2"));
    assert!(output.contains("Join key SHA-256:"));
    assert!(output.contains("maestro-next cluster approve-node node-2"));
    let key_path = directory.path().join("security/join.key");
    assert!(key_path.exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(key_path)?.permissions().mode() & 0o777,
            0o600
        );
    }
    Ok(())
}

#[tokio::test]
async fn master_bootstrap_creates_and_reuses_one_private_launch_document()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let reader = MemoryReader {
        source: cluster_document(),
    };
    let containerd_socket = std::path::Path::new("/run/containerd/containerd.sock");
    let etcd_binary = std::path::Path::new("/run/current-system/sw/bin/etcd");
    let mut first_output = Vec::new();
    bootstrap(
        "maestro.jsonc",
        directory.path(),
        containerd_socket,
        etcd_binary,
        None,
        &mut first_output,
        &reader,
    )
    .await?;
    let launch_path = directory.path().join("launch.json");
    let first_launch = std::fs::read(&launch_path)?;
    let mut second_output = Vec::new();
    bootstrap(
        "maestro.jsonc",
        directory.path(),
        containerd_socket,
        etcd_binary,
        None,
        &mut second_output,
        &reader,
    )
    .await?;
    assert_eq!(std::fs::read(&launch_path)?, first_launch);
    let launch: serde_json::Value = serde_json::from_slice(&first_launch)?;
    assert_eq!(launch.pointer("/nodeId"), Some(&"node-1".into()));
    assert_eq!(launch.pointer("/storeMode/kind"), Some(&"bootstrap".into()));
    assert_eq!(
        launch.pointer("/etcdBinary"),
        Some(&"/run/current-system/sw/bin/etcd".into())
    );
    assert!(launch.pointer("/certificateIssuer/privateKeyPem").is_some());
    assert!(launch.pointer("/operatorJwtSecret").is_some());
    assert!(launch.pointer("/storeEncryptionSecret").is_some());
    let first_output = String::from_utf8(first_output)?;
    assert!(first_output.contains("created bootstrap launch document"));
    assert!(!first_output.contains("operatorJwtSecret"));
    assert!(
        String::from_utf8(second_output)?.contains("verified existing bootstrap launch document")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(launch_path)?.permissions().mode() & 0o777,
            0o600
        );
    }
    Ok(())
}

fn cluster_document() -> String {
    r#"{
            cluster: {
                name: "test-cluster",
                nodes: {
                    "node-1": {
                        hostname: "node-1.internal",
                        endpoint: "10.20.0.11",
                        subnet: "172.22.1.0/24",
                        role: "master"
                    },
                    "node-2": {
                        hostname: "node-2.internal",
                        endpoint: "10.20.0.12",
                        subnet: "172.22.2.0/24",
                        role: "worker"
                    }
                },
                controlAllowCidrs: ["10.20.0.0/24"],
                joinSecret: "a-test-join-secret-with-at-least-32-characters"
            },
            node: "node-1"
        }"#
    .to_string()
}
