use crate::CliError;
use crate::cluster_formation::bootstrap;
use crate::cluster_jwt::rotate_jwt_key;
use crate::config_source::ConfigSourceReader;

const CURRENT_KEY: &str = "operator-test-secret-with-at-least-32-characters";
const REPLACEMENT_KEY: &str = "replacement-test-secret-with-at-least-32-characters";

struct MemoryReader {
    source: String,
}

impl ConfigSourceReader for MemoryReader {
    async fn read(&self, _source: &str) -> Result<String, CliError> {
        Ok(self.source.clone())
    }
}

#[tokio::test]
async fn rotation_atomically_replaces_only_the_private_launch_key()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let initial_reader = MemoryReader {
        source: super::cluster_formation::cluster_document(),
    };
    bootstrap(
        "maestro.jsonc",
        directory.path(),
        std::path::Path::new("/run/containerd/containerd.sock"),
        std::path::Path::new("/run/current-system/sw/bin/etcd"),
        None,
        &mut Vec::new(),
        &initial_reader,
    )
    .await?;
    let launch_path = directory.path().join("launch.json");
    let original: serde_json::Value = serde_json::from_slice(&std::fs::read(&launch_path)?)?;
    let replacement_reader = MemoryReader {
        source: super::cluster_formation::cluster_document().replace(CURRENT_KEY, REPLACEMENT_KEY),
    };
    let mut output = Vec::new();
    rotate_jwt_key(
        "maestro.jsonc",
        &launch_path,
        &mut output,
        &replacement_reader,
    )
    .await?;
    let rotated: serde_json::Value = serde_json::from_slice(&std::fs::read(&launch_path)?)?;
    assert_eq!(
        rotated.pointer("/jwtSecretKey"),
        Some(&REPLACEMENT_KEY.into())
    );
    assert_eq!(rotated.pointer("/cluster"), original.pointer("/cluster"));
    assert_eq!(
        rotated.pointer("/storeEncryptionSecret"),
        original.pointer("/storeEncryptionSecret")
    );
    let output = String::from_utf8(output)?;
    assert!(output.contains("updated JWT secret key"));
    assert!(!output.contains(REPLACEMENT_KEY));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&launch_path)?.permissions().mode() & 0o777,
            0o600
        );
    }
    Ok(())
}
