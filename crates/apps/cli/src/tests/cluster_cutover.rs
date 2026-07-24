use std::fs;
use std::path::Path;

use crate::CliError;
use crate::cluster_cutover::{CutoverBundleOptions, prepare_cutover_bundle};
use crate::cluster_formation::init_ca;
use crate::config_source::ConfigSourceReader;

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct MemoryReader {
    source: String,
}

impl ConfigSourceReader for MemoryReader {
    async fn read(&self, _source: &str) -> Result<String, CliError> {
        Ok(self.source.clone())
    }
}

#[cfg(unix)]
#[tokio::test]
async fn cutover_bundle_creates_restart_and_client_documents_idempotently() -> TestResult {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir()?;
    let authority_data = directory.path().join("authority");
    fs::create_dir(&authority_data)?;
    fs::set_permissions(&authority_data, fs::Permissions::from_mode(0o700))?;
    let reader = MemoryReader {
        source: super::cluster_formation::cluster_document(),
    };
    init_ca("maestro.jsonc", &authority_data, &mut Vec::new(), &reader).await?;
    let store_secret = directory.path().join("store-secret");
    let operator_secret = directory.path().join("operator-secret");
    private_file(
        &store_secret,
        b"exact-migrated-store-secret-at-least-32-characters",
    )?;
    private_file(
        &operator_secret,
        b"new-operator-jwt-secret-at-least-32-characters",
    )?;
    let output_directory = directory.path().join("launches");
    let options = || CutoverBundleOptions {
        config_source: "maestro.jsonc".to_owned(),
        authority_data_directory: authority_data.clone(),
        target_data_directory: Path::new("/var/lib/maestro").to_path_buf(),
        containerd_socket: Path::new("/run/containerd/containerd.sock").to_path_buf(),
        etcd_binary: Path::new("/run/current-system/sw/bin/etcd").to_path_buf(),
        store_secret_file: store_secret.clone(),
        operator_secret_file: operator_secret.clone(),
        output_directory: output_directory.clone(),
    };

    let mut first_output = Vec::new();
    prepare_cutover_bundle(options(), &mut first_output, &reader).await?;
    let master_path = output_directory.join("node-1.launch.json");
    let worker_path = output_directory.join("node-2.launch.json");
    let master_bytes = fs::read(&master_path)?;
    let worker_bytes = fs::read(&worker_path)?;
    let master: serde_json::Value = serde_json::from_slice(&master_bytes)?;
    let worker: serde_json::Value = serde_json::from_slice(&worker_bytes)?;
    assert_eq!(master.pointer("/storeMode/kind"), Some(&"restart".into()));
    assert_eq!(
        master.pointer("/etcdBinary"),
        Some(&"/run/current-system/sw/bin/etcd".into())
    );
    assert!(master.pointer("/certificateIssuer/privateKeyPem").is_some());
    assert_eq!(worker.pointer("/storeMode/kind"), Some(&"client".into()));
    assert!(worker.pointer("/etcdBinary").is_none());
    assert!(worker.pointer("/certificateIssuer").is_none());
    assert_eq!(
        master.pointer("/storeEncryptionSecret"),
        Some(&"exact-migrated-store-secret-at-least-32-characters".into())
    );
    assert_eq!(
        master.pointer("/operatorJwtSecret"),
        Some(&"new-operator-jwt-secret-at-least-32-characters".into())
    );

    let mut second_output = Vec::new();
    prepare_cutover_bundle(options(), &mut second_output, &reader).await?;
    assert_eq!(fs::read(&master_path)?, master_bytes);
    assert_eq!(fs::read(&worker_path)?, worker_bytes);
    assert!(String::from_utf8(first_output)?.contains("created cutover launch document"));
    assert!(
        String::from_utf8(second_output)?.contains("verified existing cutover launch document")
    );
    assert_eq!(
        fs::metadata(&output_directory)?.permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(master_path)?.permissions().mode() & 0o777,
        0o600
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn cutover_bundle_rejects_public_secret_files() -> TestResult {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir()?;
    let authority_data = directory.path().join("authority");
    fs::create_dir(&authority_data)?;
    fs::set_permissions(&authority_data, fs::Permissions::from_mode(0o700))?;
    let reader = MemoryReader {
        source: super::cluster_formation::cluster_document(),
    };
    init_ca("maestro.jsonc", &authority_data, &mut Vec::new(), &reader).await?;
    let store_secret = directory.path().join("store-secret");
    let operator_secret = directory.path().join("operator-secret");
    private_file(&store_secret, b"s".repeat(40).as_slice())?;
    fs::write(&operator_secret, "o".repeat(40))?;
    fs::set_permissions(&operator_secret, fs::Permissions::from_mode(0o644))?;

    let error = prepare_cutover_bundle(
        CutoverBundleOptions {
            config_source: "maestro.jsonc".to_owned(),
            authority_data_directory: authority_data,
            target_data_directory: Path::new("/var/lib/maestro").to_path_buf(),
            containerd_socket: Path::new("/run/containerd/containerd.sock").to_path_buf(),
            etcd_binary: Path::new("/run/current-system/sw/bin/etcd").to_path_buf(),
            store_secret_file: store_secret,
            operator_secret_file: operator_secret,
            output_directory: directory.path().join("launches"),
        },
        &mut Vec::new(),
        &reader,
    )
    .await
    .expect_err("public secret must be rejected");
    assert!(error.to_string().contains("insecure permissions"));
    Ok(())
}

#[cfg(unix)]
fn private_file(path: &Path, value: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, value)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}
