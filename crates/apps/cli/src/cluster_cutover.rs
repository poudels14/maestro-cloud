use std::io::Write;
use std::path::{Path, PathBuf};

use cluster::{
    CertificateValidity, ClusterCertificateAuthority, NodeCertificateBundle,
    certificate_fingerprint,
};
use kernel_api::{NodeId, SecretValue};
use time::{Duration, OffsetDateTime};

use crate::CliError;
use crate::config::load_cluster;
use crate::config_source::ConfigSourceReader;
use crate::launch_document::DaemonLaunchDocument;
use crate::private_document::{Persisted, persist_private_exact, read_private};

const MAXIMUM_SECRET_BYTES: usize = 64 * 1_024;
const NODE_VALIDITY_DAYS: i64 = 825;

#[derive(Debug)]
pub(crate) struct CutoverBundleOptions {
    pub(crate) config_source: String,
    pub(crate) authority_data_directory: PathBuf,
    pub(crate) target_data_directory: PathBuf,
    pub(crate) containerd_socket: PathBuf,
    pub(crate) etcd_binary: PathBuf,
    pub(crate) store_secret_file: PathBuf,
    pub(crate) output_directory: PathBuf,
}

pub(crate) async fn prepare_cutover_bundle(
    options: CutoverBundleOptions,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    validate_paths(&options)?;
    let loaded = load_cluster(&options.config_source, reader).await?;
    let authority = ClusterCertificateAuthority::load(
        &options.authority_data_directory.join("security/cluster-ca"),
    )
    .map_err(|error| CliError::cluster("failed to load cluster CA", error.to_string()))?;
    let store_encryption_secret =
        read_secret(&options.store_secret_file, "store encryption secret")?;
    ensure_private_directory(&options.output_directory)?;
    let provisioning = options
        .authority_data_directory
        .join("security/provisioning");
    ensure_private_directory(&provisioning)?;

    let fingerprint = certificate_fingerprint(&authority.certificate_pem).map_err(|error| {
        CliError::cluster("failed to fingerprint cluster CA", error.to_string())
    })?;
    writeln!(output, "Cluster: {}", loaded.cluster.cluster_id).map_err(output_error)?;
    writeln!(output, "CA SHA-256: {fingerprint}").map_err(output_error)?;
    for (node_id, node) in &loaded.cluster.nodes {
        let bundle_path = provisioning.join(format!("{node_id}.certificates.json"));
        let security = load_or_issue_bundle(&bundle_path, node_id, node, &authority)?;
        let launch = DaemonLaunchDocument::cutover(
            loaded.cluster.clone(),
            node_id.clone(),
            options.target_data_directory.clone(),
            options.containerd_socket.clone(),
            options.etcd_binary.clone(),
            security,
            authority.clone(),
            loaded.jwt_secret_key.clone(),
            store_encryption_secret.clone(),
            loaded.launch_policy.clone(),
        )?;
        let launch_path = options
            .output_directory
            .join(format!("{node_id}.launch.json"));
        let persisted = persist_private_exact(&launch_path, &launch, "cutover launch document")?;
        writeln!(
            output,
            "[maestro]: {} cutover launch document for `{node_id}`: {}",
            persisted.verb(),
            launch_path.display()
        )
        .map_err(output_error)?;
    }
    Ok(())
}

fn validate_paths(options: &CutoverBundleOptions) -> Result<(), CliError> {
    for (description, path) in [
        (
            "authority data directory",
            options.authority_data_directory.as_path(),
        ),
        (
            "target data directory",
            options.target_data_directory.as_path(),
        ),
        ("containerd socket", options.containerd_socket.as_path()),
        ("etcd binary", options.etcd_binary.as_path()),
        ("store secret file", options.store_secret_file.as_path()),
        ("output directory", options.output_directory.as_path()),
    ] {
        if !path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(CliError::invalid_input(format!(
                "cutover {description} must be an absolute path without parent traversal"
            )));
        }
    }
    Ok(())
}

fn read_secret(path: &Path, description: &str) -> Result<SecretValue, CliError> {
    let bytes = read_private(path, description)?;
    if bytes.len() > MAXIMUM_SECRET_BYTES {
        return Err(CliError::invalid_input(format!(
            "{description} `{}` exceeds {MAXIMUM_SECRET_BYTES} bytes",
            path.display()
        )));
    }
    let secret = String::from_utf8(bytes)
        .map_err(|_| CliError::invalid_input(format!("{description} must be UTF-8")))?;
    if secret.chars().count() < 32 || secret.contains('\0') {
        return Err(CliError::invalid_input(format!(
            "{description} must contain at least 32 characters and no NUL bytes"
        )));
    }
    Ok(SecretValue::new(secret))
}

fn load_or_issue_bundle(
    path: &Path,
    node_id: &NodeId,
    node: &cluster::NodeDefinition,
    authority: &ClusterCertificateAuthority,
) -> Result<NodeCertificateBundle, CliError> {
    match read_private(path, "node certificate bundle") {
        Ok(encoded) => {
            let bundle =
                serde_json::from_slice::<NodeCertificateBundle>(&encoded).map_err(|source| {
                    CliError::json("failed to decode node certificate bundle", source)
                })?;
            validate_bundle(&bundle, authority)?;
            Ok(bundle)
        }
        Err(CliError::NotFound { .. }) => {
            let bundle = authority
                .issue_node_certificate_for_definition(node_id, node, node_validity()?)
                .map_err(|error| {
                    CliError::cluster("failed to issue node certificate", error.to_string())
                })?;
            let persisted = persist_private_exact(path, &bundle, "node certificate bundle")?;
            debug_assert!(matches!(persisted, Persisted::Created));
            Ok(bundle)
        }
        Err(error) => Err(error),
    }
}

fn validate_bundle(
    bundle: &NodeCertificateBundle,
    authority: &ClusterCertificateAuthority,
) -> Result<(), CliError> {
    if bundle.trust_root_pem != authority.certificate_pem
        || bundle.identity.private_key_pem.expose().trim().is_empty()
    {
        return Err(CliError::invalid_input(
            "persisted node certificate bundle does not match the cutover authority",
        ));
    }
    certificate_fingerprint(&bundle.identity.certificate_pem)
        .map(|_| ())
        .map_err(|error| {
            CliError::cluster(
                "persisted node certificate bundle is invalid",
                error.to_string(),
            )
        })
}

fn ensure_private_directory(path: &Path) -> Result<(), CliError> {
    let created = match std::fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(CliError::invalid_input(format!(
                "protected cutover path `{}` must be a directory",
                path.display()
            )));
        }
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(path).map_err(|source| {
                CliError::io(
                    format!("failed to create cutover directory `{}`", path.display()),
                    source,
                )
            })?;
            true
        }
        Err(source) => {
            return Err(CliError::io(
                format!("failed to inspect cutover directory `{}`", path.display()),
                source,
            ));
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if created {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(
                |source| {
                    CliError::io(
                        format!("failed to protect cutover directory `{}`", path.display()),
                        source,
                    )
                },
            )?;
        }
        let mode = std::fs::metadata(path)
            .map_err(|source| {
                CliError::io(
                    format!("failed to inspect cutover directory `{}`", path.display()),
                    source,
                )
            })?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(CliError::invalid_input(format!(
                "protected cutover directory `{}` has insecure permissions {mode:#o}",
                path.display()
            )));
        }
    }
    #[cfg(not(unix))]
    let _ = created;
    Ok(())
}

fn node_validity() -> Result<CertificateValidity, CliError> {
    let now = OffsetDateTime::now_utc();
    let not_before = now
        .checked_sub(Duration::minutes(5))
        .ok_or_else(|| CliError::invalid_input("certificate activation time is out of range"))?;
    let not_after = now
        .checked_add(Duration::days(NODE_VALIDITY_DAYS))
        .ok_or_else(|| CliError::invalid_input("certificate expiration time is out of range"))?;
    CertificateValidity::new(not_before, not_after)
        .map_err(|error| CliError::invalid_input(error.to_string()))
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
