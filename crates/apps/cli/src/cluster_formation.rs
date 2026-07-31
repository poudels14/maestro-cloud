use std::io::Write;
use std::path::{Path, PathBuf};

use cluster::{
    CertificateValidity, ClusterCertificateAuthority, NodeCertificateBundle,
    certificate_fingerprint,
};
use kernel_api::{NodeId, NodeRole};
use time::{Duration, OffsetDateTime};

use crate::CliError;
use crate::config::{load_cluster, load_cluster_for_node};
use crate::config_source::ConfigSourceReader;
use crate::launch_document::DaemonLaunchDocument;
use crate::private_document::{Persisted, persist_private_exact, read_private};

const AUTHORITY_VALIDITY_DAYS: i64 = 3_650;
const NODE_VALIDITY_DAYS: i64 = 825;

pub(crate) async fn init_ca(
    config_source: &str,
    data_directory: &Path,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let loaded = load_cluster(config_source, reader).await?;
    let local_node =
        loaded.cluster.nodes.get(&loaded.node_id).ok_or_else(|| {
            CliError::invalid_input("selected node disappeared from the topology")
        })?;
    if local_node.role != NodeRole::Master {
        return Err(CliError::invalid_input(format!(
            "cluster init-ca must run for the declared master, not `{}`",
            loaded.node_id
        )));
    }

    let authority_directory = authority_directory(data_directory);
    let authority = ClusterCertificateAuthority::load_or_initialize(
        &authority_directory,
        &loaded.cluster.name,
        validity(AUTHORITY_VALIDITY_DAYS)?,
    )
    .map_err(|error| CliError::cluster("failed to initialize cluster CA", error.to_string()))?;
    let fingerprint = certificate_fingerprint(&authority.certificate_pem).map_err(|error| {
        CliError::cluster("failed to fingerprint cluster CA", error.to_string())
    })?;
    writeln!(output, "Cluster: {}", loaded.cluster.cluster_id).map_err(output_error)?;
    writeln!(output, "CA SHA-256: {fingerprint}").map_err(output_error)?;
    writeln!(output, "Authority: {}", authority_directory.display()).map_err(output_error)
}

pub(crate) async fn issue_node(
    config_source: &str,
    data_directory: &Path,
    node_id: String,
    destination: Option<&Path>,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let loaded = load_cluster(config_source, reader).await?;
    let node_id = NodeId::new(node_id)
        .map_err(|error| CliError::invalid_input(format!("invalid node ID: {error}")))?;
    let node = loaded.cluster.nodes.get(&node_id).ok_or_else(|| {
        CliError::invalid_input(format!("node `{node_id}` is absent from cluster.nodes"))
    })?;
    let authority = ClusterCertificateAuthority::load(&authority_directory(data_directory))
        .map_err(|error| {
            CliError::cluster("failed to load initialized cluster CA", error.to_string())
        })?;
    let bundle = authority
        .issue_node_certificate_for_definition(&node_id, node, validity(NODE_VALIDITY_DAYS)?)
        .map_err(|error| {
            CliError::cluster("failed to issue node certificate", error.to_string())
        })?;
    let bundle_path = destination
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_bundle_path(data_directory, &node_id));
    persist_bundle(&bundle_path, &bundle)?;
    writeln!(
        output,
        "[maestro]: issued {} certificate bundle for `{node_id}`",
        role_name(node.role)
    )
    .map_err(output_error)?;
    writeln!(output, "Bundle: {}", bundle_path.display()).map_err(output_error)
}

pub(crate) async fn bootstrap(
    config_source: &str,
    selected_node_id: Option<NodeId>,
    data_directory: &Path,
    containerd_socket: &Path,
    etcd_binary: &Path,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    if !data_directory.is_absolute()
        || !containerd_socket.is_absolute()
        || !etcd_binary.is_absolute()
    {
        return Err(CliError::invalid_input(
            "bootstrap data directory, containerd socket, and etcd binary must be absolute paths",
        ));
    }
    let loaded = match selected_node_id {
        Some(node_id) => load_cluster_for_node(config_source, node_id, reader).await?,
        None => load_cluster(config_source, reader).await?,
    };
    let node =
        loaded.cluster.nodes.get(&loaded.node_id).ok_or_else(|| {
            CliError::invalid_input("selected node disappeared from the topology")
        })?;
    if node.role != NodeRole::Master {
        return Err(CliError::invalid_input(format!(
            "cluster bootstrap must run for the declared master, not `{}`",
            loaded.node_id
        )));
    }
    let launch_path = data_directory.join("launch.json");

    let (launch, persisted, authority) = match read_private(&launch_path, "daemon launch document")
    {
        Ok(encoded) => {
            let launch =
                serde_json::from_slice::<DaemonLaunchDocument>(&encoded).map_err(|source| {
                    CliError::json("failed to decode daemon launch document", source)
                })?;
            launch.validate()?;
            if !launch.matches_bootstrap(
                &loaded.node_id,
                data_directory,
                containerd_socket,
                etcd_binary,
            ) {
                return Err(CliError::invalid_input(format!(
                    "existing daemon launch document `{}` does not match this bootstrap request",
                    launch_path.display()
                )));
            }
            let authority = launch.bootstrap_authority(&loaded.encryption_key)?;
            (launch, Persisted::Reused, authority)
        }
        Err(CliError::NotFound { .. }) => {
            let authority = ClusterCertificateAuthority::generate(
                &loaded.cluster.name,
                validity(AUTHORITY_VALIDITY_DAYS)?,
            )
            .map_err(|error| {
                CliError::cluster("failed to initialize cluster CA", error.to_string())
            })?;
            let security = authority
                .issue_node_certificate_for_definition(
                    &loaded.node_id,
                    node,
                    validity(NODE_VALIDITY_DAYS)?,
                )
                .map_err(|error| {
                    CliError::cluster("failed to issue master certificate", error.to_string())
                })?;
            let launch = DaemonLaunchDocument::bootstrap(
                loaded.node_id.clone(),
                data_directory.to_path_buf(),
                containerd_socket.to_path_buf(),
                etcd_binary.to_path_buf(),
                security,
                authority.clone(),
                &loaded.encryption_key,
            )?;
            let persisted = persist_private_exact(&launch_path, &launch, "daemon launch document")?;
            (launch, persisted, authority)
        }
        Err(error) => return Err(error),
    };
    let fingerprint = certificate_fingerprint(&authority.certificate_pem).map_err(|error| {
        CliError::cluster("failed to fingerprint cluster CA", error.to_string())
    })?;
    writeln!(output, "Cluster: {}", loaded.cluster.cluster_id).map_err(output_error)?;
    writeln!(output, "CA SHA-256: {fingerprint}").map_err(output_error)?;
    writeln!(
        output,
        "[maestro]: {} bootstrap launch document for node `{}`",
        persisted.verb(),
        launch.node_id(),
    )
    .map_err(output_error)?;
    writeln!(output, "Launch config: {}", launch_path.display()).map_err(output_error)?;
    writeln!(
        output,
        "Start with: maestro-daemon start --config {} --data-dir {}",
        config_source,
        data_directory.display(),
    )
    .map_err(output_error)
}

fn authority_directory(data_directory: &Path) -> PathBuf {
    data_directory.join("security").join("cluster-ca")
}

fn default_bundle_path(data_directory: &Path, node_id: &NodeId) -> PathBuf {
    data_directory
        .join("security")
        .join("provisioning")
        .join(format!("{node_id}.certificates.json"))
}

fn validity(days: i64) -> Result<CertificateValidity, CliError> {
    let now = OffsetDateTime::now_utc();
    let not_before = now
        .checked_sub(Duration::minutes(5))
        .ok_or_else(|| CliError::invalid_input("certificate activation time is out of range"))?;
    let not_after = now
        .checked_add(Duration::days(days))
        .ok_or_else(|| CliError::invalid_input("certificate expiration time is out of range"))?;
    CertificateValidity::new(not_before, not_after)
        .map_err(|error| CliError::invalid_input(error.to_string()))
}

fn persist_bundle(path: &Path, bundle: &NodeCertificateBundle) -> Result<(), CliError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|source| {
        CliError::io(
            format!("failed to create bundle directory `{}`", parent.display()),
            source,
        )
    })?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|source| {
        CliError::io(
            format!(
                "failed to create temporary bundle in `{}`",
                parent.display()
            ),
            source,
        )
    })?;
    serde_json::to_writer_pretty(&mut temporary, bundle)
        .map_err(|source| CliError::json("failed to encode node certificate bundle", source))?;
    temporary.write_all(b"\n").map_err(|source| {
        CliError::io(
            format!(
                "failed to write node certificate bundle `{}`",
                path.display()
            ),
            source,
        )
    })?;
    temporary.as_file().sync_all().map_err(|source| {
        CliError::io(
            format!(
                "failed to sync node certificate bundle `{}`",
                path.display()
            ),
            source,
        )
    })?;
    temporary.persist_noclobber(path).map_err(|error| {
        let action = if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            format!(
                "refusing to overwrite node certificate bundle `{}`",
                path.display()
            )
        } else {
            format!(
                "failed to install node certificate bundle `{}`",
                path.display()
            )
        };
        CliError::io(action, error.error)
    })?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            CliError::io(
                format!("failed to sync bundle directory `{}`", parent.display()),
                source,
            )
        })
}

fn role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Master => "master",
        NodeRole::Hybrid => "hybrid",
        NodeRole::ControlPlane => "control-plane",
        NodeRole::Worker => "worker",
    }
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
