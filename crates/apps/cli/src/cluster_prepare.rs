use std::io::Write;
use std::path::PathBuf;

use kernel_api::NodeRole;

use crate::CliError;
use crate::cluster_formation::bootstrap_loaded;
use crate::cluster_join::{JoinOptions, JoinTransport, ReqwestJoinTransport, join_loaded};
use crate::config::{ClusterConfigFallbacks, load_cluster_with_fallbacks};
use crate::config_source::{ConfigSourceReader, SystemConfigSourceReader};
use crate::launch_document::DaemonLaunchDocument;
use crate::private_document::read_private;

/// Inputs required to create the encrypted node launch document on first boot.
#[derive(Debug, Clone)]
pub struct NodeLaunchOptions {
    pub config_source: String,
    pub data_directory: PathBuf,
    pub containerd_socket: PathBuf,
    pub etcd_binary: PathBuf,
    pub fallbacks: ClusterConfigFallbacks,
}

impl NodeLaunchOptions {
    pub fn new(
        config_source: String,
        data_directory: PathBuf,
        containerd_socket: PathBuf,
        etcd_binary: PathBuf,
    ) -> Self {
        Self {
            config_source,
            data_directory,
            containerd_socket,
            etcd_binary,
            fallbacks: ClusterConfigFallbacks::default(),
        }
    }

    pub fn with_fallbacks(mut self, fallbacks: ClusterConfigFallbacks) -> Self {
        self.fallbacks = fallbacks;
        self
    }
}

/// Creates a missing launch document from the live cluster source.
///
/// Existing launch state is only validated here. The daemon still reloads the
/// live cluster source on every start before opening the encrypted bootstrap.
pub async fn prepare_node_launch(options: NodeLaunchOptions) -> Result<(), CliError> {
    prepare_node_launch_with_transport(
        &options,
        &mut std::io::sink(),
        &SystemConfigSourceReader,
        &ReqwestJoinTransport,
    )
    .await
}

pub(crate) async fn prepare_node_launch_with_transport(
    options: &NodeLaunchOptions,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
    transport: &impl JoinTransport,
) -> Result<(), CliError> {
    validate_paths(options)?;
    let launch_path = options.data_directory.join("launch.json");
    match read_private(&launch_path, "daemon launch document") {
        Ok(encoded) => {
            let launch: DaemonLaunchDocument =
                serde_json::from_slice(&encoded).map_err(|source| {
                    CliError::json("failed to decode daemon launch document", source)
                })?;
            launch.validate()?;
            if !launch.matches_runtime_paths(
                &options.data_directory,
                &options.containerd_socket,
                &options.etcd_binary,
            ) {
                return Err(CliError::invalid_input(format!(
                    "existing daemon launch document `{}` does not match the configured runtime paths",
                    launch_path.display()
                )));
            }
            writeln!(
                output,
                "[maestro]: verified existing launch document for node `{}`",
                launch.node_id()
            )
            .map_err(output_error)?;
            return Ok(());
        }
        Err(CliError::NotFound { .. }) => {}
        Err(error) => return Err(error),
    }

    let loaded =
        load_cluster_with_fallbacks(&options.config_source, reader, &options.fallbacks).await?;
    let local_node =
        loaded.cluster.nodes.get(&loaded.node_id).ok_or_else(|| {
            CliError::invalid_input("selected node disappeared from the topology")
        })?;
    if local_node.role == NodeRole::Master {
        return bootstrap_loaded(
            &options.config_source,
            loaded,
            &options.data_directory,
            &options.containerd_socket,
            &options.etcd_binary,
            output,
        );
    }

    let topology = loaded
        .cluster
        .preflight()
        .map_err(|error| CliError::cluster("cluster preflight failed", error.to_string()))?;
    let master =
        loaded.cluster.nodes.get(topology.master()).ok_or_else(|| {
            CliError::invalid_input("declared master disappeared from the topology")
        })?;
    let leader = format!(
        "https://{}:{}/",
        master.endpoint.host_address, master.endpoint.api_port
    );
    let node_id = loaded.node_id.clone();
    let role = local_node.role;
    let mut join = JoinOptions::new(
        leader,
        options.config_source.clone(),
        options.data_directory.clone(),
    );
    join.node_id = Some(node_id);
    join.containerd_socket
        .clone_from(&options.containerd_socket);
    join.etcd_binary = role.is_control_plane().then(|| options.etcd_binary.clone());
    join_loaded(join, loaded, output, transport).await
}

fn validate_paths(options: &NodeLaunchOptions) -> Result<(), CliError> {
    if !options.data_directory.is_absolute()
        || !options.containerd_socket.is_absolute()
        || !options.etcd_binary.is_absolute()
    {
        return Err(CliError::invalid_input(
            "launch data directory, containerd socket, and etcd binary must be absolute paths",
        ));
    }
    Ok(())
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
