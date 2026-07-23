use std::io::Write;
use std::path::PathBuf;

use clap::{Subcommand, ValueEnum};
use kernel_api::UpgradeMode;

use crate::CliError;
use crate::api_client::{ApiClient, request_id};
use crate::cluster::{self, NodeLifecycleAction};
use crate::cluster_formation;
use crate::cluster_join::{self, JoinOptions};
use crate::config_source::SystemConfigSourceReader;
use crate::contexts::ContextStore;
use crate::upgrades;

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterCommand {
    /// Initialize or verify the cluster certificate authority on the master.
    InitCa {
        /// Cluster configuration source.
        #[arg(long, default_value = "maestro.jsonc")]
        config: String,
        /// Protected data directory that owns cluster security material.
        #[arg(long, value_name = "PATH")]
        data_dir: PathBuf,
    },
    /// Initialize the master and create its private daemon bootstrap document.
    Bootstrap {
        /// Cluster configuration source.
        #[arg(long, default_value = "maestro.jsonc")]
        config: String,
        /// Protected absolute data directory that will own daemon state.
        #[arg(long, value_name = "PATH")]
        data_dir: PathBuf,
        /// Absolute containerd gRPC socket path.
        #[arg(
            long,
            value_name = "PATH",
            default_value = "/run/containerd/containerd.sock"
        )]
        containerd_socket: PathBuf,
        /// Absolute etcd executable used by the embedded store provider.
        #[arg(long, value_name = "PATH")]
        etcd_binary: PathBuf,
        /// Create the private daemon launch document at this path.
        #[arg(long, value_name = "PATH")]
        output: Option<PathBuf>,
    },
    /// Issue a private certificate bundle for one declared cluster node.
    IssueNode {
        /// Cluster configuration source.
        #[arg(long, default_value = "maestro.jsonc")]
        config: String,
        /// Protected data directory containing the initialized authority.
        #[arg(long, value_name = "PATH")]
        data_dir: PathBuf,
        /// Stable node identity already declared in the cluster topology.
        #[arg(long)]
        node_id: String,
        /// Create the private bundle at this path instead of the default.
        #[arg(long, value_name = "PATH")]
        output: Option<PathBuf>,
    },
    /// Prepare this declared node's durable join key for operator approval.
    PrepareJoin {
        /// Cluster configuration source.
        #[arg(long, default_value = "maestro.jsonc")]
        config: String,
        /// Protected data directory that will own the joined daemon state.
        #[arg(long, value_name = "PATH")]
        data_dir: PathBuf,
    },
    /// Approve one declared node's prepared join-key fingerprint.
    ApproveNode {
        /// Stable node identity already declared in the cluster topology.
        node_id: String,
        /// SHA-256 fingerprint printed by `cluster prepare-join` on that node.
        public_key_sha256: String,
    },
    /// Join an approved declared node through an authenticated cluster endpoint.
    Join {
        /// HTTPS origin of a running control-plane node.
        leader: String,
        /// Cluster configuration source containing this node and the join secret.
        #[arg(long, default_value = "maestro.jsonc")]
        config: String,
        /// Protected absolute data directory that will own daemon state.
        #[arg(long, value_name = "PATH")]
        data_dir: PathBuf,
        /// Absolute containerd gRPC socket path.
        #[arg(
            long,
            value_name = "PATH",
            default_value = "/run/containerd/containerd.sock"
        )]
        containerd_socket: PathBuf,
        /// Absolute etcd executable required for control-plane nodes.
        #[arg(long, value_name = "PATH")]
        etcd_binary: Option<PathBuf>,
        /// Create the private daemon launch document at this path.
        #[arg(long, value_name = "PATH")]
        output: Option<PathBuf>,
    },
    /// Show the active cluster identity and node capabilities.
    Info,
    /// List durable cluster nodes and their scheduling state.
    Nodes,
    /// Show the active cluster configuration with secrets omitted.
    Config,
    /// Stop new workload placement on a node and drain its assignments.
    Drain {
        /// Stable cluster node identity.
        node_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Make a drained node eligible for workload placement again.
    Restore {
        /// Stable cluster node identity.
        node_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Drain and permanently remove one node identity from the cluster.
    RemoveNode {
        /// Stable cluster node identity to retire permanently.
        node_id: String,
        /// Stable workflow key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// List retained cluster upgrade runs.
    Upgrades,
    /// Start a rolling or all-node cluster upgrade.
    Upgrade {
        /// Compatibility spelling retained for `cluster upgrade system`.
        #[arg(value_enum)]
        target: Option<UpgradeTarget>,
        /// Minimum daemon version every selected node must reach.
        #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
        target_version: String,
        /// Node batching strategy.
        #[arg(long, value_enum, default_value_t = UpgradeBatch::Rolling)]
        batch: UpgradeBatch,
        /// Limit the run to one or more node identities.
        #[arg(long = "node")]
        node_ids: Vec<String>,
        /// Stable upgrade resource identity to reuse on retry.
        #[arg(long)]
        upgrade_run_id: Option<String>,
        /// Stable request key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Cancel a stale upgrade run and release its owned drains.
    Unfreeze {
        /// Exact upgrade run identity to cancel.
        #[arg(long)]
        upgrade_run: String,
        /// Stable request key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum UpgradeBatch {
    Rolling,
    All,
}

impl From<UpgradeBatch> for UpgradeMode {
    fn from(batch: UpgradeBatch) -> Self {
        match batch {
            UpgradeBatch::Rolling => Self::Rolling,
            UpgradeBatch::All => Self::AllNodes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum UpgradeTarget {
    System,
}

pub(crate) async fn run(command: ClusterCommand, output: &mut dyn Write) -> Result<(), CliError> {
    match command {
        ClusterCommand::InitCa { config, data_dir } => {
            cluster_formation::init_ca(&config, &data_dir, output, &SystemConfigSourceReader).await
        }
        ClusterCommand::Bootstrap {
            config,
            data_dir,
            containerd_socket,
            etcd_binary,
            output: destination,
        } => {
            cluster_formation::bootstrap(
                &config,
                &data_dir,
                &containerd_socket,
                &etcd_binary,
                destination.as_deref(),
                output,
                &SystemConfigSourceReader,
            )
            .await
        }
        ClusterCommand::IssueNode {
            config,
            data_dir,
            node_id,
            output: destination,
        } => {
            cluster_formation::issue_node(
                &config,
                &data_dir,
                node_id,
                destination.as_deref(),
                output,
                &SystemConfigSourceReader,
            )
            .await
        }
        ClusterCommand::PrepareJoin { config, data_dir } => {
            cluster_formation::prepare_join(&config, &data_dir, output, &SystemConfigSourceReader)
                .await
        }
        ClusterCommand::ApproveNode {
            node_id,
            public_key_sha256,
        } => cluster::approve_node(&active_client()?, node_id, public_key_sha256, output).await,
        ClusterCommand::Join {
            leader,
            config,
            data_dir,
            containerd_socket,
            etcd_binary,
            output: destination,
        } => {
            let mut options = JoinOptions::new(leader, config, data_dir);
            options.containerd_socket = containerd_socket;
            options.etcd_binary = etcd_binary;
            options.output = destination;
            cluster_join::join(options, output, &SystemConfigSourceReader).await
        }
        ClusterCommand::Info => cluster::info(&active_client()?, output).await,
        ClusterCommand::Nodes => cluster::list_nodes(&active_client()?, output).await,
        ClusterCommand::Config => cluster::show_config(&active_client()?, output).await,
        ClusterCommand::Drain {
            node_id,
            idempotency_key,
        } => {
            cluster::node_lifecycle(
                &active_client()?,
                node_id,
                request_id(idempotency_key)?,
                NodeLifecycleAction::Drain,
                output,
            )
            .await
        }
        ClusterCommand::Restore {
            node_id,
            idempotency_key,
        } => {
            cluster::node_lifecycle(
                &active_client()?,
                node_id,
                request_id(idempotency_key)?,
                NodeLifecycleAction::Restore,
                output,
            )
            .await
        }
        ClusterCommand::RemoveNode {
            node_id,
            idempotency_key,
        } => {
            cluster::remove_node(
                &active_client()?,
                node_id,
                request_id(idempotency_key)?,
                output,
            )
            .await
        }
        ClusterCommand::Upgrades => upgrades::list(&active_client()?, output).await,
        ClusterCommand::Upgrade {
            target: _,
            target_version,
            batch,
            node_ids,
            upgrade_run_id,
            idempotency_key,
        } => {
            upgrades::start(
                &active_client()?,
                target_version,
                batch.into(),
                node_ids,
                upgrade_run_id,
                request_id(idempotency_key)?,
                output,
            )
            .await
        }
        ClusterCommand::Unfreeze {
            upgrade_run,
            idempotency_key,
        } => {
            upgrades::cancel(
                &active_client()?,
                upgrade_run,
                request_id(idempotency_key)?,
                output,
            )
            .await
        }
    }
}

fn active_client() -> Result<ApiClient, CliError> {
    let contexts = ContextStore::from_environment()?;
    ApiClient::new(contexts.active()?)
}
