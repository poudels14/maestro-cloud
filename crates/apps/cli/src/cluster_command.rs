use std::io::Write;

use clap::{Subcommand, ValueEnum};
use kernel_api::UpgradeMode;

use crate::CliError;
use crate::api_client::{ApiClient, request_id};
use crate::cluster::{self, NodeLifecycleAction};
use crate::contexts::ContextStore;
use crate::upgrades;

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterCommand {
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
    let contexts = ContextStore::from_environment()?;
    let client = ApiClient::new(contexts.active()?)?;
    match command {
        ClusterCommand::Info => cluster::info(&client, output).await,
        ClusterCommand::Nodes => cluster::list_nodes(&client, output).await,
        ClusterCommand::Config => cluster::show_config(&client, output).await,
        ClusterCommand::Drain {
            node_id,
            idempotency_key,
        } => {
            cluster::node_lifecycle(
                &client,
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
                &client,
                node_id,
                request_id(idempotency_key)?,
                NodeLifecycleAction::Restore,
                output,
            )
            .await
        }
        ClusterCommand::Upgrades => upgrades::list(&client, output).await,
        ClusterCommand::Upgrade {
            target: _,
            target_version,
            batch,
            node_ids,
            upgrade_run_id,
            idempotency_key,
        } => {
            upgrades::start(
                &client,
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
        } => upgrades::cancel(&client, upgrade_run, request_id(idempotency_key)?, output).await,
    }
}
