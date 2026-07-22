use std::io::Write;

use clap::Subcommand;

use crate::CliError;
use crate::api_client::{ApiClient, request_id};
use crate::cluster::{self, NodeLifecycleAction};
use crate::contexts::ContextStore;

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterCommand {
    /// Show the active cluster identity and node capabilities.
    Info,
    /// List durable cluster nodes and their scheduling state.
    Nodes,
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
}

pub(crate) async fn run(command: ClusterCommand, output: &mut dyn Write) -> Result<(), CliError> {
    let contexts = ContextStore::from_environment()?;
    let client = ApiClient::new(contexts.active()?)?;
    match command {
        ClusterCommand::Info => cluster::info(&client, output).await,
        ClusterCommand::Nodes => cluster::list_nodes(&client, output).await,
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
    }
}
