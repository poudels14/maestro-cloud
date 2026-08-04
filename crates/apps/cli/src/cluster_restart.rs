use std::io::{BufRead, Write};

use clap::Args;

use crate::CliError;
use crate::cluster;

#[derive(Debug, Args)]
pub(crate) struct RestartSelectionArgs {
    /// Stable cluster node identity; omit to select interactively.
    #[arg(value_name = "NODE_ID", conflicts_with_all = ["all", "local"])]
    node_id: Option<String>,
    /// Drain and restart every node serially.
    #[arg(long, conflicts_with = "local")]
    all: bool,
    /// Restart the node serving the active API context through the coordinated workflow.
    #[arg(long, conflicts_with = "all")]
    local: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestartSelection {
    Direct(RestartTarget),
    LocalNode,
    Prompt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestartTarget {
    EveryNode,
    Node(String),
}

impl TryFrom<RestartSelectionArgs> for RestartSelection {
    type Error = CliError;

    fn try_from(arguments: RestartSelectionArgs) -> Result<Self, Self::Error> {
        match (arguments.node_id, arguments.all, arguments.local) {
            (Some(node_id), false, false) => Ok(Self::Direct(RestartTarget::Node(node_id))),
            (None, true, false) => Ok(Self::Direct(RestartTarget::EveryNode)),
            (None, false, true) => Ok(Self::LocalNode),
            (None, false, false) => Ok(Self::Prompt),
            _ => Err(CliError::invalid_input(
                "restart accepts only one of NODE_ID, --local, or --all",
            )),
        }
    }
}

impl RestartSelection {
    pub(crate) fn direct_target(&self) -> Option<&RestartTarget> {
        match self {
            Self::Direct(target) => Some(target),
            Self::LocalNode | Self::Prompt => None,
        }
    }
}

impl RestartTarget {
    pub(crate) fn into_node_ids(self) -> Vec<String> {
        match self {
            Self::EveryNode => Vec::new(),
            Self::Node(node_id) => vec![node_id],
        }
    }

    fn description(&self) -> String {
        match self {
            Self::EveryNode => "every cluster node serially".to_string(),
            Self::Node(node_id) => format!("cluster node `{node_id}`"),
        }
    }
}

pub(crate) async fn resolve(
    client: &impl cluster::ClusterApi,
    selection: RestartSelection,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<RestartTarget, CliError> {
    match selection {
        RestartSelection::Direct(target) => Ok(target),
        RestartSelection::LocalNode => Ok(RestartTarget::Node(
            client.cluster_config().await?.local_node_id.to_string(),
        )),
        RestartSelection::Prompt => prompt(client, input, output).await,
    }
}

async fn prompt(
    client: &impl cluster::ClusterApi,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<RestartTarget, CliError> {
    let mut nodes = client.list_nodes().await?;
    nodes.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    if nodes.is_empty() {
        return Err(CliError::not_found("cluster has no restartable nodes"));
    }
    writeln!(output, "Select a cluster node to restart:").map_err(output_error)?;
    for (index, node) in nodes.iter().enumerate() {
        writeln!(
            output,
            "  {}) {} ({})",
            index.saturating_add(1),
            node.meta.id,
            node.spec.hostname,
        )
        .map_err(output_error)?;
    }
    write!(output, "Node [1-{} or ID]: ", nodes.len()).map_err(output_error)?;
    output
        .flush()
        .map_err(|source| CliError::io("failed to flush restart selector", source))?;
    let mut selection = String::new();
    input
        .read_line(&mut selection)
        .map_err(|source| CliError::io("failed to read restart selector", source))?;
    let selection = selection.trim();
    if selection.is_empty() {
        return Err(CliError::invalid_input(
            "restart target is required; pass NODE_ID, --local, or --all",
        ));
    }
    let selected = match selection.parse::<usize>() {
        Ok(index) if (1..=nodes.len()).contains(&index) => nodes.get(index - 1),
        Ok(_) => None,
        Err(_) => nodes.iter().find(|node| node.meta.id.as_str() == selection),
    }
    .ok_or_else(|| CliError::invalid_input(format!("unknown restart selection `{selection}`")))?;
    Ok(RestartTarget::Node(selected.meta.id.to_string()))
}

pub(crate) fn confirm(
    target: &RestartTarget,
    admin_origin: &str,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<bool, CliError> {
    writeln!(output, "Admin API: {admin_origin}")
        .map_err(|source| CliError::io("failed to write restart confirmation", source))?;
    write!(output, "Restart {}? [y/N]: ", target.description())
        .map_err(|source| CliError::io("failed to write restart confirmation", source))?;
    output
        .flush()
        .map_err(|source| CliError::io("failed to flush restart confirmation", source))?;
    let mut answer = String::new();
    input
        .read_line(&mut answer)
        .map_err(|source| CliError::io("failed to read restart confirmation", source))?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
