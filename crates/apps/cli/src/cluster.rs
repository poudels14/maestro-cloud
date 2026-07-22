use std::io::Write;

use kernel_api::{
    ClusterInfo, CommandRequest, ConditionState, Node, NodeCommandResponse, NodeId, NodeRole,
    RequestId,
};

use crate::CliError;
use crate::api_client::ApiClient;

pub(crate) async fn info(client: &impl ClusterApi, output: &mut dyn Write) -> Result<(), CliError> {
    let info = client.cluster_info().await?;
    writeln!(output, "Cluster: {}", info.cluster_id).map_err(output_error)?;
    writeln!(output, "Nodes: {}", info.node_count).map_err(output_error)?;
    writeln!(output, "Control plane: {}", info.control_plane_node_count).map_err(output_error)?;
    writeln!(output, "Workload eligible: {}", info.workload_node_count).map_err(output_error)
}

pub(crate) async fn list_nodes(
    client: &impl ClusterApi,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let mut nodes = client.list_nodes().await?;
    nodes.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    if nodes.is_empty() {
        return writeln!(output, "[maestro]: no cluster nodes found").map_err(output_error);
    }
    let rows = nodes.iter().map(NodeRow::from).collect::<Vec<_>>();
    let widths = NodeWidths::for_rows(&rows);
    writeln!(
        output,
        "{:<id_width$}  {:<host_width$}  {:<role_width$}  {:<address_width$}  {:<state_width$}  VERSION",
        "NODE",
        "HOSTNAME",
        "ROLE",
        "ADDRESS",
        "STATE",
        id_width = widths.id,
        host_width = widths.hostname,
        role_width = widths.role,
        address_width = widths.address,
        state_width = widths.state,
    )
    .map_err(output_error)?;
    for row in rows {
        writeln!(
            output,
            "{:<id_width$}  {:<host_width$}  {:<role_width$}  {:<address_width$}  {:<state_width$}  {}",
            row.id,
            row.hostname,
            row.role,
            row.address,
            row.state,
            row.version,
            id_width = widths.id,
            host_width = widths.hostname,
            role_width = widths.role,
            address_width = widths.address,
            state_width = widths.state,
        )
        .map_err(output_error)?;
    }
    Ok(())
}

pub(crate) async fn node_lifecycle(
    client: &impl ClusterApi,
    node_id: String,
    request_id: RequestId,
    action: NodeLifecycleAction,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let node_id =
        NodeId::new(node_id).map_err(|error| CliError::invalid_input(error.to_string()))?;
    let node = client.get_node(&node_id).await?;
    let response = client
        .command_node(
            &node_id,
            &request_id,
            action,
            CommandRequest {
                expected_revision: node.meta.revision,
            },
        )
        .await?;
    if response.node_id != node_id || response.draining != action.draining() {
        return Err(CliError::invalid_api_response(
            "node command receipt does not match the submitted command",
        ));
    }
    writeln!(
        output,
        "[maestro]: node `{}` {} accepted",
        response.node_id,
        action.verb()
    )
    .map_err(output_error)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeLifecycleAction {
    Drain,
    Restore,
}

impl NodeLifecycleAction {
    fn verb(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::Restore => "restore",
        }
    }

    fn draining(self) -> bool {
        matches!(self, Self::Drain)
    }
}

pub(crate) trait ClusterApi {
    async fn cluster_info(&self) -> Result<ClusterInfo, CliError>;

    async fn list_nodes(&self) -> Result<Vec<Node>, CliError>;

    async fn get_node(&self, node_id: &NodeId) -> Result<Node, CliError>;

    async fn command_node(
        &self,
        node_id: &NodeId,
        request_id: &RequestId,
        action: NodeLifecycleAction,
        request: CommandRequest,
    ) -> Result<NodeCommandResponse, CliError>;
}

impl ClusterApi for ApiClient {
    async fn cluster_info(&self) -> Result<ClusterInfo, CliError> {
        self.get("/api/cluster").await
    }

    async fn list_nodes(&self) -> Result<Vec<Node>, CliError> {
        self.get("/api/cluster/nodes").await
    }

    async fn get_node(&self, node_id: &NodeId) -> Result<Node, CliError> {
        self.get(&format!("/api/cluster/nodes/{node_id}")).await
    }

    async fn command_node(
        &self,
        node_id: &NodeId,
        request_id: &RequestId,
        action: NodeLifecycleAction,
        request: CommandRequest,
    ) -> Result<NodeCommandResponse, CliError> {
        self.post(
            &format!("/api/cluster/nodes/{node_id}/{}", action.verb()),
            request_id,
            &request,
        )
        .await
    }
}

struct NodeRow {
    id: String,
    hostname: String,
    role: &'static str,
    address: String,
    state: &'static str,
    version: String,
}

impl From<&Node> for NodeRow {
    fn from(node: &Node) -> Self {
        Self {
            id: node.meta.id.to_string(),
            hostname: node.spec.hostname.clone(),
            role: role_name(node.spec.role),
            address: node.spec.host_address.to_string(),
            state: node_state(node),
            version: node.status.version.clone(),
        }
    }
}

fn role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Master => "master",
        NodeRole::Hybrid => "hybrid",
        NodeRole::ControlPlane => "control",
        NodeRole::Worker => "worker",
    }
}

fn node_state(node: &Node) -> &'static str {
    let conditions = node
        .status
        .conditions
        .iter()
        .filter(|condition| condition.condition_type.0 == "Draining")
        .collect::<Vec<_>>();
    match conditions.as_slice() {
        [] => "schedulable",
        [condition] => match condition.state {
            ConditionState::True => "draining",
            ConditionState::False => "schedulable",
            ConditionState::Unknown if condition.reason.0 == "ReplicatingArtifacts" => {
                "preparing-drain"
            }
            ConditionState::Unknown => "unknown",
        },
        _ => "unknown",
    }
}

#[derive(Clone, Copy)]
struct NodeWidths {
    id: usize,
    hostname: usize,
    role: usize,
    address: usize,
    state: usize,
}

impl NodeWidths {
    fn for_rows(rows: &[NodeRow]) -> Self {
        Self {
            id: width("NODE", rows.iter().map(|row| row.id.as_str())),
            hostname: width("HOSTNAME", rows.iter().map(|row| row.hostname.as_str())),
            role: width("ROLE", rows.iter().map(|row| row.role)),
            address: width("ADDRESS", rows.iter().map(|row| row.address.as_str())),
            state: width("STATE", rows.iter().map(|row| row.state)),
        }
    }
}

fn width<'a>(heading: &'a str, values: impl Iterator<Item = &'a str>) -> usize {
    values
        .map(str::len)
        .fold(heading.len(), |current, value| current.max(value))
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
