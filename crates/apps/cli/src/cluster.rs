use std::io::Write;
use std::time::Duration;

use kernel_api::{
    ClusterInfo, CommandRequest, ConditionState, MaskedClusterConfig, Node, NodeCommandResponse,
    NodeId, NodeRemovalRequest, NodeRemovalResponse, NodeRemovalState, NodeRole, RequestId,
};
use sha2::{Digest, Sha256};

use crate::CliError;
use crate::api_client::ApiClient;

const REMOVAL_POLL_INTERVAL: Duration = Duration::from_secs(2);
const REMOVAL_TIMEOUT: Duration = Duration::from_secs(90);

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

pub(crate) async fn show_config(
    client: &impl ClusterApi,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let config = client.cluster_config().await?;
    serde_json::to_writer_pretty(&mut *output, &config)
        .map_err(|source| CliError::json("failed to format cluster configuration", source))?;
    writeln!(output).map_err(output_error)
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

pub(crate) async fn remove_node(
    client: &impl ClusterApi,
    node_id: String,
    request_seed: RequestId,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    remove_node_with_timing(
        client,
        node_id,
        request_seed,
        REMOVAL_POLL_INTERVAL,
        REMOVAL_TIMEOUT,
        output,
    )
    .await
}

pub(crate) async fn remove_node_with_timing(
    client: &impl ClusterApi,
    node_id: String,
    request_seed: RequestId,
    poll_interval: Duration,
    timeout: Duration,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let node_id =
        NodeId::new(node_id).map_err(|error| CliError::invalid_input(error.to_string()))?;
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempt = 0_u64;
    loop {
        let request_id = removal_request_id(&request_seed, &node_id, attempt)?;
        let response = client
            .remove_node(
                &node_id,
                &request_id,
                NodeRemovalRequest {
                    node_id: node_id.clone(),
                },
            )
            .await?;
        if response.node_id != node_id {
            return Err(CliError::invalid_api_response(
                "node removal receipt does not match the submitted node",
            ));
        }
        match response.state {
            NodeRemovalState::Removed => {
                writeln!(output, "[maestro]: node `{node_id}` removed").map_err(output_error)?;
                writeln!(
                    output,
                    "rotate cluster credentials if the removed node may be compromised"
                )
                .map_err(output_error)?;
                return Ok(());
            }
            NodeRemovalState::Draining => {
                writeln!(output, "[maestro]: waiting for node `{node_id}` to drain")
                    .map_err(output_error)?;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CliError::cluster(
                "remove node",
                "timed out waiting for drain and membership cleanup",
            ));
        }
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(poll_interval).await;
    }
}

fn removal_request_id(
    seed: &RequestId,
    node_id: &NodeId,
    attempt: u64,
) -> Result<RequestId, CliError> {
    match attempt {
        0 => Ok(seed.clone()),
        _ => {
            let mut digest = Sha256::new();
            digest.update(seed.as_str().as_bytes());
            digest.update([0]);
            digest.update(node_id.as_str().as_bytes());
            digest.update(attempt.to_be_bytes());
            RequestId::new(hex::encode(digest.finalize()))
                .map_err(|error| CliError::invalid_input(error.to_string()))
        }
    }
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

    async fn cluster_config(&self) -> Result<MaskedClusterConfig, CliError>;

    async fn list_nodes(&self) -> Result<Vec<Node>, CliError>;

    async fn get_node(&self, node_id: &NodeId) -> Result<Node, CliError>;

    async fn command_node(
        &self,
        node_id: &NodeId,
        request_id: &RequestId,
        action: NodeLifecycleAction,
        request: CommandRequest,
    ) -> Result<NodeCommandResponse, CliError>;

    async fn remove_node(
        &self,
        node_id: &NodeId,
        request_id: &RequestId,
        request: NodeRemovalRequest,
    ) -> Result<NodeRemovalResponse, CliError>;
}

impl ClusterApi for ApiClient {
    async fn cluster_info(&self) -> Result<ClusterInfo, CliError> {
        self.get("/api/cluster").await
    }

    async fn cluster_config(&self) -> Result<MaskedClusterConfig, CliError> {
        self.get("/api/config").await
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

    async fn remove_node(
        &self,
        node_id: &NodeId,
        request_id: &RequestId,
        request: NodeRemovalRequest,
    ) -> Result<NodeRemovalResponse, CliError> {
        self.delete(
            &format!("/api/cluster/nodes/{node_id}"),
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
    let conditions = &node.status.conditions;
    if conditions.iter().any(|condition| {
        condition.condition_type == kernel_api::ConditionType::Maintenance
            && condition.state == ConditionState::True
    }) {
        return "maintenance";
    }
    if conditions.iter().any(|condition| {
        condition.condition_type == kernel_api::ConditionType::Draining
            && condition.state == ConditionState::True
    }) {
        return "draining";
    }
    if conditions.iter().any(|condition| {
        condition.condition_type == kernel_api::ConditionType::Schedulable
            && condition.state == ConditionState::False
    }) {
        return "unschedulable";
    }
    if conditions.iter().any(|condition| {
        condition.condition_type == kernel_api::ConditionType::Draining
            && condition.state == ConditionState::Unknown
            && condition.reason.0 == "ReplicatingArtifacts"
    }) {
        return "preparing-drain";
    }
    if conditions.iter().any(|condition| {
        condition.condition_type == kernel_api::ConditionType::Draining
            && condition.state == ConditionState::Unknown
    }) {
        return "unknown";
    }
    "schedulable"
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
