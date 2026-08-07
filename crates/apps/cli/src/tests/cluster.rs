use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use kernel_api::{
    ClusterId, ClusterInfo, CommandRequest, MaskedClusterConfig, MaskedClusterConfigNode,
    MaskedClusterConfigPorts, Node, NodeCommandResponse, NodeId, NodeRemovalRequest,
    NodeRemovalResponse, NodeRemovalState, NodeRole, RequestId,
};
use serde_json::json;

use crate::CliError;
use crate::cluster::{
    ClusterApi, NodeLifecycleAction, info, list_nodes, node_lifecycle, remove_node_with_timing,
    show_config,
};
use crate::cluster_restart::{RestartSelection, RestartTarget, resolve as resolve_restart_target};

struct RecordingClusterApi {
    info: ClusterInfo,
    config: MaskedClusterConfig,
    nodes: Vec<Node>,
    commands: Mutex<Vec<(NodeId, RequestId, NodeLifecycleAction, CommandRequest)>>,
    removals: Mutex<Vec<(NodeId, RequestId, NodeRemovalRequest)>>,
    removal_states: Mutex<VecDeque<NodeRemovalState>>,
}

impl ClusterApi for RecordingClusterApi {
    async fn cluster_info(&self) -> Result<ClusterInfo, CliError> {
        Ok(self.info.clone())
    }

    async fn cluster_config(&self) -> Result<MaskedClusterConfig, CliError> {
        Ok(self.config.clone())
    }

    async fn list_nodes(&self) -> Result<Vec<Node>, CliError> {
        Ok(self.nodes.clone())
    }

    async fn get_node(&self, node_id: &NodeId) -> Result<Node, CliError> {
        self.nodes
            .iter()
            .find(|node| &node.meta.id == node_id)
            .cloned()
            .ok_or_else(|| CliError::not_found(format!("node `{node_id}`")))
    }

    async fn command_node(
        &self,
        node_id: &NodeId,
        request_id: &RequestId,
        action: NodeLifecycleAction,
        request: CommandRequest,
    ) -> Result<NodeCommandResponse, CliError> {
        self.commands.lock().map_err(|_| poisoned())?.push((
            node_id.clone(),
            request_id.clone(),
            action,
            request,
        ));
        Ok(NodeCommandResponse {
            node_id: node_id.clone(),
            draining: action == NodeLifecycleAction::Drain,
        })
    }

    async fn remove_node(
        &self,
        node_id: &NodeId,
        request_id: &RequestId,
        request: NodeRemovalRequest,
    ) -> Result<NodeRemovalResponse, CliError> {
        self.removals.lock().map_err(|_| poisoned())?.push((
            node_id.clone(),
            request_id.clone(),
            request,
        ));
        let state = self
            .removal_states
            .lock()
            .map_err(|_| poisoned())?
            .pop_front()
            .unwrap_or(NodeRemovalState::Removed);
        Ok(NodeRemovalResponse {
            node_id: node_id.clone(),
            state,
        })
    }
}

#[tokio::test]
async fn cluster_config_prints_the_secret_free_api_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let api = api()?;
    let mut output = Vec::new();
    show_config(&api, &mut output).await?;
    let document = String::from_utf8(output)?;
    let decoded: MaskedClusterConfig = serde_json::from_str(&document)?;
    assert_eq!(decoded, api.config);
    assert!(!document.contains("joinSecret"));
    Ok(())
}

#[tokio::test]
async fn restart_selection_preserves_local_all_and_interactive_compatibility()
-> Result<(), Box<dyn std::error::Error>> {
    let api = api()?;
    assert_eq!(
        resolve_restart_target(
            &api,
            RestartSelection::LocalNode,
            &mut std::io::Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await?,
        RestartTarget::Node("node-a".to_string())
    );
    assert_eq!(
        resolve_restart_target(
            &api,
            RestartSelection::Direct(RestartTarget::EveryNode),
            &mut std::io::Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await?,
        RestartTarget::EveryNode
    );

    let mut input = std::io::Cursor::new(b"2\n".to_vec());
    let mut output = Vec::new();
    assert_eq!(
        resolve_restart_target(&api, RestartSelection::Prompt, &mut input, &mut output).await?,
        RestartTarget::Node("node-z".to_string())
    );
    let output = String::from_utf8(output)?;
    let node_a = output.find("node-a").ok_or("node-a selection missing")?;
    let node_z = output.find("node-z").ok_or("node-z selection missing")?;
    assert!(node_a < node_z);
    assert!(output.contains("Node [1-2 or ID]:"));
    Ok(())
}

#[tokio::test]
async fn cluster_info_and_nodes_are_stable_and_explicit() -> Result<(), Box<dyn std::error::Error>>
{
    let api = api()?;
    let mut output = Vec::new();
    info(&api, &mut output).await?;
    list_nodes(&api, &mut output).await?;
    let output = String::from_utf8(output)?;
    assert!(output.contains("Cluster: test-cluster"));
    assert!(output.contains("Control plane: 1"));
    assert!(output.contains("NODE"));
    assert!(output.contains("draining"));
    assert!(output.contains("schedulable"));
    assert!(
        output
            .find("node-a")
            .is_some_and(|first| output.find("node-z").is_some_and(|last| first < last))
    );
    Ok(())
}

#[tokio::test]
async fn cluster_nodes_report_maintenance_as_unschedulable()
-> Result<(), Box<dyn std::error::Error>> {
    let mut api = api()?;
    let node = api
        .nodes
        .iter_mut()
        .find(|node| node.meta.id.to_string() == "node-z")
        .ok_or("node-z missing")?;
    node.status.conditions.push(serde_json::from_value(json!({
        "type": "MAINTENANCE",
        "status": "true",
        "reason": "UpgradeRun:upgrade-test",
        "message": "node reserved for an upgrade",
        "observedGeneration": 1,
        "lastTransitionTime": 1
    }))?);

    let mut output = Vec::new();
    list_nodes(&api, &mut output).await?;
    let output = String::from_utf8(output)?;
    let node_row = output
        .lines()
        .find(|line| line.starts_with("node-z"))
        .ok_or("node-z row missing")?;
    assert!(node_row.contains("maintenance"));
    assert!(!node_row.contains("schedulable"));
    Ok(())
}

#[tokio::test]
async fn node_commands_submit_the_observed_revision_and_request_id()
-> Result<(), Box<dyn std::error::Error>> {
    let api = api()?;
    let mut output = Vec::new();
    for (action, request_id) in [
        (NodeLifecycleAction::Drain, "drain-1"),
        (NodeLifecycleAction::Restore, "restore-1"),
    ] {
        node_lifecycle(
            &api,
            "node-a".to_string(),
            RequestId::new(request_id)?,
            action,
            &mut output,
        )
        .await?;
    }
    let commands = api.commands.lock().map_err(|_| "command lock poisoned")?;
    assert_eq!(commands.len(), 2);
    assert!(
        commands.iter().all(|command| {
            command.0.as_str() == "node-a" && command.3.expected_revision.0 == 7
        })
    );
    assert_eq!(
        commands
            .iter()
            .map(|command| command.1.as_str())
            .collect::<Vec<_>>(),
        ["drain-1", "restore-1"]
    );
    let output = String::from_utf8(output)?;
    assert!(output.contains("node `node-a` drain accepted"));
    assert!(output.contains("node `node-a` restore accepted"));
    Ok(())
}

#[tokio::test]
async fn node_removal_polls_with_distinct_replayable_phase_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let api = api()?;
    api.removal_states
        .lock()
        .map_err(|_| "removal state lock poisoned")?
        .extend([NodeRemovalState::Draining, NodeRemovalState::Removed]);
    let mut output = Vec::new();

    remove_node_with_timing(
        &api,
        "node-a".to_string(),
        RequestId::new("remove-flow")?,
        Duration::ZERO,
        Duration::from_secs(1),
        &mut output,
    )
    .await?;

    let removals = api.removals.lock().map_err(|_| "removal lock poisoned")?;
    let [first, second] = removals.as_slice() else {
        return Err(format!("expected two removal calls, observed {}", removals.len()).into());
    };
    assert_eq!(first.1.as_str(), "remove-flow");
    assert_ne!(second.1, first.1);
    assert!(removals.iter().all(|(node_id, _, request)| {
        node_id.as_str() == "node-a" && &request.node_id == node_id
    }));
    let output = String::from_utf8(output)?;
    assert!(output.contains("waiting for node `node-a` to drain"));
    assert!(output.contains("node `node-a` removed"));
    assert!(output.contains("rotate cluster credentials"));
    Ok(())
}

fn api() -> Result<RecordingClusterApi, Box<dyn std::error::Error>> {
    Ok(RecordingClusterApi {
        info: ClusterInfo {
            cluster_id: ClusterId::new("test-cluster")?,
            node_count: 2,
            control_plane_node_count: 1,
            workload_node_count: 2,
        },
        config: MaskedClusterConfig {
            cluster_id: ClusterId::new("test-cluster")?,
            name: "Test Cluster".to_string(),
            local_node_id: NodeId::new("node-a")?,
            nodes: vec![MaskedClusterConfigNode {
                node_id: NodeId::new("node-a")?,
                hostname: "master-a".to_string(),
                role: NodeRole::Master,
                host_address: "10.0.0.10".to_string(),
                api_port: 3_000,
                workload_subnet: "10.1.0.0/24".to_string(),
            }],
            control_allow_cidrs: vec!["10.0.0.0/24".to_string()],
            ports: MaskedClusterConfigPorts {
                gateway: 3_001,
                store_client: 2_379,
                store_peer: 2_380,
                wireguard: 51_820,
            },
            tailscale: None,
            cloudflare: None,
        },
        nodes: vec![
            node("node-z", "worker-z", "worker", "10.0.0.12", 3, false)?,
            node("node-a", "master-a", "master", "10.0.0.10", 7, true)?,
        ],
        commands: Mutex::new(Vec::new()),
        removals: Mutex::new(Vec::new()),
        removal_states: Mutex::new(VecDeque::new()),
    })
}

fn node(
    id: &str,
    hostname: &str,
    role: &str,
    address: &str,
    revision: u64,
    draining: bool,
) -> Result<Node, serde_json::Error> {
    serde_json::from_value(json!({
        "meta": {"id": id, "revision": revision, "generation": 1},
        "spec": {
            "hostname": hostname,
            "hostAddress": address,
            "role": role,
            "schedulingLabels": {}
        },
        "status": {
            "instanceId": format!("instance-{id}"),
            "version": "1.2.3",
            "lastSeen": 1,
            "conditions": if draining {
                vec![json!({
                    "type": "DRAINING",
                    "status": "true",
                    "reason": "Requested",
                    "message": "node drain requested",
                    "observedGeneration": 1,
                    "lastTransitionTime": 1
                })]
            } else {
                Vec::new()
            }
        }
    }))
}

fn poisoned() -> CliError {
    CliError::invalid_input("test lock poisoned")
}
