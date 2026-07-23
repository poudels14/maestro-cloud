use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use cluster::{NodeJoinApproval, NodeJoinApprovalRequest, NodeJoinApprovalState};
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

struct RecordingClusterApi {
    info: ClusterInfo,
    config: MaskedClusterConfig,
    nodes: Vec<Node>,
    commands: Mutex<Vec<(NodeId, RequestId, NodeLifecycleAction, CommandRequest)>>,
    approvals: Mutex<Vec<NodeJoinApprovalRequest>>,
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

    async fn approve_node(
        &self,
        request: NodeJoinApprovalRequest,
    ) -> Result<NodeJoinApproval, CliError> {
        self.approvals
            .lock()
            .map_err(|_| poisoned())?
            .push(request.clone());
        Ok(NodeJoinApproval {
            node_id: request.node_id,
            public_key_sha256: request.public_key_sha256,
            approved_at_unix_ms: 1_000,
            state: NodeJoinApprovalState::Approved,
            admitted_at_unix_ms: None,
        })
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
async fn node_approval_submits_and_checks_the_join_key_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let api = api()?;
    let fingerprint = "11".repeat(32);
    let mut output = Vec::new();
    crate::cluster::approve_node(&api, "node-a".to_string(), fingerprint.clone(), &mut output)
        .await?;
    assert_eq!(
        api.approvals
            .lock()
            .map_err(|_| "approval lock poisoned")?
            .as_slice(),
        [NodeJoinApprovalRequest {
            node_id: NodeId::new("node-a")?,
            public_key_sha256: fingerprint.clone(),
        }]
    );
    let output = String::from_utf8(output)?;
    assert!(output.contains("approved join key"));
    assert!(output.contains(&fingerprint));
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
            cluster_cidr: "10.42.0.0/16".to_string(),
            node_limit: 254,
            node_prefix: 24,
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
        },
        nodes: vec![
            node("node-z", "worker-z", "worker", "10.0.0.12", 3, false)?,
            node("node-a", "master-a", "master", "10.0.0.10", 7, true)?,
        ],
        commands: Mutex::new(Vec::new()),
        approvals: Mutex::new(Vec::new()),
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
                    "type": "Draining",
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
