use std::sync::Mutex;

use kernel_api::{
    ClusterId, ClusterInfo, CommandRequest, Node, NodeCommandResponse, NodeId, RequestId,
};
use serde_json::json;

use crate::CliError;
use crate::cluster::{ClusterApi, NodeLifecycleAction, info, list_nodes, node_lifecycle};

struct RecordingClusterApi {
    info: ClusterInfo,
    nodes: Vec<Node>,
    commands: Mutex<Vec<(NodeId, RequestId, NodeLifecycleAction, CommandRequest)>>,
}

impl ClusterApi for RecordingClusterApi {
    async fn cluster_info(&self) -> Result<ClusterInfo, CliError> {
        Ok(self.info.clone())
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

fn api() -> Result<RecordingClusterApi, Box<dyn std::error::Error>> {
    Ok(RecordingClusterApi {
        info: ClusterInfo {
            cluster_id: ClusterId::new("test-cluster")?,
            node_count: 2,
            control_plane_node_count: 1,
            workload_node_count: 2,
        },
        nodes: vec![
            node("node-z", "worker-z", "worker", "10.0.0.12", 3, false)?,
            node("node-a", "master-a", "master", "10.0.0.10", 7, true)?,
        ],
        commands: Mutex::new(Vec::new()),
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
