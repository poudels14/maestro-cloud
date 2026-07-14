use std::{path::PathBuf, sync::Arc};

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::broadcast,
};

use crate::{
    cluster::{
        NodeState,
        elector::{EtcdLeaderElector, LeaderElector},
        registry::NodeRegistry,
        types::LeadershipState,
    },
    logs::Logger,
    signal::ShutdownEvent,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "kebab-case")]
pub enum ControlCommand {
    SetNodeState {
        node_id: String,
        unschedulable: bool,
        reason: Option<String>,
    },
    ApproveNode {
        admission: crate::cluster::join::JoinAdmission,
    },
    JoinNode {
        request: crate::cluster::join::JoinRequest,
        signature: String,
        source_ip: std::net::Ipv4Addr,
    },
    RemoveNode {
        node_id: String,
    },
    StartUpgrade {
        target_version: String,
    },
    UnfreezeUpgrade {
        run_id: String,
    },
    StoreMutation {
        mutation: crate::deployment::store::ClusterMutation,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlRequest {
    token: String,
    #[serde(flatten)]
    command: ControlCommand,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlResponse {
    ok: bool,
    error: Option<String>,
    data: Option<serde_json::Value>,
}

pub struct ControlServer {
    socket_path: PathBuf,
    internal_token: String,
    elector: Arc<EtcdLeaderElector>,
    registry: Arc<dyn NodeRegistry>,
    join: Option<crate::cluster::join::JoinCoordinator>,
    upgrade: Option<Arc<crate::cluster::upgrade::ClusterUpgradeOrchestrator>>,
    store: Arc<dyn crate::deployment::store::ClusterStore>,
    logger: Logger,
}

impl ControlServer {
    pub fn new(
        socket_path: PathBuf,
        internal_token: String,
        elector: Arc<EtcdLeaderElector>,
        registry: Arc<dyn NodeRegistry>,
        join: Option<crate::cluster::join::JoinCoordinator>,
        upgrade: Option<Arc<crate::cluster::upgrade::ClusterUpgradeOrchestrator>>,
        store: Arc<dyn crate::deployment::store::ClusterStore>,
        logger: Logger,
    ) -> Self {
        Self {
            socket_path,
            internal_token,
            elector,
            registry,
            join,
            upgrade,
            store,
            logger,
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<ShutdownEvent>) {
        if let Some(parent) = self.socket_path.parent()
            && let Err(error) = protect_control_directory(parent)
        {
            self.logger.emit(
                "error",
                &format!("failed to prepare daemon control directory: {error}"),
            );
            return;
        }
        if self.socket_path.exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
        let listener = match UnixListener::bind(&self.socket_path) {
            Ok(listener) => listener,
            Err(error) => {
                self.logger.emit(
                    "error",
                    &format!("failed to bind daemon control socket: {error}"),
                );
                return;
            }
        };
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            if let Err(error) = self.handle(stream).await {
                                self.logger.emit("warn", &format!("control request failed: {error}"));
                            }
                        }
                        Err(error) => self.logger.emit("warn", &format!("control accept failed: {error}")),
                    }
                }
            }
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }

    async fn handle(&self, stream: UnixStream) -> Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut line = String::new();
        BufReader::new(reader).read_line(&mut line).await?;
        let result = self.execute(&line).await;
        let response = match result {
            Ok(data) => ControlResponse {
                ok: true,
                error: None,
                data,
            },
            Err(error) => ControlResponse {
                ok: false,
                error: Some(error.to_string()),
                data: None,
            },
        };
        writer.write_all(&serde_json::to_vec(&response)?).await?;
        writer.write_all(b"\n").await?;
        writer.shutdown().await?;
        Ok(())
    }

    async fn execute(&self, raw: &str) -> Result<Option<serde_json::Value>> {
        let request: ControlRequest = serde_json::from_str(raw)?;
        if !constant_time_matches(&self.internal_token, &request.token) {
            bail!("control authentication failed");
        }
        let LeadershipState::Leading(token) = self.elector.state() else {
            bail!("local daemon is not the cluster leader");
        };
        let output = match request.command {
            ControlCommand::SetNodeState {
                node_id,
                unschedulable,
                reason,
            } => {
                if !self
                    .registry
                    .list_nodes()
                    .await?
                    .iter()
                    .any(|node| node.node_id == node_id)
                {
                    bail!("cluster node `{node_id}` is not alive");
                }
                self.registry
                    .set_node_state(
                        &token,
                        &node_id,
                        NodeState {
                            unschedulable,
                            drained_at_ms: unschedulable.then(now_millis),
                            reason: unschedulable
                                .then(|| reason.unwrap_or_else(|| "drain".to_string())),
                        },
                    )
                    .await?;
                None
            }
            ControlCommand::ApproveNode { admission } => {
                self.join
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster join service is unavailable"))?
                    .approve(&token, admission)
                    .await?;
                None
            }
            ControlCommand::JoinNode {
                request,
                signature,
                source_ip,
            } => {
                let envelope = self
                    .join
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster join service is unavailable"))?
                    .join(&token, request, &signature, source_ip)
                    .await?;
                Some(serde_json::to_value(envelope)?)
            }
            ControlCommand::RemoveNode { node_id } => {
                if self
                    .registry
                    .list_nodes()
                    .await?
                    .iter()
                    .any(|node| node.node_id == node_id)
                {
                    self.registry
                        .set_node_state(
                            &token,
                            &node_id,
                            NodeState {
                                unschedulable: true,
                                drained_at_ms: Some(now_millis()),
                                reason: Some("remove-node".to_string()),
                            },
                        )
                        .await?;
                }
                let outcome = self
                    .join
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster lifecycle service is unavailable"))?
                    .remove_node(&token, &node_id)
                    .await?;
                if outcome == crate::cluster::join::RemoveNodeOutcome::LeadershipTransferRequired {
                    let other_voter_alive = self.registry.list_nodes().await?.iter().any(|node| {
                        node.node_id != node_id && node.role == crate::cluster::NodeRole::Voter
                    });
                    if !other_voter_alive {
                        bail!("cannot remove the leader without another live voter");
                    }
                    self.elector.resign().await?;
                    return Ok(Some(serde_json::to_value(outcome)?));
                }
                Some(serde_json::to_value(outcome)?)
            }
            ControlCommand::StartUpgrade { target_version } => {
                let run = self
                    .upgrade
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster upgrade service is unavailable"))?
                    .create_run(&token, &target_version)
                    .await?;
                Some(serde_json::to_value(run)?)
            }
            ControlCommand::UnfreezeUpgrade { run_id } => {
                let run = self
                    .upgrade
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster upgrade service is unavailable"))?
                    .manual_unfreeze(&token, &run_id)
                    .await?;
                Some(serde_json::to_value(run)?)
            }
            ControlCommand::StoreMutation { mutation } => {
                self.store.apply_cluster_mutation(&token, mutation).await?
            }
        };
        if self.elector.state() != LeadershipState::Leading(token) {
            return Err(anyhow!("leadership changed while applying control request"));
        }
        Ok(output)
    }
}

pub async fn send_command(socket_path: &str, token: &str, command: ControlCommand) -> Result<()> {
    send_command_with_response(socket_path, token, command)
        .await
        .map(|_| ())
}

pub async fn send_command_with_response(
    socket_path: &str,
    token: &str,
    command: ControlCommand,
) -> Result<Option<serde_json::Value>> {
    let stream = UnixStream::connect(socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let request = ControlRequest {
        token: token.to_string(),
        command,
    };
    writer.write_all(&serde_json::to_vec(&request)?).await?;
    writer.write_all(b"\n").await?;
    let mut line = String::new();
    BufReader::new(reader).read_line(&mut line).await?;
    let response: ControlResponse = serde_json::from_str(&line)?;
    if response.ok {
        Ok(response.data)
    } else {
        bail!(
            "{}",
            response
                .error
                .unwrap_or_else(|| "daemon rejected control request".to_string())
        )
    }
}

fn constant_time_matches(expected: &str, presented: &str) -> bool {
    let expected = Sha256::digest(expected.as_bytes());
    let presented = Sha256::digest(presented.as_bytes());
    expected
        .iter()
        .zip(presented.iter())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn protect_control_directory(path: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn now_millis() -> i64 {
    crate::utils::time::current_time_millis()
        .ok()
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_handles_equal_and_different_values() {
        assert!(constant_time_matches("secret", "secret"));
        assert!(!constant_time_matches("secret", "other"));
    }
}
