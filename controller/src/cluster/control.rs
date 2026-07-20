use std::{path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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
    DiscoverClusterCa {
        request: crate::cluster::join::CaDiscoveryRequest,
    },
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
        #[serde(default)]
        batch: crate::cluster::UpgradeBatch,
    },
    StartRestart {
        node_id: Option<String>,
    },
    UnfreezeUpgrade {
        run_id: String,
    },
    StoreMutation {
        mutation: Box<crate::deployment::store::ClusterMutation>,
    },
    ExecSession {
        service_id: String,
        deployment_id: String,
        replica_index: u32,
        argv: Vec<String>,
        tty: bool,
        initial_size: Option<crate::exec::TerminalSize>,
        client: String,
    },
    ExportImage {
        image: String,
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
    elector: Option<Arc<EtcdLeaderElector>>,
    registry: Option<Arc<dyn NodeRegistry>>,
    join: Option<crate::cluster::join::JoinCoordinator>,
    upgrade: Option<Arc<crate::cluster::upgrade::ClusterUpgradeOrchestrator>>,
    store: Arc<dyn crate::deployment::store::ClusterStore>,
    runtime: Arc<dyn crate::runtime::RuntimeProvider>,
    allow_exec: bool,
    data_dir: PathBuf,
    local_node_id: Option<String>,
    runtime_suffix: Option<String>,
    logger: Logger,
}

impl ControlServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        socket_path: PathBuf,
        internal_token: String,
        elector: Option<Arc<EtcdLeaderElector>>,
        registry: Option<Arc<dyn NodeRegistry>>,
        join: Option<crate::cluster::join::JoinCoordinator>,
        upgrade: Option<Arc<crate::cluster::upgrade::ClusterUpgradeOrchestrator>>,
        store: Arc<dyn crate::deployment::store::ClusterStore>,
        runtime: Arc<dyn crate::runtime::RuntimeProvider>,
        allow_exec: bool,
        data_dir: PathBuf,
        local_node_id: Option<String>,
        runtime_suffix: Option<String>,
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
            runtime,
            allow_exec,
            data_dir,
            local_node_id,
            runtime_suffix,
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
        let server = Arc::new(self);
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            let server = server.clone();
                            tokio::spawn(async move {
                                if let Err(error) = server.handle(stream).await {
                                    server.logger.emit("warn", &format!("control request failed: {error}"));
                                }
                            });
                        }
                        Err(error) => server.logger.emit("warn", &format!("control accept failed: {error}")),
                    }
                }
            }
        }
        let _ = std::fs::remove_file(&server.socket_path);
    }

    async fn handle(&self, mut stream: UnixStream) -> Result<()> {
        let line = read_control_line(&mut stream).await?;
        let request: ControlRequest = serde_json::from_slice(&line)?;
        if !constant_time_matches(&self.internal_token, &request.token) {
            write_control_response(&mut stream, Err(anyhow!("control authentication failed")))
                .await?;
            return Ok(());
        }
        if let ControlCommand::ExecSession {
            service_id,
            deployment_id,
            replica_index,
            argv,
            tty,
            initial_size,
            client,
        } = request.command
        {
            let prepared = self
                .prepare_exec(
                    service_id,
                    deployment_id,
                    replica_index,
                    argv,
                    tty,
                    initial_size,
                    client,
                )
                .await;
            match prepared {
                Ok(prepared) => {
                    write_control_response(&mut stream, Ok(None)).await?;
                    self.run_exec(stream, prepared).await?;
                }
                Err(error) => write_control_response(&mut stream, Err(error)).await?,
            }
            return Ok(());
        }
        if let ControlCommand::ExportImage { image } = request.command {
            let prepared = self.prepare_image_export(&image).await;
            match prepared {
                Ok(()) => {
                    write_control_response(&mut stream, Ok(None)).await?;
                    self.runtime.export_image(&image, Box::pin(stream)).await?;
                }
                Err(error) => write_control_response(&mut stream, Err(error)).await?,
            }
            return Ok(());
        }
        let result = self.execute(request.command).await;
        write_control_response(&mut stream, result).await?;
        stream.shutdown().await?;
        Ok(())
    }

    async fn execute(&self, command: ControlCommand) -> Result<Option<serde_json::Value>> {
        let command = match command {
            ControlCommand::ExecSession { .. } | ControlCommand::ExportImage { .. } => {
                unreachable!("streaming commands are handled before execute")
            }
            ControlCommand::DiscoverClusterCa { request } => {
                let response = self
                    .join
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster join service is unavailable"))?
                    .discover_ca(&request)?;
                return Ok(Some(serde_json::to_value(response)?));
            }
            command => command,
        };
        let elector = self
            .elector
            .as_ref()
            .ok_or_else(|| anyhow!("cluster leadership service is unavailable"))?;
        let LeadershipState::Leading(token) = elector.state() else {
            bail!("local daemon is not the cluster leader");
        };
        let output = match command {
            ControlCommand::ExecSession { .. } | ControlCommand::ExportImage { .. } => {
                unreachable!("streaming commands are handled before execute")
            }
            ControlCommand::DiscoverClusterCa { .. } => {
                unreachable!("handled without leadership")
            }
            ControlCommand::SetNodeState {
                node_id,
                unschedulable,
                reason,
            } => {
                let registry = self
                    .registry
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster registry is unavailable"))?;
                if !registry
                    .list_nodes()
                    .await?
                    .iter()
                    .any(|node| node.node_id == node_id)
                {
                    bail!("cluster node `{node_id}` is not alive");
                }
                registry
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
                let registry = self
                    .registry
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster registry is unavailable"))?;
                if registry
                    .list_nodes()
                    .await?
                    .iter()
                    .any(|node| node.node_id == node_id)
                {
                    registry
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
                    let other_voter_alive = registry
                        .list_nodes()
                        .await?
                        .iter()
                        .any(|node| node.node_id != node_id && node.role.is_voter());
                    if !other_voter_alive {
                        bail!("cannot remove the leader without another live voter");
                    }
                    elector.resign().await?;
                    return Ok(Some(serde_json::to_value(outcome)?));
                }
                Some(serde_json::to_value(outcome)?)
            }
            ControlCommand::StartUpgrade {
                target_version,
                batch,
            } => {
                let run = self
                    .upgrade
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster upgrade service is unavailable"))?
                    .create_run_with_batch(&token, &target_version, batch)
                    .await?;
                Some(serde_json::to_value(run)?)
            }
            ControlCommand::StartRestart { node_id } => {
                let run = self
                    .upgrade
                    .as_ref()
                    .ok_or_else(|| anyhow!("cluster maintenance service is unavailable"))?
                    .create_restart_run(&token, node_id.as_deref())
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
                self.store.apply_cluster_mutation(&token, *mutation).await?
            }
        };
        if elector.state() != LeadershipState::Leading(token) {
            return Err(anyhow!("leadership changed while applying control request"));
        }
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_exec(
        &self,
        service_id: String,
        deployment_id: String,
        replica_index: u32,
        command: Vec<String>,
        tty: bool,
        initial_size: Option<crate::exec::TerminalSize>,
        client: String,
    ) -> Result<PreparedExec> {
        if !self.allow_exec {
            bail!("CLI exec is disabled; set allow-exec to true in the cluster config");
        }
        crate::validation::validate_service_id(&service_id, "serviceId")
            .map_err(anyhow::Error::msg)?;
        crate::validation::validate_service_id(&deployment_id, "deploymentId")
            .map_err(anyhow::Error::msg)?;
        if command.is_empty() || command.iter().any(|argument| argument.contains('\0')) {
            bail!("exec command must contain at least one valid argument");
        }
        let deployment_key = crate::deployment::types::Deployment {
            id: deployment_id.clone(),
            service_id: service_id.clone(),
            replica_index,
        };
        let deployment = self
            .store
            .read_service_deployment(&deployment_key)
            .await?
            .ok_or_else(|| {
                anyhow!("deployment `{deployment_id}` for service `{service_id}` was not found")
            })?;
        if !deployment.config.deploy.exec {
            bail!("CLI exec is disabled for service `{service_id}`");
        }
        let container = if let Some(local_node_id) = self.local_node_id.as_deref() {
            self.store
                .list_placement_history(
                    Some(&service_id),
                    Some(&deployment_id),
                    Some(replica_index),
                )
                .await?
                .into_iter()
                .filter(|placement| {
                    placement.ended_at_ms.is_none() && placement.node_id == local_node_id
                })
                .max_by_key(|placement| placement.started_at_ms)
                .map(|placement| placement.container_hostname)
                .ok_or_else(|| {
                    anyhow!(
                        "replica #{replica_index} of deployment `{deployment_id}` is not running on this node"
                    )
                })?
        } else {
            let running = self
                .store
                .list_replica_states(&service_id, &deployment_id)
                .await?
                .into_iter()
                .any(|replica| {
                    replica.replica_index == replica_index
                        && matches!(
                            replica.status,
                            crate::deployment::types::DeploymentStatus::PendingReady
                                | crate::deployment::types::DeploymentStatus::Ready
                                | crate::deployment::types::DeploymentStatus::Draining
                        )
                });
            if !running {
                bail!("replica #{replica_index} of deployment `{deployment_id}` is not running");
            }
            crate::deployment::provider::replica_container_name(
                &service_id,
                &deployment_id,
                replica_index,
                self.runtime_suffix.as_deref(),
            )
        };
        let session = self
            .runtime
            .interactive_exec(crate::runtime::InteractiveExecRequest {
                container,
                command: command.clone(),
                tty,
                initial_size,
                session_root: self.data_dir.join("system/exec"),
            })
            .await?;
        Ok(PreparedExec {
            session,
            service_id,
            deployment_id,
            replica_index,
            command,
            client,
        })
    }

    async fn prepare_image_export(&self, image: &str) -> Result<()> {
        if image.trim().is_empty() {
            bail!("image reference cannot be empty");
        }
        let mut authorized = false;
        for service in self.store.list_service_infos().await? {
            let deployments = self
                .store
                .list_service_deployments(&service.config.id)
                .await?;
            let latest_peer_image = deployments
                .iter()
                .filter(|deployment| {
                    deployment
                        .config
                        .build
                        .as_ref()
                        .is_some_and(|build| build.registry.is_none())
                })
                .filter_map(|deployment| {
                    deployment
                        .build
                        .as_ref()
                        .map(|build| (deployment.created_at, build.docker_image_id.clone()))
                })
                .max_by_key(|(created_at, _)| *created_at)
                .map(|(_, image)| image);
            for deployment in deployments {
                if deployment
                    .build
                    .as_ref()
                    .is_some_and(|build| build.docker_image_id == image)
                    && deployment
                        .config
                        .build
                        .as_ref()
                        .is_some_and(|build| build.registry.is_none())
                    && (matches!(
                        deployment.status,
                        crate::deployment::types::DeploymentStatus::Building
                            | crate::deployment::types::DeploymentStatus::PendingReady
                            | crate::deployment::types::DeploymentStatus::Ready
                            | crate::deployment::types::DeploymentStatus::Draining
                    ) || latest_peer_image.as_deref() == Some(image))
                {
                    authorized = true;
                    break;
                }
            }
            if authorized {
                break;
            }
        }
        if !authorized {
            bail!("image is not an active registry-free Maestro build");
        }
        if !self.runtime.image_exists(image).await? {
            bail!("image `{image}` is not available on this node");
        }
        self.registry
            .as_ref()
            .ok_or_else(|| anyhow!("cluster registry is unavailable"))?
            .publish_image_holder(image)
            .await?;
        Ok(())
    }

    async fn run_exec(&self, stream: UnixStream, prepared: PreparedExec) -> Result<()> {
        let PreparedExec {
            session,
            service_id,
            deployment_id,
            replica_index,
            command,
            client,
        } = prepared;
        let started = Instant::now();
        self.logger.emit(
            "info",
            &format!(
                "exec opened client={client} service={service_id} deployment={deployment_id} replica={replica_index} command={command:?}"
            ),
        );
        let result = pump_exec_session(stream, session).await;
        let exit_code = result.as_ref().ok().and_then(|code| *code);
        self.logger.emit(
            "info",
            &format!(
                "exec closed client={client} service={service_id} deployment={deployment_id} replica={replica_index} duration_ms={} exit_code={exit_code:?}",
                started.elapsed().as_millis()
            ),
        );
        result.map(|_| ())
    }
}

struct PreparedExec {
    session: crate::runtime::ExecSession,
    service_id: String,
    deployment_id: String,
    replica_index: u32,
    command: Vec<String>,
    client: String,
}

pub(crate) async fn pump_exec_session(
    stream: UnixStream,
    session: crate::runtime::ExecSession,
) -> Result<Option<i32>> {
    let (mut socket_reader, mut socket_writer) = stream.into_split();
    let control = session.control();
    let mut stdin = Some(session.stdin);
    let mut output = session.output;
    let wait_control = control.clone();
    let mut wait = Box::pin(async move { wait_control.wait().await });
    let mut wait_result = None;
    let mut output_closed = false;
    let mut output_buffer = vec![0_u8; 16 * 1024];
    loop {
        if output_closed && let Some(exit_code) = wait_result {
            crate::exec::write_length_prefixed(
                &mut socket_writer,
                &crate::exec::ExecFrame::Exit(exit_code),
            )
            .await?;
            return Ok(Some(exit_code));
        }
        tokio::select! {
            incoming = crate::exec::read_length_prefixed(&mut socket_reader) => {
                let Some(frame) = incoming? else {
                    control.kill().await?;
                    return Ok(None);
                };
                match frame {
                    crate::exec::ExecFrame::Stdin(bytes) if bytes.is_empty() => {
                        if let Some(mut writer) = stdin.take() {
                            writer.shutdown().await?;
                        }
                    }
                    crate::exec::ExecFrame::Stdin(bytes) => {
                        stdin
                            .as_mut()
                            .ok_or_else(|| anyhow!("stdin was already closed"))?
                            .write_all(&bytes)
                            .await?;
                    }
                    crate::exec::ExecFrame::Resize(size) => control.resize(size.cols, size.rows).await?,
                    crate::exec::ExecFrame::Ping => {
                        crate::exec::write_length_prefixed(
                            &mut socket_writer,
                            &crate::exec::ExecFrame::Ping,
                        ).await?;
                    }
                    _ => bail!("client sent an invalid exec frame"),
                }
            }
            read = output.read(&mut output_buffer), if !output_closed => {
                let count = read?;
                if count == 0 {
                    output_closed = true;
                } else {
                    crate::exec::write_length_prefixed(
                        &mut socket_writer,
                        &crate::exec::ExecFrame::Output(output_buffer[..count].to_vec()),
                    ).await?;
                }
            }
            result = &mut wait, if wait_result.is_none() => {
                match result {
                    Ok(exit_code) => wait_result = Some(exit_code),
                    Err(error) => {
                        let _ = crate::exec::write_length_prefixed(
                            &mut socket_writer,
                            &crate::exec::ExecFrame::Error(error.to_string()),
                        ).await;
                        return Err(error);
                    }
                }
            }
        }
    }
}

async fn read_control_line(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let byte = stream.read_u8().await?;
        if byte == b'\n' {
            return Ok(line);
        }
        line.push(byte);
        if line.len() > 1024 * 1024 {
            bail!("control request exceeds 1 MiB");
        }
    }
}

async fn write_control_response(
    stream: &mut UnixStream,
    result: Result<Option<serde_json::Value>>,
) -> Result<()> {
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
    stream.write_all(&serde_json::to_vec(&response)?).await?;
    stream.write_all(b"\n").await?;
    stream.flush().await?;
    Ok(())
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
    let mut stream = stream;
    let request = ControlRequest {
        token: token.to_string(),
        command,
    };
    stream.write_all(&serde_json::to_vec(&request)?).await?;
    stream.write_all(b"\n").await?;
    let line = read_control_line(&mut stream).await?;
    let response: ControlResponse = serde_json::from_slice(&line)?;
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

pub async fn open_exec_stream(
    socket_path: &str,
    token: &str,
    command: ControlCommand,
) -> Result<UnixStream> {
    if !matches!(command, ControlCommand::ExecSession { .. }) {
        bail!("open_exec_stream requires an exec-session command");
    }
    let mut stream = UnixStream::connect(socket_path).await?;
    let request = ControlRequest {
        token: token.to_string(),
        command,
    };
    stream.write_all(&serde_json::to_vec(&request)?).await?;
    stream.write_all(b"\n").await?;
    let line = read_control_line(&mut stream).await?;
    let response: ControlResponse = serde_json::from_slice(&line)?;
    if response.ok {
        Ok(stream)
    } else {
        bail!(
            "{}",
            response
                .error
                .unwrap_or_else(|| "daemon rejected exec request".to_string())
        )
    }
}

pub async fn open_image_export_stream(
    socket_path: &str,
    token: &str,
    image: String,
) -> Result<UnixStream> {
    let mut stream = UnixStream::connect(socket_path).await?;
    let request = ControlRequest {
        token: token.to_string(),
        command: ControlCommand::ExportImage { image },
    };
    stream.write_all(&serde_json::to_vec(&request)?).await?;
    stream.write_all(b"\n").await?;
    let line = read_control_line(&mut stream).await?;
    let response: ControlResponse = serde_json::from_slice(&line)?;
    if response.ok {
        Ok(stream)
    } else {
        bail!(
            "{}",
            response
                .error
                .unwrap_or_else(|| "daemon rejected image export request".to_string())
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

    #[tokio::test]
    async fn image_export_control_stream_preserves_archive_bytes() {
        use tokio::io::AsyncReadExt;

        let directory = std::env::temp_dir().join(format!(
            "maestro-image-control-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let socket_path = directory.join("control.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let line = read_control_line(&mut stream).await.unwrap();
            let request: ControlRequest = serde_json::from_slice(&line).unwrap();
            assert_eq!(request.token, "secret");
            assert!(matches!(
                request.command,
                ControlCommand::ExportImage { image } if image == "api:deployment"
            ));
            write_control_response(&mut stream, Ok(None)).await.unwrap();
            stream.write_all(b"archive-bytes").await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let mut stream = open_image_export_stream(
            socket_path.to_str().unwrap(),
            "secret",
            "api:deployment".to_string(),
        )
        .await
        .unwrap();
        let mut archive = Vec::new();
        stream.read_to_end(&mut archive).await.unwrap();

        server.await.unwrap();
        assert_eq!(archive, b"archive-bytes");
        let _ = std::fs::remove_file(socket_path);
        let _ = std::fs::remove_dir(directory);
    }
}
