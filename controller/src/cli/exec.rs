use std::fmt;
use std::io::IsTerminal;

use clap::Args;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use futures_util::{SinkExt, StreamExt};
use inquire::Select;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

use crate::deployment::types::{DeploymentStatus, DeploymentWithReplicas};
use crate::error::{Error, Result};

#[derive(Debug, Args)]
pub struct ExecArgs {
    #[arg(help = "Service ID")]
    pub service_id: String,
    #[arg(short = 'r', long = "replica", help = "Replica index")]
    pub replica: Option<u32>,
    #[arg(long = "deployment", help = "Still-running deployment ID")]
    pub deployment: Option<String>,
    #[arg(long = "node", help = "Only consider replicas running on this node")]
    pub node: Option<String>,
    #[arg(
        long = "shell",
        default_value = "/bin/sh",
        help = "Shell used when no command is provided"
    )]
    pub shell: String,
    #[arg(long = "no-tty", help = "Disable terminal allocation for piping")]
    pub no_tty: bool,
    #[arg(last = true, num_args = 1.., value_name = "COMMAND")]
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Placement {
    replica_index: u32,
    node_id: String,
    container_hostname: String,
    started_at_ms: i64,
    ended_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct ReplicaChoice {
    replica_index: u32,
    node_id: String,
    status: DeploymentStatus,
    container_hostname: String,
}

impl fmt::Display for ReplicaChoice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "#{} — {} · {:?} · {}",
            self.replica_index, self.node_id, self.status, self.container_hostname
        )
    }
}

pub async fn run_exec(host: &str, args: ExecArgs) -> Result<i32> {
    crate::validation::validate_service_id(&args.service_id, "service id")
        .map_err(Error::invalid_input)?;
    if let Some(deployment_id) = args.deployment.as_deref() {
        crate::validation::validate_service_id(deployment_id, "deployment id")
            .map_err(Error::invalid_input)?;
    }
    let base = crate::cli::contexts::normalize_base_url(host)?;
    let client = crate::cli::contexts::build_http_client()?;
    let selected_deployment = select_deployment(&client, &base, &args).await?;
    let placements = list_placements(
        &client,
        &base,
        &args.service_id,
        &selected_deployment.deployment.id,
    )
    .await?;
    let choices = replica_choices(&selected_deployment, &placements, args.node.as_deref());
    let selected_replica = select_replica(choices, args.replica, args.node.is_some())?;
    let command = if args.command.is_empty() {
        vec![args.shell]
    } else {
        args.command
    };
    let tty = !args.no_tty;
    if tty && !std::io::stdin().is_terminal() {
        return Err(Error::invalid_input(
            "stdin is not a terminal; use --no-tty when piping input",
        ));
    }
    let terminal_guard = TerminalGuard::enter(tty)?;
    let initial_size = terminal_size(tty)?;
    let url = exec_url(
        &base,
        &args.service_id,
        &selected_deployment.deployment.id,
        selected_replica.replica_index,
        &command,
        tty,
        initial_size,
    )?;
    let mut request = url.into_client_request().map_err(|error| {
        Error::internal(format!("failed to build exec WebSocket request: {error}"))
    })?;
    if let Some(token) = crate::cli::contexts::active_token()? {
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().map_err(|error| {
                Error::invalid_config(format!("invalid token in active context: {error}"))
            })?,
        );
    }
    let websocket = tokio_tungstenite::connect_async(request)
        .await
        .map(|(websocket, _)| websocket)
        .map_err(websocket_connect_error)?;
    let exit_code = drive_terminal(websocket, tty).await;
    drop(terminal_guard);
    exit_code
}

async fn select_deployment(
    client: &reqwest::Client,
    base: &str,
    args: &ExecArgs,
) -> Result<DeploymentWithReplicas> {
    let response = client
        .get(format!(
            "{base}/api/services/{}/deployments",
            args.service_id
        ))
        .send()
        .await
        .map_err(|error| Error::external(format!("failed to list deployments: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "deployment request failed ({status}): {body}"
        )));
    }
    let deployments = response
        .json::<Vec<DeploymentWithReplicas>>()
        .await
        .map_err(|error| Error::external(format!("invalid deployments response: {error}")))?;
    if let Some(deployment_id) = args.deployment.as_deref() {
        deployments
            .into_iter()
            .find(|item| {
                item.deployment.id == deployment_id
                    && matches!(
                        item.deployment.status,
                        DeploymentStatus::PendingReady
                            | DeploymentStatus::Ready
                            | DeploymentStatus::Draining
                    )
            })
            .ok_or_else(|| {
                Error::not_found(format!(
                    "deployment `{deployment_id}` is not present or no longer running"
                ))
            })
    } else {
        deployments
            .into_iter()
            .filter(|item| {
                matches!(
                    item.deployment.status,
                    DeploymentStatus::PendingReady
                        | DeploymentStatus::Ready
                        | DeploymentStatus::Draining
                ) && item.replicas.iter().any(|replica| {
                    matches!(
                        replica.status,
                        DeploymentStatus::PendingReady
                            | DeploymentStatus::Ready
                            | DeploymentStatus::Draining
                    )
                })
            })
            .max_by_key(|item| item.deployment.created_at)
            .ok_or_else(|| Error::not_found("service has no active deployment"))
    }
}

async fn list_placements(
    client: &reqwest::Client,
    base: &str,
    service_id: &str,
    deployment_id: &str,
) -> Result<Vec<Placement>> {
    let mut url = reqwest::Url::parse(&format!("{base}/api/cluster/placements"))
        .map_err(|error| Error::invalid_input(format!("invalid context host: {error}")))?;
    url.query_pairs_mut()
        .append_pair("serviceId", service_id)
        .append_pair("deploymentId", deployment_id);
    let response =
        client.get(url).send().await.map_err(|error| {
            Error::external(format!("failed to list replica placements: {error}"))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "placement request failed ({status}): {body}"
        )));
    }
    response
        .json()
        .await
        .map_err(|error| Error::external(format!("invalid placement response: {error}")))
}

fn replica_choices(
    deployment: &DeploymentWithReplicas,
    placements: &[Placement],
    node_filter: Option<&str>,
) -> Vec<ReplicaChoice> {
    let mut live_placements = placements
        .iter()
        .filter(|placement| placement.ended_at_ms.is_none())
        .cloned()
        .collect::<Vec<_>>();
    live_placements.sort_by_key(|placement| std::cmp::Reverse(placement.started_at_ms));
    live_placements.dedup_by_key(|placement| placement.replica_index);
    let mut choices = deployment
        .replicas
        .iter()
        .filter(|replica| {
            matches!(
                replica.status,
                DeploymentStatus::PendingReady
                    | DeploymentStatus::Ready
                    | DeploymentStatus::Draining
            )
        })
        .filter_map(|replica| {
            let placement = live_placements
                .iter()
                .find(|placement| placement.replica_index == replica.replica_index);
            let node_id = placement
                .map(|placement| placement.node_id.clone())
                .or_else(|| replica.node_id.clone())
                .unwrap_or_else(|| "local".to_string());
            if node_filter.is_some_and(|filter| filter != node_id) {
                None
            } else {
                Some(ReplicaChoice {
                    replica_index: replica.replica_index,
                    node_id,
                    status: replica.status.clone(),
                    container_hostname: placement
                        .map(|placement| placement.container_hostname.clone())
                        .unwrap_or_else(|| {
                            deployment
                                .deployment
                                .hostname_for_replica(replica.replica_index)
                        }),
                })
            }
        })
        .collect::<Vec<_>>();
    if choices.is_empty() && !live_placements.is_empty() {
        choices = live_placements
            .into_iter()
            .filter(|placement| {
                node_filter.is_none_or(|filter| filter == placement.node_id.as_str())
            })
            .map(|placement| ReplicaChoice {
                replica_index: placement.replica_index,
                node_id: placement.node_id,
                status: DeploymentStatus::Ready,
                container_hostname: placement.container_hostname,
            })
            .collect();
    }
    choices.sort_by_key(|choice| choice.replica_index);
    choices
}

fn select_replica(
    mut choices: Vec<ReplicaChoice>,
    requested: Option<u32>,
    node_filtered: bool,
) -> Result<ReplicaChoice> {
    if let Some(replica_index) = requested {
        return choices
            .into_iter()
            .find(|choice| choice.replica_index == replica_index)
            .ok_or_else(|| Error::not_found(format!("replica #{replica_index} is not running")));
    }
    match choices.len() {
        0 => Err(Error::not_found("no running replicas match the selection")),
        1 => Ok(choices.remove(0)),
        _ if node_filtered => Err(Error::invalid_input(
            "more than one replica runs on the selected node; pass --replica",
        )),
        _ => Select::new("Select a replica", choices)
            .prompt()
            .map_err(|error| Error::invalid_input(format!("replica selection canceled: {error}"))),
    }
}

#[allow(clippy::too_many_arguments)]
fn exec_url(
    base: &str,
    service_id: &str,
    deployment_id: &str,
    replica_index: u32,
    command: &[String],
    tty: bool,
    size: Option<crate::exec::TerminalSize>,
) -> Result<String> {
    let mut url = reqwest::Url::parse(base)
        .map_err(|error| Error::invalid_input(format!("invalid context host: {error}")))?;
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        other => {
            return Err(Error::invalid_input(format!(
                "unsupported context URL scheme `{other}`"
            )));
        }
    };
    url.set_scheme(scheme)
        .map_err(|_| Error::invalid_input("failed to convert context URL to WebSocket URL"))?;
    url.set_path(&format!("/api/services/{service_id}/exec"));
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("deploymentId", deployment_id);
        query.append_pair("replicaIndex", &replica_index.to_string());
        query.append_pair(
            "command",
            &serde_json::to_string(command)
                .map_err(|error| Error::internal(format!("failed to encode command: {error}")))?,
        );
        query.append_pair("tty", if tty { "true" } else { "false" });
        if let Some(size) = size {
            query.append_pair("cols", &size.cols.to_string());
            query.append_pair("rows", &size.rows.to_string());
        }
    }
    Ok(url.into())
}

async fn drive_terminal<S>(
    websocket: tokio_tungstenite::WebSocketStream<S>,
    tty: bool,
) -> Result<i32>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut websocket_writer, mut websocket_reader) = websocket.split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut input_buffer = vec![0_u8; 16 * 1024];
    let mut stdin_closed = false;
    let mut escape = EscapeState::default();
    let mut resize_signal = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::window_change(),
    )
    .map_err(|error| Error::internal(format!("failed to watch terminal resize events: {error}")))?;
    let mut keepalive = tokio::time::interval(std::time::Duration::from_secs(20));
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    keepalive.tick().await;
    loop {
        tokio::select! {
            read = stdin.read(&mut input_buffer), if !stdin_closed => {
                let count = read?;
                if count == 0 {
                    websocket_writer
                        .send(Message::Binary(crate::exec::ExecFrame::Stdin(Vec::new()).encode()?.into()))
                        .await
                        .map_err(|error| Error::external(format!("failed to close remote stdin: {error}")))?;
                    stdin_closed = true;
                } else {
                    let outcome = if tty {
                        escape.process(&input_buffer[..count])
                    } else {
                        EscapeOutcome::Send(input_buffer[..count].to_vec())
                    };
                    match outcome {
                        EscapeOutcome::Detach => {
                            let _ = websocket_writer.close().await;
                            return Ok(0);
                        }
                        EscapeOutcome::Send(bytes) if !bytes.is_empty() => {
                            websocket_writer
                                .send(Message::Binary(crate::exec::ExecFrame::Stdin(bytes).encode()?.into()))
                                .await
                                .map_err(|error| Error::external(format!("failed to send terminal input: {error}")))?;
                        }
                        EscapeOutcome::Send(_) => {}
                    }
                }
            }
            incoming = websocket_reader.next() => {
                match incoming {
                    Some(Ok(Message::Binary(encoded))) => match crate::exec::ExecFrame::decode(&encoded)? {
                        crate::exec::ExecFrame::Output(bytes) => {
                            stdout.write_all(&bytes).await?;
                            stdout.flush().await?;
                        }
                        crate::exec::ExecFrame::Exit(code) => return Ok(code),
                        crate::exec::ExecFrame::Error(message) => return Err(Error::external(message)),
                        crate::exec::ExecFrame::Ping => {
                            websocket_writer
                                .send(Message::Binary(crate::exec::ExecFrame::Ping.encode()?.into()))
                                .await
                                .map_err(|error| Error::external(format!("failed to answer exec ping: {error}")))?;
                        }
                        _ => return Err(Error::external("server sent an invalid exec frame")),
                    },
                    Some(Ok(Message::Ping(payload))) => {
                        websocket_writer.send(Message::Pong(payload)).await
                            .map_err(|error| Error::external(format!("failed to answer WebSocket ping: {error}")))?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => {
                        return Err(Error::external("exec connection closed before an exit status was received"));
                    }
                    Some(Ok(Message::Text(_) | Message::Frame(_))) => {
                        return Err(Error::external("server sent a non-binary exec message"));
                    }
                    Some(Err(error)) => return Err(Error::external(format!("exec WebSocket failed: {error}"))),
                }
            }
            _ = resize_signal.recv(), if tty => {
                if let Some(size) = terminal_size(true)? {
                    websocket_writer
                        .send(Message::Binary(crate::exec::ExecFrame::Resize(size).encode()?.into()))
                        .await
                        .map_err(|error| Error::external(format!("failed to resize remote terminal: {error}")))?;
                }
            }
            _ = keepalive.tick() => {
                websocket_writer.send(Message::Ping(Vec::new().into())).await
                    .map_err(|error| Error::external(format!("failed to send exec keepalive: {error}")))?;
            }
        }
    }
}

fn terminal_size(tty: bool) -> Result<Option<crate::exec::TerminalSize>> {
    if tty {
        let (cols, rows) = crossterm::terminal::size()
            .map_err(|error| Error::internal(format!("failed to read terminal size: {error}")))?;
        Ok(Some(crate::exec::TerminalSize { cols, rows }))
    } else {
        Ok(None)
    }
}

fn websocket_connect_error(error: tokio_tungstenite::tungstenite::Error) -> Error {
    if let tokio_tungstenite::tungstenite::Error::Http(response) = &error
        && let Some(body) = response.body()
        && let Ok(message) = std::str::from_utf8(body)
        && !message.trim().is_empty()
    {
        return Error::external(message.trim().to_string());
    }
    Error::external(format!("failed to connect exec WebSocket: {error}"))
}

struct TerminalGuard {
    raw: bool,
}

impl TerminalGuard {
    fn enter(raw: bool) -> Result<Self> {
        if raw {
            enable_raw_mode().map_err(|error| {
                Error::internal(format!("failed to enable terminal raw mode: {error}"))
            })?;
        }
        Ok(Self { raw })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if self.raw {
            let _ = disable_raw_mode();
        }
    }
}

#[derive(Default)]
struct EscapeState {
    line_start: bool,
    pending_tilde: bool,
    initialized: bool,
}

enum EscapeOutcome {
    Send(Vec<u8>),
    Detach,
}

impl EscapeState {
    fn process(&mut self, input: &[u8]) -> EscapeOutcome {
        if !self.initialized {
            self.line_start = true;
            self.initialized = true;
        }
        let mut output = Vec::with_capacity(input.len() + 1);
        for &byte in input {
            if self.pending_tilde {
                self.pending_tilde = false;
                if byte == b'.' {
                    return EscapeOutcome::Detach;
                }
                output.push(b'~');
                output.push(byte);
                self.line_start = matches!(byte, b'\r' | b'\n');
            } else if self.line_start && byte == b'~' {
                self.pending_tilde = true;
            } else {
                output.push(byte);
                self.line_start = matches!(byte, b'\r' | b'\n');
            }
        }
        EscapeOutcome::Send(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn exec_command_parses_replica_and_remote_argv() {
        let cli = crate::Cli::try_parse_from([
            "maestro",
            "exec",
            "api",
            "--replica",
            "2",
            "--no-tty",
            "--",
            "env",
            "-0",
        ])
        .unwrap();
        let Some(crate::CliCommand::Exec(args)) = cli.command else {
            panic!("expected exec command");
        };
        assert_eq!(args.service_id, "api");
        assert_eq!(args.replica, Some(2));
        assert!(args.no_tty);
        assert_eq!(args.command, ["env", "-0"]);
        assert!(crate::Cli::try_parse_from(["maestro", "ssh", "api"]).is_err());
    }

    #[test]
    fn terminal_escape_detaches_only_at_line_start() {
        let mut state = EscapeState::default();
        assert!(matches!(state.process(b"~."), EscapeOutcome::Detach));

        let mut state = EscapeState::default();
        match state.process(b"echo ~.\n") {
            EscapeOutcome::Send(bytes) => assert_eq!(bytes, b"echo ~.\n"),
            EscapeOutcome::Detach => panic!("mid-line escape must not detach"),
        }
    }

    #[test]
    fn exec_url_uses_websocket_scheme_and_encoded_argv() {
        let url = exec_url(
            "https://maestro.example.test",
            "api",
            "deploy123",
            2,
            &["printf".to_string(), "hello world".to_string()],
            false,
            None,
        )
        .unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert_eq!(parsed.scheme(), "wss");
        assert_eq!(parsed.path(), "/api/services/api/exec");
        let query = parsed
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(query.get("deploymentId").unwrap(), "deploy123");
        assert_eq!(query.get("replicaIndex").unwrap(), "2");
        assert_eq!(query.get("command").unwrap(), r#"["printf","hello world"]"#);
        assert_eq!(query.get("tty").unwrap(), "false");
    }
}
