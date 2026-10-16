use std::io::IsTerminal;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use maestro_core::{Assignment, DeploymentId, DeploymentStatus, ServiceId};
use maestro_proto::pb::ExecFrame;
use maestro_runtime::{ContainerdRuntime, ExecRequest};
use maestro_store::Consensus;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite;
use tonic::{Status, Streaming};

use crate::client::Endpoint;
use crate::node_service::NodeClients;

const STDIN: u8 = 0;
const OUTPUT: u8 = 1;
const RESIZE: u8 = 2;
const EXIT: u8 = 3;
const ERROR: u8 = 4;
const PING: u8 = 5;
const START: u8 = 6;
const MAX_SESSIONS: usize = 8;
const DEFAULT_TERM: &str = "xterm-256color";
const IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const OUTPUT_DRAIN: Duration = Duration::from_millis(200);
const MAX_ARGS: usize = 256;
const MAX_COMMAND_BYTES: usize = 64 * 1024;
const FRAME_BUFFER: usize = 32;
const CLI_PING_INTERVAL: Duration = Duration::from_secs(20);
const DEFAULT_COMMAND: &str = "/bin/sh";

#[derive(Clone)]
pub struct ExecState {
    pub consensus: Arc<Consensus>,
    pub clients: NodeClients,
    pub allowed: bool,
}

#[derive(Clone)]
pub struct ExecLimiter(Arc<Semaphore>);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecQuery {
    deployment_id: Option<String>,
    replica_index: Option<u32>,
    command: Option<String>,
    tty: Option<bool>,
    term: Option<String>,
    cols: Option<u16>,
    rows: Option<u16>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecStart {
    container_id: String,
    argv: Vec<String>,
    tty: bool,
    #[serde(default)]
    term: Option<String>,
    size: Option<(u16, u16)>,
    service: String,
    deployment: String,
    replica: u32,
}

#[derive(Serialize, Deserialize)]
struct TerminalSize {
    cols: u16,
    rows: u16,
}

impl Default for ExecLimiter {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(MAX_SESSIONS)))
    }
}

pub async fn exec_service(
    State(state): State<ExecState>,
    Path(service_id): Path<String>,
    Query(query): Query<ExecQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    match prepare(&state, ServiceId::new(service_id), query) {
        Ok((address, start)) => {
            upgrade.on_upgrade(move |socket| relay(socket, state.clients, address, start))
        }
        Err((status, message)) => (status, Json(json!({ "error": message }))).into_response(),
    }
}

pub async fn serve_node_exec(
    runtime: Arc<ContainerdRuntime>,
    limiter: &ExecLimiter,
    mut inbound: Streaming<ExecFrame>,
) -> Result<ReceiverStream<Result<ExecFrame, Status>>, Status> {
    let permit = limiter.0.clone().try_acquire_owned().map_err(|_| {
        Status::resource_exhausted(format!(
            "this node already has {MAX_SESSIONS} active exec sessions"
        ))
    })?;
    let first = inbound
        .message()
        .await?
        .ok_or_else(|| Status::invalid_argument("exec stream closed before start"))?;
    let start: ExecStart = match first.data.split_first() {
        Some((&START, payload)) => serde_json::from_slice(payload)
            .map_err(|error| Status::invalid_argument(error.to_string()))?,
        _ => Err(Status::invalid_argument(
            "the first exec frame must be start",
        ))?,
    };
    let session = runtime
        .exec(ExecRequest {
            container_id: start.container_id.clone(),
            argv: start.argv.clone(),
            tty: start.tty,
            term: start.term.clone(),
            size: start.size,
        })
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    tracing::info!(
        service = %start.service,
        deployment = %start.deployment,
        replica = start.replica,
        command = ?start.argv,
        "exec opened"
    );
    let (outbound, receiver) = mpsc::channel(FRAME_BUFFER);
    tokio::spawn(async move {
        let opened = Instant::now();
        let maestro_runtime::ExecSession {
            container_id,
            exec_id,
            mut stdin,
            mut output,
            mut exit,
        } = session;
        let mut deadline = tokio::time::Instant::now() + IDLE_TIMEOUT;
        let mut exit_code = None;
        loop {
            tokio::select! {
                frame = inbound.message() => {
                    deadline = tokio::time::Instant::now() + IDLE_TIMEOUT;
                    match frame.ok().flatten().map(|frame| frame.data) {
                        Some(data) => match data.split_first() {
                            Some((&STDIN, [])) => {
                                stdin = None;
                                let _ = runtime.close_exec_stdin(&container_id, &exec_id).await;
                            }
                            Some((&STDIN, bytes)) => {
                                if let Some(writer) = stdin.as_mut() {
                                    let _ = writer.write_all(bytes).await;
                                }
                            }
                            Some((&RESIZE, payload)) => {
                                if let Ok(size) = serde_json::from_slice::<TerminalSize>(payload) {
                                    let _ = runtime.resize_exec(&container_id, &exec_id, size.cols, size.rows).await;
                                }
                            }
                            Some((&PING, _)) => {
                                let _ = outbound.send(Ok(frame_of(PING, &[]))).await;
                            }
                            _ => {}
                        },
                        None => break,
                    }
                }
                chunk = output.recv() => {
                    deadline = tokio::time::Instant::now() + IDLE_TIMEOUT;
                    if let Some(chunk) = chunk {
                        let _ = outbound.send(Ok(frame_of(OUTPUT, &chunk))).await;
                    }
                }
                status = &mut exit => {
                    while let Ok(Some(chunk)) = tokio::time::timeout(OUTPUT_DRAIN, output.recv()).await {
                        let _ = outbound.send(Ok(frame_of(OUTPUT, &chunk))).await;
                    }
                    let frame = match status {
                        Ok(Ok(code)) => {
                            exit_code = Some(code);
                            frame_of(EXIT, json!({ "code": code }).to_string().as_bytes())
                        }
                        Ok(Err(error)) => frame_of(ERROR, error.to_string().as_bytes()),
                        Err(_) => frame_of(ERROR, b"exec process ended unexpectedly"),
                    };
                    let _ = outbound.send(Ok(frame)).await;
                    break;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let _ = outbound.send(Ok(frame_of(ERROR, b"exec session idle timeout"))).await;
                    break;
                }
            }
        }
        runtime.remove_exec(&container_id, &exec_id).await;
        tracing::info!(
            service = %start.service,
            deployment = %start.deployment,
            replica = start.replica,
            duration_ms = opened.elapsed().as_millis() as u64,
            exit_code = ?exit_code,
            "exec closed"
        );
        drop(permit);
    });
    Ok(ReceiverStream::new(receiver))
}

pub async fn run_cli(
    endpoint: &Endpoint,
    service: &str,
    replica: Option<u32>,
    deployment: Option<&str>,
    tty: bool,
    command: Vec<String>,
) -> anyhow::Result<i32> {
    anyhow::ensure!(
        !tty || std::io::stdin().is_terminal(),
        "stdin is not a terminal; use --no-tty when piping input"
    );
    let size = tty
        .then(crossterm::terminal::size)
        .transpose()?
        .filter(|(cols, rows)| *cols > 0 && *rows > 0);
    let mut query = vec![("tty".to_string(), tty.to_string())];
    query.extend(tty.then(|| {
        let term = std::env::var("TERM").unwrap_or_else(|_| DEFAULT_TERM.to_string());
        ("term".to_string(), term)
    }));
    query.extend(replica.map(|index| ("replicaIndex".to_string(), index.to_string())));
    query.extend(deployment.map(|id| ("deploymentId".to_string(), id.to_string())));
    query
        .extend((!command.is_empty()).then(|| ("command".to_string(), json!(command).to_string())));
    query.extend(size.into_iter().flat_map(|(cols, rows)| {
        [
            ("cols".to_string(), cols.to_string()),
            ("rows".to_string(), rows.to_string()),
        ]
    }));
    let url = reqwest::Url::parse_with_params(
        &format!(
            "{}/api/services/{service}/exec",
            endpoint.api.trim_end_matches('/')
        ),
        &query,
    )?;
    let websocket_url = url
        .as_str()
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    let mut request = tungstenite::client::IntoClientRequest::into_client_request(websocket_url)?;
    if let Some(token) = &endpoint.token {
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
    }
    let (socket, _) =
        tokio_tungstenite::connect_async(request)
            .await
            .map_err(|error| match error {
                tungstenite::Error::Http(response) => {
                    let reason = response
                        .body()
                        .as_deref()
                        .and_then(|body| serde_json::from_slice::<serde_json::Value>(body).ok())
                        .and_then(|body| body["error"].as_str().map(str::to_string))
                        .unwrap_or_default();
                    anyhow::anyhow!("{}: {reason}", response.status())
                }
                other => anyhow::Error::from(other).context("failed to open the exec session"),
            })?;
    let _raw_mode = tty.then(RawMode::enable).transpose()?;
    session(socket, tty).await
}

fn prepare(
    state: &ExecState,
    service_id: ServiceId,
    query: ExecQuery,
) -> Result<(String, ExecStart), (StatusCode, String)> {
    let store = state.consensus.state();
    let internal =
        |error: maestro_store::StoreError| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
    let spec = store
        .service(&service_id)
        .map_err(internal)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("service {service_id} not found"),
            )
        })?;
    let argv = parse_command(query.command.as_deref())?;
    let size = match (query.cols, query.rows) {
        (Some(cols), Some(rows)) if cols > 0 && rows > 0 => Ok(Some((cols, rows))),
        (None, None) => Ok(None),
        _ => Err((
            StatusCode::BAD_REQUEST,
            "cols and rows must be sent together and be positive".to_string(),
        )),
    }?;
    let mut deployments = store.deployments(&service_id).map_err(internal)?;
    deployments.sort_by_key(|deployment| std::cmp::Reverse(deployment.created_at_ms));
    let deployment = match &query.deployment_id {
        Some(id) => deployments
            .into_iter()
            .find(|deployment| deployment.id == DeploymentId::new(id.clone()))
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("deployment {id} not found")))?,
        None => deployments
            .into_iter()
            .find(|deployment| deployment.status == DeploymentStatus::Ready)
            .ok_or_else(|| {
                (
                    StatusCode::CONFLICT,
                    format!("service {service_id} has no ready deployment"),
                )
            })?,
    };
    let execable = matches!(
        deployment.status,
        DeploymentStatus::Deploying | DeploymentStatus::Ready | DeploymentStatus::Draining
    );
    let mut replicas = store
        .nodes()
        .map_err(internal)?
        .iter()
        .map(|node| store.assignments(node.id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(internal)?
        .concat()
        .into_iter()
        .filter(|assignment| assignment.deployment_id == deployment.id)
        .collect::<Vec<Assignment>>();
    replicas.sort_by_key(|assignment| assignment.index);
    let replica = match query.replica_index {
        Some(index) => replicas
            .into_iter()
            .find(|assignment| assignment.index == index),
        None if replicas.len() == 1 => replicas.pop(),
        None if replicas.is_empty() => None,
        None => {
            let indexes = replicas
                .iter()
                .map(|assignment| assignment.index.to_string())
                .collect::<Vec<_>>();
            Err((
                StatusCode::BAD_REQUEST,
                format!("replicaIndex is required; replicas: {}", indexes.join(", ")),
            ))?
        }
    };
    match (state.allowed, spec.deploy.exec, execable, replica) {
        (false, _, _, _) => Err((
            StatusCode::FORBIDDEN,
            "CLI exec is disabled; set allowExec to true in the cluster config".to_string(),
        )),
        (true, false, _, _) => Err((
            StatusCode::FORBIDDEN,
            format!("CLI exec is disabled for service `{service_id}`"),
        )),
        (true, true, false, _) => Err((
            StatusCode::CONFLICT,
            format!("deployment {} is {:?}", deployment.id, deployment.status),
        )),
        (true, true, true, None) => Err((StatusCode::NOT_FOUND, "replica not found".to_string())),
        (true, true, true, Some(replica)) => {
            let node = store
                .node(replica.node_id)
                .map_err(internal)?
                .ok_or_else(|| {
                    (
                        StatusCode::NOT_FOUND,
                        format!("node {} not found", replica.node_id),
                    )
                })?;
            Ok((
                node.endpoint,
                ExecStart {
                    container_id: replica.replica_id.to_string(),
                    argv,
                    tty: query.tty.unwrap_or(true),
                    term: query.term.clone(),
                    size,
                    service: service_id.to_string(),
                    deployment: deployment.id.to_string(),
                    replica: replica.index,
                },
            ))
        }
    }
}

fn parse_command(command: Option<&str>) -> Result<Vec<String>, (StatusCode, String)> {
    let argv = match command {
        Some(command) => serde_json::from_str::<Vec<String>>(command).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "command must be a JSON array of strings".to_string(),
            )
        })?,
        None => vec![DEFAULT_COMMAND.to_string()],
    };
    let total = argv.iter().map(String::len).sum::<usize>();
    match (1..=MAX_ARGS).contains(&argv.len()) && total <= MAX_COMMAND_BYTES {
        true => Ok(argv),
        false => Err((
            StatusCode::BAD_REQUEST,
            format!(
                "command must have 1-{MAX_ARGS} arguments and at most {MAX_COMMAND_BYTES} bytes"
            ),
        )),
    }
}

async fn relay(mut socket: WebSocket, clients: NodeClients, address: String, start: ExecStart) {
    let (frames, receiver) = mpsc::channel(FRAME_BUFFER);
    let start_frame = frame_of(START, &serde_json::to_vec(&start).unwrap_or_default());
    let _ = frames.send(start_frame).await;
    match clients.exec(&address, receiver).await {
        Err(status) => {
            let _ = socket
                .send(Message::Binary(
                    frame_of(ERROR, status.message().as_bytes()).data.into(),
                ))
                .await;
        }
        Ok(mut node) => loop {
            tokio::select! {
                message = socket.recv() => match message {
                    Some(Ok(Message::Binary(data))) if matches!(data.first(), Some(&STDIN | &RESIZE | &PING)) => {
                        if frames.send(ExecFrame { data: data.to_vec() }).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {
                        let _ = socket
                            .send(Message::Binary(frame_of(ERROR, b"exec accepts binary stdin, resize, and ping frames only").data.into()))
                            .await;
                        break;
                    }
                },
                frame = node.message() => match frame {
                    Ok(Some(frame)) => {
                        let terminal = matches!(frame.data.first(), Some(&EXIT | &ERROR));
                        if socket.send(Message::Binary(frame.data.into())).await.is_err() || terminal {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        let _ = socket
                            .send(Message::Binary(frame_of(ERROR, status.message().as_bytes()).data.into()))
                            .await;
                        break;
                    }
                },
            }
        },
    }
    let _ = socket.send(Message::Close(None)).await;
}

async fn session(
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    tty: bool,
) -> anyhow::Result<i32> {
    use futures_util::{SinkExt, StreamExt};

    let (mut sink, mut stream) = socket.split();
    let (outgoing, mut outgoing_receiver) = mpsc::channel::<Vec<u8>>(FRAME_BUFFER);
    tokio::spawn(read_stdin(outgoing.clone(), tty));
    tokio::spawn(watch_terminal(outgoing.clone(), tty));
    let mut stdout = tokio::io::stdout();
    loop {
        tokio::select! {
            frame = outgoing_receiver.recv() => match frame {
                Some(frame) => sink.send(tungstenite::Message::Binary(frame.into())).await?,
                None => return Ok(0),
            },
            message = stream.next() => match message {
                Some(Ok(tungstenite::Message::Binary(data))) => match data.split_first() {
                    Some((&OUTPUT, bytes)) => {
                        stdout.write_all(bytes).await?;
                        stdout.flush().await?;
                    }
                    Some((&EXIT, payload)) => {
                        let code = serde_json::from_slice::<serde_json::Value>(payload)
                            .ok()
                            .and_then(|value| value["code"].as_i64())
                            .unwrap_or(1);
                        return Ok(code as i32);
                    }
                    Some((&ERROR, payload)) => {
                        eprint!("\r\nexec: {}\r\n", String::from_utf8_lossy(payload));
                        return Ok(1);
                    }
                    _ => {}
                },
                Some(Ok(tungstenite::Message::Close(_))) | None => return Ok(0),
                Some(Ok(_)) => {}
                Some(Err(error)) => Err(error)?,
            },
        }
    }
}

async fn read_stdin(outgoing: mpsc::Sender<Vec<u8>>, tty: bool) {
    let mut stdin = tokio::io::stdin();
    let mut buffer = vec![0u8; 4096];
    let mut at_line_start = true;
    let mut pending_tilde = false;
    while let Ok(read) = stdin.read(&mut buffer).await {
        let chunk = &buffer[..read];
        let detached = tty && detach_requested(chunk, &mut at_line_start, &mut pending_tilde);
        let sent = match (read, detached) {
            (0, _) => {
                let _ = outgoing.send(frame_of(STDIN, &[]).data).await;
                false
            }
            (_, true) => false,
            (_, false) => outgoing.send(frame_of(STDIN, chunk).data).await.is_ok(),
        };
        if !sent {
            break;
        }
    }
}

async fn watch_terminal(outgoing: mpsc::Sender<Vec<u8>>, tty: bool) {
    let mut resized =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change()).ok();
    let mut ping = tokio::time::interval(CLI_PING_INTERVAL);
    loop {
        let frame = tokio::select! {
            _ = ping.tick() => frame_of(PING, &[]),
            Some(()) = async { resized.as_mut()?.recv().await }, if tty => {
                match crossterm::terminal::size() {
                    Ok((cols, rows)) => frame_of(RESIZE, json!({ "cols": cols, "rows": rows }).to_string().as_bytes()),
                    Err(_) => continue,
                }
            }
        };
        if outgoing.send(frame.data).await.is_err() {
            break;
        }
    }
}

fn detach_requested(chunk: &[u8], at_line_start: &mut bool, pending_tilde: &mut bool) -> bool {
    chunk.iter().any(|byte| {
        let detach = *pending_tilde && *byte == b'.';
        *pending_tilde = *at_line_start && *byte == b'~';
        *at_line_start = matches!(byte, b'\r' | b'\n');
        detach
    })
}

fn frame_of(kind: u8, payload: &[u8]) -> ExecFrame {
    let mut data = Vec::with_capacity(payload.len() + 1);
    data.push(kind);
    data.extend_from_slice(payload);
    ExecFrame { data }
}

struct RawMode;

impl RawMode {
    fn enable() -> anyhow::Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_exec_commands() {
        assert_eq!(parse_command(None).unwrap(), ["/bin/sh"]);
        assert_eq!(
            parse_command(Some(r#"["ls","-la"]"#)).unwrap(),
            ["ls", "-la"]
        );
        assert!(parse_command(Some("[]")).is_err());
        assert!(parse_command(Some("ls -la")).is_err());
    }

    #[test]
    fn detaches_on_tilde_dot_at_line_start() {
        let mut at_line_start = true;
        let mut pending = false;
        assert!(!detach_requested(
            b"echo a~.",
            &mut at_line_start,
            &mut pending
        ));
        assert!(detach_requested(b"\r~.", &mut at_line_start, &mut pending));
        let mut at_line_start = true;
        let mut pending = false;
        assert!(!detach_requested(b"~", &mut at_line_start, &mut pending));
        assert!(detach_requested(b".", &mut at_line_start, &mut pending));
    }
}
