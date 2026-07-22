use std::io::{BufRead, IsTerminal, Write};
use std::sync::Arc;
use std::time::Duration;

use clap::Args;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use futures_util::{SinkExt, StreamExt};
use kernel_api::{
    Assignment, AssignmentPhase, Deployment, DeploymentId, DeploymentPhase, ExecStreamFrame,
    NodeId, Service, ServiceId,
};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

use crate::CliError;
use crate::api_client::ApiClient;
use crate::contexts::{Context, ContextStore};

const MAXIMUM_FRAME_BYTES: usize = 16 * 1_024 * 1_024;
const INPUT_BYTES: usize = 16 * 1_024;
const RESIZE_POLL_INTERVAL: Duration = Duration::from_millis(200);

type ExecSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Interactive exec target and terminal options.
#[derive(Debug, Args)]
pub(crate) struct ExecCommand {
    /// Service containing the target replica.
    service_id: String,
    /// Still-running deployment; defaults to the active or newest running deployment.
    #[arg(long)]
    deployment: Option<String>,
    /// Zero-based replica index.
    #[arg(short = 'r', long)]
    replica: Option<u32>,
    /// Limit candidate replicas to one node.
    #[arg(long)]
    node: Option<String>,
    /// Shell used when COMMAND is omitted.
    #[arg(long, default_value = "/bin/sh")]
    shell: String,
    /// Use separate pipes instead of allocating a terminal.
    #[arg(long)]
    no_tty: bool,
    /// Executable and arguments passed verbatim after `--`.
    #[arg(last = true, num_args = 1.., value_name = "COMMAND")]
    command: Vec<String>,
}

pub(crate) async fn run(
    command: ExecCommand,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let stdin_is_terminal = std::io::stdin().is_terminal();
    let tty = !command.no_tty;
    if tty && !stdin_is_terminal {
        return Err(CliError::invalid_input(
            "stdin is not a terminal; use --no-tty when piping input",
        ));
    }
    let service_id = ServiceId::new(command.service_id)
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    let deployment_id = command
        .deployment
        .map(DeploymentId::new)
        .transpose()
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    let node_id = command
        .node
        .map(NodeId::new)
        .transpose()
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    let context = ContextStore::from_environment()?.active()?;
    let client = ApiClient::new(context.clone())?;
    let service = client
        .get::<Service>(&format!("/api/services/{service_id}"))
        .await?;
    let deployment = select_deployment(
        client
            .get::<Vec<Deployment>>(&format!("/api/services/{service_id}/deployments"))
            .await?,
        deployment_id.as_ref(),
        service.status.active_deployment_id.as_ref(),
    )?;
    let assignments = client
        .get::<Vec<Assignment>>(&format!(
            "/api/services/{service_id}/deployments/{}/assignments",
            deployment.meta.id
        ))
        .await?;
    let assignment = select_assignment(
        assignments,
        command.replica,
        node_id.as_ref(),
        stdin_is_terminal,
        input,
        output,
    )?;
    let arguments = if command.command.is_empty() {
        vec![command.shell]
    } else {
        command.command
    };
    let initial_size = tty.then(terminal_size).transpose()?;
    let endpoint = exec_endpoint(
        &context,
        &service_id,
        &deployment.meta.id,
        &assignment.meta.id,
        &arguments,
        initial_size,
    )?;
    let socket = connect(&context, endpoint).await?;
    let raw_mode = RawModeGuard::enter(tty)?;
    let exit = drive(socket, tty, initial_size).await;
    drop(raw_mode);
    match exit? {
        Some(0) => Ok(()),
        Some(code) => Err(CliError::ExecExit { code }),
        None => Err(CliError::exec(
            "runtime closed without reporting an exit status",
        )),
    }
}

pub(crate) fn select_deployment(
    mut deployments: Vec<Deployment>,
    requested: Option<&DeploymentId>,
    active: Option<&DeploymentId>,
) -> Result<Deployment, CliError> {
    deployments.retain(|deployment| {
        matches!(
            deployment.status.phase,
            DeploymentPhase::PendingReady | DeploymentPhase::Ready | DeploymentPhase::Draining
        )
    });
    if let Some(requested) = requested {
        return deployments
            .into_iter()
            .find(|deployment| &deployment.meta.id == requested)
            .ok_or_else(|| {
                CliError::not_found(format!(
                    "deployment `{requested}` is not running for this service"
                ))
            });
    }
    if let Some(active) = active
        && let Some(position) = deployments
            .iter()
            .position(|deployment| &deployment.meta.id == active)
    {
        return Ok(deployments.remove(position));
    }
    deployments.sort_by(|left, right| {
        right
            .status
            .created_at
            .cmp(&left.status.created_at)
            .then_with(|| right.meta.id.cmp(&left.meta.id))
    });
    deployments
        .into_iter()
        .next()
        .ok_or_else(|| CliError::not_found("service has no running deployment"))
}

pub(crate) fn select_assignment(
    assignments: Vec<Assignment>,
    replica: Option<u32>,
    node_id: Option<&NodeId>,
    allow_prompt: bool,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<Assignment, CliError> {
    let mut candidates = assignments
        .into_iter()
        .filter(|assignment| assignment.status.phase == AssignmentPhase::Running)
        .filter(|assignment| replica.is_none_or(|replica| assignment.spec.replica_index == replica))
        .filter(|assignment| node_id.is_none_or(|node_id| &assignment.spec.node_id == node_id))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.spec
            .replica_index
            .cmp(&right.spec.replica_index)
            .then_with(|| right.spec.placement_epoch.cmp(&left.spec.placement_epoch))
    });
    match candidates.len() {
        0 => Err(CliError::not_found(
            "no running replica matches the exec target",
        )),
        1 => candidates
            .pop()
            .ok_or_else(|| CliError::exec("selected replica disappeared")),
        _ if allow_prompt => prompt_assignment(candidates, input, output),
        _ => Err(CliError::invalid_input(
            "multiple running replicas match; use --replica or --node when piping input",
        )),
    }
}

fn prompt_assignment(
    candidates: Vec<Assignment>,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<Assignment, CliError> {
    writeln!(output, "Select a running replica:")
        .map_err(|source| CliError::io("failed to write exec prompt", source))?;
    for (index, assignment) in candidates.iter().enumerate() {
        writeln!(
            output,
            "  {}. replica #{} on {}",
            index + 1,
            assignment.spec.replica_index,
            assignment.spec.node_id
        )
        .map_err(|source| CliError::io("failed to write exec prompt", source))?;
    }
    write!(output, "Replica [1-{}]: ", candidates.len())
        .and_then(|()| output.flush())
        .map_err(|source| CliError::io("failed to flush exec prompt", source))?;
    let mut selection = String::new();
    input
        .read_line(&mut selection)
        .map_err(|source| CliError::io("failed to read exec selection", source))?;
    let index = selection
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|index| (1..=candidates.len()).contains(index))
        .ok_or_else(|| CliError::invalid_input("replica selection is out of range"))?;
    candidates
        .into_iter()
        .nth(index - 1)
        .ok_or_else(|| CliError::exec("selected replica disappeared"))
}

pub(crate) fn exec_endpoint(
    context: &Context,
    service_id: &ServiceId,
    deployment_id: &DeploymentId,
    assignment_id: &kernel_api::AssignmentId,
    command: &[String],
    terminal_size: Option<TerminalSize>,
) -> Result<reqwest::Url, CliError> {
    let mut endpoint = reqwest::Url::parse(&context.host).map_err(|error| {
        CliError::invalid_contexts(format!("active context is invalid: {error}"))
    })?;
    let websocket_scheme = match endpoint.scheme() {
        "http" => "ws",
        "https" => "wss",
        _ => return Err(CliError::invalid_contexts("active context is not HTTP(S)")),
    };
    endpoint
        .set_scheme(websocket_scheme)
        .map_err(|()| CliError::invalid_contexts("failed to construct WebSocket origin"))?;
    endpoint.set_path(&format!(
        "/api/services/{service_id}/deployments/{deployment_id}/assignments/{assignment_id}/exec"
    ));
    let encoded = serde_json::to_string(command)
        .map_err(|source| CliError::json("failed to encode exec command", source))?;
    let mut query = endpoint.query_pairs_mut();
    query.append_pair("command", &encoded);
    match terminal_size {
        Some(size) => {
            query.append_pair("tty", "true");
            query.append_pair("columns", &size.columns.to_string());
            query.append_pair("rows", &size.rows.to_string());
        }
        None => {
            query.append_pair("tty", "false");
        }
    }
    drop(query);
    Ok(endpoint)
}

async fn connect(context: &Context, endpoint: reqwest::Url) -> Result<ExecSocket, CliError> {
    let mut request = endpoint
        .as_str()
        .into_client_request()
        .map_err(|error| CliError::exec(format!("failed to build WebSocket request: {error}")))?;
    if let Some(token) = &context.token {
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
            format!("Bearer {}", token.expose())
                .parse()
                .map_err(|_| CliError::invalid_contexts("active context token is invalid"))?,
        );
    }
    let configuration = WebSocketConfig::default()
        .max_message_size(Some(MAXIMUM_FRAME_BYTES))
        .max_frame_size(Some(MAXIMUM_FRAME_BYTES));
    let connected = if endpoint.scheme() == "wss" {
        tokio_tungstenite::connect_async_tls_with_config(
            request,
            Some(configuration),
            true,
            Some(Connector::Rustls(Arc::new(client_tls(context)?))),
        )
        .await
    } else {
        tokio_tungstenite::connect_async_with_config(request, Some(configuration), true).await
    };
    connected
        .map(|connected| connected.0)
        .map_err(|error| CliError::exec(format!("failed to connect exec WebSocket: {error}")))
}

fn client_tls(context: &Context) -> Result<rustls::ClientConfig, CliError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(certificate) = &context.ca_certificate_pem {
        let certificates = CertificateDer::pem_slice_iter(certificate.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| CliError::invalid_contexts("active context CA certificate is invalid"))?;
        let (accepted, _) = roots.add_parsable_certificates(certificates);
        if accepted == 0 {
            return Err(CliError::invalid_contexts(
                "active context CA certificate contains no certificates",
            ));
        }
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

async fn drive(
    mut socket: ExecSocket,
    tty: bool,
    initial_size: Option<TerminalSize>,
) -> Result<Option<i32>, CliError> {
    let mut stdin = duplicate_file(libc::STDIN_FILENO, "standard input")?;
    let mut stdout = duplicate_file(libc::STDOUT_FILENO, "standard output")?;
    let mut stderr = duplicate_file(libc::STDERR_FILENO, "standard error")?;
    let mut input = vec![0_u8; INPUT_BYTES];
    let mut input_open = true;
    let mut last_size = initial_size;
    let mut resize = tokio::time::interval(RESIZE_POLL_INTERVAL);
    resize.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    resize.tick().await;
    loop {
        tokio::select! {
            read = stdin.read(&mut input), if input_open => {
                let count = read.map_err(|source| CliError::io("failed to read exec input", source))?;
                let frame = if count == 0 {
                    input_open = false;
                    ExecStreamFrame::CloseStdin
                } else {
                    let bytes = input
                        .get(..count)
                        .ok_or_else(|| CliError::exec("exec input read exceeded its buffer"))?;
                    ExecStreamFrame::Stdin(bytes.to_vec())
                };
                send(&mut socket, frame).await?;
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Binary(encoded))) => {
                        match ExecStreamFrame::decode(&encoded)
                            .map_err(|error| CliError::exec(format!("invalid exec response: {error}")))? {
                            ExecStreamFrame::Stdout(bytes) => stdout.write_all(&bytes).await
                                .map_err(|source| CliError::io("failed to write exec output", source))?,
                            ExecStreamFrame::Stderr(bytes) => stderr.write_all(&bytes).await
                                .map_err(|source| CliError::io("failed to write exec error output", source))?,
                            ExecStreamFrame::Exited { code } => {
                                stdout.flush().await.map_err(|source| CliError::io("failed to flush exec output", source))?;
                                stderr.flush().await.map_err(|source| CliError::io("failed to flush exec error output", source))?;
                                let _ = socket.close(None).await;
                                return Ok(code);
                            }
                            ExecStreamFrame::Error(message) => return Err(CliError::exec(message)),
                            _ => return Err(CliError::exec("server sent a client-input exec frame")),
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await
                        .map_err(|error| CliError::exec(format!("failed to answer exec ping: {error}")))?,
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => return Ok(None),
                    Some(Ok(Message::Text(_) | Message::Frame(_))) => {
                        return Err(CliError::exec("server sent a non-binary exec message"));
                    }
                    Some(Err(error)) => return Err(CliError::exec(format!("exec WebSocket failed: {error}"))),
                }
            }
            _ = resize.tick(), if tty => {
                let size = terminal_size()?;
                if last_size != Some(size) {
                    send(&mut socket, ExecStreamFrame::Resize {
                        columns: size.columns,
                        rows: size.rows,
                    }).await?;
                    last_size = Some(size);
                }
            }
        }
    }
}

async fn send(socket: &mut ExecSocket, frame: ExecStreamFrame) -> Result<(), CliError> {
    let encoded = frame
        .encode()
        .map_err(|error| CliError::exec(format!("failed to encode exec input: {error}")))?;
    socket
        .send(Message::Binary(encoded.into()))
        .await
        .map_err(|error| CliError::exec(format!("failed to send exec input: {error}")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TerminalSize {
    pub(crate) columns: u16,
    pub(crate) rows: u16,
}

fn terminal_size() -> Result<TerminalSize, CliError> {
    let (columns, rows) = crossterm::terminal::size()
        .map_err(|source| CliError::io("failed to read terminal size", source))?;
    if columns == 0 || rows == 0 {
        return Err(CliError::invalid_input(
            "terminal dimensions must be non-zero",
        ));
    }
    Ok(TerminalSize { columns, rows })
}

struct RawModeGuard {
    enabled: bool,
}

impl RawModeGuard {
    fn enter(enabled: bool) -> Result<Self, CliError> {
        if enabled {
            enable_raw_mode()
                .map_err(|source| CliError::io("failed to enter terminal raw mode", source))?;
        }
        Ok(Self { enabled })
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = disable_raw_mode();
        }
    }
}

fn duplicate_file(file_descriptor: i32, description: &str) -> Result<tokio::fs::File, CliError> {
    use std::os::fd::FromRawFd;

    // SAFETY: `dup` does not borrow the source descriptor and returns a new owned descriptor.
    let duplicated = unsafe { libc::dup(file_descriptor) };
    if duplicated < 0 {
        return Err(CliError::io(
            format!("failed to duplicate {description}"),
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: a successful `dup` result is a fresh descriptor now owned by this file.
    let file = unsafe { std::fs::File::from_raw_fd(duplicated) };
    Ok(tokio::fs::File::from_std(file))
}
