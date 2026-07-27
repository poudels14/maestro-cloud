use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use kernel_api::{
    Assignment, AssignmentId, BuiltinKind, CommandSpec, DeploymentId, ExecStreamFrame, ServiceId,
};
use runtime::{ExecInput, ExecMode, ExecOutput, ExecRequest, ExecSession, RuntimeError};
use serde::Deserialize;
use tokio::sync::OwnedSemaphorePermit;

use crate::exec_service::ClusterExecSessions;
use crate::{ApiError, AppState, resource};

const IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAXIMUM_FRAME_BYTES: usize = 16 * 1_024 * 1_024;
const MAXIMUM_ARGUMENTS: usize = 256;
const MAXIMUM_COMMAND_BYTES: usize = 64 * 1_024;

pub(super) fn router() -> Router<AppState> {
    Router::new().route(
        "/api/services/{service_id}/deployments/{deployment_id}/assignments/{assignment_id}/exec",
        get(exec_assignment),
    )
}

pub(super) fn node_router() -> Router<AppState> {
    Router::new().route(
        "/api/node/assignments/{assignment_id}/exec",
        get(exec_local_assignment),
    )
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecParameters {
    command: Option<String>,
    tty: Option<bool>,
    columns: Option<u16>,
    rows: Option<u16>,
}

async fn exec_assignment(
    State(state): State<AppState>,
    Path((service_id, deployment_id, assignment_id)): Path<(String, String, String)>,
    Query(parameters): Query<ExecParameters>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let assignment = scoped_assignment(&state, service_id, deployment_id, assignment_id).await?;
    let request = exec_request(parameters)?;
    upgrade_exec(
        state,
        upgrade,
        OpenTarget::Node(assignment.spec.node_id),
        assignment.meta.id,
        request,
    )
}

async fn exec_local_assignment(
    State(state): State<AppState>,
    Path(assignment_id): Path<String>,
    Query(parameters): Query<ExecParameters>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let assignment_id = AssignmentId::new(assignment_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let request = exec_request(parameters)?;
    upgrade_exec(state, upgrade, OpenTarget::Local, assignment_id, request)
}

fn upgrade_exec(
    state: AppState,
    upgrade: WebSocketUpgrade,
    target: OpenTarget,
    assignment_id: AssignmentId,
    request: ExecRequest,
) -> Result<Response, ApiError> {
    let sessions = state.exec_sessions.clone().ok_or_else(|| {
        ApiError::service_unavailable("interactive exec is not configured on this node")
    })?;
    let permit =
        state.exec_relays.clone().try_acquire_owned().map_err(|_| {
            ApiError::service_unavailable("this node already has 8 active exec relays")
        })?;
    Ok(upgrade
        .max_message_size(MAXIMUM_FRAME_BYTES)
        .max_frame_size(MAXIMUM_FRAME_BYTES)
        .on_upgrade(move |socket| async move {
            open_and_relay(socket, sessions, target, assignment_id, request, permit).await;
        })
        .into_response())
}

async fn open_and_relay(
    mut socket: WebSocket,
    sessions: Arc<dyn ClusterExecSessions>,
    target: OpenTarget,
    assignment_id: AssignmentId,
    request: ExecRequest,
    permit: OwnedSemaphorePermit,
) {
    let opened = match target {
        OpenTarget::Node(node_id) => sessions.open(&node_id, &assignment_id, request).await,
        OpenTarget::Local => sessions.open_local(&assignment_id, request).await,
    };
    match opened {
        Ok(session) => relay(socket, session, permit).await,
        Err(error) => {
            let _ = send_frame(&mut socket, ExecStreamFrame::Error(error.to_string())).await;
            let _ = socket.close().await;
        }
    }
}

async fn relay(
    socket: WebSocket,
    mut session: Box<dyn ExecSession>,
    _permit: OwnedSemaphorePermit,
) {
    let (mut writer, mut reader) = socket.split();
    let idle = tokio::time::sleep(IDLE_TIMEOUT);
    tokio::pin!(idle);
    loop {
        tokio::select! {
            incoming = reader.next() => {
                idle.as_mut().reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
                match apply_client_message(&mut writer, session.as_mut(), incoming).await {
                    RelayControl::Continue => {}
                    RelayControl::Stop => {
                        terminate(session.as_mut()).await;
                        break;
                    }
                }
            }
            output = session.next() => {
                idle.as_mut().reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
                let frame = match output {
                    Ok(Some(ExecOutput::Stdout(bytes))) => ExecStreamFrame::Stdout(bytes),
                    Ok(Some(ExecOutput::Stderr(bytes))) => ExecStreamFrame::Stderr(bytes),
                    Ok(Some(ExecOutput::Exited { code })) => ExecStreamFrame::Exited { code },
                    Ok(None) => ExecStreamFrame::Error("exec runtime closed without an exit status".to_owned()),
                    Err(error) => ExecStreamFrame::Error(error.to_string()),
                };
                let terminal = frame.is_terminal();
                if send_split_frame(&mut writer, frame).await.is_err() || terminal {
                    break;
                }
            }
            _ = &mut idle => {
                let _ = send_split_frame(
                    &mut writer,
                    ExecStreamFrame::Error("exec session idle timeout".to_owned()),
                ).await;
                terminate(session.as_mut()).await;
                break;
            }
        }
    }
    let _ = writer.close().await;
}

async fn terminate(session: &mut dyn ExecSession) {
    if kill_session(session).await.is_err() {
        return;
    }
    let reaped = async {
        while let Ok(Some(output)) = session.next().await {
            if matches!(output, ExecOutput::Exited { .. }) {
                break;
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(5), reaped).await;
}

async fn apply_client_message<Writer>(
    writer: &mut Writer,
    session: &mut dyn ExecSession,
    incoming: Option<Result<Message, axum::Error>>,
) -> RelayControl
where
    Writer: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    let frame = match incoming {
        Some(Ok(Message::Binary(encoded))) => match ExecStreamFrame::decode(&encoded) {
            Ok(frame) if frame.is_client_input() => frame,
            Ok(_) => return protocol_stop(writer, "client sent an output exec frame").await,
            Err(error) => return protocol_stop(writer, &error.to_string()).await,
        },
        Some(Ok(Message::Ping(payload))) => {
            if writer.send(Message::Pong(payload)).await.is_err() {
                return RelayControl::Stop;
            }
            return RelayControl::Continue;
        }
        Some(Ok(Message::Pong(_))) => return RelayControl::Continue,
        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
            return RelayControl::Stop;
        }
        Some(Ok(Message::Text(_))) => {
            return protocol_stop(writer, "exec accepts binary messages only").await;
        }
    };
    let result = match frame {
        ExecStreamFrame::Stdin(bytes) => session.send(ExecInput::Stdin(bytes)).await,
        ExecStreamFrame::Resize { columns, rows } => {
            session.send(ExecInput::Resize { columns, rows }).await
        }
        ExecStreamFrame::CloseStdin => session.send(ExecInput::CloseStdin).await,
        ExecStreamFrame::Kill => kill_session(session).await,
        _ => return protocol_stop(writer, "client sent an output exec frame").await,
    };
    match result {
        Ok(()) => RelayControl::Continue,
        Err(error) => protocol_stop(writer, &error.to_string()).await,
    }
}

async fn kill_session(session: &mut dyn ExecSession) -> Result<(), RuntimeError> {
    match session.killer() {
        Some(killer) => killer.kill().await,
        None => Err(RuntimeError::Unsupported {
            capability: runtime::RuntimeCapability::KillExec,
        }),
    }
}

async fn protocol_stop<Writer>(writer: &mut Writer, message: &str) -> RelayControl
where
    Writer: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    let _ = send_split_frame(writer, ExecStreamFrame::Error(message.to_owned())).await;
    RelayControl::Stop
}

async fn send_frame(socket: &mut WebSocket, frame: ExecStreamFrame) -> Result<(), axum::Error> {
    let encoded = frame.encode().map_err(axum::Error::new)?;
    socket.send(Message::Binary(encoded.into())).await
}

async fn send_split_frame<Writer>(
    writer: &mut Writer,
    frame: ExecStreamFrame,
) -> Result<(), axum::Error>
where
    Writer: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    let encoded = frame.encode().map_err(axum::Error::new)?;
    writer.send(Message::Binary(encoded.into())).await
}

fn exec_request(parameters: ExecParameters) -> Result<ExecRequest, ApiError> {
    let command = parameters
        .command
        .as_deref()
        .map(serde_json::from_str::<Vec<String>>)
        .transpose()
        .map_err(|error| ApiError::bad_request(format!("command must be a JSON argv: {error}")))?
        .unwrap_or_else(|| vec!["/bin/sh".to_owned()]);
    validate_command(&command)?;
    let tty = parameters.tty.unwrap_or(true);
    let mode = match (tty, parameters.columns, parameters.rows) {
        (false, None, None) => ExecMode::Pipes,
        (true, None, None) => ExecMode::Terminal {
            columns: 80,
            rows: 24,
        },
        (true, Some(columns), Some(rows)) if columns > 0 && rows > 0 => {
            ExecMode::Terminal { columns, rows }
        }
        (false, _, _) => {
            return Err(ApiError::bad_request(
                "pipe-mode exec cannot specify terminal dimensions",
            ));
        }
        _ => {
            return Err(ApiError::bad_request(
                "terminal columns and rows must be supplied together and be non-zero",
            ));
        }
    };
    let mut command = command.into_iter();
    let executable = command
        .next()
        .ok_or_else(|| ApiError::bad_request("exec command cannot be empty"))?;
    Ok(ExecRequest {
        command: CommandSpec {
            executable,
            arguments: command.collect(),
        },
        environment: BTreeMap::new(),
        mode,
    })
}

fn validate_command(command: &[String]) -> Result<(), ApiError> {
    if command.is_empty()
        || command.len() > MAXIMUM_ARGUMENTS
        || command.iter().map(String::len).sum::<usize>() > MAXIMUM_COMMAND_BYTES
        || command.iter().any(|argument| argument.contains('\0'))
    {
        Err(ApiError::bad_request(
            "command must contain 1 to 256 NUL-free arguments totaling at most 64 KiB",
        ))
    } else {
        Ok(())
    }
}

async fn scoped_assignment(
    state: &AppState,
    service_id: String,
    deployment_id: String,
    assignment_id: String,
) -> Result<Assignment, ApiError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let deployment_id = DeploymentId::new(deployment_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let assignment_id = AssignmentId::new(assignment_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let assignment: Assignment =
        resource::get(state, BuiltinKind::Assignment, assignment_id.clone()).await?;
    if assignment.spec.service_id != service_id || assignment.spec.deployment_id != deployment_id {
        return Err(ApiError::not_found(format!(
            "Assignment `{assignment_id}` does not exist in this deployment"
        )));
    }
    Ok(assignment)
}

enum OpenTarget {
    Node(kernel_api::NodeId),
    Local,
}

enum RelayControl {
    Continue,
    Stop,
}
