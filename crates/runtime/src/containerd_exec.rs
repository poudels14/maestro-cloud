use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use containerd::services::v1::{
    CloseIoRequest, DeleteProcessRequest, ExecProcessRequest, GetContainerRequest, KillRequest,
    ResizePtyRequest, StartRequest, WaitRequest,
};
use containerd::tonic::transport::Channel;
use prost_types::Any;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::containerd_exec_io::{
    exec_io_error, open_anchors, open_async_io, prepare_exec_paths, remove_exec_directory,
};
use crate::containerd_io::path_text;
use crate::containerd_settings::ContainerdRuntimeSettings;
use crate::containerd_support::namespaced;
use crate::{
    ExecInput, ExecMode, ExecOutput, ExecRequest, ExecSession, ExecSessionKiller, RuntimeError,
};

const PROCESS_SPEC_TYPE: &str = "types.containerd.io/opencontainers/runtime-spec/1/Process";
const READ_BYTES: usize = 16 * 1024;

pub(crate) async fn start_exec(
    channel: Channel,
    settings: &ContainerdRuntimeSettings,
    next_exec: Arc<AtomicU64>,
    container_id: String,
    request: ExecRequest,
) -> Result<Box<dyn ExecSession>, RuntimeError> {
    let sequence = next_exec.fetch_add(1, Ordering::Relaxed);
    let exec_id = format!("maestro-exec-{}-{sequence}", std::process::id());
    let process = exec_process_spec(channel.clone(), settings, &container_id, &request).await?;
    let paths = prepare_exec_paths(&settings.state_root, &exec_id, request.mode).await?;
    let anchors = match open_anchors(&paths).await {
        Ok(anchors) => anchors,
        Err(error) => {
            remove_exec_directory(&paths.directory).await;
            return Err(error);
        }
    };
    let create = containerd::services::v1::tasks_client::TasksClient::new(channel.clone())
        .exec(namespaced(
            ExecProcessRequest {
                container_id: container_id.clone(),
                stdin: path_text(&paths.stdin)?,
                stdout: path_text(&paths.stdout)?,
                stderr: paths
                    .stderr
                    .as_deref()
                    .map(path_text)
                    .transpose()?
                    .unwrap_or_default(),
                terminal: matches!(request.mode, ExecMode::Terminal { .. }),
                spec: Some(process),
                exec_id: exec_id.clone(),
            },
            &settings.namespace,
        )?)
        .await;
    if let Err(error) = create {
        remove_exec_directory(&paths.directory).await;
        return Err(exec_error("create", error));
    }
    let opened = open_async_io(&paths).await;
    let (stdin, stdout, stderr) = match opened {
        Ok(opened) => opened,
        Err(error) => {
            cleanup_failed_exec(
                channel,
                &settings.namespace,
                &container_id,
                &exec_id,
                &paths.directory,
            )
            .await;
            return Err(error);
        }
    };
    let start_result = containerd::services::v1::tasks_client::TasksClient::new(channel.clone())
        .start(namespaced(
            StartRequest {
                container_id: container_id.clone(),
                exec_id: exec_id.clone(),
            },
            &settings.namespace,
        )?)
        .await;
    if let Err(error) = start_result {
        cleanup_failed_exec(
            channel,
            &settings.namespace,
            &container_id,
            &exec_id,
            &paths.directory,
        )
        .await;
        return Err(exec_error("start", error));
    }
    if let ExecMode::Terminal { columns, rows } = request.mode {
        let resize = containerd::services::v1::tasks_client::TasksClient::new(channel.clone())
            .resize_pty(namespaced(
                ResizePtyRequest {
                    container_id: container_id.clone(),
                    exec_id: exec_id.clone(),
                    width: u32::from(columns),
                    height: u32::from(rows),
                },
                &settings.namespace,
            )?)
            .await;
        if let Err(error) = resize {
            cleanup_failed_exec(
                channel,
                &settings.namespace,
                &container_id,
                &exec_id,
                &paths.directory,
            )
            .await;
            return Err(exec_error("resize", error));
        }
    }
    drop(anchors);
    Ok(Box::new(ContainerdExecSession {
        channel,
        namespace: settings.namespace.clone(),
        container_id,
        exec_id,
        directory: paths.directory,
        mode: request.mode,
        stdin: Some(stdin),
        stdout,
        stderr,
        stdout_open: true,
        stderr_open: paths.stderr.is_some(),
        exit_emitted: false,
    }))
}

struct ContainerdExecSession {
    channel: Channel,
    namespace: String,
    container_id: String,
    exec_id: String,
    directory: PathBuf,
    mode: ExecMode,
    stdin: Option<tokio::fs::File>,
    stdout: tokio::fs::File,
    stderr: Option<tokio::fs::File>,
    stdout_open: bool,
    stderr_open: bool,
    exit_emitted: bool,
}

#[async_trait]
impl ExecSession for ContainerdExecSession {
    async fn send(&mut self, input: ExecInput) -> Result<(), RuntimeError> {
        match input {
            ExecInput::Stdin(bytes) => self
                .stdin
                .as_mut()
                .ok_or_else(|| RuntimeError::Rejected {
                    message: "containerd exec standard input is closed".to_owned(),
                })?
                .write_all(&bytes)
                .await
                .map_err(exec_io_error),
            ExecInput::Resize { columns, rows }
                if matches!(self.mode, ExecMode::Terminal { .. }) =>
            {
                containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
                    .resize_pty(namespaced(
                        ResizePtyRequest {
                            container_id: self.container_id.clone(),
                            exec_id: self.exec_id.clone(),
                            width: u32::from(columns),
                            height: u32::from(rows),
                        },
                        &self.namespace,
                    )?)
                    .await
                    .map_err(|error| exec_error("resize", error))?;
                Ok(())
            }
            ExecInput::Resize { .. } => Err(RuntimeError::Rejected {
                message: "cannot resize a pipe-mode containerd exec session".to_owned(),
            }),
            ExecInput::CloseStdin => {
                if let Some(mut stdin) = self.stdin.take() {
                    stdin.shutdown().await.map_err(exec_io_error)?;
                    containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
                        .close_io(namespaced(
                            CloseIoRequest {
                                container_id: self.container_id.clone(),
                                exec_id: self.exec_id.clone(),
                                stdin: true,
                            },
                            &self.namespace,
                        )?)
                        .await
                        .map_err(|error| exec_error("close input", error))?;
                }
                Ok(())
            }
        }
    }

    async fn next(&mut self) -> Result<Option<ExecOutput>, RuntimeError> {
        loop {
            if let Some(output) = self.read_output().await? {
                return Ok(Some(output));
            }
            if self.stdout_open || self.stderr_open {
                continue;
            }
            if self.exit_emitted {
                return Ok(None);
            }
            let response =
                containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
                    .wait(namespaced(
                        WaitRequest {
                            container_id: self.container_id.clone(),
                            exec_id: self.exec_id.clone(),
                        },
                        &self.namespace,
                    )?)
                    .await
                    .map_err(|error| exec_error("wait", error))?
                    .into_inner();
            self.cleanup().await?;
            self.exit_emitted = true;
            return Ok(Some(ExecOutput::Exited {
                code: i32::try_from(response.exit_status).ok(),
            }));
        }
    }

    fn killer(&mut self) -> Option<&mut dyn ExecSessionKiller> {
        Some(self)
    }
}

#[async_trait]
impl ExecSessionKiller for ContainerdExecSession {
    async fn kill(&mut self) -> Result<(), RuntimeError> {
        if self.exit_emitted {
            return Ok(());
        }
        let result = containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
            .kill(namespaced(
                KillRequest {
                    container_id: self.container_id.clone(),
                    exec_id: self.exec_id.clone(),
                    signal: 9,
                    all: false,
                },
                &self.namespace,
            )?)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if error.code() == containerd::tonic::Code::NotFound => Ok(()),
            Err(error) => Err(exec_error("kill", error)),
        }
    }
}

impl ContainerdExecSession {
    async fn read_output(&mut self) -> Result<Option<ExecOutput>, RuntimeError> {
        let mut stdout = vec![0_u8; READ_BYTES];
        let mut stderr = vec![0_u8; READ_BYTES];
        let read = if self.stdout_open && self.stderr_open {
            let stderr_file = self.stderr.as_mut().ok_or_else(|| RuntimeError::Stream {
                message: "containerd exec stderr state is inconsistent".to_owned(),
            })?;
            tokio::select! {
                result = self.stdout.read(&mut stdout) => ReadOutput::Stdout(result),
                result = stderr_file.read(&mut stderr) => ReadOutput::Stderr(result),
            }
        } else if self.stdout_open {
            ReadOutput::Stdout(self.stdout.read(&mut stdout).await)
        } else if self.stderr_open {
            let stderr_file = self.stderr.as_mut().ok_or_else(|| RuntimeError::Stream {
                message: "containerd exec stderr state is inconsistent".to_owned(),
            })?;
            ReadOutput::Stderr(stderr_file.read(&mut stderr).await)
        } else {
            return Ok(None);
        };
        match read {
            ReadOutput::Stdout(Ok(0)) => {
                self.stdout_open = false;
                Ok(None)
            }
            ReadOutput::Stderr(Ok(0)) => {
                self.stderr_open = false;
                Ok(None)
            }
            ReadOutput::Stdout(Ok(count)) => {
                stdout.truncate(count);
                Ok(Some(ExecOutput::Stdout(stdout)))
            }
            ReadOutput::Stderr(Ok(count)) => {
                stderr.truncate(count);
                Ok(Some(ExecOutput::Stderr(stderr)))
            }
            ReadOutput::Stdout(Err(error)) | ReadOutput::Stderr(Err(error)) => {
                Err(exec_io_error(error))
            }
        }
    }

    async fn cleanup(&self) -> Result<(), RuntimeError> {
        let result = containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
            .delete_process(namespaced(
                DeleteProcessRequest {
                    container_id: self.container_id.clone(),
                    exec_id: self.exec_id.clone(),
                },
                &self.namespace,
            )?)
            .await;
        if let Err(error) = result
            && error.code() != containerd::tonic::Code::NotFound
        {
            return Err(exec_error("delete", error));
        }
        match tokio::fs::remove_dir_all(&self.directory).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(exec_io_error(error)),
        }
    }
}

enum ReadOutput {
    Stdout(Result<usize, std::io::Error>),
    Stderr(Result<usize, std::io::Error>),
}

async fn exec_process_spec(
    channel: Channel,
    settings: &ContainerdRuntimeSettings,
    container_id: &str,
    request: &ExecRequest,
) -> Result<Any, RuntimeError> {
    let container = containerd::services::v1::containers_client::ContainersClient::new(channel)
        .get(namespaced(
            GetContainerRequest {
                id: container_id.to_owned(),
            },
            &settings.namespace,
        )?)
        .await
        .map_err(|error| exec_error("inspect container", error))?
        .into_inner()
        .container
        .ok_or_else(|| RuntimeError::Unavailable {
            message: "containerd exec lookup omitted container metadata".to_owned(),
        })?;
    let spec = container.spec.ok_or_else(|| RuntimeError::Rejected {
        message: format!("containerd container `{container_id}` has no OCI specification"),
    })?;
    let complete: Value =
        serde_json::from_slice(&spec.value).map_err(|error| RuntimeError::Rejected {
            message: format!("containerd OCI specification is invalid: {error}"),
        })?;
    let mut process = complete
        .get("process")
        .cloned()
        .and_then(|process| process.as_object().cloned())
        .ok_or_else(|| RuntimeError::Rejected {
            message: "containerd OCI specification omitted its process".to_owned(),
        })?;
    let mut command = Vec::with_capacity(request.command.arguments.len() + 1);
    command.push(request.command.executable.clone());
    command.extend(request.command.arguments.clone());
    process.insert("args".to_owned(), json!(command));
    process.insert(
        "terminal".to_owned(),
        json!(matches!(request.mode, ExecMode::Terminal { .. })),
    );
    let mut environment = process
        .get("env")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    environment.extend(
        request
            .environment
            .iter()
            .map(|(name, value)| json!(format!("{name}={value}"))),
    );
    process.insert("env".to_owned(), Value::Array(environment));
    Ok(Any {
        type_url: PROCESS_SPEC_TYPE.to_owned(),
        value: serde_json::to_vec(&process).map_err(|error| RuntimeError::Rejected {
            message: format!("failed to encode containerd exec process: {error}"),
        })?,
    })
}

async fn cleanup_failed_exec(
    channel: Channel,
    namespace: &str,
    container_id: &str,
    exec_id: &str,
    directory: &Path,
) {
    let mut tasks = containerd::services::v1::tasks_client::TasksClient::new(channel);
    if let Ok(request) = namespaced(
        KillRequest {
            container_id: container_id.to_owned(),
            exec_id: exec_id.to_owned(),
            signal: 9,
            all: false,
        },
        namespace,
    ) {
        let _result = tasks.kill(request).await;
    }
    if let Ok(request) = namespaced(
        DeleteProcessRequest {
            container_id: container_id.to_owned(),
            exec_id: exec_id.to_owned(),
        },
        namespace,
    ) {
        let _result = tasks.delete_process(request).await;
    }
    remove_exec_directory(directory).await;
}

fn exec_error(operation: &str, error: containerd::tonic::Status) -> RuntimeError {
    RuntimeError::Stream {
        message: format!("containerd exec {operation} failed: {error}"),
    }
}
