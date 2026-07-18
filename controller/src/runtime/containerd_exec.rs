use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use containerd_client::services::v1::{
    DeleteProcessRequest, ExecProcessRequest, KillRequest, ListContainersRequest,
    ListNamespacesRequest, ResizePtyRequest, StartRequest, WaitRequest,
};
use containerd_client::tonic::{Request, transport::Channel};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{ExecControl, ExecSession, InteractiveExecRequest};

const PROCESS_SPEC_TYPE: &str = "types.containerd.io/opencontainers/runtime-spec/1/Process";

pub(super) async fn interactive_exec(request: InteractiveExecRequest) -> Result<ExecSession> {
    if request.command.is_empty() {
        bail!("interactive exec command cannot be empty");
    }
    let socket = containerd_socket()?;
    let client = containerd_client::Client::from_path(&socket)
        .await
        .with_context(|| format!("failed to connect to containerd at {}", socket.display()))?;
    let (namespace, container) = find_container(&client, &request.container).await?;
    let container_id = container.id.clone();
    let mut process = container
        .spec
        .ok_or_else(|| anyhow!("container `{}` has no OCI spec", request.container))?;
    let mut spec: serde_json::Value = serde_json::from_slice(&process.value)
        .context("failed to decode the container OCI spec")?;
    let process_spec = spec
        .get_mut("process")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| anyhow!("container `{}` has no OCI process spec", request.container))?;
    process_spec.insert("args".to_string(), serde_json::to_value(&request.command)?);
    process_spec.insert("terminal".to_string(), serde_json::Value::Bool(request.tty));
    process.type_url = PROCESS_SPEC_TYPE.to_string();
    process.value = serde_json::to_vec(process_spec)?;

    let exec_id = format!("maestro-ssh-{}", crate::utils::nanoid::unique_id(16));
    let session_dir = request.session_root.join(&exec_id);
    tokio::fs::create_dir_all(&session_dir).await?;
    protect_session_dir(&session_dir)?;
    let stdin_path = session_dir.join("stdin");
    let stdout_path = session_dir.join("stdout");
    let stderr_path = (!request.tty).then(|| session_dir.join("stderr"));
    create_fifo(&stdin_path)?;
    create_fifo(&stdout_path)?;
    if let Some(stderr_path) = &stderr_path {
        create_fifo(stderr_path)?;
    }

    let result = create_process(
        &client,
        &namespace,
        &container_id,
        &exec_id,
        process,
        request.tty,
        request.initial_size,
        &stdin_path,
        &stdout_path,
        stderr_path.as_deref(),
    )
    .await;
    if result.is_err() {
        let mut tasks = client.tasks();
        let _ = tasks
            .delete_process(namespaced(
                DeleteProcessRequest {
                    container_id: container_id.clone(),
                    exec_id: exec_id.clone(),
                },
                &namespace,
            ))
            .await;
        let _ = tokio::fs::remove_dir_all(&session_dir).await;
    }
    result.map(|(stdin, output)| {
        let control = Arc::new(ContainerdExecControl {
            channel: client.channel(),
            namespace,
            container_id,
            exec_id,
            session_dir,
            cleaned: AtomicBool::new(false),
        });
        ExecSession::new(stdin, output, control)
    })
}

#[allow(clippy::too_many_arguments)]
async fn create_process(
    client: &containerd_client::Client,
    namespace: &str,
    container_id: &str,
    exec_id: &str,
    spec: prost_types::Any,
    tty: bool,
    initial_size: Option<crate::exec::TerminalSize>,
    stdin_path: &Path,
    stdout_path: &Path,
    stderr_path: Option<&Path>,
) -> Result<(tokio::fs::File, tokio::io::DuplexStream)> {
    let anchors = open_fifo_anchors(stdin_path, stdout_path, stderr_path)?;
    let mut tasks = client.tasks();
    tasks
        .exec(namespaced(
            ExecProcessRequest {
                container_id: container_id.to_string(),
                stdin: path_string(stdin_path)?,
                stdout: path_string(stdout_path)?,
                stderr: stderr_path
                    .map(path_string)
                    .transpose()?
                    .unwrap_or_default(),
                terminal: tty,
                spec: Some(spec),
                exec_id: exec_id.to_string(),
            },
            namespace,
        ))
        .await
        .context("containerd rejected the exec process")?;

    let stdin = tokio::fs::OpenOptions::new()
        .write(true)
        .open(stdin_path)
        .await?;
    let stdout = tokio::fs::OpenOptions::new()
        .read(true)
        .open(stdout_path)
        .await?;
    let stderr = match stderr_path {
        Some(path) => Some(tokio::fs::OpenOptions::new().read(true).open(path).await?),
        None => None,
    };
    tasks
        .start(namespaced(
            StartRequest {
                container_id: container_id.to_string(),
                exec_id: exec_id.to_string(),
            },
            namespace,
        ))
        .await
        .context("containerd failed to start the exec process")?;
    if let Some(size) = initial_size
        && tty
    {
        tasks
            .resize_pty(namespaced(
                ResizePtyRequest {
                    container_id: container_id.to_string(),
                    exec_id: exec_id.to_string(),
                    width: u32::from(size.cols),
                    height: u32::from(size.rows),
                },
                namespace,
            ))
            .await?;
    }
    drop(anchors);

    let (output_writer, output_reader) = tokio::io::duplex(64 * 1024);
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(32);
    tokio::spawn(pump_fifo(stdout, sender.clone()));
    if let Some(stderr) = stderr {
        tokio::spawn(pump_fifo(stderr, sender.clone()));
    }
    drop(sender);
    tokio::spawn(async move {
        let mut output_writer = output_writer;
        while let Some(bytes) = receiver.recv().await {
            if output_writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = output_writer.shutdown().await;
    });
    Ok((stdin, output_reader))
}

async fn find_container(
    client: &containerd_client::Client,
    name: &str,
) -> Result<(String, containerd_client::services::v1::Container)> {
    let mut namespaces = client
        .namespaces()
        .list(ListNamespacesRequest::default())
        .await?
        .into_inner()
        .namespaces;
    let preferred_namespace = std::env::var("NERDCTL_NAMESPACE")
        .or_else(|_| std::env::var("CONTAINERD_NAMESPACE"))
        .unwrap_or_else(|_| "default".to_string());
    namespaces.sort_by_key(|namespace| namespace.name != preferred_namespace);
    for namespace in namespaces {
        let response = client
            .containers()
            .list(namespaced(
                ListContainersRequest::default(),
                &namespace.name,
            ))
            .await?;
        if let Some(container) = response
            .into_inner()
            .containers
            .into_iter()
            .find(|container| {
                container.id == name
                    || container
                        .labels
                        .get("nerdctl/name")
                        .is_some_and(|container_name| container_name == name)
            })
        {
            return Ok((namespace.name, container));
        }
    }
    bail!("nerdctl container `{name}` was not found in containerd")
}

fn namespaced<T>(message: T, namespace: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "containerd-namespace",
        namespace
            .parse()
            .expect("containerd namespace is metadata-safe"),
    );
    request
}

async fn pump_fifo(mut fifo: tokio::fs::File, sender: tokio::sync::mpsc::Sender<Vec<u8>>) {
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        match fifo.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(count) => {
                if sender.send(buffer[..count].to_vec()).await.is_err() {
                    break;
                }
            }
        }
    }
}

struct ContainerdExecControl {
    channel: Channel,
    namespace: String,
    container_id: String,
    exec_id: String,
    session_dir: PathBuf,
    cleaned: AtomicBool,
}

#[async_trait]
impl ExecControl for ContainerdExecControl {
    async fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        let mut tasks =
            containerd_client::services::v1::tasks_client::TasksClient::new(self.channel.clone());
        tasks
            .resize_pty(namespaced(
                ResizePtyRequest {
                    container_id: self.container_id.clone(),
                    exec_id: self.exec_id.clone(),
                    width: u32::from(cols),
                    height: u32::from(rows),
                },
                &self.namespace,
            ))
            .await?;
        Ok(())
    }

    async fn wait(&self) -> Result<i32> {
        let mut tasks =
            containerd_client::services::v1::tasks_client::TasksClient::new(self.channel.clone());
        let response = tasks
            .wait(namespaced(
                WaitRequest {
                    container_id: self.container_id.clone(),
                    exec_id: self.exec_id.clone(),
                },
                &self.namespace,
            ))
            .await?
            .into_inner();
        self.cleanup().await;
        Ok(i32::try_from(response.exit_status).unwrap_or(i32::MAX))
    }

    async fn kill(&self) -> Result<()> {
        let mut tasks =
            containerd_client::services::v1::tasks_client::TasksClient::new(self.channel.clone());
        let _ = tasks
            .kill(namespaced(
                KillRequest {
                    container_id: self.container_id.clone(),
                    exec_id: self.exec_id.clone(),
                    signal: 9,
                    all: false,
                },
                &self.namespace,
            ))
            .await;
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tasks.wait(namespaced(
                WaitRequest {
                    container_id: self.container_id.clone(),
                    exec_id: self.exec_id.clone(),
                },
                &self.namespace,
            )),
        )
        .await;
        self.cleanup().await;
        Ok(())
    }
}

impl ContainerdExecControl {
    async fn cleanup(&self) {
        if self.cleaned.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut tasks =
            containerd_client::services::v1::tasks_client::TasksClient::new(self.channel.clone());
        let _ = tasks
            .delete_process(namespaced(
                DeleteProcessRequest {
                    container_id: self.container_id.clone(),
                    exec_id: self.exec_id.clone(),
                },
                &self.namespace,
            ))
            .await;
        let _ = tokio::fs::remove_dir_all(&self.session_dir).await;
    }
}

impl Drop for ContainerdExecControl {
    fn drop(&mut self) {
        if self.cleaned.swap(true, Ordering::AcqRel) {
            return;
        }
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let container_id = self.container_id.clone();
        let exec_id = self.exec_id.clone();
        let session_dir = self.session_dir.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut tasks =
                    containerd_client::services::v1::tasks_client::TasksClient::new(channel);
                let _ = tasks
                    .kill(namespaced(
                        KillRequest {
                            container_id: container_id.clone(),
                            exec_id: exec_id.clone(),
                            signal: 9,
                            all: false,
                        },
                        &namespace,
                    ))
                    .await;
                let _ = tasks
                    .delete_process(namespaced(
                        DeleteProcessRequest {
                            container_id,
                            exec_id,
                        },
                        &namespace,
                    ))
                    .await;
                let _ = tokio::fs::remove_dir_all(session_dir).await;
            });
        }
    }
}

fn containerd_socket() -> Result<PathBuf> {
    let configured = std::env::var_os("CONTAINERD_ADDRESS").map(PathBuf::from);
    let mut candidates = configured.into_iter().collect::<Vec<_>>();
    candidates.extend([
        PathBuf::from("/run/containerd/containerd.sock"),
        PathBuf::from("/var/run/containerd/containerd.sock"),
    ]);
    if let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        candidates.push(PathBuf::from(runtime_dir).join("containerd/containerd.sock"));
    }
    candidates
        .into_iter()
        .find(|path| path.exists())
        .ok_or_else(|| anyhow!("containerd socket was not found; set CONTAINERD_ADDRESS"))
}

fn create_fifo(path: &Path) -> Result<()> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    let status = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
    if status == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn open_fifo_anchors(
    stdin: &Path,
    stdout: &Path,
    stderr: Option<&Path>,
) -> Result<Vec<std::fs::File>> {
    [Some(stdin), Some(stdout), stderr]
        .into_iter()
        .flatten()
        .map(|path| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .with_context(|| format!("failed to open exec FIFO {}", path.display()))
        })
        .collect()
}

fn protect_session_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn path_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("exec FIFO path is not UTF-8: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    #[ignore = "requires a running nerdctl/containerd runtime and the alpine:3.21 image"]
    async fn executes_with_native_containerd_io() {
        let container = format!("maestro-exec-test-{}", crate::utils::nanoid::unique_id(10));
        let started = tokio::process::Command::new("nerdctl")
            .args([
                "run",
                "-d",
                "--name",
                &container,
                "alpine:3.21",
                "sleep",
                "120",
            ])
            .output()
            .await
            .expect("start integration container");
        assert!(
            started.status.success(),
            "failed to start integration container: {}",
            String::from_utf8_lossy(&started.stderr)
        );

        let result = async {
            let session_root = std::env::temp_dir().join(format!(
                "maestro-containerd-exec-test-{}",
                crate::utils::nanoid::unique_id(10)
            ));
            let session = interactive_exec(InteractiveExecRequest {
                container: container.clone(),
                command: vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "read value; printf 'reply:%s' \"$value\"".to_string(),
                ],
                tty: false,
                initial_size: None,
                session_root: session_root.clone(),
            })
            .await?;
            let (controller_stream, mut probe_stream) = tokio::net::UnixStream::pair()?;
            let pump = tokio::spawn(crate::cluster::control::pump_exec_session(
                controller_stream,
                session,
            ));
            crate::exec::write_length_prefixed(
                &mut probe_stream,
                &crate::exec::ExecFrame::Stdin(b"hello\n".to_vec()),
            )
            .await?;
            crate::exec::write_length_prefixed(
                &mut probe_stream,
                &crate::exec::ExecFrame::Stdin(Vec::new()),
            )
            .await?;
            let mut output = Vec::new();
            let exit_code = loop {
                match crate::exec::read_length_prefixed(&mut probe_stream).await? {
                    Some(crate::exec::ExecFrame::Output(bytes)) => output.extend(bytes),
                    Some(crate::exec::ExecFrame::Exit(code)) => break code,
                    Some(crate::exec::ExecFrame::Ping) => {}
                    Some(frame) => anyhow::bail!("unexpected control frame {frame:?}"),
                    None => anyhow::bail!("control stream closed before exit"),
                }
            };
            let pumped_exit = pump.await??;
            let mut session_entries = tokio::fs::read_dir(&session_root).await?;
            anyhow::ensure!(
                session_entries.next_entry().await?.is_none(),
                "non-TTY exec session files were not cleaned up"
            );
            let _ = tokio::fs::remove_dir_all(session_root).await;
            anyhow::ensure!(exit_code == 0, "unexpected exit code {exit_code}");
            anyhow::ensure!(
                pumped_exit == Some(0),
                "unexpected pump result {pumped_exit:?}"
            );
            anyhow::ensure!(output == b"reply:hello", "unexpected output {output:?}");

            let tty_root = std::env::temp_dir().join(format!(
                "maestro-containerd-tty-test-{}",
                crate::utils::nanoid::unique_id(10)
            ));
            let mut tty_session = interactive_exec(InteractiveExecRequest {
                container: container.clone(),
                command: vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "sleep 1; stty size".to_string(),
                ],
                tty: true,
                initial_size: Some(crate::exec::TerminalSize { cols: 80, rows: 24 }),
                session_root: tty_root.clone(),
            })
            .await?;
            tty_session.control().resize(100, 40).await?;
            let mut tty_output = Vec::new();
            tty_session.output.read_to_end(&mut tty_output).await?;
            let tty_exit = tty_session.control().wait().await?;
            let mut tty_entries = tokio::fs::read_dir(&tty_root).await?;
            anyhow::ensure!(
                tty_entries.next_entry().await?.is_none(),
                "TTY exec session files were not cleaned up"
            );
            let _ = tokio::fs::remove_dir_all(tty_root).await;
            anyhow::ensure!(tty_exit == 0, "unexpected TTY exit code {tty_exit}");
            anyhow::ensure!(
                String::from_utf8_lossy(&tty_output).contains("40 100"),
                "unexpected TTY output {tty_output:?}"
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;

        let removed = tokio::process::Command::new("nerdctl")
            .args(["rm", "-f", &container])
            .output()
            .await
            .expect("remove integration container");
        assert!(
            removed.status.success(),
            "failed to remove integration container: {}",
            String::from_utf8_lossy(&removed.stderr)
        );
        result.expect("native containerd exec integration");
    }
}
