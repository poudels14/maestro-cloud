use std::collections::VecDeque;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex};

use etcd_client::{Certificate, Client, ConnectOptions, Identity, TlsOptions};
use kernel_store::{Clock, EtcdStore, EtcdTlsConfig, MonotonicTime, Store, derive_key};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::{
    EmbeddedEtcdSettings, StoreProviderConfig, StoreProviderError, StoreRuntime, StoreShutdown,
    embedded_etcd_files::create_private_directory,
    embedded_etcd_plan::{EtcdReadiness, EtcdStartPlan},
};

pub(crate) struct EtcdProcess {
    child: Child,
    log_tasks: JoinSet<()>,
    recent_output: Arc<StdMutex<VecDeque<String>>>,
}

pub(crate) struct RunningEtcd {
    process: Mutex<EtcdProcess>,
    store: Arc<EtcdStore>,
    clock: Arc<dyn Clock>,
}

impl RunningEtcd {
    pub fn new(process: EtcdProcess, store: EtcdStore, clock: Arc<dyn Clock>) -> Self {
        Self {
            process: Mutex::new(process),
            store: Arc::new(store),
            clock,
        }
    }
}

#[async_trait::async_trait]
impl StoreRuntime for RunningEtcd {
    fn store(&self) -> Arc<dyn Store> {
        self.store.clone()
    }

    async fn shutdown(self: Box<Self>, request: StoreShutdown) -> Result<(), StoreProviderError> {
        self.process
            .lock()
            .await
            .shutdown(request, self.clock.as_ref())
            .await
    }
}

impl EtcdProcess {
    pub fn spawn(
        binary: &Path,
        plan: &EtcdStartPlan,
        node_id: &kernel_api::NodeId,
    ) -> Result<Self, StoreProviderError> {
        create_private_directory(&plan.data_directory, "store data")?;
        let mut command = Command::new(binary);
        command
            .args(&plan.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| StoreProviderError::Lifecycle {
                reason: format!("failed to start `{}`: {error}", binary.display()),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| StoreProviderError::Lifecycle {
                reason: "store backend omitted its stdout pipe".to_owned(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| StoreProviderError::Lifecycle {
                reason: "store backend omitted its stderr pipe".to_owned(),
            })?;
        let mut log_tasks = JoinSet::new();
        let recent_output = Arc::new(StdMutex::new(VecDeque::with_capacity(50)));
        spawn_log_reader(
            &mut log_tasks,
            stdout,
            node_id.clone(),
            "stdout",
            recent_output.clone(),
        );
        spawn_log_reader(
            &mut log_tasks,
            stderr,
            node_id.clone(),
            "stderr",
            recent_output.clone(),
        );
        Ok(Self {
            child,
            log_tasks,
            recent_output,
        })
    }

    pub async fn wait_until_ready(
        &mut self,
        config: &StoreProviderConfig,
        plan: &EtcdStartPlan,
        settings: EmbeddedEtcdSettings,
        clock: &dyn Clock,
    ) -> Result<(), StoreProviderError> {
        let deadline = clock.now().saturating_add(settings.startup_timeout());
        let mut delay = settings.initial_retry_delay();
        let mut last_error = "readiness was not observed".to_owned();
        loop {
            if clock.now() >= deadline {
                return Err(StoreProviderError::Unavailable {
                    reason: format!("startup deadline elapsed: {last_error}"),
                });
            }
            if let Some(status) =
                self.child
                    .try_wait()
                    .map_err(|error| StoreProviderError::Lifecycle {
                        reason: format!("failed to inspect store backend process: {error}"),
                    })?
            {
                self.drain_log_tasks().await;
                return Err(StoreProviderError::Lifecycle {
                    reason: format!(
                        "store backend exited before readiness with {status}; recent output: {}",
                        self.recent_output()
                    ),
                });
            }

            let probe_deadline = std::cmp::min(
                deadline,
                clock.now().saturating_add(settings.operation_timeout()),
            );
            match probe_readiness_before(
                config,
                &plan.local_client_url,
                plan.readiness,
                settings.operation_timeout(),
                probe_deadline,
                clock,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = error;
                    if clock.now() >= deadline {
                        return Err(StoreProviderError::Unavailable {
                            reason: format!("startup deadline elapsed: {last_error}"),
                        });
                    }
                    let wake = std::cmp::min(deadline, clock.now().saturating_add(delay));
                    clock.sleep_until(wake).await;
                    delay = delay.saturating_mul(2).min(settings.maximum_retry_delay());
                }
            }
        }
    }

    pub(crate) async fn shutdown(
        &mut self,
        request: StoreShutdown,
        clock: &dyn Clock,
    ) -> Result<(), StoreProviderError> {
        match request {
            StoreShutdown::Immediate => {
                self.child
                    .start_kill()
                    .map_err(|error| StoreProviderError::Lifecycle {
                        reason: format!("failed to kill store backend: {error}"),
                    })?;
                wait_for_exit(&mut self.child).await?;
            }
            StoreShutdown::Graceful { deadline } => {
                terminate(&mut self.child)?;
                tokio::select! {
                    result = self.child.wait() => {
                        result.map_err(|error| StoreProviderError::Lifecycle {
                            reason: format!("failed to wait for store backend: {error}"),
                        })?;
                    }
                    () = clock.sleep_until(deadline) => {
                        self.child.start_kill().map_err(|error| StoreProviderError::Lifecycle {
                            reason: format!("failed to kill store backend at shutdown deadline: {error}"),
                        })?;
                        wait_for_exit(&mut self.child).await?;
                    }
                }
            }
        }
        self.drain_log_tasks().await;
        Ok(())
    }

    async fn drain_log_tasks(&mut self) {
        while let Some(result) = self.log_tasks.join_next().await {
            if let Err(error) = result {
                tracing::warn!(error = %error, "store backend log task failed");
            }
        }
    }

    fn recent_output(&self) -> String {
        self.recent_output
            .lock()
            .map(|output| output.iter().cloned().collect::<Vec<_>>().join(" | "))
            .unwrap_or_else(|_| "[unavailable]".to_owned())
    }
}

pub(crate) async fn connect_store(
    config: &StoreProviderConfig,
    local_client_url: &str,
) -> Result<EtcdStore, StoreProviderError> {
    let tls = EtcdTlsConfig::new(
        config.local_member().host_address.to_string(),
        config.security().trust_root_pem.as_bytes().to_vec(),
        config
            .security()
            .identity
            .certificate_pem
            .as_bytes()
            .to_vec(),
        config
            .security()
            .identity
            .private_key_pem
            .expose()
            .as_bytes()
            .to_vec(),
    );
    let encryption_key =
        derive_key(config.store_encryption_secret().expose()).map_err(|error| {
            StoreProviderError::InvalidConfiguration {
                reason: error.to_string(),
            }
        })?;
    EtcdStore::connect_with_tls_and_encryption([local_client_url], tls, encryption_key)
        .await
        .map_err(Into::into)
}

pub(crate) fn client_tls(config: &StoreProviderConfig) -> TlsOptions {
    TlsOptions::new()
        .ca_certificate(Certificate::from_pem(
            config.security().trust_root_pem.as_bytes(),
        ))
        .identity(Identity::from_pem(
            config.security().identity.certificate_pem.as_bytes(),
            config
                .security()
                .identity
                .private_key_pem
                .expose()
                .as_bytes(),
        ))
}

pub(crate) fn connect_options(
    config: &StoreProviderConfig,
    operation_timeout: std::time::Duration,
) -> ConnectOptions {
    ConnectOptions::new()
        .with_tls(client_tls(config))
        .with_connect_timeout(operation_timeout)
        .with_timeout(operation_timeout)
}

async fn probe_readiness(
    config: &StoreProviderConfig,
    endpoint: &str,
    readiness: EtcdReadiness,
    operation_timeout: std::time::Duration,
) -> Result<(), String> {
    let options = connect_options(config, operation_timeout);
    let mut client = Client::connect([endpoint], Some(options))
        .await
        .map_err(|error| format!("connection failed: {error}"))?;
    let status = client
        .status()
        .await
        .map_err(|error| format!("status failed: {error}"))?;
    if status.leader() == 0 {
        return Err("no leader has been elected".to_owned());
    }
    if !status.errors().is_empty() {
        return Err(format!("backend reported: {}", status.errors().join(", ")));
    }
    if let EtcdReadiness::JoinedLearner { member_id } = readiness {
        let observed_member_id = status
            .header()
            .ok_or_else(|| "status response omitted its member identity".to_owned())?
            .member_id();
        return if observed_member_id == member_id {
            Ok(())
        } else {
            Err(format!(
                "joined member identity {observed_member_id} differs from ticket identity {member_id}"
            ))
        };
    }
    client
        .get(
            "/maestro/system/readiness",
            Some(etcd_client::GetOptions::new().with_limit(1)),
        )
        .await
        .map_err(|error| format!("linearizable read failed: {error}"))?;
    Ok(())
}

pub(crate) async fn probe_readiness_before(
    config: &StoreProviderConfig,
    endpoint: &str,
    readiness: EtcdReadiness,
    operation_timeout: std::time::Duration,
    deadline: MonotonicTime,
    clock: &dyn Clock,
) -> Result<(), String> {
    tokio::select! {
        biased;
        result = probe_readiness(config, endpoint, readiness, operation_timeout) => result,
        () = clock.sleep_until(deadline) => {
            Err("readiness probe deadline elapsed".to_owned())
        }
    }
}

fn spawn_log_reader<Reader>(
    tasks: &mut JoinSet<()>,
    reader: Reader,
    node_id: kernel_api::NodeId,
    stream: &'static str,
    recent_output: Arc<StdMutex<VecDeque<String>>>,
) where
    Reader: AsyncRead + Send + Unpin + 'static,
{
    tasks.spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let line = line.chars().take(2_048).collect::<String>();
                    if let Ok(mut output) = recent_output.lock() {
                        if output.len() == 50 {
                            output.pop_front();
                        }
                        output.push_back(format!("{stream}: {line}"));
                    }
                    tracing::info!(node_id = %node_id, store_stream = stream, message = %line, "store backend output");
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(node_id = %node_id, store_stream = stream, error = %error, "failed to read store backend output");
                    break;
                }
            }
        }
    });
}

async fn wait_for_exit(child: &mut Child) -> Result<(), StoreProviderError> {
    child
        .wait()
        .await
        .map(|_| ())
        .map_err(|error| StoreProviderError::Lifecycle {
            reason: format!("failed to wait for store backend: {error}"),
        })
}

#[cfg(unix)]
fn terminate(child: &mut Child) -> Result<(), StoreProviderError> {
    let Some(process_id) = child.id() else {
        return Ok(());
    };
    let process_id = i32::try_from(process_id).map_err(|_| StoreProviderError::Lifecycle {
        reason: "store backend process ID exceeds the platform range".to_owned(),
    })?;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(process_id),
        nix::sys::signal::Signal::SIGTERM,
    )
    .map_err(|error| StoreProviderError::Lifecycle {
        reason: format!("failed to terminate store backend: {error}"),
    })
}

#[cfg(not(unix))]
fn terminate(child: &mut Child) -> Result<(), StoreProviderError> {
    child
        .start_kill()
        .map_err(|error| StoreProviderError::Lifecycle {
            reason: format!("failed to stop store backend: {error}"),
        })
}
