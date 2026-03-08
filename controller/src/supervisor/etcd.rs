use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use etcd_client::{
    Client as EtcdClient, Compare, CompareOp, GetOptions, SortOrder, SortTarget, Txn, TxnOp,
};
use nanoid::nanoid;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

use crate::service::{
    ActiveDeployment, DeploymentStatus, SERVICES_ROOT, ServiceDeployment, ServiceInfo,
    service_active_deployment_key, service_deployment_history_key, service_info_key,
};

use super::{
    model::ServiceRuntimeConfig,
    worker::{WorkerExitStatus, run_managed_service},
};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_RESTART_DELAY_MS: u64 = 1_000;
const DEFAULT_MAX_RESTARTS: u32 = 5;
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 5_000;
const MAX_STATUS_TXN_RETRIES: usize = 8;
const SERVICE_HISTORY_NEXT_INDEX_SUFFIX: &str = "/deployments/history-next-index";
const URL_SAFE_ALPHABET: [char; 62] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B',
    'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
    'V', 'W', 'X', 'Y', 'Z',
];

#[derive(Clone)]
struct EtcdV3HttpClient {
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
}

struct RunningService {
    deployment_id: String,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<WorkerExitStatus>,
}

struct QueuedDeployment {
    service_id: String,
    key: String,
    mod_revision: u64,
    deployment: ServiceDeployment,
}

struct DeploymentSnapshot {
    key: String,
    mod_revision: u64,
    deployment: ServiceDeployment,
}

#[derive(Debug, Clone)]
struct ActiveDeploymentSnapshot {
    key: String,
    mod_revision: u64,
    deployment: ActiveDeployment,
}

#[derive(Debug, Clone)]
struct CounterSnapshot {
    next_index: u64,
    mod_revision: Option<u64>,
}

#[derive(Debug, Clone)]
struct InfoSnapshot {
    key: String,
    mod_revision: u64,
    info: ServiceInfo,
}

enum ShutdownSignal {
    CtrlC,
    Terminate(&'static str),
}

#[async_trait]
trait DeploymentStore: Send + Sync {
    async fn list_service_ids(&self) -> Result<Vec<String>, String>;
    async fn queue_terminated_deployment(&self, service_id: &str) -> Result<bool, String>;
    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>, String>;
    async fn claim_deployment_building(
        &self,
        queued_deployment: &QueuedDeployment,
    ) -> Result<bool, String>;
    async fn mark_deployment_ready(
        &self,
        service_id: &str,
        deployment_key: &str,
        deployment_id: &str,
    ) -> Result<(), String>;
    async fn mark_deployment_crashed(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<(), String>;
    async fn mark_deployment_terminated(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<(), String>;
}

trait ServiceCommandPlanner: Send + Sync {
    fn build_command(&self, deployment: &ServiceDeployment) -> Option<String>;
    fn deploy_command(&self, deployment: &ServiceDeployment) -> Option<String>;
}

trait WorkerRunner: Send + Sync {
    fn spawn(
        &self,
        service_name: String,
        config: ServiceRuntimeConfig,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<WorkerExitStatus>;
}

struct DefaultServiceCommandPlanner;

impl ServiceCommandPlanner for DefaultServiceCommandPlanner {
    fn build_command(&self, deployment: &ServiceDeployment) -> Option<String> {
        let build = deployment.config.build.as_ref()?;
        Some(format!(
            "git clone {} && docker build -f {} .",
            build.repo, build.dockerfile_path
        ))
    }

    fn deploy_command(&self, deployment: &ServiceDeployment) -> Option<String> {
        if let Some(image) = deployment
            .config
            .image
            .as_deref()
            .map(str::trim)
            .filter(|image| !image.is_empty())
        {
            Some(docker_run_command(
                &deployment.config.id,
                &deployment.id,
                image,
                &deployment.config.deploy.ports,
                &deployment.config.deploy.flags,
            ))
        } else if let Some(deploy_command) = deployment.config.deploy.command.as_ref() {
            Some(to_shell_command(
                &deploy_command.command,
                &deploy_command.args,
            ))
        } else {
            None
        }
    }
}

struct DefaultWorkerRunner;

impl WorkerRunner for DefaultWorkerRunner {
    fn spawn(
        &self,
        service_name: String,
        config: ServiceRuntimeConfig,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<WorkerExitStatus> {
        tokio::spawn(run_managed_service(service_name, config, shutdown_rx))
    }
}

pub async fn run(etcd_endpoint: &str) -> crate::error::Result<()> {
    let mut supervisor = QueueSupervisor::new(etcd_endpoint).await?;
    supervisor.run().await.map_err(Into::into)
}

struct QueueSupervisor {
    store: Arc<dyn DeploymentStore>,
    command_planner: Arc<dyn ServiceCommandPlanner>,
    worker_runner: Arc<dyn WorkerRunner>,
    running: HashMap<String, RunningService>,
}

impl QueueSupervisor {
    async fn new(etcd_endpoint: &str) -> Result<Self, String> {
        let store = Arc::new(EtcdV3HttpClient::new(etcd_endpoint).await?);
        Ok(Self::with_parts(
            store,
            Arc::new(DefaultServiceCommandPlanner),
            Arc::new(DefaultWorkerRunner),
        ))
    }

    fn with_parts(
        store: Arc<dyn DeploymentStore>,
        command_planner: Arc<dyn ServiceCommandPlanner>,
        worker_runner: Arc<dyn WorkerRunner>,
    ) -> Self {
        Self {
            store,
            command_planner,
            worker_runner,
            running: HashMap::new(),
        }
    }

    async fn run(&mut self) -> Result<(), String> {
        const CTRL_C_CONFIRM_WINDOW: Duration = Duration::from_secs(2);
        let mut first_ctrl_c_at: Option<std::time::Instant> = None;
        let (signal_tx, mut signal_rx) = mpsc::unbounded_channel();
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|err| format!("failed to install SIGTERM handler: {err}"))?;
        let mut sigquit = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::quit())
            .map_err(|err| format!("failed to install SIGQUIT handler: {err}"))?;
        let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .map_err(|err| format!("failed to install SIGHUP handler: {err}"))?;

        let signal_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    ctrl_c = tokio::signal::ctrl_c() => {
                        if ctrl_c.is_err() {
                            break;
                        }
                        if signal_tx.send(ShutdownSignal::CtrlC).is_err() {
                            break;
                        }
                    }
                    _ = sigterm.recv() => {
                        if signal_tx.send(ShutdownSignal::Terminate("SIGTERM")).is_err() {
                            break;
                        }
                    }
                    _ = sigquit.recv() => {
                        if signal_tx.send(ShutdownSignal::Terminate("SIGQUIT")).is_err() {
                            break;
                        }
                    }
                    _ = sighup.recv() => {
                        if signal_tx.send(ShutdownSignal::Terminate("SIGHUP")).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        if let Err(err) = self.queue_terminated_active_deployments().await {
            eprintln!("[maestro]: failed to queue terminated deployments on startup: {err}");
        }

        loop {
            tokio::select! {
                Some(signal) = signal_rx.recv() => {
                    match signal {
                        ShutdownSignal::CtrlC => {
                            let now = std::time::Instant::now();
                            let should_shutdown = first_ctrl_c_at
                                .is_some_and(|first| now.duration_since(first) <= CTRL_C_CONFIRM_WINDOW);

                            if should_shutdown {
                                eprintln!("[maestro]: stopping all jobs and terminate cleanly.");
                                self.shutdown_all().await;
                                signal_task.abort();
                                return Ok(());
                            }

                            first_ctrl_c_at = Some(now);
                            eprintln!("[maestro]: press ctrl+c again to stop.");
                        }
                        ShutdownSignal::Terminate(signal_name) => {
                            eprintln!("[maestro]: received {signal_name}; stopping all jobs and terminate cleanly.");
                            self.shutdown_all().await;
                            signal_task.abort();
                            return Ok(());
                        }
                    }
                }
                _ = sleep(POLL_INTERVAL) => {
                    self.reap_finished_tasks().await;
                    if let Err(err) = self.process_queued_deployments().await {
                        eprintln!("[maestro]: supervisor queue scan error: {err}");
                    }
                }
            }
        }
    }

    async fn process_queued_deployments(&mut self) -> Result<(), String> {
        let queued = self.store.list_queued_deployments().await?;
        for queued_deployment in queued {
            if self.running.contains_key(&queued_deployment.service_id) {
                continue;
            }

            let _build_command = self
                .command_planner
                .build_command(&queued_deployment.deployment);
            let Some(deploy_command) = self
                .command_planner
                .deploy_command(&queued_deployment.deployment)
            else {
                eprintln!(
                    "[maestro]: skipping queued deployment `{}` for service `{}`: no image or deploy command",
                    queued_deployment.deployment.id, queued_deployment.service_id
                );
                continue;
            };
            let worker_config = ServiceRuntimeConfig {
                command: deploy_command,
                restart_delay_ms: DEFAULT_RESTART_DELAY_MS,
                max_restarts: DEFAULT_MAX_RESTARTS,
                shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
            };

            let claimed = self
                .store
                .claim_deployment_building(&queued_deployment)
                .await?;
            if !claimed {
                continue;
            }

            let service_name = format!(
                "{}/{}",
                queued_deployment.service_id, queued_deployment.deployment.id
            );
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = self
                .worker_runner
                .spawn(service_name, worker_config, shutdown_rx);
            self.running.insert(
                queued_deployment.service_id.clone(),
                RunningService {
                    deployment_id: queued_deployment.deployment.id.clone(),
                    shutdown_tx,
                    task,
                },
            );

            if let Err(err) = self
                .store
                .mark_deployment_ready(
                    &queued_deployment.service_id,
                    &queued_deployment.key,
                    &queued_deployment.deployment.id,
                )
                .await
            {
                eprintln!(
                    "[maestro]: failed to mark deployment `{}` ready: {err}",
                    queued_deployment.deployment.id
                );
            }
        }

        Ok(())
    }

    async fn queue_terminated_active_deployments(&self) -> Result<(), String> {
        let service_ids = self.store.list_service_ids().await?;
        for service_id in service_ids {
            let queued = self.store.queue_terminated_deployment(&service_id).await?;
            let _ = queued;
        }

        Ok(())
    }

    async fn reap_finished_tasks(&mut self) {
        let done = self
            .running
            .iter()
            .filter_map(|(service_id, running)| {
                if running.task.is_finished() {
                    Some(service_id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for service_id in done {
            if let Some(running) = self.running.remove(&service_id) {
                eprintln!(
                    "[maestro]: service `{service_id}` deployment `{}` worker finished",
                    running.deployment_id
                );
                let deployment_id = running.deployment_id.clone();
                let outcome = match running.task.await {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        eprintln!(
                            "[maestro]: service `{service_id}` deployment `{deployment_id}` task join error: {err}",
                        );
                        WorkerExitStatus::Crashed
                    }
                };

                if outcome == WorkerExitStatus::Crashed
                    && let Err(err) = self
                        .store
                        .mark_deployment_crashed(&service_id, &deployment_id)
                        .await
                {
                    eprintln!(
                        "[maestro]: failed to mark deployment `{deployment_id}` crashed: {err}",
                    );
                }
                if outcome == WorkerExitStatus::Stopped
                    && let Err(err) = self
                        .store
                        .mark_deployment_terminated(&service_id, &deployment_id)
                        .await
                {
                    eprintln!(
                        "[maestro]: failed to mark deployment `{deployment_id}` terminated: {err}",
                    );
                }
            }
        }
    }

    async fn shutdown_all(&mut self) {
        let running = self.running.drain().collect::<Vec<_>>();

        for (_, running) in &running {
            let _ = running.shutdown_tx.send(true);
        }

        for (service_id, mut running) in running {
            let join_result = match timeout(Duration::from_secs(8), &mut running.task).await {
                Ok(result) => result,
                Err(_) => {
                    eprintln!(
                        "[maestro]: service deployment `{}` did not stop in time; waiting for worker cleanup",
                        running.deployment_id
                    );
                    running.task.await
                }
            };

            let deployment_id = running.deployment_id.clone();
            let outcome = match join_result {
                Ok(outcome) => outcome,
                Err(err) => {
                    eprintln!(
                        "[maestro]: service `{service_id}` deployment `{deployment_id}` task join error during shutdown: {err}",
                    );
                    WorkerExitStatus::Crashed
                }
            };

            if outcome == WorkerExitStatus::Stopped
                && let Err(err) = self
                    .store
                    .mark_deployment_terminated(&service_id, &deployment_id)
                    .await
            {
                eprintln!(
                    "[maestro]: failed to mark deployment `{deployment_id}` terminated: {err}",
                );
            }

            if outcome == WorkerExitStatus::Crashed
                && let Err(err) = self
                    .store
                    .mark_deployment_crashed(&service_id, &deployment_id)
                    .await
            {
                eprintln!("[maestro]: failed to mark deployment `{deployment_id}` crashed: {err}",);
            }
        }
    }
}

impl EtcdV3HttpClient {
    async fn new(endpoint: &str) -> Result<Self, String> {
        let endpoint = normalize_base_url(endpoint)?;
        let client = EtcdClient::connect([endpoint.as_str()], None)
            .await
            .map_err(|_| "failed to connect etcd client".to_string())?;
        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
        })
    }

    async fn get(
        &self,
        key: Vec<u8>,
        options: Option<GetOptions>,
    ) -> Result<etcd_client::GetResponse, String> {
        let mut client = self.client.lock().await;
        client
            .get(key, options)
            .await
            .map_err(|_| "failed etcd get request".to_string())
    }

    async fn txn(&self, compare: Vec<Compare>, success: Vec<TxnOp>) -> Result<bool, String> {
        let txn = Txn::new().when(compare).and_then(success);
        let mut client = self.client.lock().await;
        let response = client
            .txn(txn)
            .await
            .map_err(|_| "failed etcd txn request".to_string())?;
        Ok(response.succeeded())
    }

    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>, String> {
        let prefix_key = format!("{SERVICES_ROOT}/");
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| "failed to compute range end for services prefix".to_string())?;
        let options = GetOptions::new().with_range(range_end);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut queued = Vec::new();
        for kv in response.kvs() {
            let key = String::from_utf8(kv.key().to_vec())
                .map_err(|err| format!("deployment key for `{prefix_key}` is not utf8: {err}"))?;
            if !key.contains("/deployments/history/") {
                continue;
            }

            let Some(service_id) = service_id_from_history_key(&key) else {
                continue;
            };

            let deployment = serde_json::from_slice::<ServiceDeployment>(kv.value())
                .map_err(|err| format!("invalid deployment JSON at key `{key}`: {err}"))?;
            if deployment.status != DeploymentStatus::Queued {
                continue;
            }

            let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;

            queued.push(QueuedDeployment {
                service_id,
                key,
                mod_revision,
                deployment,
            });
        }

        queued.sort_by(|a, b| {
            a.deployment
                .created_at
                .cmp(&b.deployment.created_at)
                .then_with(|| a.key.cmp(&b.key))
        });

        Ok(queued)
    }

    async fn list_service_ids(&self) -> Result<Vec<String>, String> {
        let prefix_key = format!("{SERVICES_ROOT}/");
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| "failed to compute range end for services prefix".to_string())?;
        let options = GetOptions::new()
            .with_range(range_end)
            .with_sort(SortTarget::Key, SortOrder::Ascend);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        let mut ids = HashSet::new();
        for kv in response.kvs() {
            let key = String::from_utf8(kv.key().to_vec())
                .map_err(|err| format!("service key for `{prefix_key}` is not utf8: {err}"))?;
            if !key.ends_with("/info") {
                continue;
            }
            let Some(service_id) = service_id_from_info_key(&key) else {
                continue;
            };
            ids.insert(service_id);
        }

        let mut service_ids = ids.into_iter().collect::<Vec<_>>();
        service_ids.sort();
        Ok(service_ids)
    }

    async fn read_active_deployment(
        &self,
        service_id: &str,
    ) -> Result<Option<ActiveDeploymentSnapshot>, String> {
        let key = service_active_deployment_key(service_id);
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let deployment = serde_json::from_slice::<ActiveDeployment>(kv.value())
            .map_err(|err| format!("invalid active deployment JSON at key `{key}`: {err}"))?;
        let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;

        Ok(Some(ActiveDeploymentSnapshot {
            key,
            mod_revision,
            deployment,
        }))
    }

    async fn read_counter(&self, key: &str) -> Result<CounterSnapshot, String> {
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(CounterSnapshot {
                next_index: 0,
                mod_revision: None,
            });
        };

        let text = String::from_utf8(kv.value().to_vec())
            .map_err(|err| format!("counter value for key `{key}` is not utf8: {err}"))?;
        let next_index = text
            .trim()
            .parse::<u64>()
            .map_err(|err| format!("counter value for key `{key}` is not u64: {err}"))?;
        let mod_revision = decode_mod_revision(kv.mod_revision(), key)?;

        Ok(CounterSnapshot {
            next_index,
            mod_revision: Some(mod_revision),
        })
    }

    async fn read_service_info(&self, service_id: &str) -> Result<Option<InfoSnapshot>, String> {
        let key = service_info_key(service_id);
        let response = self.get(key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let info = serde_json::from_slice::<ServiceInfo>(kv.value())
            .map_err(|err| format!("invalid service info JSON at key `{key}`: {err}"))?;
        let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;

        Ok(Some(InfoSnapshot {
            key,
            mod_revision,
            info,
        }))
    }

    async fn update_service_info_status(
        &self,
        service_id: &str,
        status: DeploymentStatus,
    ) -> Result<(), String> {
        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(info_snapshot) = self.read_service_info(service_id).await? else {
                return Ok(());
            };

            let mut updated_info = info_snapshot.info.clone();
            updated_info.status = Some(status.clone());
            let info_json = serde_json::to_string(&updated_info)
                .map_err(|err| format!("failed to serialize service info: {err}"))?;

            let committed = self
                .txn(
                    vec![compare_mod_revision_or_absent(
                        &info_snapshot.key,
                        Some(info_snapshot.mod_revision),
                    )],
                    vec![request_put(&info_snapshot.key, &info_json)],
                )
                .await?;

            if committed {
                return Ok(());
            }
        }

        Err(format!(
            "failed to update service info status for `{service_id}` due to concurrent updates"
        ))
    }

    async fn queue_terminated_deployment(&self, service_id: &str) -> Result<bool, String> {
        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(active) = self.read_active_deployment(service_id).await? else {
                return Ok(false);
            };

            let Some(snapshot) = self
                .find_deployment_snapshot_by_id(service_id, &active.deployment.deployment_id)
                .await?
            else {
                return Ok(false);
            };
            if snapshot.deployment.status != DeploymentStatus::Terminated {
                return Ok(false);
            }

            let Some(info_snapshot) = self.read_service_info(service_id).await? else {
                return Ok(false);
            };

            let counter_key = service_history_next_index_key(service_id);
            let counter = self.read_counter(&counter_key).await?;
            let deployment_index = usize::try_from(counter.next_index)
                .map_err(|_| "deployment index overflowed usize".to_string())?;
            let deployment_key = service_deployment_history_key(service_id, deployment_index);

            let queued = ServiceDeployment {
                id: generate_deployment_id(),
                created_at: current_time_millis()?,
                deployed_at: None,
                status: DeploymentStatus::Queued,
                config: info_snapshot.info.config.clone(),
                git_commit: None,
                build: None,
            };

            let deployment_json = serde_json::to_string(&queued)
                .map_err(|err| format!("failed to serialize queued deployment: {err}"))?;
            let active_json = serde_json::to_string(&ActiveDeployment {
                deployment_id: queued.id.clone(),
                version: Some(queued.config.version.clone()),
            })
            .map_err(|err| format!("failed to serialize active deployment: {err}"))?;

            let updated_info = ServiceInfo {
                config: info_snapshot.info.config.clone(),
                status: Some(DeploymentStatus::Queued),
            };
            let info_json = serde_json::to_string(&updated_info)
                .map_err(|err| format!("failed to serialize service info: {err}"))?;

            let committed = self
                .txn(
                    vec![
                        compare_mod_revision_or_absent(&active.key, Some(active.mod_revision)),
                        compare_mod_revision_or_absent(&snapshot.key, Some(snapshot.mod_revision)),
                        compare_mod_revision_or_absent(
                            &info_snapshot.key,
                            Some(info_snapshot.mod_revision),
                        ),
                        compare_counter(&counter_key, &counter),
                    ],
                    vec![
                        request_put(&counter_key, &(counter.next_index + 1).to_string()),
                        request_put(&deployment_key, &deployment_json),
                        request_put(&active.key, &active_json),
                        request_put(&info_snapshot.key, &info_json),
                    ],
                )
                .await?;
            if committed {
                return Ok(true);
            }
        }

        Err(format!(
            "failed to queue replacement deployment for service `{service_id}` due to concurrent updates"
        ))
    }

    async fn claim_deployment_building(
        &self,
        queued_deployment: &QueuedDeployment,
    ) -> Result<bool, String> {
        let mut building = queued_deployment.deployment.clone();
        building.status = DeploymentStatus::Building;

        let deployment_json = serde_json::to_string(&building)
            .map_err(|err| format!("failed to serialize building deployment: {err}"))?;
        let active = ActiveDeployment {
            deployment_id: building.id.clone(),
            version: Some(building.config.version.clone()),
        };
        let active_json = serde_json::to_string(&active)
            .map_err(|err| format!("failed to serialize active deployment: {err}"))?;
        let active_key = service_active_deployment_key(&queued_deployment.service_id);

        let info_key = service_info_key(&queued_deployment.service_id);
        let existing_info = self
            .read_service_info(&queued_deployment.service_id)
            .await?;

        let updated_info = ServiceInfo {
            config: queued_deployment.deployment.config.clone(),
            status: Some(DeploymentStatus::Building),
        };
        let info_json = serde_json::to_string(&updated_info)
            .map_err(|err| format!("failed to serialize service info: {err}"))?;

        self.txn(
            vec![
                compare_mod_revision_or_absent(
                    &queued_deployment.key,
                    Some(queued_deployment.mod_revision),
                ),
                compare_mod_revision_or_absent(
                    &info_key,
                    existing_info.as_ref().map(|s| s.mod_revision),
                ),
            ],
            vec![
                request_put(&queued_deployment.key, &deployment_json),
                request_put(&active_key, &active_json),
                request_put(&info_key, &info_json),
            ],
        )
        .await
    }

    async fn mark_deployment_ready(
        &self,
        service_id: &str,
        deployment_key: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(snapshot) = self.read_deployment_snapshot(deployment_key).await? else {
                return Err(format!("deployment key `{deployment_key}` not found"));
            };

            if snapshot.deployment.id != deployment_id {
                return Err(format!(
                    "deployment key `{deployment_key}` does not match deployment id `{deployment_id}`",
                ));
            }

            if snapshot.deployment.status == DeploymentStatus::Ready {
                return Ok(());
            }
            if snapshot.deployment.status != DeploymentStatus::Building {
                return Ok(());
            }

            let mut updated = snapshot.deployment.clone();
            updated.status = DeploymentStatus::Ready;
            if updated.deployed_at.is_none() {
                updated.deployed_at = Some(current_time_millis()?);
            }
            let deployment_json = serde_json::to_string(&updated)
                .map_err(|err| format!("failed to serialize ready deployment: {err}"))?;

            let committed = self
                .txn(
                    vec![compare_mod_revision_or_absent(
                        &snapshot.key,
                        Some(snapshot.mod_revision),
                    )],
                    vec![request_put(&snapshot.key, &deployment_json)],
                )
                .await?;

            if committed {
                let _ = self
                    .update_service_info_status(service_id, DeploymentStatus::Ready)
                    .await;
                return Ok(());
            }
        }

        Err(format!(
            "failed to mark deployment `{deployment_id}` ready due to concurrent updates"
        ))
    }

    async fn mark_deployment_crashed(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(snapshot) = self
                .find_deployment_snapshot_by_id(service_id, deployment_id)
                .await?
            else {
                return Err(format!(
                    "deployment `{deployment_id}` for service `{service_id}` not found",
                ));
            };

            if snapshot.deployment.status == DeploymentStatus::Crashed
                || snapshot.deployment.status == DeploymentStatus::Canceled
                || snapshot.deployment.status == DeploymentStatus::Terminated
            {
                return Ok(());
            }

            let mut updated = snapshot.deployment.clone();
            updated.status = DeploymentStatus::Crashed;
            let deployment_json = serde_json::to_string(&updated)
                .map_err(|err| format!("failed to serialize crashed deployment: {err}"))?;

            let committed = self
                .txn(
                    vec![compare_mod_revision_or_absent(
                        &snapshot.key,
                        Some(snapshot.mod_revision),
                    )],
                    vec![request_put(&snapshot.key, &deployment_json)],
                )
                .await?;

            if committed {
                let _ = self
                    .update_service_info_status(service_id, DeploymentStatus::Crashed)
                    .await;
                return Ok(());
            }
        }

        Err(format!(
            "failed to mark deployment `{deployment_id}` crashed due to concurrent updates"
        ))
    }

    async fn mark_deployment_terminated(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        for _attempt in 0..MAX_STATUS_TXN_RETRIES {
            let Some(snapshot) = self
                .find_deployment_snapshot_by_id(service_id, deployment_id)
                .await?
            else {
                return Err(format!(
                    "deployment `{deployment_id}` for service `{service_id}` not found",
                ));
            };

            if snapshot.deployment.status == DeploymentStatus::Terminated {
                return Ok(());
            }

            let mut updated = snapshot.deployment.clone();
            updated.status = DeploymentStatus::Terminated;
            let deployment_json = serde_json::to_string(&updated)
                .map_err(|err| format!("failed to serialize terminated deployment: {err}"))?;

            let committed = self
                .txn(
                    vec![compare_mod_revision_or_absent(
                        &snapshot.key,
                        Some(snapshot.mod_revision),
                    )],
                    vec![request_put(&snapshot.key, &deployment_json)],
                )
                .await?;

            if committed {
                let _ = self
                    .update_service_info_status(service_id, DeploymentStatus::Terminated)
                    .await;
                return Ok(());
            }
        }

        Err(format!(
            "failed to mark deployment `{deployment_id}` terminated due to concurrent updates"
        ))
    }

    async fn read_deployment_snapshot(
        &self,
        deployment_key: &str,
    ) -> Result<Option<DeploymentSnapshot>, String> {
        let response = self.get(deployment_key.as_bytes().to_vec(), None).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let deployment = serde_json::from_slice::<ServiceDeployment>(kv.value())
            .map_err(|err| format!("invalid deployment JSON at key `{deployment_key}`: {err}"))?;
        let mod_revision = decode_mod_revision(kv.mod_revision(), deployment_key)?;

        Ok(Some(DeploymentSnapshot {
            key: deployment_key.to_string(),
            mod_revision,
            deployment,
        }))
    }

    async fn find_deployment_snapshot_by_id(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<Option<DeploymentSnapshot>, String> {
        let prefix_key = format!("{SERVICES_ROOT}/{service_id}/deployments/history/");
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| "failed to compute range end for deployment lookup".to_string())?;
        let options = GetOptions::new().with_range(range_end);
        let response = self.get(prefix.to_vec(), Some(options)).await?;

        for kv in response.kvs() {
            let deployment = serde_json::from_slice::<ServiceDeployment>(kv.value())
                .map_err(|err| format!("invalid deployment JSON under `{prefix_key}`: {err}"))?;
            if deployment.id != deployment_id {
                continue;
            }

            let key = String::from_utf8(kv.key().to_vec())
                .map_err(|err| format!("deployment key for `{prefix_key}` is not utf8: {err}"))?;
            let mod_revision = decode_mod_revision(kv.mod_revision(), &key)?;

            return Ok(Some(DeploymentSnapshot {
                key,
                mod_revision,
                deployment,
            }));
        }

        Ok(None)
    }
}

#[async_trait]
impl DeploymentStore for EtcdV3HttpClient {
    async fn list_service_ids(&self) -> Result<Vec<String>, String> {
        EtcdV3HttpClient::list_service_ids(self).await
    }

    async fn queue_terminated_deployment(&self, service_id: &str) -> Result<bool, String> {
        EtcdV3HttpClient::queue_terminated_deployment(self, service_id).await
    }

    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>, String> {
        EtcdV3HttpClient::list_queued_deployments(self).await
    }

    async fn claim_deployment_building(
        &self,
        queued_deployment: &QueuedDeployment,
    ) -> Result<bool, String> {
        EtcdV3HttpClient::claim_deployment_building(self, queued_deployment).await
    }

    async fn mark_deployment_ready(
        &self,
        service_id: &str,
        deployment_key: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        EtcdV3HttpClient::mark_deployment_ready(self, service_id, deployment_key, deployment_id)
            .await
    }

    async fn mark_deployment_crashed(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        EtcdV3HttpClient::mark_deployment_crashed(self, service_id, deployment_id).await
    }

    async fn mark_deployment_terminated(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        EtcdV3HttpClient::mark_deployment_terminated(self, service_id, deployment_id).await
    }
}

fn to_shell_command(command: &str, args: &[String]) -> String {
    if args.is_empty() {
        command.to_string()
    } else {
        format!("{} {}", command, args.join(" "))
    }
}

fn docker_run_command(
    service_id: &str,
    deployment_id: &str,
    image: &str,
    ports: &[String],
    flags: &[String],
) -> String {
    let short_deployment_id = deployment_id.chars().take(6).collect::<String>();
    let container_name = format!("{service_id}-{short_deployment_id}");
    let docker_port_flags = ports
        .iter()
        .map(|port| format!("-p {port}"))
        .collect::<Vec<_>>();

    if docker_port_flags.is_empty() && flags.is_empty() {
        format!("exec docker run --rm --name {container_name} {image}")
    } else if flags.is_empty() {
        format!(
            "exec docker run --rm --name {container_name} {} {image}",
            docker_port_flags.join(" ")
        )
    } else if docker_port_flags.is_empty() {
        format!(
            "exec docker run --rm --name {container_name} {image} {}",
            flags.join(" ")
        )
    } else {
        format!(
            "exec docker run --rm --name {container_name} {} {image} {}",
            docker_port_flags.join(" "),
            flags.join(" "),
        )
    }
}

fn normalize_base_url(host: &str) -> Result<String, String> {
    let host = host.trim();
    if host.is_empty() {
        return Err("host cannot be empty".to_string());
    }

    let base = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    };

    Ok(base.trim_end_matches('/').to_string())
}

fn service_id_from_history_key(key: &str) -> Option<String> {
    let remainder = key.strip_prefix(&format!("{SERVICES_ROOT}/"))?;
    let (service_id, _) = remainder.split_once("/deployments/history/")?;
    Some(service_id.to_string())
}

fn service_id_from_info_key(key: &str) -> Option<String> {
    let remainder = key.strip_prefix(&format!("{SERVICES_ROOT}/"))?;
    let (service_id, suffix) = remainder.split_once('/')?;
    if suffix == "info" {
        Some(service_id.to_string())
    } else {
        None
    }
}

fn compare_counter(key: &str, snapshot: &CounterSnapshot) -> Compare {
    compare_mod_revision_or_absent(key, snapshot.mod_revision)
}

fn service_history_next_index_key(service_id: &str) -> String {
    format!("{SERVICES_ROOT}/{service_id}{SERVICE_HISTORY_NEXT_INDEX_SUFFIX}")
}

fn generate_deployment_id() -> String {
    nanoid!(24, &URL_SAFE_ALPHABET)
}

fn current_time_millis() -> Result<u64, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system clock is before unix epoch: {err}"))?;
    u64::try_from(now.as_millis())
        .map_err(|_| "system time milliseconds overflowed u64".to_string())
}

fn compare_mod_revision_or_absent(key: &str, mod_revision: Option<u64>) -> Compare {
    match mod_revision {
        Some(rev) => Compare::mod_revision(
            key.as_bytes().to_vec(),
            CompareOp::Equal,
            i64::try_from(rev).expect("mod_revision from etcd must fit i64"),
        ),
        None => Compare::version(key.as_bytes().to_vec(), CompareOp::Equal, 0),
    }
}

fn request_put(key: &str, value: &str) -> TxnOp {
    TxnOp::put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
}

fn decode_mod_revision(mod_revision: i64, key: &str) -> Result<u64, String> {
    u64::try_from(mod_revision)
        .map_err(|err| format!("invalid mod_revision `{mod_revision}` for key `{key}`: {err}"))
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] < 0xff {
            end[idx] += 1;
            end.truncate(idx + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
#[path = "../tests/supervisor/etcd.rs"]
mod tests;
