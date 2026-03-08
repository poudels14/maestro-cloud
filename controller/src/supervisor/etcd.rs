use std::{collections::HashMap, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

use crate::service::{
    ActiveDeployment, DeploymentStatus, SERVICES_ROOT, ServiceDeployment,
    service_active_deployment_key,
};

use super::{model::ServiceRuntimeConfig, worker::run_managed_service};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_RESTART_DELAY_MS: u64 = 1_000;
const DEFAULT_MAX_RESTARTS: u32 = 5;
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 5_000;

#[derive(Clone)]
struct EtcdV3HttpClient {
    client: reqwest::Client,
    base_url: String,
}

struct RunningService {
    deployment_id: String,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct QueuedDeployment {
    service_id: String,
    key: String,
    mod_revision: u64,
    deployment: ServiceDeployment,
}

#[derive(Debug, Deserialize)]
struct RangeResponse {
    #[serde(default)]
    kvs: Vec<RangeKv>,
}

#[derive(Debug, Deserialize)]
struct RangeKv {
    key: String,
    value: String,
    mod_revision: String,
}

#[derive(Debug, Deserialize)]
struct TxnResponse {
    succeeded: bool,
}

pub async fn run(etcd_endpoint: &str) -> Result<(), String> {
    let mut supervisor = EtcdQueueSupervisor::new(etcd_endpoint)?;
    supervisor.run().await
}

struct EtcdQueueSupervisor {
    etcd: EtcdV3HttpClient,
    running: HashMap<String, RunningService>,
}

impl EtcdQueueSupervisor {
    fn new(etcd_endpoint: &str) -> Result<Self, String> {
        Ok(Self {
            etcd: EtcdV3HttpClient::new(etcd_endpoint)?,
            running: HashMap::new(),
        })
    }

    async fn run(&mut self) -> Result<(), String> {
        const CTRL_C_CONFIRM_WINDOW: Duration = Duration::from_secs(2);
        let mut first_ctrl_c_at: Option<std::time::Instant> = None;
        let (ctrl_c_tx, mut ctrl_c_rx) = mpsc::unbounded_channel();

        let signal_task = tokio::spawn(async move {
            loop {
                if tokio::signal::ctrl_c().await.is_err() {
                    break;
                }
                if ctrl_c_tx.send(()).is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                Some(()) = ctrl_c_rx.recv() => {
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
        let queued = self.etcd.list_queued_deployments().await?;
        for queued_deployment in queued {
            if self.running.contains_key(&queued_deployment.service_id) {
                continue;
            }

            let Some(worker_config) = runtime_config_from_deployment(&queued_deployment.deployment)
            else {
                continue;
            };

            let claimed = self
                .etcd
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
            let task = tokio::spawn(run_managed_service(
                service_name,
                worker_config,
                shutdown_rx,
            ));
            self.running.insert(
                queued_deployment.service_id.clone(),
                RunningService {
                    deployment_id: queued_deployment.deployment.id.clone(),
                    shutdown_tx,
                    task,
                },
            );
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
                let _ = running.task.await;
            }
        }
    }

    async fn shutdown_all(&mut self) {
        let running = self
            .running
            .drain()
            .map(|(_, running)| running)
            .collect::<Vec<_>>();

        for running in &running {
            let _ = running.shutdown_tx.send(true);
        }

        for mut running in running {
            if timeout(Duration::from_secs(8), &mut running.task)
                .await
                .is_err()
            {
                eprintln!(
                    "[maestro]: service deployment `{}` did not stop in time; aborting task",
                    running.deployment_id
                );
                running.task.abort();
            }
        }
    }
}

impl EtcdV3HttpClient {
    fn new(endpoint: &str) -> Result<Self, String> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: normalize_base_url(endpoint)?,
        })
    }

    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>, String> {
        let prefix_key = format!("{SERVICES_ROOT}/");
        let prefix = prefix_key.as_bytes();
        let range_end = prefix_range_end(prefix)
            .ok_or_else(|| "failed to compute range end for services prefix".to_string())?;

        let response = self
            .range(json!({
                "key": encode(prefix),
                "range_end": encode(&range_end),
            }))
            .await?;

        let mut queued = Vec::new();
        for kv in response.kvs {
            let key_bytes = decode_to_bytes(&kv.key, &prefix_key)?;
            let key = String::from_utf8(key_bytes)
                .map_err(|err| format!("deployment key for `{prefix_key}` is not utf8: {err}"))?;
            if !key.contains("/deployments/history/") {
                continue;
            }

            let Some(service_id) = service_id_from_history_key(&key) else {
                continue;
            };

            let value_bytes = decode_to_bytes(&kv.value, &key)?;
            let deployment = serde_json::from_slice::<ServiceDeployment>(&value_bytes)
                .map_err(|err| format!("invalid deployment JSON at key `{key}`: {err}"))?;
            if deployment.status != DeploymentStatus::Queued {
                continue;
            }

            let mod_revision = kv
                .mod_revision
                .parse::<u64>()
                .map_err(|err| format!("invalid mod_revision for key `{key}`: {err}"))?;

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

        self.txn(
            vec![compare_mod_revision_or_absent(
                &queued_deployment.key,
                Some(queued_deployment.mod_revision),
            )],
            vec![
                request_put(&queued_deployment.key, &deployment_json),
                request_put(&active_key, &active_json),
            ],
        )
        .await
    }

    async fn txn(&self, compare: Vec<Value>, success: Vec<Value>) -> Result<bool, String> {
        let body = json!({
            "compare": compare,
            "success": success,
            "failure": [],
        });

        let response = self
            .client
            .post(format!("{}/v3/kv/txn", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|err| format!("failed etcd txn request: {err}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let raw = response.text().await.unwrap_or_default();
            return Err(format!("etcd txn failed with status {status}: {raw}"));
        }

        let parsed = response
            .json::<TxnResponse>()
            .await
            .map_err(|err| format!("failed to decode etcd txn response: {err}"))?;
        Ok(parsed.succeeded)
    }

    async fn range(&self, body: Value) -> Result<RangeResponse, String> {
        let response = self
            .client
            .post(format!("{}/v3/kv/range", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|err| format!("failed etcd range request: {err}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let raw = response.text().await.unwrap_or_default();
            return Err(format!("etcd range failed with status {status}: {raw}"));
        }

        response
            .json::<RangeResponse>()
            .await
            .map_err(|err| format!("failed to decode etcd range response: {err}"))
    }
}

fn runtime_config_from_deployment(deployment: &ServiceDeployment) -> Option<ServiceRuntimeConfig> {
    let deploy_command = deployment.config.deploy.command.as_ref()?;
    let command = to_shell_command(&deploy_command.command, &deploy_command.args);

    Some(ServiceRuntimeConfig {
        command,
        restart_delay_ms: DEFAULT_RESTART_DELAY_MS,
        max_restarts: DEFAULT_MAX_RESTARTS,
        shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
    })
}

fn to_shell_command(command: &str, args: &[String]) -> String {
    if args.is_empty() {
        command.to_string()
    } else {
        format!("{} {}", command, args.join(" "))
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

fn compare_mod_revision_or_absent(key: &str, mod_revision: Option<u64>) -> Value {
    match mod_revision {
        Some(rev) => json!({
            "key": encode(key.as_bytes()),
            "target": "MOD",
            "mod_revision": rev.to_string(),
        }),
        None => json!({
            "key": encode(key.as_bytes()),
            "target": "VERSION",
            "version": "0",
        }),
    }
}

fn request_put(key: &str, value: &str) -> Value {
    json!({
        "request_put": {
            "key": encode(key.as_bytes()),
            "value": encode(value.as_bytes()),
        }
    })
}

fn encode(input: &[u8]) -> String {
    STANDARD.encode(input)
}

fn decode_to_bytes(encoded: &str, key_hint: &str) -> Result<Vec<u8>, String> {
    STANDARD
        .decode(encoded.as_bytes())
        .map_err(|err| format!("invalid base64 value in etcd for `{key_hint}`: {err}"))
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
