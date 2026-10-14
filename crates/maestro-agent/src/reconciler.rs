use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::{ReplicaId, ReplicaPhase, ReplicaPlan, ReplicaStatus};
use maestro_runtime::{
    BindMount, ContainerLimits, ContainerSpec, ContainerState, ContainerStatus, NetworkMode,
    Runtime, RuntimeEvent,
};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::error::AgentError;
use crate::secret_files;
use crate::source::PlanSource;
use crate::status::StatusBoard;

const REPLICA_LABEL: &str = "maestro.replica";
const SERVICE_LABEL: &str = "maestro.service";
const DEPLOYMENT_LABEL: &str = "maestro.deployment";
const PLAN_HASH_LABEL: &str = "maestro.plan-hash";

#[derive(Clone, Debug)]
pub struct ReconcilerSettings {
    pub stop_grace: Duration,
    pub restart_base_delay: Duration,
    pub restart_max_delay: Duration,
    pub stable_after: Duration,
    pub default_max_restarts: u32,
    pub resync_interval: Duration,
    pub pids_limit: u32,
    pub cgroup_parent: String,
    pub secrets_dir: PathBuf,
}

pub struct Reconciler<R: Runtime, S: PlanSource> {
    runtime: Arc<R>,
    source: Arc<S>,
    board: StatusBoard,
    settings: ReconcilerSettings,
    restarts: HashMap<ReplicaId, RestartState>,
    replaced: HashSet<ReplicaId>,
    fetcher: Option<Arc<dyn ImageFetcher>>,
    plans: watch::Sender<Vec<ReplicaPlan>>,
}

pub trait ImageFetcher: Send + Sync + 'static {
    fn fetch<'a>(
        &'a self,
        image: &'a str,
        source: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

#[derive(Clone, Copy, Debug)]
struct RestartState {
    attempts: u32,
    not_before: Instant,
    started_at: Option<Instant>,
}

impl<R: Runtime, S: PlanSource> Reconciler<R, S> {
    pub fn new(
        runtime: Arc<R>,
        source: Arc<S>,
        board: StatusBoard,
        settings: ReconcilerSettings,
    ) -> Self {
        let (plans, _) = watch::channel(Vec::new());
        Self {
            runtime,
            source,
            board,
            settings,
            restarts: HashMap::new(),
            replaced: HashSet::new(),
            fetcher: None,
            plans,
        }
    }

    pub fn with_image_fetcher(self, fetcher: Arc<dyn ImageFetcher>) -> Self {
        Self {
            fetcher: Some(fetcher),
            ..self
        }
    }

    pub fn plans(&self) -> watch::Receiver<Vec<ReplicaPlan>> {
        self.plans.subscribe()
    }

    pub async fn run(mut self) -> Result<(), AgentError> {
        let mut events = self.runtime.events().await?;
        loop {
            if let Err(error) = self.reconcile().await {
                tracing::warn!(%error, "reconcile failed");
            }
            tokio::select! {
                _ = self.source.changed() => {}
                Some(event) = events.recv() => self.record_event(event),
                _ = tokio::time::sleep(self.settings.resync_interval) => {}
            }
        }
    }

    pub async fn reconcile(&mut self) -> Result<(), AgentError> {
        let desired = self
            .source
            .plans()
            .await?
            .into_iter()
            .map(|plan| (plan.replica_id.clone(), plan))
            .collect::<BTreeMap<_, _>>();
        let actual = self
            .runtime
            .list()
            .await?
            .into_iter()
            .filter_map(|container| {
                let replica_id = container.labels.get(REPLICA_LABEL)?.as_str().into();
                Some((replica_id, container))
            })
            .collect::<BTreeMap<ReplicaId, ContainerStatus>>();

        for replica_id in actual.keys().filter(|id| !desired.contains_key(*id)) {
            self.retire(replica_id).await;
        }
        secret_files::sweep(
            &self.settings.secrets_dir,
            &actual.keys().cloned().collect::<BTreeSet<_>>(),
        );
        let stale_records = self
            .board
            .snapshot()
            .into_iter()
            .map(|status| status.replica_id)
            .filter(|id| !desired.contains_key(id) && !actual.contains_key(id))
            .collect::<Vec<_>>();
        stale_records.iter().for_each(|replica_id| {
            self.board.remove(replica_id);
            self.restarts.remove(replica_id);
        });
        for plan in desired.values() {
            self.converge(plan, actual.get(&plan.replica_id)).await;
        }
        self.plans.send_replace(desired.into_values().collect());
        Ok(())
    }

    async fn converge(&mut self, plan: &ReplicaPlan, container: Option<&ContainerStatus>) {
        let hash = plan_hash(plan);
        let state = container.map(|container| {
            let current = container.labels.get(PLAN_HASH_LABEL) == Some(&hash);
            (container.state, current)
        });
        match state {
            Some((ContainerState::Running, true)) => {
                if self.board.get(&plan.replica_id).is_none() {
                    self.adopt(plan, container.and_then(|container| container.ip));
                }
            }
            Some((ContainerState::Running, false)) => {
                self.replaced.insert(plan.replica_id.clone());
                self.retire(&plan.replica_id).await;
                self.restarts.remove(&plan.replica_id);
                self.launch(plan, &hash).await;
            }
            Some((ContainerState::Stopped { exit_code }, _)) => {
                self.handle_exit(plan, Some(exit_code)).await;
            }
            Some((ContainerState::Created | ContainerState::Unknown, _)) => {
                self.handle_exit(plan, None).await;
            }
            None => {
                let failed = self
                    .board
                    .get(&plan.replica_id)
                    .is_some_and(|status| status.phase == ReplicaPhase::Failed);
                let due = self
                    .restarts
                    .get(&plan.replica_id)
                    .is_none_or(|restart| restart.not_before <= Instant::now());
                if !failed && due {
                    self.launch(plan, &hash).await;
                }
            }
        }
    }

    async fn launch(&mut self, plan: &ReplicaPlan, hash: &str) {
        self.board.upsert(ReplicaStatus {
            phase: ReplicaPhase::Starting,
            ..self.status_for(plan)
        });
        let started = match self.prepare_mounts(plan) {
            Ok(mounts) => match self.obtain_image(plan).await {
                Ok(()) => self
                    .runtime
                    .start(&self.container_spec(plan, hash, mounts))
                    .await
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        match started {
            Ok(container) => {
                let phase = match plan.healthcheck {
                    Some(_) => ReplicaPhase::Running,
                    None => ReplicaPhase::Healthy,
                };
                self.restarts
                    .entry(plan.replica_id.clone())
                    .or_insert(RestartState {
                        attempts: 0,
                        not_before: Instant::now(),
                        started_at: None,
                    })
                    .started_at = Some(Instant::now());
                self.board.update(&plan.replica_id, |status| {
                    status.phase = phase;
                    status.ip = container.ip;
                    status.consecutive_failures = 0;
                    status.last_error = None;
                });
            }
            Err(error) => {
                tracing::warn!(replica = %plan.replica_id, %error, "failed to start replica");
                let _ = self.runtime.remove(plan.replica_id.as_str()).await;
                self.schedule_restart(plan, error);
            }
        }
    }

    fn adopt(&mut self, plan: &ReplicaPlan, ip: Option<std::net::IpAddr>) {
        let phase = match plan.healthcheck {
            Some(_) => ReplicaPhase::Running,
            None => ReplicaPhase::Healthy,
        };
        self.restarts.insert(
            plan.replica_id.clone(),
            RestartState {
                attempts: 0,
                not_before: Instant::now(),
                started_at: Some(Instant::now()),
            },
        );
        self.board.upsert(ReplicaStatus {
            phase,
            ip,
            ..self.status_for(plan)
        });
    }

    async fn handle_exit(&mut self, plan: &ReplicaPlan, exit_code: Option<u32>) {
        let _ = self.runtime.remove(plan.replica_id.as_str()).await;
        secret_files::remove_for(&self.settings.secrets_dir, &plan.replica_id);
        let reason = exit_code.map_or_else(
            || "container is not running".to_string(),
            |code| format!("exited with code {code}"),
        );
        self.schedule_restart(plan, reason);
    }

    fn schedule_restart(&mut self, plan: &ReplicaPlan, reason: String) {
        let now = Instant::now();
        let settings = &self.settings;
        let previous = self.restarts.get(&plan.replica_id).copied();
        let was_stable = previous
            .and_then(|restart| restart.started_at)
            .is_some_and(|started_at| now.duration_since(started_at) >= settings.stable_after);
        let attempts = match was_stable {
            true => 1,
            false => previous.map_or(1, |restart| restart.attempts + 1),
        };
        let max_restarts = plan.max_restarts.unwrap_or(settings.default_max_restarts);
        let delay = settings
            .restart_base_delay
            .saturating_mul(2u32.saturating_pow(attempts.saturating_sub(1)))
            .min(settings.restart_max_delay);
        self.restarts.insert(
            plan.replica_id.clone(),
            RestartState {
                attempts,
                not_before: now + delay,
                started_at: None,
            },
        );
        let phase = match attempts > max_restarts {
            true => ReplicaPhase::Failed,
            false => ReplicaPhase::Restarting,
        };
        let template = self.status_for(plan);
        self.board.upsert(ReplicaStatus {
            phase,
            ip: None,
            restarts: attempts,
            last_error: Some(reason),
            ..template
        });
    }

    async fn retire(&mut self, replica_id: &ReplicaId) {
        let stopped = self
            .runtime
            .stop(replica_id.as_str(), self.settings.stop_grace)
            .await;
        let removed = self.runtime.remove(replica_id.as_str()).await;
        secret_files::remove_for(&self.settings.secrets_dir, replica_id);
        match stopped.and(removed) {
            Ok(()) => {
                self.board.remove(replica_id);
                self.restarts.remove(replica_id);
            }
            Err(error) => tracing::warn!(replica = %replica_id, %error, "failed to retire replica"),
        }
    }

    pub fn record_event(&mut self, event: RuntimeEvent) {
        match event {
            RuntimeEvent::OomKilled { id } => self.board.update(&id.as_str().into(), |status| {
                status.last_error = Some("killed: out of memory".to_string());
            }),
            RuntimeEvent::Exited { id, exit_code } => {
                let replica_id = ReplicaId::from(id.as_str());
                if !self.replaced.remove(&replica_id) {
                    self.board.update(&replica_id, |status| {
                        if status.phase.is_live() {
                            status.phase = ReplicaPhase::Restarting;
                            status.last_error = Some(format!("exited with code {exit_code}"));
                        }
                    })
                }
            }
        }
    }

    fn status_for(&self, plan: &ReplicaPlan) -> ReplicaStatus {
        let previous = self.board.get(&plan.replica_id);
        ReplicaStatus {
            replica_id: plan.replica_id.clone(),
            service_id: plan.service_id.clone(),
            deployment_id: plan.deployment_id.clone(),
            phase: ReplicaPhase::Starting,
            ip: None,
            restarts: previous.as_ref().map_or(0, |status| status.restarts),
            consecutive_failures: 0,
            last_error: previous.and_then(|status| status.last_error),
        }
    }

    async fn obtain_image(&self, plan: &ReplicaPlan) -> Result<(), String> {
        match (&plan.image_source, &self.fetcher) {
            (Some(source), Some(fetcher)) => fetcher.fetch(&plan.image, source).await,
            _ => self
                .runtime
                .pull(&plan.image)
                .await
                .map_err(|error| error.to_string()),
        }
    }

    fn prepare_mounts(&self, plan: &ReplicaPlan) -> Result<Vec<BindMount>, String> {
        let volumes = plan
            .volumes
            .iter()
            .map(|volume| {
                let host_path = PathBuf::from(&volume.host_path);
                std::fs::create_dir_all(&host_path).map_err(|error| {
                    format!("failed to create volume {}: {error}", volume.host_path)
                })?;
                if let Some(owner) = &volume.owner {
                    std::os::unix::fs::chown(&host_path, Some(owner.uid), owner.gid).map_err(
                        |error| format!("failed to chown volume {}: {error}", volume.host_path),
                    )?;
                }
                Ok(BindMount {
                    source: host_path,
                    destination: volume.mount_path.clone(),
                    read_only: volume.read_only,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let secrets = plan
            .secrets
            .as_ref()
            .map(|secrets| {
                secret_files::write(&self.settings.secrets_dir, &plan.replica_id, &secrets.items)
                    .map(|source| BindMount {
                        source,
                        destination: secrets.mount_path.clone(),
                        read_only: true,
                    })
                    .map_err(|error| format!("failed to write the secrets file: {error}"))
            })
            .transpose()?;
        Ok(volumes.into_iter().chain(secrets).collect())
    }

    fn container_spec(
        &self,
        plan: &ReplicaPlan,
        hash: &str,
        mounts: Vec<BindMount>,
    ) -> ContainerSpec {
        let labels = BTreeMap::from([
            (REPLICA_LABEL.to_string(), plan.replica_id.to_string()),
            (SERVICE_LABEL.to_string(), plan.service_id.to_string()),
            (DEPLOYMENT_LABEL.to_string(), plan.deployment_id.to_string()),
            (PLAN_HASH_LABEL.to_string(), hash.to_string()),
        ]);
        ContainerSpec {
            id: plan.replica_id.to_string(),
            image: plan.image.clone(),
            entrypoint: plan
                .command
                .as_ref()
                .map(|command| vec![command.command.clone()]),
            args: plan.command.as_ref().map(|command| command.args.clone()),
            env: plan.env.clone(),
            labels,
            limits: ContainerLimits {
                cpu: plan.resources.cpu_limit,
                memory: plan.resources.memory_limit,
                pids: self.settings.pids_limit,
                oom_score_adj: plan.oom_priority.oom_score_adj(),
            },
            cgroup_parent: self.settings.cgroup_parent.clone(),
            mounts,
            network: NetworkMode::Bridge,
        }
    }
}

impl Default for ReconcilerSettings {
    fn default() -> Self {
        Self {
            stop_grace: Duration::from_secs(10),
            restart_base_delay: Duration::from_secs(5),
            restart_max_delay: Duration::from_secs(300),
            stable_after: Duration::from_secs(60),
            default_max_restarts: 10,
            resync_interval: Duration::from_secs(10),
            pids_limit: 4096,
            cgroup_parent: "maestro-workloads.slice".to_string(),
            secrets_dir: PathBuf::from("/run/maestro/secrets"),
        }
    }
}

fn plan_hash(plan: &ReplicaPlan) -> String {
    let encoded = serde_json::to_vec(plan).unwrap_or_default();
    hex::encode(Sha256::digest(encoded))[..16].to_string()
}
